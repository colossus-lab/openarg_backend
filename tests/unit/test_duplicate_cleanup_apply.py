"""The apply path of the most destructive code here, exercised rather than read.

`test_duplicate_cleanup.py` asserts things about SQL strings and constants. That
is worth having and it is not the same as running the code: measured coverage of
the apply path was **zero** — the branch that issues `DROP TABLE` against
production had never executed in a test.

These drive it with a fake connection and assert the exact sequence of
statements, because in this function the *order* is the safety property.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

from app.application.catalog.duplicate_cleanup import cleanup_duplicate_tables


class _Conn:
    """Records every statement and answers the probes the sweep makes."""

    def __init__(self, *, candidates, survivor_alive=True, schema="raw", fail_on=None):
        self.candidates = candidates
        self.survivor_alive = survivor_alive
        self.schema = schema
        self.fail_on = fail_on
        self.sql: list[str] = []

    def execute(self, stmt, params=None):
        s = " ".join(str(stmt).split())
        self.sql.append(s)
        if self.fail_on and self.fail_on in s:
            raise RuntimeError("boom")
        if "to_regclass" in s:
            return SimpleNamespace(scalar=lambda: "public.raw_table_versions")
        if "count(*) FROM public.raw_table_versions" in s:
            return SimpleNamespace(scalar=lambda: 5000)
        if "WITH grupos AS" in s:
            return SimpleNamespace(fetchall=lambda: self.candidates)
        if "table_type = 'BASE TABLE'" in s and "SELECT 1" in s:
            return SimpleNamespace(fetchone=lambda: (1,) if self.survivor_alive else None)
        if "SELECT table_schema FROM information_schema.tables" in s:
            return SimpleNamespace(scalar=lambda: self.schema)
        return SimpleNamespace(scalar=lambda: None, fetchone=lambda: None, fetchall=lambda: [])

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Engine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn

    def begin(self):
        return self._conn


def _cand(table="cache_dup", survivor="raw_survivor", rows=42):
    return SimpleNamespace(
        oi="p::s", dataset_id="d1", cd_id=7, table_name=table,
        row_count=rows, survivor=survivor,
    )


def _run(**kw):
    conn = _Conn(**kw)
    out = cleanup_duplicate_tables(
        _Engine(conn), run_id=uuid.uuid4(), dry_run=False, limit=10
    )
    return out, conn


def test_the_drop_actually_runs_and_names_the_right_table():
    out, conn = _run(candidates=[_cand()])
    assert out.by_reason.get("dropped") == 1
    drops = [s for s in conn.sql if s.startswith("DROP TABLE")]
    assert drops == ['DROP TABLE IF EXISTS "raw"."cache_dup" CASCADE']


def test_the_four_writes_happen_in_the_order_that_makes_them_safe():
    """Drop, retire the registry row, delete the catalogue row, mark cached.

    Each later step assumes the earlier one happened. A registry row still
    saying `live` after the drop is a phantom; a catalogue row left `is_cached
    = false` puts the resource straight back in the collection queue.
    """
    _, conn = _run(candidates=[_cand()])
    idx = {}
    for i, s in enumerate(conn.sql):
        for key, needle in (
            ("drop", "DROP TABLE"),
            ("retire", "SET superseded_at = now()"),
            ("forget", "DELETE FROM raw.cached_datasets"),
            ("cached", "SET is_cached = true"),
        ):
            if needle in s and key not in idx:
                idx[key] = i
    assert set(idx) == {"drop", "retire", "forget", "cached"}, idx
    assert idx["drop"] < idx["retire"] < idx["forget"] < idx["cached"]


def test_a_vanished_survivor_aborts_before_anything_is_dropped():
    """The candidate list is built earlier; a sweep may have moved since."""
    out, conn = _run(candidates=[_cand()], survivor_alive=False)
    assert out.by_reason.get("dropped") is None
    assert not any(s.startswith("DROP TABLE") for s in conn.sql)
    assert any("failed" in k for k in out.by_reason)


def test_a_protected_table_is_never_dropped_even_as_a_candidate():
    out, conn = _run(candidates=[_cand(table="users")])
    assert out.by_reason.get("protected_or_no_survivor") == 1
    assert not any(s.startswith("DROP TABLE") for s in conn.sql)


def test_a_candidate_without_a_survivor_is_skipped():
    out, conn = _run(candidates=[_cand(survivor=None)])
    assert out.by_reason.get("protected_or_no_survivor") == 1
    assert not any(s.startswith("DROP TABLE") for s in conn.sql)


def test_a_failure_mid_table_does_not_cost_the_batch():
    bad, good = _cand(table="malo"), _cand(table="bueno")
    conn = _Conn(candidates=[bad, good], fail_on='"malo"')
    out = cleanup_duplicate_tables(_Engine(conn), dry_run=False, limit=10)
    assert out.by_reason.get("dropped") == 1
    assert any(k.startswith("failed") for k in out.by_reason)


def test_rows_freed_counts_only_what_was_actually_dropped():
    out, _ = _run(candidates=[_cand(rows=1000), _cand(table="users", rows=999)])
    assert out.rows_freed == 1000


def test_a_truncated_registry_refuses_before_reading_candidates():
    class _Empty(_Conn):
        def execute(self, stmt, params=None):
            s = " ".join(str(stmt).split())
            self.sql.append(s)
            if "to_regclass" in s:
                return SimpleNamespace(scalar=lambda: "public.raw_table_versions")
            if "count(*) FROM public.raw_table_versions" in s:
                return SimpleNamespace(scalar=lambda: 3)
            return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None)

    import pytest

    from app.application.catalog.registry_reconcile import RegistryUnavailable

    conn = _Empty(candidates=[_cand()])
    # Refuses outright rather than relying on the candidate query happening to
    # come back empty. Safe-by-accident was the shape of the 2026-08-03
    # incident.
    with pytest.raises(RegistryUnavailable):
        cleanup_duplicate_tables(_Engine(conn), dry_run=False, limit=10)
    assert not any(s.startswith("DROP TABLE") for s in conn.sql)
