"""The reconciliation must never invent a location, and never touch what it must not.

The defect it repairs was invisible for weeks precisely because a registry that
lies is internally consistent. The tests that matter here are therefore about
refusal: what the sweep declines to do when it cannot be sure.
"""

from __future__ import annotations

import uuid

import pytest

from app.application.catalog.registry_reconcile import (
    _NEVER_TOUCH,
    RegistryUnavailable,
    reconcile_locations,
    require_registry,
    retire_phantom_rows,
)


class _Result:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class _Conn:
    """Answers by matching on the SQL text, which is enough to drive the flow."""

    def __init__(self, *, registry_rows=1000, present=True, misplaced=None, phantom=None):
        self.registry_rows = registry_rows
        self.present = present
        self.misplaced = misplaced or []
        self.phantom = phantom or []
        self.executed: list[str] = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append(sql)
        if "to_regclass" in sql:
            return _Result(scalar="public.raw_table_versions" if self.present else None)
        if "count(*) FROM public.raw_table_versions" in sql:
            return _Result(scalar=self.registry_rows)
        if "t.table_schema <> v.schema_name" in sql:
            return _Result(rows=self.misplaced)
        if "AND NOT EXISTS" in sql and "cached_datasets" in sql:
            return _Result(rows=self.phantom)
        # the per-row collision probe
        if "WHERE table_name = :t AND table_schema = :s" in sql:
            return _Result(rows=[])
        return _Result()

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


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def test_refuses_when_registry_is_missing():
    engine = _Engine(_Conn(present=False))
    with pytest.raises(RegistryUnavailable):
        require_registry(engine, task="t")


def test_refuses_when_registry_looks_truncated():
    """The 2026-08-03 shape: the table exists but holds almost nothing."""
    engine = _Engine(_Conn(registry_rows=12))
    with pytest.raises(RegistryUnavailable):
        require_registry(engine, task="t")


def test_dry_run_moves_nothing():
    conn = _Conn(
        misplaced=[
            _Row(resource_identity="p::s", version=1, table_name="cache_x",
                 declared="raw", actual="public")
        ]
    )
    out = reconcile_locations(_Engine(conn), run_id=uuid.uuid4(), dry_run=True)
    assert out.by_reason.get("would_move") == 1
    assert not any("SET SCHEMA" in s for s in conn.executed)


def test_apply_moves_to_the_schema_the_registry_names():
    """It agrees with the registry; it never edits the registry to agree."""
    conn = _Conn(
        misplaced=[
            _Row(resource_identity="p::s", version=1, table_name="cache_x",
                 declared="raw", actual="public")
        ]
    )
    out = reconcile_locations(_Engine(conn), run_id=uuid.uuid4(), dry_run=False)
    assert out.by_reason.get("moved") == 1
    moves = [s for s in conn.executed if "SET SCHEMA" in s]
    assert moves == ['ALTER TABLE "public"."cache_x" SET SCHEMA "raw"']
    assert not any("UPDATE public.raw_table_versions" in s for s in conn.executed)


def test_protected_tables_are_never_moved():
    conn = _Conn(
        misplaced=[
            _Row(resource_identity="p::s", version=1, table_name="users",
                 declared="raw", actual="public")
        ]
    )
    out = reconcile_locations(_Engine(conn), run_id=uuid.uuid4(), dry_run=False)
    assert out.by_reason.get("protected_table") == 1
    assert not any("SET SCHEMA" in s for s in conn.executed)


def test_user_and_conversation_tables_are_in_the_protected_set():
    for name in ("users", "user_queries", "conversations", "api_keys"):
        assert name in _NEVER_TOUCH


def test_phantoms_are_retired_not_deleted():
    """The row is the only record the table ever existed. Keep it."""
    conn = _Conn(
        phantom=[_Row(resource_identity="p::s", version=3,
                      table_name="gone_tbl", schema_name="raw")]
    )
    out = retire_phantom_rows(_Engine(conn), run_id=uuid.uuid4(), dry_run=False)
    assert out.by_reason.get("retired") == 1
    assert any("SET superseded_at = now()" in s for s in conn.executed)
    assert not any("DELETE FROM public.raw_table_versions" in s for s in conn.executed)


def test_embedding_signature_ignores_the_portal_timestamp():
    """A timestamp the chunks never mention must not buy new embeddings.

    Production, 2026-08-23: 21,729 chunks in the hour the scrape runs, 20,113
    datasets, 1,807 of them actually re-collected. The other ~18,300 paid for an
    embedding of text that had not changed, because the portals moved
    `last_updated_at` and the signature was watching it.
    """
    from app.infrastructure.celery.tasks.scraper_tasks import _embedding_signature

    fields = dict(
        title="Pobreza",
        description="Serie",
        organization="INDEC",
        portal="indec",
        download_url="https://x/y.csv",
        fmt="csv",
        columns=["a", "b"],
        tags="social",
    )
    # The signature takes no timestamp at all any more — passing one is an error
    # rather than something silently ignored.
    import inspect

    assert "last_updated" not in inspect.signature(_embedding_signature).parameters

    base = _embedding_signature(**fields)
    assert base == _embedding_signature(**fields)
    # Fields that DO reach the chunk text must still move it.
    assert _embedding_signature(**{**fields, "title": "Otra"}) != base
    assert _embedding_signature(**{**fields, "columns": ["a", "c"]}) != base
