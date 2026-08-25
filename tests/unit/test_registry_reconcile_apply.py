"""The apply paths of the reconciliation sweeps, exercised.

The existing tests cover the refusals, which is where most of the danger is.
The branches that actually move a table, retire a row or register one were at
zero: they had never run in a test, and each of them writes to production.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

from app.application.catalog.registry_reconcile import (
    backfill_legacy_registry,
    reconcile_locations,
    retire_phantom_rows,
)


class _Conn:
    def __init__(self, *, misplaced=None, phantom=None, unregistered=None, taken=False):
        self.misplaced = misplaced or []
        self.phantom = phantom or []
        self.unregistered = unregistered or []
        self.taken = taken
        self.sql: list[str] = []

    def execute(self, stmt, params=None):
        s = " ".join(str(stmt).split())
        self.sql.append(s)
        if "to_regclass" in s:
            return SimpleNamespace(scalar=lambda: "public.raw_table_versions")
        if "count(*) FROM public.raw_table_versions" in s:
            return SimpleNamespace(scalar=lambda: 5000)
        if "t.table_schema <> v.schema_name" in s:
            return SimpleNamespace(fetchall=lambda: self.misplaced)
        if "cached_datasets cd WHERE cd.table_name = v.table_name" in s:
            return SimpleNamespace(fetchall=lambda: self.phantom)
        if "JOIN datasets d ON d.id = cd.dataset_id" in s and "NOT EXISTS" in s:
            return SimpleNamespace(fetchall=lambda: self.unregistered)
        if "WHERE table_name = :t AND table_schema = :s" in s:
            return SimpleNamespace(fetchone=lambda: (1,) if self.taken else None)
        return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None, scalar=lambda: None)

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


def _row(**kw):
    base = dict(
        resource_identity="p::s", version=1, table_name="cache_x", declared="raw", actual="public"
    )
    base.update(kw)
    return SimpleNamespace(**base)


def test_a_misplaced_table_is_moved_to_the_schema_the_row_names():
    conn = _Conn(misplaced=[_row()])
    out = reconcile_locations(_Engine(conn), run_id=uuid.uuid4(), dry_run=False)
    assert out.by_reason.get("moved") == 1
    moves = [s for s in conn.sql if "SET SCHEMA" in s]
    assert moves == ['ALTER TABLE "public"."cache_x" SET SCHEMA "raw"']
    # The row is never edited to match the table.
    assert not any("UPDATE public.raw_table_versions SET schema_name" in s for s in conn.sql)


def test_a_name_already_taken_in_the_destination_blocks_the_move():
    conn = _Conn(misplaced=[_row()], taken=True)
    out = reconcile_locations(_Engine(conn), run_id=uuid.uuid4(), dry_run=False)
    assert out.by_reason.get("name_taken_in_destination") == 1
    assert not any("SET SCHEMA" in s for s in conn.sql)


def test_a_phantom_row_is_retired_and_not_deleted():
    conn = _Conn(
        phantom=[
            SimpleNamespace(
                resource_identity="p::s", version=3, table_name="gone", schema_name="raw"
            )
        ]
    )
    out = retire_phantom_rows(_Engine(conn), run_id=uuid.uuid4(), dry_run=False)
    assert out.by_reason.get("retired") == 1
    assert any("SET superseded_at = now()" in s for s in conn.sql)
    assert not any("DELETE FROM public.raw_table_versions" in s for s in conn.sql)


def test_a_backfilled_row_lands_in_the_schema_the_table_is_in():
    """Writing `raw` because that is where new tables go is the defect the
    location sweep exists to repair. This must not manufacture more."""
    conn = _Conn(
        unregistered=[
            SimpleNamespace(
                table_name="cache_y",
                table_schema="public",
                row_count=42,
                resource_identity="datos_gob_ar::abc",
            )
        ]
    )
    out = backfill_legacy_registry(_Engine(conn), run_id=uuid.uuid4(), dry_run=False)
    assert out.by_reason.get("registered") == 1
    ins = [s for s in conn.sql if "INSERT INTO public.raw_table_versions" in s]
    assert len(ins) == 1
    assert ":schema" in ins[0] and "'raw'" not in ins[0]
    assert "'legacy:unknown'" in ins[0]


def test_a_failed_move_is_recorded_and_the_batch_continues():
    class _Flaky(_Conn):
        def execute(self, stmt, params=None):
            s = " ".join(str(stmt).split())
            if "SET SCHEMA" in s and "malo" in s:
                self.sql.append(s)
                raise RuntimeError("nope")
            return super().execute(stmt, params)

    conn = _Flaky(misplaced=[_row(table_name="malo"), _row(table_name="bueno")])
    out = reconcile_locations(_Engine(conn), run_id=uuid.uuid4(), dry_run=False)
    assert out.by_reason.get("moved") == 1
    assert out.by_reason.get("move_failed") == 1


def test_protected_tables_survive_every_sweep():
    for fn, kw in (
        (reconcile_locations, {"misplaced": [_row(table_name="users")]}),
        (
            retire_phantom_rows,
            {
                "phantom": [
                    SimpleNamespace(
                        resource_identity="p::s",
                        version=1,
                        table_name="conversations",
                        schema_name="public",
                    )
                ]
            },
        ),
    ):
        conn = _Conn(**kw)
        out = fn(_Engine(conn), run_id=uuid.uuid4(), dry_run=False)
        assert out.by_reason.get("protected_table") == 1, fn.__name__
        # Match the write, not the word: `superseded_at IS NULL` appears in the
        # SELECT that finds candidates. A looser assertion here would fail on
        # the query rather than on a mutation, which is how a test ends up
        # testing itself.
        assert not any("SET SCHEMA" in s or "SET superseded_at" in s for s in conn.sql)
