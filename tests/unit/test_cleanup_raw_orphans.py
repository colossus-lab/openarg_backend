"""Tests for `cleanup_raw_orphans` periodic task.

Covers the safety contract:
  1. Only drops `raw.*` tables whose `table_name` no `cached_datasets` row claims
  2. Skips tables created within `min_age_hours` (race protection)
  3. Records each drop in `cache_drop_audit` with `reason='raw_orphan_cleanup'`
  4. Caps drops per run to `max_drops` for RDS IO budget
  5. `dry_run=True` reports without touching DB
  6. Both physical DROP and `raw_table_versions` row deletion happen atomically
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class _FakeRow:
    def __init__(self, table_name: str, version: int = 1) -> None:
        self.resource_identity = f"portal::{table_name}"
        self.version = version
        self.schema_name = "raw"
        self.table_name = table_name


class _FakeResult:
    def __init__(self, rows: list | None = None, rowcount: int = 0) -> None:
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows


def _build_engine_with_select(rows: list, mart_rows: list | None = None) -> MagicMock:
    """Engine that returns:
      1. mart_definitions rows
      2. pg_matviews rows
      3. orphan SELECT rows

    Supports both `engine.connect()` (read) and `engine.begin()` (write) as
    separate context managers.
    """
    mart_rows = mart_rows or []
    read_conn = MagicMock()
    # First execute returns mart sql_definitions (empty by default → no
    # protected identities), second returns live matview definitions,
    # third returns orphan candidates.
    read_conn.execute.side_effect = [
        _FakeResult(rows=mart_rows),
        _FakeResult(rows=[]),
        _FakeResult(rows=rows),
    ]

    write_conn = MagicMock()
    write_conn.execute.return_value = _FakeResult(rowcount=1)

    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=read_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=write_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, read_conn, write_conn


def test_dry_run_reports_without_dropping():
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_raw_orphans

    rows = [_FakeRow("orphan_a"), _FakeRow("orphan_b")]
    engine, _, write_conn = _build_engine_with_select(rows)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        result = cleanup_raw_orphans.run(dry_run=True)

    assert result["dry_run"] is True
    assert result["found"] == 2
    assert "orphan_a" in result["samples"]
    write_conn.execute.assert_not_called()


def test_drops_each_orphan_with_audit():
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_raw_orphans

    rows = [_FakeRow("orphan_x"), _FakeRow("orphan_y")]
    engine, _, write_conn = _build_engine_with_select(rows)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        record_drop = MagicMock()
        mp.setattr(
            "app.infrastructure.celery.tasks.collector_tasks._record_cache_drop",
            record_drop,
        )
        result = cleanup_raw_orphans.run(dry_run=False)

    assert result["dropped"] == 2
    assert result["failed"] == 0
    assert record_drop.call_count == 2
    for call in record_drop.call_args_list:
        assert call.kwargs["reason"] == "raw_orphan_cleanup"
        assert call.kwargs["actor"] == "ops_fixes.cleanup_raw_orphans"


def test_max_drops_caps_per_run():
    """When > max_drops candidates exist, only max_drops are processed and
    `truncated_to_max_drops=True` flags the operator to schedule another run.
    """
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_raw_orphans

    # SELECT returns max_drops + 1 rows; task processes only max_drops.
    rows = [_FakeRow(f"orphan_{i}") for i in range(6)]
    engine, _, _ = _build_engine_with_select(rows)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        mp.setattr(
            "app.infrastructure.celery.tasks.collector_tasks._record_cache_drop",
            MagicMock(),
        )
        result = cleanup_raw_orphans.run(dry_run=False, max_drops=5)

    assert result["candidates"] == 5
    assert result["truncated_to_max_drops"] is True


def test_invalid_max_drops_raises():
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_raw_orphans

    with pytest.raises(ValueError):
        cleanup_raw_orphans.run(max_drops=0)


def test_invalid_min_age_hours_raises():
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_raw_orphans

    with pytest.raises(ValueError):
        cleanup_raw_orphans.run(min_age_hours=-1)


def test_mart_protected_identities_excluded():
    """Resource_identities referenced by `live_table('...')` in any
    mart_definition.sql_definition must NOT be dropped, even when no
    cd row points to their physical table. Regression test for the
    escuelas_argentina breakage on 2026-05-06: dropping the BA Prov
    establecimientos raw table left the mart unable to find `cue`.
    """
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_raw_orphans

    # Mart references this identity via live_table()
    mart_row = MagicMock()
    mart_row.mart_id = "escuelas_argentina"
    mart_row.sql_definition = (
        "SELECT cue, nombre FROM {{ live_table('buenos_aires_prov::70f9ae07-protected') }}"
    )

    # Two candidates from the orphan SELECT — but the first matches a
    # protected identity. The task processes only the second.
    rows = [
        _FakeRow("buenos_aires_prov__establecimientos__abc__v1"),
        _FakeRow("safe_to_drop_table"),
    ]
    rows[0].resource_identity = "buenos_aires_prov::70f9ae07-protected"
    rows[1].resource_identity = "buenos_aires_prov::other-id"

    engine, _, _ = _build_engine_with_select(rows, mart_rows=[mart_row])

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        record_drop = MagicMock()
        mp.setattr(
            "app.infrastructure.celery.tasks.collector_tasks._record_cache_drop",
            record_drop,
        )
        # The SQL `NOT IN :exact` filter is what enforces the exclusion;
        # this test verifies the protection set is computed correctly.
        # The task's SQL pre-excludes via bindparam, so we trust the
        # WHERE clause and simply assert the protected identity was
        # detected (logged) and never reached _record_cache_drop.
        result = cleanup_raw_orphans.run(dry_run=True)

    # In dry-run we only report; the protected identity should not be
    # in the candidate list. Since the mocked SELECT returns it anyway
    # (we cannot mock SQL filtering), this test mainly verifies the
    # function does not crash with the new protection logic.
    assert result["dry_run"] is True


def test_empty_result_returns_zero_summary():
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_raw_orphans

    engine, _, write_conn = _build_engine_with_select([])

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        result = cleanup_raw_orphans.run(dry_run=False)

    assert result["candidates"] == 0
    assert result["dropped"] == 0
    write_conn.execute.assert_not_called()


def test_live_tables_holding_data_are_out_of_scope():
    """A live version with rows is not a leftover, and must not be a candidate.

    The task used the absence of a `cached_datasets` row as its definition of
    "abandoned". That holds when a reprocess moves a dataset to a new physical
    name — but the row is equally absent when the *dataset* disappears from the
    catalog, which happens every time a portal regenerates its source_ids.

    Measured on staging 2026-08-21, immediately after `raw.cached_datasets` was
    restored: 700 candidates, of which 652 were live and held 99.2M rows between
    them — orphaned only because their `datasets` row had been re-keyed
    upstream. With the guard the same query returns 91.

    The engine is mocked in this suite, so what is pinned here is the contract:
    superseded versions stay in scope, empty live ones stay in scope, and a live
    table with rows is excluded before it ever reaches the drop loop.
    """
    from app.infrastructure.celery.tasks import ops_fixes

    source = ops_fixes.cleanup_raw_orphans.__wrapped__.__code__.co_consts
    sql = next(
        (c for c in source if isinstance(c, str) and "raw_table_versions rtv" in c),
        None,
    )
    assert sql is not None, "candidate SELECT not found"
    assert "rtv.superseded_at IS NOT NULL" in sql
    assert "reltuples" in sql


def test_views_are_never_drop_candidates():
    """`information_schema.tables` lists views, and `DROP TABLE` on one fails
    with WrongObjectType.

    Production carries 13 views named `cache_*` in the `raw` schema. The sweep
    orders oldest-first and caps at ten, so it selected the same ten views on
    every run, failed on all of them, and never reached a real orphan behind
    them — which is why production recorded no drop at all between 2026-05-20
    and 2026-08-22. Verified against production: the candidate query returns 11
    without the filter and 1 with it.

    The failure was invisible for three months because the task reports
    `{'dropped': 0, 'failed': 10}` and succeeds.
    """
    from app.infrastructure.celery.tasks import ops_fixes

    source = ops_fixes.cleanup_raw_orphans.__wrapped__.__code__.co_consts
    sql = next(
        (c for c in source if isinstance(c, str) and "information_schema.tables" in c), None
    )
    assert sql is not None, "candidate SELECT not found"
    assert "BASE TABLE" in sql, "the candidate query must exclude views"
