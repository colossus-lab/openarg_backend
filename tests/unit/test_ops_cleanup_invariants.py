"""Tests for `cleanup_invariants` periodic task."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class _FakeResult:
    """Mocks a SQLAlchemy `Result` for `cleanup_invariants` exec calls.

    Supports the two access patterns used by the function:
      - `.rowcount` for INSERT/UPDATE/DELETE
      - `.fetchall()` for the empty-orphan SELECT
    """

    def __init__(self, rowcount: int = 0, rows: list | None = None) -> None:
        self.rowcount = rowcount
        self._rows = rows or []

    def fetchall(self):
        return self._rows


def _build_engine_with_results(results: list[_FakeResult]) -> MagicMock:
    """Build a mock engine that returns the given results in order from
    consecutive `conn.execute(...)` calls.

    The mutation paths consume results in this exact order:
      1 classifier
      2 retry-clamp
      3 zombie-errors
      4 zero-retry-zombies
      5 canonical-orphan
      6 fallback-orphan
      7 is_cached-drift
      8 mart-rowcount-drift
      9 empty-orphan-SELECT
      10..N one-DROP-per-empty-orphan
      final 2 statements: double_cd_ready purge + dataset row_count drift
    """
    conn = MagicMock()
    conn.execute.side_effect = results
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


# Standard baseline for tests that exercise no empty-orphan drops.
# 8 rowcount paths + 1 SELECT + 1 double_cd_ready + 1 dataset_row_count.
def _baseline(*rowcounts: int) -> list[_FakeResult]:
    """Build the 11-step baseline (no empty orphan drops).

    Order MUST match `cleanup_invariants` exec order:
      1 classifier
      2 retry-clamp
      3 zombie errors
      4 zero-retry zombies
      5 canonical-orphan
      6 fallback-orphan
      7 is_cached-drift
      8 mart-rowcount-drift
      9 empty-orphan-SELECT (rows=[])
      10 double_cd_ready
      11 dataset-row-count-drift

    `fixed_unknown_on_ready` is reported in the summary but always 0:
    the retroactive fix is blocked on a schema migration.
    """
    assert len(rowcounts) == 10, "need 10 rowcounts (8 invariant + double_ready + dataset_rc)"
    return [
        *(_FakeResult(rowcount=rc) for rc in rowcounts[:8]),
        _FakeResult(rows=[]),  # empty-orphan SELECT, no candidates
        _FakeResult(rowcount=rowcounts[8]),  # double_cd_ready purge
        _FakeResult(rowcount=rowcounts[9]),  # dataset row_count drift
    ]


def test_no_drift_returns_zero_summary():
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_invariants

    engine, _conn = _build_engine_with_results(_baseline(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        summary = cleanup_invariants.run()

    assert summary == {
        "fixed_unknown_category": 0,
        "clamped_retry_count": 0,
        "fixed_zombie_errors": 0,
        "fixed_zero_retry_zombies": 0,
        "registered_orphan_tables": 0,
        "canonical_orphans_registered": 0,
        "fixed_is_cached_drift": 0,
        "fixed_mart_row_count": 0,
        "dropped_empty_orphans": 0,
        "fixed_dataset_row_count": 0,
        "fixed_unknown_on_ready": 0,
        "fixed_double_cd_ready": 0,
    }


def test_all_paths_count_correctly():
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_invariants

    engine, _conn = _build_engine_with_results(_baseline(3, 2, 8, 9, 10, 4, 1, 2, 7, 99))

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        summary = cleanup_invariants.run()

    assert summary["fixed_unknown_category"] == 3
    assert summary["clamped_retry_count"] == 2
    assert summary["fixed_zombie_errors"] == 8
    assert summary["fixed_zero_retry_zombies"] == 9
    assert summary["canonical_orphans_registered"] == 10
    # registered_orphan_tables sums canonical + fallback (legacy field
    # for dashboards that already track this metric).
    assert summary["registered_orphan_tables"] == 14
    assert summary["fixed_is_cached_drift"] == 1
    assert summary["fixed_mart_row_count"] == 2
    assert summary["dropped_empty_orphans"] == 0
    assert summary["fixed_dataset_row_count"] == 99
    assert summary["fixed_double_cd_ready"] == 7
    # Always 0 — see cleanup_invariants for why this is blocked.
    assert summary["fixed_unknown_on_ready"] == 0


def test_drops_empty_orphans():
    """When the empty-orphan SELECT returns rows, each one must be DROPped
    individually and counted in `dropped_empty_orphans`."""
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_invariants

    fake_orphan_a = MagicMock()
    fake_orphan_a.table_name = "energia__broken_a__deadbeef__v1"
    fake_orphan_b = MagicMock()
    fake_orphan_b.table_name = "tucuman__broken_b__cafef00d__v1"

    engine, conn = _build_engine_with_results(
        [
            _FakeResult(rowcount=0),  # classifier
            _FakeResult(rowcount=0),  # retry clamp
            _FakeResult(rowcount=0),  # zombie errors
            _FakeResult(rowcount=0),  # zero-retry zombies
            _FakeResult(rowcount=0),  # canonical orphan registration
            _FakeResult(rowcount=0),  # fallback orphan registration
            _FakeResult(rowcount=0),  # is_cached drift
            _FakeResult(rowcount=0),  # mart row_count drift
            _FakeResult(rows=[fake_orphan_a, fake_orphan_b]),  # SELECT
            _FakeResult(rowcount=0),  # DROP a
            _FakeResult(rowcount=0),  # DROP b
            _FakeResult(rowcount=0),  # double_cd_ready purge
            _FakeResult(rowcount=0),  # dataset row_count drift
        ]
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        summary = cleanup_invariants.run()

    assert summary["dropped_empty_orphans"] == 2
    # 8 rowcount + 1 SELECT + 2 DROPs + 1 double_cd_ready + 1 dataset row_count = 13.
    assert conn.execute.call_count == 13


def test_idempotent_after_first_run():
    """Running twice with no new drift between calls returns zeros on the
    second call. The function must not assume any state from the first run."""
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_invariants

    # First call: drift exists.
    engine_first, _ = _build_engine_with_results(_baseline(5, 5, 5, 5, 5, 5, 5, 5, 5, 5))
    # Second call: nothing left to fix.
    engine_second, _ = _build_engine_with_results(_baseline(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    with pytest.MonkeyPatch.context() as mp:
        # First run.
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine_first,
        )
        summary_first = cleanup_invariants.run()

        # Second run.
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine_second,
        )
        summary_second = cleanup_invariants.run()

    assert summary_first["registered_orphan_tables"] == 10  # 5 + 5
    assert summary_second["registered_orphan_tables"] == 0


def test_counts_canonical_and_fallback_separately():
    """`canonical_orphans_registered` should reflect ONLY the canonical
    (portal::source_id) registrations, not the fallback ones. This invariant
    matters for dashboards that track migration progress."""
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_invariants

    engine, _ = _build_engine_with_results(_baseline(0, 0, 0, 0, 15, 3, 0, 0, 0, 0))

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        summary = cleanup_invariants.run()

    assert summary["canonical_orphans_registered"] == 15
    assert summary["registered_orphan_tables"] == 18  # canonical + fallback


def test_emits_11_sql_statements_when_no_orphans():
    """Sanity: 8 rowcount + 1 empty-orphan SELECT + 1 double_cd_ready
    + 1 dataset row_count = 11 total. Adding new paths is fine —
    silently removing one would skip a drift class, which is what
    this test guards against."""
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_invariants

    engine, conn = _build_engine_with_results(_baseline(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        cleanup_invariants.run()

    assert conn.execute.call_count == 11


def test_mart_dependency_guards_extract_physical_raw_tables_from_resolved_sql():
    from app.infrastructure.celery.tasks.ops_fixes import _mart_dependency_guards

    row = MagicMock()
    row.sql_definition = (
        'SELECT * FROM raw."energia__petroleo__deadbeef__v1" '
        "UNION ALL SELECT * FROM raw.gas__prod__cafef00d__v2"
    )
    live_row = MagicMock()
    live_row.definition = 'SELECT * FROM raw."legacy__still_live__feedface__v1"'
    conn = MagicMock()
    conn.execute.side_effect = [
        [row],
        [live_row],
    ]
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    exact, patterns, tables = _mart_dependency_guards(engine)

    assert exact == set()
    assert patterns == []
    assert tables == {
        "energia__petroleo__deadbeef__v1",
        "gas__prod__cafef00d__v2",
        "legacy__still_live__feedface__v1",
    }


def test_backfill_error_categories_requeries_remaining_unknown_rows_without_offset():
    from app.infrastructure.celery.tasks.ops_fixes import backfill_error_categories

    first_batch = [MagicMock(id="1", error_message="timeout upstream")]
    second_batch = [MagicMock(id="2", error_message="403 forbidden")]
    final_batch: list = []

    select_conn = MagicMock()
    select_conn.execute.side_effect = [
        _FakeResult(rows=first_batch),
        _FakeResult(rows=second_batch),
        _FakeResult(rows=final_batch),
    ]
    update_conn = MagicMock()
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=select_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=update_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        summary = backfill_error_categories.run(batch_size=1)

    assert summary["seen"] == 2
    assert summary["updated"] == 2
    assert select_conn.execute.call_count == 3
