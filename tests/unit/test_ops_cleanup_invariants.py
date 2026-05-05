"""Tests for `cleanup_invariants` periodic task.

Covers the seven mutation paths plus idempotency:
  1. classifier override for stale `unknown` rows
  2. retry_count clamp
  3. canonical orphan registration (resolves through cached_datasets → datasets)
  4. fallback orphan registration when no dataset owns the table
  5. is_cached drift fix
  6. mart_definitions.last_row_count drift (matview has rows but metadata = 0)
  7. drop empty orphan raw tables (0 rows, no rtv, no cached_datasets)
  8. running twice in a row is a no-op
"""

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
      1 classifier  2 retry-clamp  3 canonical-orphan  4 fallback-orphan
      5 is_cached-drift  6 mart-rowcount-drift  7 empty-orphan-SELECT
      8..N one-DROP-per-empty-orphan returned by step 7
    """
    conn = MagicMock()
    conn.execute.side_effect = results
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


# Standard 7-result baseline for tests that exercise no empty-orphan drops.
def _baseline(*rowcounts: int) -> list[_FakeResult]:
    """Build the 7-step baseline (no empty orphan drops)."""
    assert len(rowcounts) == 6, "need 6 rowcounts for the 6 rowcount-paths"
    return [
        *(_FakeResult(rowcount=rc) for rc in rowcounts),
        _FakeResult(rows=[]),  # empty-orphan SELECT, no candidates
    ]


def test_no_drift_returns_zero_summary():
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_invariants

    engine, _conn = _build_engine_with_results(_baseline(0, 0, 0, 0, 0, 0))

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        summary = cleanup_invariants.run()

    assert summary == {
        "fixed_unknown_category": 0,
        "clamped_retry_count": 0,
        "registered_orphan_tables": 0,
        "canonical_orphans_registered": 0,
        "fixed_is_cached_drift": 0,
        "fixed_mart_row_count": 0,
        "dropped_empty_orphans": 0,
    }


def test_all_paths_count_correctly():
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_invariants

    engine, _conn = _build_engine_with_results(_baseline(3, 2, 10, 4, 1, 2))

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        summary = cleanup_invariants.run()

    assert summary["fixed_unknown_category"] == 3
    assert summary["clamped_retry_count"] == 2
    assert summary["canonical_orphans_registered"] == 10
    # registered_orphan_tables sums canonical + fallback (legacy field
    # for dashboards that already track this metric).
    assert summary["registered_orphan_tables"] == 14
    assert summary["fixed_is_cached_drift"] == 1
    assert summary["fixed_mart_row_count"] == 2
    assert summary["dropped_empty_orphans"] == 0


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
            _FakeResult(rowcount=0),  # canonical orphan registration
            _FakeResult(rowcount=0),  # fallback orphan registration
            _FakeResult(rowcount=0),  # is_cached drift
            _FakeResult(rowcount=0),  # mart row_count drift
            _FakeResult(rows=[fake_orphan_a, fake_orphan_b]),  # SELECT
            _FakeResult(rowcount=0),  # DROP a
            _FakeResult(rowcount=0),  # DROP b
        ]
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        summary = cleanup_invariants.run()

    assert summary["dropped_empty_orphans"] == 2
    # 6 rowcount paths + 1 SELECT + 2 DROPs.
    assert conn.execute.call_count == 9


def test_idempotent_after_first_run():
    """Running twice with no new drift between calls returns zeros on the
    second call. The function must not assume any state from the first run."""
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_invariants

    # First call: drift exists.
    engine_first, _ = _build_engine_with_results(_baseline(5, 5, 5, 5, 5, 5))
    # Second call: nothing left to fix.
    engine_second, _ = _build_engine_with_results(_baseline(0, 0, 0, 0, 0, 0))

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

    engine, _ = _build_engine_with_results(_baseline(0, 0, 15, 3, 0, 0))

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        summary = cleanup_invariants.run()

    assert summary["canonical_orphans_registered"] == 15
    assert summary["registered_orphan_tables"] == 18  # canonical + fallback


def test_emits_7_sql_statements_when_no_orphans():
    """Sanity: the function must emit exactly 7 statements per invocation
    when there are no empty orphans to drop (6 rowcount paths + 1 SELECT).
    Adding new paths is fine — silently removing one would skip a drift
    class, which is what this test guards against."""
    from app.infrastructure.celery.tasks.ops_fixes import cleanup_invariants

    engine, conn = _build_engine_with_results(_baseline(0, 0, 0, 0, 0, 0))

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.celery.tasks.ops_fixes.get_sync_engine",
            lambda: engine,
        )
        cleanup_invariants.run()

    assert conn.execute.call_count == 7
