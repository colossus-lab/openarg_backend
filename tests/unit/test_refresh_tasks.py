"""Tests for the refresh sweep.

Two things carry the risk. It must stay inert until a cadence exists, and it
must never dispatch more than its budget — that cap is what separates this from
the load that restarted the database in May.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from app.application.collection import freshness

NOW = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)


class _Row:
    def __init__(self, i, days_old, portal="caba"):
        self.dataset_id = f"d{i}"
        self.portal = portal
        self.source_id = f"s{i}"
        self.last_collected_at = NOW - timedelta(days=days_old)
        self.table_name = f"t{i}"


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(freshness, "_PORTAL_CADENCE", {})
    monkeypatch.setattr(freshness, "_RESOURCE_CADENCE", {})
    monkeypatch.delenv("OPENARG_REFRESH_DEFAULT_DAYS", raising=False)


def _run(rows, *, mart_busy=False, **kw):
    from app.infrastructure.celery.tasks import collector_tasks, refresh_tasks

    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = MagicMock(fetchall=lambda: rows)
    refresh_tasks.get_sync_engine = lambda: engine
    collector_tasks._mart_rebuild_in_progress = lambda _e: mart_busy
    dispatched: list[str] = []
    collector_tasks.collect_dataset.delay = lambda did: dispatched.append(did)
    return refresh_tasks.refresh_stale_datasets.run(**kw), dispatched


# ── inerte hasta que exista una cadencia ───────────────────────


def test_it_does_nothing_while_no_cadence_is_configured():
    """The expected state today. Reporting zeros without saying why would read
    like 'nothing is stale', which is a different claim."""
    result, dispatched = _run([_Row(1, 400)], dry_run=False)

    assert result["enabled"] is False
    assert result["reason"] == "no_cadence_configured"
    assert dispatched == []


# ── el presupuesto ─────────────────────────────────────────────


def test_it_never_dispatches_more_than_its_budget():
    """152 concurrent collects and a 52M-row matview restarted RDS in May. The
    cap is the design, not a safety valve."""
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)
    rows = [_Row(i, 400) for i in range(500)]

    result, dispatched = _run(rows, limit=10, dry_run=False)

    assert result["stale_found"] == 10
    assert len(dispatched) == 10


def test_a_mart_rebuild_defers_the_whole_cycle():
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)

    result, dispatched = _run([_Row(1, 400)], mart_busy=True, dry_run=False)

    assert result["dispatched"] == 0
    assert result["reason"] == "mart_rebuild_in_progress"
    assert dispatched == []


# ── selección ──────────────────────────────────────────────────


def test_only_resources_past_their_policy_are_dispatched():
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=30)
    rows = [_Row(1, 400), _Row(2, 5), _Row(3, 90), _Row(4, 1)]

    result, dispatched = _run(rows, dry_run=False)

    assert result["stale_found"] == 2
    assert set(dispatched) == {"d1", "d3"}


def test_a_portal_without_a_cadence_is_left_alone():
    """Switching one portal on must not switch on the rest."""
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)
    rows = [_Row(1, 400, portal="caba"), _Row(2, 400, portal="otro")]

    _, dispatched = _run(rows, dry_run=False)

    assert dispatched == ["d1"]


def test_dry_run_dispatches_nothing_but_still_reports_what_it_found():
    """A sweep that re-reads sources is not something to switch on by deploying
    it, so the default has to be observable without acting."""
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)

    result, dispatched = _run([_Row(1, 400), _Row(2, 400)], dry_run=True)

    assert result["stale_found"] == 2
    assert result["dispatched"] == 0
    assert dispatched == []


def test_one_failed_dispatch_does_not_cost_the_batch():
    """The resource stays stale and the next cycle picks it up, which is the
    whole shape of this sweep."""
    from app.infrastructure.celery.tasks import collector_tasks, refresh_tasks

    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value = MagicMock(
        fetchall=lambda: [_Row(1, 400), _Row(2, 400), _Row(3, 400)]
    )
    refresh_tasks.get_sync_engine = lambda: engine
    collector_tasks._mart_rebuild_in_progress = lambda _e: False

    ok: list[str] = []

    def _flaky(did):
        if did == "d2":
            raise RuntimeError("broker hiccup")
        ok.append(did)

    collector_tasks.collect_dataset.delay = _flaky
    result = refresh_tasks.refresh_stale_datasets.run(dry_run=False)

    assert result["dispatched"] == 2
    assert ok == ["d1", "d3"]
