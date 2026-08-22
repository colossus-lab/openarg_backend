"""Tests for the refresh sweep.

Two things carry the risk: it must stay inert until a portal is switched on, and
it must never dispatch more than its budget — that cap is what separates this
from the load that restarted the database in May.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from app.application.collection import freshness

NOW = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)


class _Row:
    def __init__(self, i, declared=True, portal="energia", days_old=10):
        self.dataset_id = f"d{i}"
        self.portal = portal
        self.source_id = f"s{i}"
        self.last_collected_at = NOW - timedelta(days=days_old)
        self.portal_last_updated_at = NOW - timedelta(days=1 if declared else 900)
        self.portal_declares_change = declared


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(freshness, "_ENABLED_PORTALS", set())
    monkeypatch.setattr(freshness, "_NEVER_REFRESH", set())
    monkeypatch.delenv("OPENARG_REFRESH_PORTALS", raising=False)
    monkeypatch.delenv("OPENARG_REFRESH_BACKSTOP_DAYS", raising=False)


def _run(rows, *, mart_busy=False, **kw):
    from app.infrastructure.celery.tasks import collector_tasks, refresh_tasks

    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value = MagicMock(
        fetchall=lambda: rows
    )
    refresh_tasks.get_sync_engine = lambda: engine
    collector_tasks._mart_rebuild_in_progress = lambda _e: mart_busy
    sent: list[str] = []
    collector_tasks.collect_dataset.delay = lambda did: sent.append(did)
    return refresh_tasks.refresh_stale_datasets.run(**kw), sent, engine


def test_it_does_nothing_while_no_portal_is_enabled():
    """The expected state today. Zeros without that context would read like
    'nothing is stale' while 3,431 resources are."""
    result, sent, _ = _run([_Row(1)], dry_run=False)

    assert result["enabled"] is False
    assert result["reason"] == "no_portal_enabled"
    assert sent == []


def test_it_never_dispatches_more_than_its_budget():
    """The cap is the design. 152 concurrent collects and a 52M-row matview
    restarted RDS in May."""
    freshness._ENABLED_PORTALS.add("energia")
    # The query applies LIMIT, so the sweep must not exceed what it asked for.
    result, sent, engine = _run([_Row(i) for i in range(10)], limit=10, dry_run=False)

    assert len(sent) == 10
    params = engine.connect.return_value.__enter__.return_value.execute.call_args.args[1]
    assert params["limit"] == 10


def test_a_mart_rebuild_defers_the_whole_cycle():
    freshness._ENABLED_PORTALS.add("energia")

    result, sent, _ = _run([_Row(1)], mart_busy=True, dry_run=False)

    assert result["dispatched"] == 0
    assert result["reason"] == "mart_rebuild_in_progress"
    assert sent == []


def test_only_enabled_portals_reach_the_query():
    freshness._ENABLED_PORTALS.update({"energia", "caba"})

    _, _, engine = _run([_Row(1)], dry_run=True)

    params = engine.connect.return_value.__enter__.return_value.execute.call_args.args[1]
    assert params["portals"] == ["caba", "energia"]
    assert params["backstop_days"] == 90


def test_the_two_reasons_are_counted_apart():
    """One is the portal telling us the file moved; the other is us admitting we
    have not looked. A run that is mostly backstop means the metadata is not
    carrying its weight, and that is worth seeing."""
    freshness._ENABLED_PORTALS.add("energia")
    rows = [_Row(1), _Row(2), _Row(3, declared=False), _Row(4, declared=False)]

    result, _, _ = _run(rows, dry_run=False)

    assert result["by_reason"] == {"portal_declares_change": 2, "backstop_age": 2}
    assert result["eligible"] == 4


def test_dry_run_dispatches_nothing_but_still_reports():
    """A sweep that re-reads sources is not something to switch on by deploying
    it, so the default has to be observable without acting."""
    freshness._ENABLED_PORTALS.add("energia")

    result, sent, _ = _run([_Row(1), _Row(2)], dry_run=True)

    assert result["eligible"] == 2
    assert result["dispatched"] == 0
    assert sent == []


def test_one_failed_dispatch_does_not_cost_the_batch():
    from app.infrastructure.celery.tasks import collector_tasks, refresh_tasks

    freshness._ENABLED_PORTALS.add("energia")
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value = MagicMock(
        fetchall=lambda: [_Row(1), _Row(2), _Row(3)]
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
