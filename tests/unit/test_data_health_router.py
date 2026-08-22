"""Tests for the data-health endpoint.

Its purpose is to make a silent system legible, so the tests are about the
distinctions it has to preserve — the ones that were invisible before and cost
three months of a cleanup sweep doing nothing while reporting success.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class _Repair:
    def __init__(self, phase, operation, ok, n, last_seen=None):
        self.phase = phase
        self.operation = operation
        self.ok = ok
        self.n = n
        self.last_seen = last_seen


@pytest.fixture
def health(monkeypatch):
    from app.presentation.http.controllers.admin import data_health_router as mod

    def _make(repairs, fresh=None, parse=None, drift=None, attributable=0):
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.side_effect = [
            MagicMock(fetchone=lambda: fresh or MagicMock(
                total=100, week=5, month=10, quarter=20, older=80,
                oldest=None, portal_says_changed=30)),
            MagicMock(fetchone=lambda: parse or MagicMock(
                tables=1000, col_n=10, unnamed=20, long_names=30,
                one_or_two_columns=40, any_symptom=90)),
            MagicMock(fetchall=lambda: repairs),
            MagicMock(fetchone=lambda: drift or MagicMock(
                snapshots=500, tables=490, with_real_provenance=0, last_capture=None)),
            MagicMock(scalar=lambda: attributable),
        ]
        monkeypatch.setattr(mod, "get_sync_engine", lambda: engine)
        return mod.data_health()

    return _make


def test_a_sweep_that_declines_everything_is_not_a_sweep_that_is_idle(health):
    """From outside they look identical, and that is exactly how
    cleanup_raw_orphans reported {'dropped': 0, 'failed': 10} and succeeded
    every hour for three months while doing nothing."""
    result = health(
        [
            _Repair("col_n", "skip", False, 186),
            _Repair("unsplit_csv", "apply", True, 122),
            _Repair("unsplit_csv", "skip", False, 87),
        ]
    )

    assert result["repairs"]["by_phase"]["col_n"] == {
        "applied": 0,
        "declined": 186,
        "dry_run": 0,
    }
    assert result["repairs"]["by_phase"]["unsplit_csv"]["applied"] == 122
    assert result["repairs"]["by_phase"]["unsplit_csv"]["declined"] == 87


def test_it_reports_what_the_portals_say_changed_not_only_age(health):
    """Age alone says nothing when three quarters of the catalogue never
    changes. The queue that matters is what the sources declare moved."""
    result = health([])

    assert result["freshness"]["portal_says_changed"] == 30
    assert result["freshness"]["older_than_90_days_pct"] == 80.0


def test_zero_attributable_pairs_is_surfaced_rather_than_implied(health):
    """A drift report of zeros means 'nothing comparable' when no pair is
    attributable, and 'nothing wrong' when they are. Without this number the
    two are indistinguishable."""
    result = health([], attributable=0)

    assert result["drift_observability"]["attributable_pairs"] == 0
    assert result["drift_observability"]["with_real_provenance"] == 0


def test_percentages_do_not_divide_by_zero_on_an_empty_database(health):
    empty = MagicMock(
        total=0, week=0, month=0, quarter=0, older=0, oldest=None, portal_says_changed=0
    )
    result = health([], fresh=empty)

    assert result["freshness"]["older_than_90_days_pct"] == 0.0


def test_the_endpoint_requires_an_admin_key():
    """It exposes the shape of the whole corpus; it is not public."""
    from app.presentation.http.controllers.admin import data_health_router as mod

    deps = [d.dependency.__name__ for d in mod.router.routes[0].dependencies]
    assert "verify_admin_key" in deps
