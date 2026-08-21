"""Tests for the freshness policy.

The property that matters most is that it ships **off**. A cadence invented in
code would be a confident guess about data nobody in this file understands, and
the first run of a sweep that treats every resource as eligible is the load
pattern that restarted the database in May.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.application.collection import freshness
from app.application.collection.freshness import (
    MIN_CADENCE,
    is_enabled,
    is_stale,
    refresh_age_for,
)

NOW = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(freshness, "_PORTAL_CADENCE", {})
    monkeypatch.setattr(freshness, "_RESOURCE_CADENCE", {})
    monkeypatch.delenv("OPENARG_REFRESH_DEFAULT_DAYS", raising=False)


# ── viene apagado ──────────────────────────────────────────────


def test_refresh_is_off_until_someone_decides_a_cadence():
    assert refresh_age_for("caba") is None
    assert not is_enabled()


def test_nothing_is_stale_while_it_is_off():
    ancient = NOW - timedelta(days=400)
    assert not is_stale(last_collected_at=ancient, portal="caba", now=NOW)


# ── resolución de política ─────────────────────────────────────


def test_a_portal_cadence_switches_that_portal_on_and_nothing_else():
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)

    assert refresh_age_for("caba") == timedelta(days=7)
    assert refresh_age_for("otro_portal") is None
    assert is_enabled()


def test_a_resource_override_beats_its_portal():
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)
    freshness._RESOURCE_CADENCE["caba::x"] = timedelta(days=90)

    assert refresh_age_for("caba", "caba::x") == timedelta(days=90)


def test_a_resource_can_be_exempted_from_a_refreshed_portal():
    """A closed historical series inside a live catalogue is common enough to be
    worth expressing."""
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)
    freshness._RESOURCE_CADENCE["caba::cerrada"] = None

    assert refresh_age_for("caba", "caba::cerrada") is None
    assert not is_stale(
        last_collected_at=NOW - timedelta(days=400),
        portal="caba",
        resource_identity="caba::cerrada",
        now=NOW,
    )


def test_the_environment_default_applies_where_nothing_else_does(monkeypatch):
    monkeypatch.setenv("OPENARG_REFRESH_DEFAULT_DAYS", "30")

    assert refresh_age_for("cualquiera") == timedelta(days=30)
    assert is_enabled()


def test_an_unparseable_default_leaves_refresh_off(monkeypatch):
    """Failing open here would turn a typo into a catalogue-wide re-read."""
    monkeypatch.setenv("OPENARG_REFRESH_DEFAULT_DAYS", "quincenal")

    assert refresh_age_for("caba") is None


# ── guardas ────────────────────────────────────────────────────


def test_a_cadence_below_the_floor_is_clamped():
    """A typo here costs a download storm, not a wrong number."""
    freshness._PORTAL_CADENCE["caba"] = timedelta(minutes=5)

    assert refresh_age_for("caba") == MIN_CADENCE


def test_an_unknown_collection_time_is_not_stale():
    """Unknown is not old. Treating it as stale would make the first sweep
    eligible to re-read the entire catalogue at once."""
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)

    assert not is_stale(last_collected_at=None, portal="caba", now=NOW)


def test_staleness_is_measured_against_the_policy():
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)

    assert is_stale(last_collected_at=NOW - timedelta(days=8), portal="caba", now=NOW)
    assert not is_stale(last_collected_at=NOW - timedelta(days=6), portal="caba", now=NOW)


def test_a_naive_timestamp_is_read_as_utc_rather_than_crashing():
    """The column is timestamptz, but a fixture or a driver can hand back a
    naive value, and a comparison error here would take out the sweep."""
    freshness._PORTAL_CADENCE["caba"] = timedelta(days=7)
    naive = (NOW - timedelta(days=8)).replace(tzinfo=None)

    assert is_stale(last_collected_at=naive, portal="caba", now=NOW)
