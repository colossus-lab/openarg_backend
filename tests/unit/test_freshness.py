"""Tests for the refresh policy.

The design point under test is that evidence beats a clock: the portal saying a
file changed is the primary signal, and age is only a backstop for when that
metadata is wrong or absent.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.application.collection import freshness
from app.application.collection.freshness import (
    BACKSTOP_MAX_AGE,
    MIN_BACKSTOP,
    backstop_age,
    is_enabled,
    should_refresh,
)

NOW = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(freshness, "_ENABLED_PORTALS", set())
    monkeypatch.setattr(freshness, "_NEVER_REFRESH", set())
    monkeypatch.delenv("OPENARG_REFRESH_PORTALS", raising=False)
    monkeypatch.delenv("OPENARG_REFRESH_BACKSTOP_DAYS", raising=False)


def _ask(**kw):
    # Default case is an eligible one — the portal declares a change we have
    # not read — so a test that only flips the portal is asserting enablement
    # rather than accidentally asserting the eligibility rules.
    base = {
        "portal": "energia",
        "portal_last_updated_at": NOW - timedelta(days=1),
        "last_collected_at": NOW - timedelta(days=10),
        "now": NOW,
    }
    base.update(kw)
    return should_refresh(**base)


# ── viene apagado ──────────────────────────────────────────────


def test_refresh_is_off_until_a_portal_is_switched_on():
    assert not is_enabled()
    assert _ask() == (False, "portal_not_enabled")


def test_switching_one_portal_on_leaves_the_rest_alone():
    """026 Phase E starts with one portal and a week of watching, not with the
    catalogue."""
    freshness._ENABLED_PORTALS.add("energia")

    assert _ask(portal="energia")[0]
    assert _ask(portal="caba") == (False, "portal_not_enabled")


def test_portals_can_be_enabled_from_the_environment(monkeypatch):
    monkeypatch.setenv("OPENARG_REFRESH_PORTALS", "energia, caba")

    assert is_enabled()
    assert _ask(portal="caba")[0]


# ── la evidencia gana al reloj ─────────────────────────────────


def test_the_portal_declaring_a_change_is_the_reason():
    """3,431 of 29,012 ready resources. This is the queue worth draining."""
    freshness._ENABLED_PORTALS.add("energia")

    assert _ask(
        portal_last_updated_at=NOW - timedelta(days=1),
        last_collected_at=NOW - timedelta(days=10),
    ) == (True, "portal_declares_change")


def test_a_resource_the_portal_has_not_touched_is_left_alone():
    """25,580 of them. A TTL would have re-downloaded these for years — three
    quarters of the catalogue has not moved in over a year."""
    freshness._ENABLED_PORTALS.add("energia")

    assert _ask(
        portal_last_updated_at=NOW - timedelta(days=900),
        last_collected_at=NOW - timedelta(days=10),
    ) == (False, "current")


def test_age_is_a_backstop_for_metadata_that_lies():
    """Portals do not always update the field, so a long silence still earns one
    read — but as a precaution, and labelled as one."""
    freshness._ENABLED_PORTALS.add("energia")

    assert _ask(
        portal_last_updated_at=NOW - timedelta(days=900),
        last_collected_at=NOW - timedelta(days=200),
    ) == (True, "backstop_age")


def test_a_missing_declared_date_falls_through_to_the_backstop():
    freshness._ENABLED_PORTALS.add("energia")

    assert _ask(portal_last_updated_at=None, last_collected_at=NOW - timedelta(days=200)) == (
        True,
        "backstop_age",
    )
    assert _ask(portal_last_updated_at=None, last_collected_at=NOW - timedelta(days=10)) == (
        False,
        "current",
    )


# ── guardas ────────────────────────────────────────────────────


def test_a_resource_can_be_exempted_from_an_enabled_portal():
    """A closed historical series inside a live catalogue."""
    freshness._ENABLED_PORTALS.add("energia")
    freshness._NEVER_REFRESH.add("energia::cerrada")

    assert _ask(
        resource_identity="energia::cerrada",
        portal_last_updated_at=NOW - timedelta(days=1),
    ) == (False, "resource_exempt")


def test_never_collected_is_not_stale():
    """Unknown is not old. Treating it as stale would make the first sweep
    eligible to re-read the whole catalogue at once."""
    freshness._ENABLED_PORTALS.add("energia")

    assert _ask(last_collected_at=None) == (False, "never_collected")


def test_the_backstop_is_configurable_and_floored(monkeypatch):
    assert backstop_age() == BACKSTOP_MAX_AGE

    monkeypatch.setenv("OPENARG_REFRESH_BACKSTOP_DAYS", "30")
    assert backstop_age() == timedelta(days=30)

    monkeypatch.setenv("OPENARG_REFRESH_BACKSTOP_DAYS", "0.01")
    assert backstop_age() == MIN_BACKSTOP

    monkeypatch.setenv("OPENARG_REFRESH_BACKSTOP_DAYS", "trimestral")
    assert backstop_age() == BACKSTOP_MAX_AGE


def test_naive_timestamps_do_not_take_out_the_sweep():
    """The columns are timestamptz, but a driver or fixture can return naive
    values and a comparison error here would kill the whole run."""
    freshness._ENABLED_PORTALS.add("energia")

    assert _ask(
        portal_last_updated_at=(NOW - timedelta(days=1)).replace(tzinfo=None),
        last_collected_at=(NOW - timedelta(days=10)).replace(tzinfo=None),
    ) == (True, "portal_declares_change")
