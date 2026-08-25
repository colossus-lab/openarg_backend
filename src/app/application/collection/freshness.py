"""When a resource should be read again.

The obvious design is a time-to-live, and measuring the catalogue ruled it out.
`datasets.last_updated_at` — the modification date the portal itself declares —
is populated for 32,565 of 32,566 rows, and it says most of this catalogue is
static:

| Last declared modification | Datasets |
|---|---|
| under a week | 493 |
| under a month | 1,889 |
| 1–3 months | 2,420 |
| 3–12 months | 2,866 |
| **over a year** | **24,897** |

Medians per portal run from 89 days (`neuquen_legislatura`) to 3,021
(`cordoba_estadistica` — eight years). Any TTL short enough to keep `energia`
current would re-download Córdoba's static series hundreds of times for nothing.

So the primary signal is not age, it is **the portal saying it changed**:
`last_updated_at > cd.updated_at`. That is exact rather than guessed, free —
the scraper already fetches it daily — and it names a finite queue: 3,431 of
29,012 ready resources, against 25,580 that have not moved since we read them.

Age survives only as a **backstop**, because portals lie about this field: some
never update it, some touch it without changing the file. Ninety days is long
enough to cost little and short enough that a silent change is not invisible
forever. It is the one number here that is chosen rather than measured, and it
is deliberately the one that matters least.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)

# Which portals participate. Empty means refresh is off everywhere — Phase E of
# 026 wants to start with one portal and watch it, not with the catalogue.
#
# `energia` is the natural first choice: 251 resources with a declared change
# pending, a 243-day median so it genuinely moves, and small enough that a week
# of watching says something.
_ENABLED_PORTALS: set[str] = set()

# Resources exempted from refresh even inside an enabled portal. A closed
# historical series in a live catalogue is common enough to be worth expressing.
_NEVER_REFRESH: set[str] = set()

# The backstop, for when the portal's metadata is wrong or absent. Not the
# mechanism — see the module docstring.
BACKSTOP_MAX_AGE = timedelta(days=90)

# No policy may ask for a resource to be re-read more often than this. A guard
# against a typo costing a download storm, not a cadence.
MIN_BACKSTOP = timedelta(hours=6)

_ENV_PORTALS = "OPENARG_REFRESH_PORTALS"
_ENV_BACKSTOP_DAYS = "OPENARG_REFRESH_BACKSTOP_DAYS"


def enabled_portals() -> set[str]:
    """Portals participating in refresh, from config or the environment.

    Read at call time rather than import time so that turning a portal on is a
    restart rather than a deploy — which matters while the answer is being
    tuned.
    """
    from_env = os.getenv(_ENV_PORTALS, "")
    env_portals = {p.strip() for p in from_env.split(",") if p.strip()}
    return _ENABLED_PORTALS | env_portals


def backstop_age() -> timedelta:
    """How long we may go without re-reading a resource the portal calls static."""
    raw = os.getenv(_ENV_BACKSTOP_DAYS)
    if not raw:
        return BACKSTOP_MAX_AGE
    try:
        days = float(raw)
    except ValueError:
        logger.warning(
            "%s is not a number: %r — using the %s default",
            _ENV_BACKSTOP_DAYS,
            raw,
            BACKSTOP_MAX_AGE,
        )
        return BACKSTOP_MAX_AGE
    candidate = timedelta(days=days)
    if candidate < MIN_BACKSTOP:
        logger.warning(
            "refresh backstop %s is below the %s floor; using the floor", candidate, MIN_BACKSTOP
        )
        return MIN_BACKSTOP
    return candidate


def is_enabled() -> bool:
    """Is refresh on for anything at all?

    Lets a caller skip the eligibility query rather than run it and find nothing,
    which matters while the answer is "no" for every portal.
    """
    return bool(enabled_portals())


def should_refresh(
    *,
    portal: str | None,
    resource_identity: str | None = None,
    portal_last_updated_at: datetime | None,
    last_collected_at: datetime | None,
    now: datetime | None = None,
) -> tuple[bool, str]:
    """Should this resource be read again, and on what grounds?

    Returns the decision and the reason for it, because "the portal says it
    changed" and "we have not looked in three months" are different claims that
    deserve to be counted separately — one is evidence and the other is a
    precaution.
    """
    if not portal or portal not in enabled_portals():
        return False, "portal_not_enabled"
    if resource_identity and resource_identity in _NEVER_REFRESH:
        return False, "resource_exempt"
    if last_collected_at is None:
        # Unknown is not old. Treating it as stale would make the first sweep
        # eligible to re-read the whole catalogue at once, which is the load
        # pattern that restarted the database in May.
        return False, "never_collected"

    reference = now or datetime.now(UTC)
    last_collected_at = _as_utc(last_collected_at)

    if portal_last_updated_at is not None:
        if _as_utc(portal_last_updated_at) > last_collected_at:
            return True, "portal_declares_change"

    if (reference - last_collected_at) > backstop_age():
        return True, "backstop_age"

    return False, "current"


def _as_utc(value: datetime) -> datetime:
    """The columns are timestamptz, but a driver or a fixture can hand back a
    naive value, and a comparison error here would take out the whole sweep."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
