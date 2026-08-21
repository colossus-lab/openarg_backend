"""How old a resource's data may be before it should be read again.

A single global TTL is the obvious design and it is wrong in both directions at
once. Too short and the platform re-downloads a decennial census every week for
nothing; too long and BCRA indicators go stale in a system built to report them.
The cadence has to come from what the resource *is*.

So this module holds a policy, not a number, and it ships **disabled**. Every
cadence is `None` until someone who knows these sources says otherwise — see
CL-026-001. Shipping a default would be inventing an answer, and the difference
between a mechanism that takes a policy and one that hardcodes a guess is the
whole point of separating them.

Disabled is a real state, not a placeholder: `None` also means "this resource
genuinely does not change", which is the correct answer for a closed historical
series and one that should survive whatever defaults get chosen later.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)

# Per-portal cadences. Empty on purpose (CL-026-001): the numbers are a judgement
# about the data, not about the code, and this file is the wrong place to decide
# them. Populating one entry switches refresh on for that portal and nothing
# else, which is also how Phase E wants to start.
_PORTAL_CADENCE: dict[str, timedelta] = {}

# Overrides for a single resource, by `resource_identity`. Wins over the portal
# entry, including to *disable* refresh for one resource in an otherwise
# refreshed portal — a closed series in a live catalogue is common enough to be
# worth expressing.
_RESOURCE_CADENCE: dict[str, timedelta | None] = {}

# A floor, so that no policy — configured, overridden, or mistyped — can ask for
# a resource to be re-read more often than this. It is a guard against a typo
# costing a download storm, not a cadence.
MIN_CADENCE = timedelta(hours=6)

_ENV_DEFAULT = "OPENARG_REFRESH_DEFAULT_DAYS"


def _default_cadence() -> timedelta | None:
    """The fallback cadence, off unless an operator sets one.

    Read from the environment rather than a constant so that turning refresh on
    globally is a deploy-free decision, and so that the value that ends up in
    production is visible in configuration instead of buried in a module.
    """
    raw = os.getenv(_ENV_DEFAULT)
    if not raw:
        return None
    try:
        days = float(raw)
    except ValueError:
        logger.warning("%s is not a number: %r — refresh stays off", _ENV_DEFAULT, raw)
        return None
    if days <= 0:
        return None
    return timedelta(days=days)


def refresh_age_for(portal: str | None, resource_identity: str | None = None) -> timedelta | None:
    """How old this resource's data may get. `None` means never refresh it.

    Resolution order is most-specific-first: the resource override, then the
    portal, then the environment default. An explicit `None` at any level stops
    the search, so a resource can be exempted from a portal that is otherwise
    refreshed.
    """
    if resource_identity is not None and resource_identity in _RESOURCE_CADENCE:
        cadence = _RESOURCE_CADENCE[resource_identity]
        return _clamp(cadence)

    if portal and portal in _PORTAL_CADENCE:
        return _clamp(_PORTAL_CADENCE[portal])

    return _clamp(_default_cadence())


def _clamp(cadence: timedelta | None) -> timedelta | None:
    if cadence is None:
        return None
    if cadence < MIN_CADENCE:
        # Loud, because a cadence below the floor is almost always a typo, and a
        # typo here costs a download storm rather than a wrong number.
        logger.warning(
            "refresh cadence %s is below the %s floor; using the floor", cadence, MIN_CADENCE
        )
        return MIN_CADENCE
    return cadence


def is_stale(
    *,
    last_collected_at: datetime | None,
    portal: str | None,
    resource_identity: str | None = None,
    now: datetime | None = None,
) -> bool:
    """Is this resource past its policy age?

    A resource with no recorded collection time is **not** stale. It is unknown,
    and treating unknown as stale would make the first run of this sweep eligible
    to re-read the entire catalogue at once — which is the load pattern that
    restarted the database in May.
    """
    cadence = refresh_age_for(portal, resource_identity)
    if cadence is None:
        return False
    if last_collected_at is None:
        return False

    reference = now or datetime.now(UTC)
    if last_collected_at.tzinfo is None:
        last_collected_at = last_collected_at.replace(tzinfo=UTC)
    return (reference - last_collected_at) > cadence


def is_enabled() -> bool:
    """Is refresh switched on for anything at all?

    Lets a caller skip the eligibility query entirely rather than run it and find
    nothing, which matters while the answer is "no" for every resource.
    """
    return bool(_PORTAL_CADENCE) or bool(_RESOURCE_CADENCE) or _default_cadence() is not None
