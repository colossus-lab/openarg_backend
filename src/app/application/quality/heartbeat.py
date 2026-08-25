"""When did each source last actually arrive, and is it late?

Refusing to write a bad batch leaves the previous good batch in place. That is
the right call — a blank payroll is worse than a stale one — but on its own it
converts a loud failure into a silent one: the mart keeps answering, the numbers
keep looking plausible, and nothing anywhere says the data stopped moving. The
HCDN payroll spent three weeks like that and the only reason it surfaced was an
unrelated row count that did not add up.

The registry cannot answer this. `register_via_b_table` is idempotent on
`(resource_identity, version)` and never touches `created_at`, so for the
connectors that overwrite in place the stored date is the day the resource was
*first* seen — years ago for some. There was no record of a successful ingest
anywhere.

So this keeps one. One row per resource, holding when it last arrived, when it
arrived before that, and a running estimate of its own cadence.

**The cadence is learned, not declared.** A weekly payroll, an hourly exchange
rate and a yearly census cannot share a threshold, and a config file listing the
expected period of every resource would be wrong within a month and nobody would
notice. An exponentially-weighted mean of the observed gaps adapts on its own and
costs one column.

`CREATE TABLE IF NOT EXISTS` rather than a migration, matching `alert_log` and
`connector_field_aliases`: this is operational bookkeeping that must never be the
reason an ingest fails.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

# How many times its own cadence a resource may be late before it is late.
# Three, because a weekly job that runs a day early and then a day late still
# lands well inside it, and a job that skipped two whole cycles does not.
LATE_MULTIPLE = 3.0

# Nothing is called late before this, whatever its cadence says. An hourly job
# that misses three hours is not news; the sweep runs daily anyway.
FLOOR_HOURS = 36.0

# Weight of the newest observed gap in the running mean. Low, so one delayed run
# does not move the estimate much and a genuine schedule change still lands
# within a few cycles.
_ALPHA = 0.3

_ENSURE_SQL = text(
    """
    CREATE TABLE IF NOT EXISTS public.ingest_heartbeat (
        resource_identity TEXT PRIMARY KEY,
        last_ok_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        prev_ok_at        TIMESTAMPTZ,
        cadence_seconds   DOUBLE PRECISION,
        times_seen        INTEGER NOT NULL DEFAULT 1
    )
    """
)

# The running mean lives in SQL so two workers cannot read-modify-write over each
# other. `EXCLUDED.last_ok_at` is now(); the previous value becomes `prev_ok_at`,
# and the gap between them updates the estimate.
_BEAT_SQL = text(
    """
    INSERT INTO public.ingest_heartbeat (resource_identity)
    VALUES (:rid)
    ON CONFLICT (resource_identity) DO UPDATE SET
        prev_ok_at = public.ingest_heartbeat.last_ok_at,
        last_ok_at = now(),
        times_seen = public.ingest_heartbeat.times_seen + 1,
        cadence_seconds = CASE
            WHEN public.ingest_heartbeat.cadence_seconds IS NULL
                THEN EXTRACT(EPOCH FROM (now() - public.ingest_heartbeat.last_ok_at))
            ELSE (1 - :alpha) * public.ingest_heartbeat.cadence_seconds
                 + :alpha * EXTRACT(EPOCH FROM (now() - public.ingest_heartbeat.last_ok_at))
        END
    """
)

# Only resources that have arrived enough times to have a cadence worth
# trusting. Two sightings give one gap, which is an anecdote; four give three.
_STALE_SQL = text(
    """
    SELECT resource_identity,
           last_ok_at,
           cadence_seconds,
           times_seen,
           EXTRACT(EPOCH FROM (now() - last_ok_at)) AS gap_seconds
    FROM public.ingest_heartbeat
    WHERE times_seen >= :min_seen
      AND cadence_seconds IS NOT NULL
      AND cadence_seconds > 0
      AND EXTRACT(EPOCH FROM (now() - last_ok_at))
          > GREATEST(cadence_seconds * :multiple, :floor_seconds)
    ORDER BY EXTRACT(EPOCH FROM (now() - last_ok_at)) / cadence_seconds DESC
    LIMIT :limit
    """
)


@dataclass(frozen=True)
class Late:
    """A source that stopped arriving on its own schedule."""

    resource_identity: str
    days_late: float
    cadence_days: float
    times_seen: int

    def phrase_es(self) -> str:
        return (
            f"llega cada ~{self.cadence_days:.1f} día(s) y hace {self.days_late:.1f} que no llega"
        )


def record_ingest(engine: Any, resource_identity: str) -> None:
    """Mark that this resource arrived successfully, just now. Never raises."""
    if not resource_identity:
        return
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_SQL)
            conn.execute(_BEAT_SQL, {"rid": resource_identity, "alpha": _ALPHA})
    except Exception:
        # Bookkeeping must never be why an ingest fails.
        logger.debug("heartbeat: could not record %s", resource_identity, exc_info=True)


def find_late(
    engine: Any,
    *,
    multiple: float = LATE_MULTIPLE,
    floor_hours: float = FLOOR_HOURS,
    min_seen: int = 4,
    limit: int = 50,
) -> list[Late]:
    """Which sources are late by their own standards. Never raises."""
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_SQL)
            rows = conn.execute(
                _STALE_SQL,
                {
                    "multiple": multiple,
                    "floor_seconds": floor_hours * 3600,
                    "min_seen": min_seen,
                    "limit": limit,
                },
            ).fetchall()
    except Exception:
        logger.warning("heartbeat: could not read", exc_info=True)
        return []

    return [
        Late(
            resource_identity=r.resource_identity,
            days_late=float(r.gap_seconds) / 86400,
            cadence_days=float(r.cadence_seconds) / 86400,
            times_seen=int(r.times_seen),
        )
        for r in rows
    ]
