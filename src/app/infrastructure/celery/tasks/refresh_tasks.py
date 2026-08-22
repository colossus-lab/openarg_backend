"""Re-read the sources the portals say have changed.

`bulk_collect_all` selects on `is_cached = false AND NOT EXISTS (a ready row)`,
so reaching `ready` is terminal and a resource is read from source exactly once.
Measured 2026-08-21: 24,097 ready rows untouched for over three months, and
26,651 of 27,431 resources holding exactly one version.

This sweep is the second reading, and it is driven by evidence rather than by a
clock. `datasets.last_updated_at` — the modification date the portal declares,
populated for 32,565 of 32,566 rows — separates the 3,431 resources that have
genuinely moved from the 25,580 that have not. A time-to-live would have
re-downloaded the second group for years: three quarters of this catalogue has
not been touched by its portal in over a year, and one portal's median is eight.

**A separate query and a separate budget** from first collection (026 FR-002).
Sharing the path would let one backlog starve the other, with the winner decided
by an ordering nobody chose.

It dispatches `collect_dataset`, which declines a resource that is mid-download
but not one that is merely `ready`. So nothing has to be flipped to `pending`
first, and no resource is left in a state a failure could strand it in.

**What makes it safe to re-read something that already works.** The collector
downloads and parses before touching the existing table, so a source that has
started 404ing fails while the current version is still serving. The residual
risk is the schema-mismatch path, which drops and rewrites from a DataFrame
already in memory: if that rewrite then fails for an unrelated reason the data is
gone and only the shape survives in `raw_schema_snapshots`. Recorded rather than
papered over.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from app.application.collection.freshness import backstop_age, enabled_portals, is_enabled
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# Eligibility in SQL rather than in Python, because both predicates are
# expressible and fetching 2,000 rows to discard most of them was an artefact of
# the time-based design that measurement replaced.
#
# The order matters: a resource the portal says changed is evidence, and one that
# merely got old is a precaution. Evidence goes first, so a bounded run spends
# its budget on the 3,431 known-stale before the 25,580 that have not moved.
_CANDIDATES_SQL = text(
    """
    SELECT d.id AS dataset_id,
           d.portal,
           d.source_id,
           cd.updated_at        AS last_collected_at,
           d.last_updated_at    AS portal_last_updated_at,
           (d.last_updated_at > cd.updated_at) AS portal_declares_change
    FROM raw.cached_datasets cd
    JOIN datasets d ON d.id = cd.dataset_id
    WHERE cd.status = 'ready'
      AND cd.updated_at IS NOT NULL
      AND d.portal = ANY(:portals)
      AND (
          d.last_updated_at > cd.updated_at
          OR cd.updated_at < NOW() - make_interval(days => :backstop_days)
      )
    ORDER BY (d.last_updated_at > cd.updated_at) DESC NULLS LAST,
             cd.updated_at ASC
    LIMIT :limit
    """
)


@celery_app.task(
    name="openarg.refresh_stale_datasets",
    bind=True,
    soft_time_limit=900,
    time_limit=1080,
)
def refresh_stale_datasets(self, *, limit: int = 50, dry_run: bool = True) -> dict[str, Any]:
    """Dispatch a bounded number of refreshes, evidence first.

    `limit` caps dispatches per run — the constraint that keeps this from
    repeating the load that restarted the database in May, when 152 concurrent
    collects met a 52M-row matview rebuild.

    Defaults to `dry_run=True`. A sweep that re-reads sources is not something to
    switch on by deploying it.
    """
    if not is_enabled():
        # The expected state until a portal is switched on (026 Phase E).
        # Saying so beats reporting zeros that read like "nothing is stale"
        # while 3,431 resources are and nothing is looking.
        logger.info("refresh: no portal is enabled, so nothing is eligible")
        return {"enabled": False, "dispatched": 0, "reason": "no_portal_enabled"}

    from app.infrastructure.celery.tasks.collector_tasks import (
        _mart_rebuild_in_progress,
        collect_dataset,
    )

    engine = get_sync_engine()

    # Same backpressure the first-collection path respects (FR-009). Refresh
    # multiplies steady-state load rather than adding a one-off, so skipping a
    # cycle costs nothing the next one does not recover.
    if _mart_rebuild_in_progress(engine):
        logger.info("refresh: a mart is being rebuilt; deferring to the next cycle")
        return {"enabled": True, "dispatched": 0, "reason": "mart_rebuild_in_progress"}

    portals = sorted(enabled_portals())
    with engine.connect() as conn:
        rows = conn.execute(
            _CANDIDATES_SQL,
            {
                "portals": portals,
                "backstop_days": backstop_age().days,
                "limit": limit,
            },
        ).fetchall()
        conn.rollback()

    declared = sum(1 for r in rows if r.portal_declares_change)

    result: dict[str, Any] = {
        "enabled": True,
        "portals": portals,
        "eligible": len(rows),
        # Counted apart because they are different claims: one is the portal
        # telling us the file moved, the other is us admitting we have not
        # looked. A run that is mostly backstop means the metadata is not
        # carrying its weight, and that is worth seeing.
        "by_reason": {
            "portal_declares_change": declared,
            "backstop_age": len(rows) - declared,
        },
        "limit": limit,
        "dry_run": dry_run,
        "oldest": rows[0].last_collected_at.isoformat() if rows else None,
        "samples": [f"{r.portal}::{r.source_id}" for r in rows[:5]],
    }

    if dry_run:
        result["dispatched"] = 0
        logger.info("refresh dry-run: %s", result)
        return result

    dispatched = 0
    for row in rows:
        try:
            collect_dataset.delay(str(row.dataset_id))
            dispatched += 1
        except Exception:
            # One failed dispatch must not cost the rest of the batch. The
            # resource stays stale and the next cycle picks it up again, which
            # is the whole shape of this sweep.
            logger.warning("refresh: could not dispatch %s", row.dataset_id, exc_info=True)

    result["dispatched"] = dispatched
    logger.info(
        "refresh: dispatched %d of %d eligible (%d declared by the portal)",
        dispatched,
        len(rows),
        declared,
    )
    return result
