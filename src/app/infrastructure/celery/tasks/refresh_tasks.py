"""Re-read sources that are past their policy age.

`bulk_collect_all` selects on `is_cached = false AND NOT EXISTS (a ready row)`,
so reaching `ready` is terminal and a resource is read from source exactly once.
Measured on staging 2026-08-21: 24,097 of the ready rows had not been updated in
over three months, and 26,651 of 27,431 resources had exactly one version.

This sweep is the second reading. It is **a separate query and a separate
budget** by design (026 FR-002): if refresh shared the first-collection path, one
backlog would starve the other — 24,000 stale resources crowding out every new
dataset, or the reverse, with the winner decided by an ordering nobody chose.

It dispatches `collect_dataset`, which does not skip a resource that is already
`ready` — it only declines one that is mid-download. So no status has to be
flipped first, and nothing is put into a state a failure could strand it in.

**What makes it safe to re-read a resource that already works.** The collector
downloads and parses before it touches the existing table, so a source that has
started 404ing fails while the current version is still serving. The narrow
residual risk is the schema-mismatch path, which drops and re-writes from a
DataFrame already in memory: if the rewrite then fails for an unrelated reason,
the data is gone and only the shape survives in `raw_schema_snapshots`. Recorded
rather than papered over.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from app.application.collection.freshness import is_enabled, is_stale
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# Oldest first, so the backlog drains from the end that has been wrong longest.
# `cd.updated_at` is when we last wrote the row, which is the closest thing the
# schema has to "when we last read the source" — see the note in the task.
_CANDIDATES_SQL = text(
    """
    SELECT d.id AS dataset_id,
           d.portal,
           d.source_id,
           cd.updated_at AS last_collected_at,
           cd.table_name
    FROM raw.cached_datasets cd
    JOIN datasets d ON d.id = cd.dataset_id
    WHERE cd.status = 'ready'
      AND cd.updated_at IS NOT NULL
    ORDER BY cd.updated_at ASC
    LIMIT :scan
    """
)


@celery_app.task(
    name="openarg.refresh_stale_datasets",
    bind=True,
    soft_time_limit=900,
    time_limit=1080,
)
def refresh_stale_datasets(
    self, *, limit: int = 50, scan: int = 2000, dry_run: bool = True
) -> dict[str, Any]:
    """Dispatch a bounded number of refreshes for resources past their policy age.

    `limit` caps dispatches per run — the constraint that keeps this from
    repeating the load that restarted the database in May, when 152 concurrent
    collects met a 52M-row matview rebuild. `scan` bounds the read, since the
    policy is per-portal and cannot be expressed in the query.

    Defaults to `dry_run=True`. A sweep that re-reads sources is not something to
    switch on by deploying it.
    """
    if not is_enabled():
        # The expected state until someone decides a cadence (CL-026-001).
        # Saying so beats reporting zeros that read like "nothing is stale".
        logger.info("refresh: no cadence is configured, so nothing is eligible")
        return {"enabled": False, "dispatched": 0, "reason": "no_cadence_configured"}

    from app.infrastructure.celery.tasks.collector_tasks import (
        _mart_rebuild_in_progress,
        collect_dataset,
    )

    engine = get_sync_engine()

    # Same backpressure the first-collection path respects (FR-009). Refresh
    # multiplies steady-state load rather than adding a one-off, so skipping a
    # cycle costs nothing that the next one does not recover.
    if _mart_rebuild_in_progress(engine):
        logger.info("refresh: a mart is being rebuilt; deferring to the next cycle")
        return {"enabled": True, "dispatched": 0, "reason": "mart_rebuild_in_progress"}

    with engine.connect() as conn:
        rows = conn.execute(_CANDIDATES_SQL, {"scan": scan}).fetchall()
        conn.rollback()

    stale = []
    for row in rows:
        # `resource_identity` is not on this row, and reconstructing it here
        # would duplicate the collector's own naming rules. Portal-level policy
        # covers the common case; a per-resource override needs the identity and
        # is left until the policy is populated at all.
        if is_stale(last_collected_at=row.last_collected_at, portal=row.portal):
            stale.append(row)
        if len(stale) >= limit:
            break

    result: dict[str, Any] = {
        "enabled": True,
        "scanned": len(rows),
        "stale_found": len(stale),
        "limit": limit,
        "dry_run": dry_run,
        "oldest": rows[0].last_collected_at.isoformat() if rows else None,
        "samples": [f"{r.portal}::{r.source_id}" for r in stale[:5]],
    }

    if dry_run:
        result["dispatched"] = 0
        logger.info("refresh dry-run: %s", result)
        return result

    dispatched = 0
    for row in stale:
        try:
            collect_dataset.delay(str(row.dataset_id))
            dispatched += 1
        except Exception:
            # One failed dispatch must not cost the rest of the batch. The
            # resource stays stale and the next cycle picks it up again, which
            # is the whole shape of this sweep.
            logger.warning("refresh: could not dispatch %s", row.dataset_id, exc_info=True)

    result["dispatched"] = dispatched
    logger.info("refresh: dispatched %d of %d stale resource(s)", dispatched, len(stale))
    return result
