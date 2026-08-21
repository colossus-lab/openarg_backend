"""Snapshot every table still on disk, so drift becomes measurable without a drop.

Migration 0056 records a table's shape as it is being destroyed. That is the
moment the evidence would otherwise be lost, so it is the right hook — but it
means a resource becomes comparable only after it has been dropped *twice*.
Production has recorded no drop since 2026-05-20. Waiting for two would put
the first measurable diff months out.

This pass removes that wait, twice over:

1. Snapshotting a **live** table means the *first* drop already lands beside a
   stored "before".
2. Snapshotting **superseded** versions that are still on disk is better still.
   Where a resource holds two physical versions, those two tables already *are*
   a format change that happened — comparable now, with nothing left to wait
   for. Staging holds 110 such resources (112 pairs); production holds none,
   because `retain_raw_versions` removed 19,906 superseded tables in May.

Nothing is destroyed to produce these rows. The capture is the same `pg_stats`
read the drop path performs: an index scan per table, no table scan.

`reason='baseline'` and `extra.alive` distinguish these from pre-drop captures.
`diff_snapshots` treats them identically — the distinction is for the operator
reading the row.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# Every physically-present version, not only the live one.
#
# Restricting this to `superseded_at IS NULL` was leaving the most valuable
# evidence on the floor. When a resource has two versions still on disk, those
# two tables ARE a format change that already happened — a comparison available
# today rather than after the next drop. Measured on staging 2026-08-21: 110
# resources hold 2+ physical versions, giving 112 pairs that no amount of
# waiting would have produced sooner. (Production holds none: `retain_raw_
# versions` removed 19,906 superseded tables in May, which is why staging is
# the environment that can calibrate this.)
#
# Newest first, so a partial run covers the most recently active resources.
# Tables already carrying a snapshot are skipped, so repeated runs walk forward
# through the backlog instead of re-snapshotting the head.
_CANDIDATES_SQL = text(
    """
    SELECT rtv.schema_name, rtv.table_name, rtv.resource_identity, rtv.version,
           (rtv.superseded_at IS NULL) AS is_live
    FROM public.raw_table_versions rtv
    WHERE EXISTS (
          SELECT 1 FROM information_schema.tables t
          WHERE t.table_schema = rtv.schema_name AND t.table_name = rtv.table_name
      )
      AND NOT EXISTS (
          SELECT 1 FROM raw.raw_schema_snapshots s
          WHERE s.schema_name = rtv.schema_name AND s.table_name = rtv.table_name
      )
    ORDER BY
        -- Resources that already hold more than one physical version come
        -- first: each one completes a pair the moment its siblings are
        -- captured, so they turn into measurable drift immediately.
        (SELECT count(*) FROM public.raw_table_versions sib
         WHERE sib.resource_identity = rtv.resource_identity) DESC,
        rtv.created_at DESC
    LIMIT :limit
    """
)


@celery_app.task(
    name="openarg.baseline_schema_snapshots",
    bind=True,
    soft_time_limit=1500,
    time_limit=1800,
)
def baseline_schema_snapshots(self, *, limit: int = 2000) -> dict[str, Any]:
    """Capture a baseline snapshot for up to `limit` live tables that lack one.

    Reads only. Nothing is dropped, altered or blocked by this task; a table
    that fails to capture is counted and skipped, exactly as on the drop path.
    """
    from app.application.catalog.schema_snapshot import capture_table_snapshot

    engine = get_sync_engine()
    with engine.connect() as conn:
        rows = conn.execute(_CANDIDATES_SQL, {"limit": limit}).fetchall()
        conn.rollback()

    captured = 0
    skipped = 0
    for row in rows:
        snapshot_id = capture_table_snapshot(
            engine,
            table_name=row.table_name,
            schema_name=row.schema_name,
            reason="baseline",
            actor="schema_baseline_tasks.baseline_schema_snapshots",
            extra={
                "resource_identity": row.resource_identity,
                "version": row.version,
                "alive": bool(row.is_live),
            },
        )
        if snapshot_id:
            captured += 1
        else:
            # A table can vanish between the SELECT and the capture, and one
            # that was never analysed yields no profile. Neither is worth
            # failing the run over — the next run picks the table up again.
            skipped += 1

    with engine.connect() as conn:
        remaining = conn.execute(
            text(
                """
                SELECT count(*) FROM public.raw_table_versions rtv
                WHERE EXISTS (SELECT 1 FROM information_schema.tables t
                              WHERE t.table_schema = rtv.schema_name
                                AND t.table_name = rtv.table_name)
                  AND NOT EXISTS (SELECT 1 FROM raw.raw_schema_snapshots s
                                  WHERE s.schema_name = rtv.schema_name
                                    AND s.table_name = rtv.table_name)
                """
            )
        ).scalar()
        conn.rollback()

    result = {
        "candidates": len(rows),
        "captured": captured,
        "skipped": skipped,
        "remaining_without_baseline": int(remaining or 0),
    }
    logger.info("baseline_schema_snapshots: %s", result)
    return result
