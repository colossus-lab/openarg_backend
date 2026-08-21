"""Snapshot tables that are still alive, so the first drop already makes a pair.

Migration 0056 records a table's shape as it is being destroyed. That is the
moment the evidence would otherwise be lost, so it is the right hook — but it
means a resource becomes comparable only after it has been dropped *twice*.
Production has recorded no drop since 2026-05-20. Waiting for two would put
the first measurable diff months out.

Taking a baseline of what exists now removes one of those two waits. The
first drop of any baselined table lands beside a stored "before" and is
immediately comparable. Nothing is destroyed to produce these rows; the
capture is the same `pg_stats` read the drop path already does, so it costs an
index scan per table and no table scan at all.

The reason is recorded as `baseline` rather than a drop reason, so a consumer
can always tell "this is what the table looked like while it was alive" from
"this is what it looked like the moment before it died". `diff_snapshots`
does not care which is which — but an operator reading the row does.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# Baseline the live registered tables first: those are the ones a future
# re-ingest will actually replace, which makes them the ones most likely to
# produce a pair. Tables already carrying a baseline are skipped, so repeated
# runs walk forward through the backlog instead of re-snapshotting the head.
_CANDIDATES_SQL = text(
    """
    SELECT rtv.schema_name, rtv.table_name, rtv.resource_identity, rtv.version
    FROM raw_table_versions rtv
    WHERE rtv.superseded_at IS NULL
      AND EXISTS (
          SELECT 1 FROM information_schema.tables t
          WHERE t.table_schema = rtv.schema_name AND t.table_name = rtv.table_name
      )
      AND NOT EXISTS (
          SELECT 1 FROM raw.raw_schema_snapshots s
          WHERE s.schema_name = rtv.schema_name AND s.table_name = rtv.table_name
      )
    ORDER BY rtv.created_at DESC
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
                "alive": True,
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
                SELECT count(*) FROM raw_table_versions rtv
                WHERE rtv.superseded_at IS NULL
                  AND EXISTS (SELECT 1 FROM information_schema.tables t
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
