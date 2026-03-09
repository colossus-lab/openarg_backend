"""
Reporting Tasks — Daily summaries of failed and stuck datasets.

Provides visibility into permanently_failed downloads and other
error states so operators can take action without digging into logs.
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


@celery_app.task(
    name="openarg.report_failed_tasks",
    soft_time_limit=60,
    time_limit=120,
)
def report_failed_tasks():
    """Daily report of datasets in error / permanently_failed states.

    Logs a summary with counts per portal and per error status, plus
    the top 10 most recent failures with their error messages.
    """
    engine = get_sync_engine()
    try:
        # ------------------------------------------------------------------
        # 1. Counts by status
        # ------------------------------------------------------------------
        with engine.connect() as conn:
            status_rows = conn.execute(
                text("""
                    SELECT status, COUNT(*) AS cnt
                    FROM cached_datasets
                    WHERE status IN ('error', 'permanently_failed')
                    GROUP BY status
                    ORDER BY status
                """)
            ).fetchall()

        total_failures = sum(r.cnt for r in status_rows)
        if total_failures == 0:
            logger.info("[DLQ Report] No failed datasets — all clear.")
            return {"total_failures": 0}

        status_summary = {r.status: r.cnt for r in status_rows}
        logger.warning(
            "[DLQ Report] %d datasets in failure state: %s",
            total_failures,
            status_summary,
        )

        # ------------------------------------------------------------------
        # 2. Breakdown by portal
        # ------------------------------------------------------------------
        with engine.connect() as conn:
            portal_rows = conn.execute(
                text("""
                    SELECT d.portal, cd.status, COUNT(*) AS cnt
                    FROM cached_datasets cd
                    JOIN datasets d ON d.id = cd.dataset_id
                    WHERE cd.status IN ('error', 'permanently_failed')
                    GROUP BY d.portal, cd.status
                    ORDER BY cnt DESC
                """)
            ).fetchall()

        for row in portal_rows:
            logger.warning(
                "[DLQ Report]   portal=%-25s status=%-20s count=%d",
                row.portal,
                row.status,
                row.cnt,
            )

        # ------------------------------------------------------------------
        # 3. Top 10 most recent failures (with error messages)
        # ------------------------------------------------------------------
        with engine.connect() as conn:
            recent_rows = conn.execute(
                text("""
                    SELECT cd.table_name,
                           cd.status,
                           cd.retry_count,
                           cd.error_message,
                           d.portal,
                           d.title,
                           cd.updated_at
                    FROM cached_datasets cd
                    JOIN datasets d ON d.id = cd.dataset_id
                    WHERE cd.status IN ('error', 'permanently_failed')
                    ORDER BY cd.updated_at DESC
                    LIMIT 10
                """)
            ).fetchall()

        logger.warning("[DLQ Report] Top 10 most recent failures:")
        for row in recent_rows:
            logger.warning(
                "[DLQ Report]   [%s] %s | %s | retries=%d | %s | %s",
                row.portal,
                row.title[:60] if row.title else "?",
                row.status,
                row.retry_count or 0,
                (row.error_message or "")[:120],
                row.updated_at,
            )

        # ------------------------------------------------------------------
        # 4. Stuck downloads (downloading > 30 min)
        # ------------------------------------------------------------------
        with engine.connect() as conn:
            stuck_count = conn.execute(
                text("""
                    SELECT COUNT(*) FROM cached_datasets
                    WHERE status = 'downloading'
                      AND updated_at < NOW() - INTERVAL '30 minutes'
                """)
            ).scalar() or 0

        if stuck_count:
            logger.warning(
                "[DLQ Report] %d datasets stuck in 'downloading' > 30 min",
                stuck_count,
            )

        return {
            "total_failures": total_failures,
            "status_summary": status_summary,
            "portals_affected": len({r.portal for r in portal_rows}),
            "stuck_downloading": stuck_count,
        }
    finally:
        engine.dispose()
