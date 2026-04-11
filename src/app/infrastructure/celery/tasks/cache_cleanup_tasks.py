"""FIX-007 — Semantic cache cleanup.

Periodic Celery task that deletes expired entries from ``query_cache``.
Without this, expired entries accumulate indefinitely, degrading the
HNSW index and wasting storage.

Schedule: every 6 hours on the ``ingest`` queue (low priority, non-
blocking for user-path queries). Configured in ``celery/app.py`` beat
schedule.

The 1-hour grace window (`expires_at < now() - INTERVAL '1 hour'`)
avoids a race condition where an entry that just expired could still
be read by a concurrent get() on another worker.
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


@celery_app.task(
    name="openarg.cleanup_semantic_cache",
    bind=True,
    max_retries=2,
    soft_time_limit=120,
    time_limit=180,
)
def cleanup_semantic_cache(self):
    """Delete expired entries from query_cache table.

    Returns a dict with the count of rows deleted for observability.
    """
    engine = get_sync_engine()
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    "DELETE FROM query_cache "
                    "WHERE expires_at < now() - INTERVAL '1 hour'"
                )
            )
            deleted = result.rowcount or 0

        if deleted > 0:
            logger.info("semantic cache cleanup: %d expired entries deleted", deleted)
        else:
            logger.debug("semantic cache cleanup: nothing to delete")

        return {"deleted": deleted}

    except Exception as exc:
        logger.exception("semantic cache cleanup failed")
        raise self.retry(exc=exc, countdown=300) from exc
    finally:
        engine.dispose()
