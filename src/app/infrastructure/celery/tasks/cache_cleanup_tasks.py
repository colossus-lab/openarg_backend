"""Cleanup tasks for caches and catalogs.

Two periodic tasks live here:

1. ``cleanup_semantic_cache`` (FIX-007) — deletes expired entries
   from ``query_cache`` so the HNSW index does not accumulate
   dead rows. Runs every 6 hours. 1-hour grace window avoids a
   race with concurrent reads.

2. ``cleanup_orphan_catalog_entries`` (011-table-catalog FR-007,
   added 2026-04-11) — deletes ``table_catalog`` rows whose
   ``table_name`` no longer exists in ``information_schema.tables``.
   Closes the orphan accumulation that used to leak stale table
   names into the NL2SQL vector search when a ``cache_*`` table
   was renamed or dropped. Runs once a day.

Both tasks target the ``ingest`` queue (low priority, non-blocking
for user-path queries). Beat schedule is configured in
``celery/app.py``.
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
                text("DELETE FROM query_cache WHERE expires_at < now() - INTERVAL '1 hour'")
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


@celery_app.task(
    name="openarg.cleanup_orphan_catalog_entries",
    bind=True,
    max_retries=2,
    soft_time_limit=60,
    time_limit=120,
)
def cleanup_orphan_catalog_entries(self):
    """Delete ``table_catalog`` rows whose target table no longer exists.

    Implements FR-007 of ``specs/011-table-catalog/`` and closes
    DEBT-001 of the same spec. A single atomic DELETE removes every
    row whose ``table_name`` is not in ``information_schema.tables``
    for the ``public`` schema. Idempotent — re-running on a clean
    catalog is a no-op that returns ``{"deleted": 0}``.

    Runs on the ``ingest`` queue once a day. See ``celery/app.py``
    beat schedule.
    """
    engine = get_sync_engine()
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    "DELETE FROM table_catalog "
                    "WHERE table_name NOT IN ("
                    "    SELECT table_name FROM information_schema.tables "
                    "    WHERE table_schema = 'public'"
                    ")"
                )
            )
            deleted = result.rowcount or 0

        if deleted > 0:
            logger.info("catalog orphan cleanup: %d orphan entries deleted", deleted)
        else:
            logger.debug("catalog orphan cleanup: nothing to delete")

        return {"deleted": deleted}

    except Exception as exc:
        logger.exception("catalog orphan cleanup failed")
        raise self.retry(exc=exc, countdown=300) from exc
    finally:
        engine.dispose()
