"""Periodic refresh of curated sources (weekly default).

Reads `config/curated_sources.json`, upserts each entry into `datasets` and
triggers a collect for newly-added or refresh-due ones.
"""

from __future__ import annotations

import logging

from app.infrastructure.adapters.connectors.curated_loader import (
    load_curated_sources,
    upsert_curated_sources,
)
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


@celery_app.task(
    name="openarg.refresh_curated_sources",
    bind=True,
    soft_time_limit=120,
    time_limit=180,
)
def refresh_curated_sources(self) -> dict:
    sources = load_curated_sources()
    if not sources:
        return {"upserted": 0, "skipped": 0, "total": 0}
    engine = get_sync_engine()
    summary = upsert_curated_sources(engine, sources)
    logger.info("curated_sources refresh: %s", summary)
    return summary
