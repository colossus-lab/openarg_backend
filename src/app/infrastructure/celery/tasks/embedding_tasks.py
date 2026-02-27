"""
Embedding Worker — Re-indexación masiva.

Permite re-generar embeddings de todos los datasets o de un portal específico.
"""
from __future__ import annotations

import logging
import os

from sqlalchemy import create_engine, text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

logger = logging.getLogger(__name__)


def _get_sync_engine():
    url = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db")
    return create_engine(url)


@celery_app.task(name="openarg.reindex_all_embeddings", bind=True)
def reindex_all_embeddings(self, portal: str | None = None):
    """Re-genera embeddings para todos los datasets (o filtrado por portal)."""
    engine = _get_sync_engine()

    try:
        with engine.begin() as conn:
            if portal:
                rows = conn.execute(
                    text("SELECT CAST(id AS text) FROM datasets WHERE portal = :portal"),
                    {"portal": portal},
                ).fetchall()
            else:
                rows = conn.execute(text("SELECT CAST(id AS text) FROM datasets")).fetchall()

        dataset_ids = [row[0] for row in rows]
        logger.info(f"Reindexing {len(dataset_ids)} datasets")

        for did in dataset_ids:
            index_dataset_embedding.delay(did)

        return {"dispatched": len(dataset_ids), "portal": portal or "all"}

    finally:
        engine.dispose()
