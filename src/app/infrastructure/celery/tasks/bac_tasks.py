"""
Buenos Aires Compras (BAC) — ETL de datos de compras públicas OCDS.

Descarga CSVs de Buenos Aires Compras (estándar OCDS),
los parsea en chunks y cachea en PostgreSQL.
"""
from __future__ import annotations

import io
import json
import logging
from datetime import UTC, datetime

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

BASE_URL = "https://cdn.buenosaires.gob.ar/datosabiertos/datasets/ministerio-de-economia-y-finanzas/buenos-aires-compras"

BAC_FILES = {
    "tenders": f"{BASE_URL}/tender.csv",
    "parties": f"{BASE_URL}/parties.csv",
    "contracts": f"{BASE_URL}/contracts.csv",
    "awards": f"{BASE_URL}/award.csv",
}

MAX_ROWS = 500_000
MAX_DOWNLOAD_BYTES = 500 * 1024 * 1024
CHUNK_SIZE = 50_000


def _register_dataset(engine, file_type: str, table_name: str, df: pd.DataFrame):
    """Upsert into datasets and cached_datasets tables."""
    source_id = f"bac-{file_type}"
    portal = "bac"
    title = f"Buenos Aires Compras - {file_type.title()}"
    columns_json = json.dumps(list(df.columns))
    now = datetime.now(UTC)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO datasets
                    (source_id, title, description, organization, portal, url,
                     download_url, format, columns, tags, last_updated_at, is_cached, row_count)
                VALUES
                    (:sid, :title, :desc, :org, :portal, :url, :dl, 'csv', :cols, :tags,
                     :now, true, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = true, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, last_updated_at = :now, updated_at = :now
            """),
            {
                "sid": source_id, "title": title,
                "desc": f"Datos de contrataciones públicas de CABA ({file_type}), estándar OCDS.",
                "org": "Ministerio de Hacienda y Finanzas - CABA",
                "portal": portal,
                "url": BAC_FILES[file_type],
                "dl": BAC_FILES[file_type],
                "cols": columns_json,
                "tags": f"compras,contrataciones,OCDS,{file_type},CABA",
                "now": now, "rows": len(df),
            },
        )
        dataset_row = conn.execute(
            text(
                "SELECT CAST(id AS text) FROM datasets "
                "WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": source_id, "portal": portal},
        ).fetchone()
        dataset_id = dataset_row[0] if dataset_row else None

        if dataset_id:
            conn.execute(
                text("""
                    INSERT INTO cached_datasets (dataset_id, table_name, status, row_count,
                                                  columns_json, updated_at)
                    VALUES (CAST(:did AS uuid), :tn, 'ready', :rows, :cols, :now)
                    ON CONFLICT (table_name) DO UPDATE SET
                        status = 'ready', row_count = EXCLUDED.row_count,
                        columns_json = EXCLUDED.columns_json, updated_at = :now
                """),
                {"did": dataset_id, "tn": table_name, "rows": len(df),
                 "cols": columns_json, "now": now},
            )

    return dataset_id


@celery_app.task(
    name="openarg.ingest_bac", bind=True, max_retries=3,
    soft_time_limit=600, time_limit=720,
)
def ingest_bac(self):
    """Download and cache Buenos Aires Compras OCDS datasets."""
    import httpx
    import tempfile

    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}

    try:
        for file_type, url in BAC_FILES.items():
            table_name = f"cache_bac_{file_type}"

            try:
                # Stream download to temp file to avoid OOM
                with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tmp:
                    with httpx.Client(timeout=300.0) as client:
                        with client.stream("GET", url, follow_redirects=True) as resp:
                            resp.raise_for_status()
                            downloaded = 0
                            for data in resp.iter_bytes(chunk_size=1024 * 1024):
                                tmp.write(data)
                                downloaded += len(data)
                                if downloaded > MAX_DOWNLOAD_BYTES:
                                    logger.warning("BAC %s too large, stopping at %d bytes", file_type, downloaded)
                                    break

                    tmp.flush()
                    tmp.seek(0)

                    # Parse CSV in chunks — read directly from file, not memory
                    chunks = []
                    total_rows = 0
                    try:
                        csv_reader = pd.read_csv(
                            tmp.name, encoding="utf-8",
                            on_bad_lines="skip", chunksize=CHUNK_SIZE,
                        )
                    except UnicodeDecodeError:
                        csv_reader = pd.read_csv(
                            tmp.name, encoding="latin-1",
                            on_bad_lines="skip", chunksize=CHUNK_SIZE,
                        )
                    for chunk in csv_reader:
                        chunks.append(chunk)
                        total_rows += len(chunk)
                        if total_rows >= MAX_ROWS:
                            break

                if not chunks:
                    logger.warning("BAC %s: no data parsed", file_type)
                    continue

                df = pd.concat(chunks, ignore_index=True)
                if len(df) > MAX_ROWS:
                    df = df.head(MAX_ROWS)

                # Write to PG
                df.to_sql(table_name, engine, if_exists="replace", index=False)

                # Register and dispatch embeddings
                dataset_id = _register_dataset(engine, file_type, table_name, df)
                if dataset_id:
                    from app.infrastructure.celery.tasks.scraper_tasks import (
                        index_dataset_embedding,
                    )
                    index_dataset_embedding.delay(dataset_id)

                results["ingested"] += 1
                logger.info("BAC %s: %d rows cached", file_type, len(df))

            except Exception:
                results["errors"] += 1
                logger.warning("Failed to ingest BAC %s", file_type, exc_info=True)

        return results

    except SoftTimeLimitExceeded:
        logger.error("BAC ingestion timed out")
        raise
    except Exception as exc:
        logger.exception("BAC ingestion failed")
        raise self.retry(exc=exc, countdown=120)
    finally:
        engine.dispose()
