"""
Georef Argentina — ETL de datos georreferenciados.

Consulta la API de Georef (apis.datos.gob.ar/georef/api)
y cachea provincias, departamentos, municipios y localidades
en PostgreSQL para consultas NL2SQL.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

API_URL = "https://apis.datos.gob.ar/georef/api"

ENDPOINTS = {
    "provincias": "provincias",
    "departamentos": "departamentos",
    "municipios": "municipios",
    "localidades": "localidades",
}


def _register_dataset(engine, endpoint: str, table_name: str, df: pd.DataFrame):
    """Upsert into datasets and cached_datasets tables."""
    source_id = f"georef-{endpoint}"
    portal = "georef"
    title = f"Georef — {endpoint.replace('_', ' ').title()}"
    columns_json = json.dumps(list(df.columns))
    now = datetime.now(UTC)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO datasets
                    (source_id, title, description, organization, portal, url,
                     download_url, format, columns, tags, last_updated_at, is_cached, row_count)
                VALUES
                    (:sid, :title, :desc, :org, :portal, :url, '', 'json', :cols, :tags,
                     :now, true, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = true, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, last_updated_at = :now, updated_at = :now
            """),
            {
                "sid": source_id, "title": title,
                "desc": f"Datos georreferenciados de {endpoint} de Argentina (API Georef).",
                "org": "Ministerio de Modernización — Georef AR",
                "portal": portal,
                "url": f"{API_URL}/{endpoint}",
                "cols": columns_json,
                "tags": f"georef,{endpoint},geografía,argentina",
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
    name="openarg.ingest_georef", bind=True, max_retries=3,
    soft_time_limit=300, time_limit=360,
)
def ingest_georef(self):
    """Fetch geographic reference data from the Georef API and cache it."""
    import httpx

    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}

    try:
        for endpoint, response_key in ENDPOINTS.items():
            table_name = f"cache_georef_{endpoint}"

            # Skip if already cached
            with engine.begin() as conn:
                cached = conn.execute(
                    text(
                        "SELECT id FROM cached_datasets "
                        "WHERE table_name = :tn AND status = 'ready'"
                    ),
                    {"tn": table_name},
                ).fetchone()
            if cached:
                results["skipped"] += 1
                continue

            try:
                with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                    resp = client.get(
                        f"{API_URL}/{endpoint}",
                        params={"max": 5000},
                    )
                    resp.raise_for_status()

                data = resp.json()
                records = data.get(response_key, [])
                if not records:
                    results["skipped"] += 1
                    logger.warning("Georef %s: no data returned", endpoint)
                    continue

                df = pd.DataFrame(records)

                # Flatten nested dict columns (e.g. provincia, departamento inside localidades)
                for col in list(df.columns):
                    if df[col].apply(type).eq(dict).any():
                        nested = pd.json_normalize(df[col])
                        nested.columns = [f"{col}_{c}" for c in nested.columns]
                        nested.index = df.index
                        df = df.drop(columns=[col]).join(nested)

                df.to_sql(table_name, engine, if_exists="replace", index=False)

                dataset_id = _register_dataset(engine, endpoint, table_name, df)
                if dataset_id:
                    from app.infrastructure.celery.tasks.scraper_tasks import (
                        index_dataset_embedding,
                    )
                    index_dataset_embedding.delay(dataset_id)

                results["ingested"] += 1
                logger.info("Georef %s: %d rows cached", endpoint, len(df))

            except Exception:
                results["errors"] += 1
                logger.warning("Failed to ingest Georef %s", endpoint, exc_info=True)

        return results

    except SoftTimeLimitExceeded:
        logger.error("Georef ingestion timed out")
        raise
    except Exception as exc:
        logger.exception("Georef ingestion failed")
        raise self.retry(exc=exc, countdown=120)
    finally:
        engine.dispose()
