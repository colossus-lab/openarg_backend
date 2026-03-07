"""
Presupuesto Abierto — ETL de datos presupuestarios nacionales.

Consulta la API de Presupuesto Abierto (presupuestoabierto.gob.ar/api/v1)
y cachea los resultados en PostgreSQL para consultas NL2SQL.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import UTC, datetime

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

API_URL = "https://www.presupuestoabierto.gob.ar/api/v1"

# Endpoints and their default columns
ENDPOINTS = {
    "credito": [
        "jurisdiccion_desc", "finalidad_desc", "funcion_desc",
        "programa_desc", "credito_presupuestado", "credito_vigente",
        "credito_comprometido", "credito_devengado", "credito_pagado",
    ],
    "recurso": [
        "concepto_desc", "rubro_desc", "recurso_presupuestado",
        "recurso_vigente", "recurso_ingresado_devengado",
        "recurso_ingresado_percibido",
    ],
}

YEARS = list(range(2020, 2027))

MAX_ROWS = 500_000


def _sanitize_table_name(endpoint: str, year: int) -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", endpoint.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    return f"cache_presupuesto_{clean}_{year}"


def _register_dataset(engine, endpoint: str, year: int, table_name: str, df: pd.DataFrame):
    """Upsert into datasets and cached_datasets tables."""
    source_id = f"presupuesto-{endpoint}-{year}"
    portal = "presupuesto_abierto"
    title = f"Presupuesto — {endpoint.replace('_', ' ').title()} {year}"
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
                    columns = EXCLUDED.columns, updated_at = :now
            """),
            {
                "sid": source_id, "title": title,
                "desc": f"Datos de {endpoint} del presupuesto nacional, ejercicio {year}.",
                "org": "Ministerio de Economía — Presupuesto Abierto",
                "portal": portal,
                "url": f"{API_URL}/{endpoint}",
                "cols": columns_json,
                "tags": f"presupuesto,{endpoint},{year},finanzas públicas",
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
    name="openarg.ingest_presupuesto", bind=True, max_retries=3,
    soft_time_limit=600, time_limit=720,
)
def ingest_presupuesto(self):
    """Fetch budget data from the Presupuesto Abierto API and cache it."""
    import httpx

    token = os.getenv("PRESUPUESTO_API_TOKEN")
    if not token:
        logger.error("PRESUPUESTO_API_TOKEN not set — skipping presupuesto ingestion")
        return {"ingested": 0, "skipped": 0, "errors": 0, "reason": "no token"}

    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}

    try:
        for endpoint, columns in ENDPOINTS.items():
            for year in YEARS:
                table_name = _sanitize_table_name(endpoint, year)

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
                    body = {
                        "title": f"OpenArg — {endpoint} {year}",
                        "ejercicios": [year],
                        "columns": columns,
                        "filters": [],
                    }
                    with httpx.Client(timeout=30.0) as client:
                        resp = client.post(
                            f"{API_URL}/{endpoint}?format=json",
                            headers={
                                "Authorization": token,
                                "Content-Type": "application/json",
                            },
                            json=body,
                        )
                        if resp.status_code == 401:
                            logger.error("Presupuesto API: invalid token")
                            return {**results, "reason": "auth_failed"}
                        resp.raise_for_status()

                    data = resp.json()
                    records = data if isinstance(data, list) else data.get("data", [])
                    if not records:
                        results["skipped"] += 1
                        continue

                    df = pd.DataFrame(records)
                    if len(df) > MAX_ROWS:
                        df = df.head(MAX_ROWS)

                    df.to_sql(table_name, engine, if_exists="replace", index=False)

                    dataset_id = _register_dataset(engine, endpoint, year, table_name, df)
                    if dataset_id:
                        from app.infrastructure.celery.tasks.scraper_tasks import (
                            index_dataset_embedding,
                        )
                        index_dataset_embedding.delay(dataset_id)

                    results["ingested"] += 1
                    logger.info("Presupuesto %s %d: %d rows cached", endpoint, year, len(df))

                except Exception:
                    results["errors"] += 1
                    logger.warning(
                        "Failed to ingest presupuesto %s %d", endpoint, year, exc_info=True,
                    )

        return results

    except SoftTimeLimitExceeded:
        logger.error("Presupuesto ingestion timed out")
        raise
    except Exception as exc:
        logger.exception("Presupuesto ingestion failed")
        raise self.retry(exc=exc, countdown=120)
    finally:
        engine.dispose()
