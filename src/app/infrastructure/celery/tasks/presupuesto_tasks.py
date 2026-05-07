"""
Presupuesto Abierto — ETL de datos presupuestarios nacionales.

Consulta la API de Presupuesto Abierto (presupuestoabierto.gob.ar/api/v1)
y cachea los resultados en PostgreSQL para consultas NL2SQL.

Además descarga tablas de dimensiones (clasificadores presupuestarios)
desde el repositorio DGSIAF del Ministerio de Economía.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import zipfile
from datetime import UTC, datetime

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.infrastructure.celery.tasks.collector_tasks import _finalize_cached_dataset

logger = logging.getLogger(__name__)

API_URL = "https://www.presupuestoabierto.gob.ar/api/v1"
DGSIAF_URL = "https://dgsiaf-repo.mecon.gob.ar/repository/pa/datasets"

# ── API Endpoints (requieren PRESUPUESTO_API_TOKEN) ──────────
ENDPOINTS = {
    "credito": [
        "ejercicio_presupuestario",
        "jurisdiccion_desc",
        "subjurisdiccion_desc",
        "entidad_desc",
        "finalidad_desc",
        "funcion_desc",
        "programa_desc",
        "fuente_financiamiento_desc",
        "ubicacion_geografica_desc",
        "credito_presupuestado",
        "credito_vigente",
        "credito_comprometido",
        "credito_devengado",
        "credito_pagado",
    ],
    "recurso": [
        "ejercicio_presupuestario",
        "sector_desc",
        "tipo_desc",
        "clase_desc",
        "concepto_desc",
        "subconcepto_desc",
        "fuente_financiamiento_desc",
        "recurso_inicial",
        "recurso_vigente",
        "recurso_ingresado_percibido",
    ],
    "pef": [
        "ejercicio_presupuestario",
        "trimestre",
        "jurisdiccion_desc",
        "programa_desc",
        "actividad_desc",
        "medicion_fisica_desc",
        "unidad_medida_desc",
        "ubicacion_geografica_desc",
        "programacion_inicial_DA",
        "programacion_anual_vig_trim",
        "ejecutado_acumulado_trim",
    ],
    "transversal_financiero": [
        "ejercicio_presupuestario",
        "jurisdiccion_desc",
        "programa_desc",
        "finalidad_desc",
        "funcion_desc",
        "fuente_financiamiento_desc",
        "ubicacion_geografica_desc",
        "inciso_desc",
        "etiqueta_desc",
        "credito_inicial",
        "credito_vigente",
        "credito_ejecutado",
    ],
}

# ── Tablas de dimensiones (CSV públicos, sin auth) ───────────
# URL: {DGSIAF_URL}/{año}/d-{slug}-{año}.zip → contiene CSV
DIMENSION_TABLES = {
    "apertura_programatica": {
        "slug": "apertura-programatica",
        "name": "Apertura Programática (Programas, Subprogramas, Proyectos)",
    },
    "clasificador_economico": {
        "slug": "clasificador-economico",
        "name": "Clasificador Económico del Gasto",
    },
    "finalidad_funcion": {
        "slug": "finalidad-funcion",
        "name": "Finalidad y Función del Gasto",
    },
    "fuente_financiamiento": {
        "slug": "fuente-financiamiento",
        "name": "Fuente de Financiamiento",
    },
    "ubicacion_geografica": {
        "slug": "ubicacion-geografica",
        "name": "Ubicación Geográfica del Gasto",
    },
}

YEARS = list(range(2016, 2027))
DIMENSION_YEARS = list(range(2016, 2027))

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

    # Commit the `datasets` row before calling `_finalize_cached_dataset`
    # — it opens its own transaction for the FK-bound insert into
    # `cached_datasets`. Sibling transactions don't see uncommitted writes.
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO datasets
                    (source_id, title, description, organization, portal, url,
                     download_url, format, columns, tags, last_updated_at, is_cached, row_count)
                VALUES
                    (:sid, :title, :desc, :org, :portal, :url, '', 'json', :cols, :tags,
                     :now, false, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = false, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, updated_at = :now
            """),
            {
                "sid": source_id,
                "title": title,
                "desc": f"Datos de {endpoint} del presupuesto nacional, ejercicio {year}.",
                "org": "Ministerio de Economía — Presupuesto Abierto",
                "portal": portal,
                "url": f"{API_URL}/{endpoint}",
                "cols": columns_json,
                "tags": f"presupuesto,{endpoint},{year},finanzas públicas",
                "now": now,
                "rows": len(df),
            },
        )
        dataset_row = conn.execute(
            text(
                "SELECT CAST(id AS text) FROM datasets WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": source_id, "portal": portal},
        ).fetchone()
        dataset_id = dataset_row[0] if dataset_row else None

    if dataset_id:
        finalized = _finalize_cached_dataset(
            engine,
            dataset_id=dataset_id,
            portal=portal,
            source_id=source_id,
            table_name=table_name,
            row_count=len(df),
            columns=list(df.columns),
            declared_format="json",
            download_url=f"{API_URL}/{endpoint}",
            now=now,
        )
        if not finalized["ok"]:
            return None

    return dataset_id


@celery_app.task(
    name="openarg.ingest_presupuesto",
    bind=True,
    max_retries=3,
    soft_time_limit=600,
    time_limit=720,
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

                    # `if_exists='replace'` does DROP + CREATE under the hood,
                    # which fails with `DependentObjectsStillExist` when a
                    # mart materialized view (e.g. `presupuesto_consolidado`)
                    # already references this table. Fall back to TRUNCATE +
                    # append so the table identity (and downstream views) is
                    # preserved across re-runs.
                    try:
                        df.to_sql(table_name, engine, if_exists="replace", index=False)
                    except Exception as exc:
                        if "DependentObjectsStillExist" not in type(exc).__name__ \
                                and "depend on it" not in str(exc):
                            raise
                        logger.info(
                            "Presupuesto %s %d: replace blocked by dependent view; "
                            "falling back to TRUNCATE + append",
                            endpoint, year,
                        )
                        from app.infrastructure.celery.tasks._db import safe_truncate_table
                        safe_truncate_table(engine, table_name)
                        df.to_sql(table_name, engine, if_exists="append", index=False)

                    dataset_id = _register_dataset(engine, endpoint, year, table_name, df)
                    if dataset_id:
                        from app.infrastructure.celery.tasks.scraper_tasks import (
                            index_dataset_embedding,
                        )

                        index_dataset_embedding.delay(dataset_id)

                    # Register in `raw_table_versions` so the
                    # `presupuesto_consolidado` mart finds it.
                    from app.infrastructure.celery.tasks._db import register_via_b_table

                    register_via_b_table(
                        engine,
                        resource_identity=f"presupuesto_abierto::{endpoint}::{year}",
                        table_name=table_name,
                        schema_name="public",
                        row_count=len(df),
                    )

                    results["ingested"] += 1
                    logger.info("Presupuesto %s %d: %d rows cached", endpoint, year, len(df))

                except Exception:
                    results["errors"] += 1
                    logger.warning(
                        "Failed to ingest presupuesto %s %d",
                        endpoint,
                        year,
                        exc_info=True,
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


def _dimension_table_name(dim_key: str, year: int) -> str:
    return f"cache_presupuesto_dim_{dim_key}_{year}"


def _register_dimension(
    engine, dim_key: str, dim_info: dict, year: int, table_name: str, df: pd.DataFrame
):
    """Upsert dimension table into datasets + cached_datasets."""
    source_id = f"presupuesto-dim-{dim_key}-{year}"
    portal = "presupuesto_abierto"
    title = f"Presupuesto — {dim_info['name']} {year}"
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
                    columns = EXCLUDED.columns, updated_at = :now
            """),
            {
                "sid": source_id,
                "title": title,
                "desc": f"Clasificador presupuestario: {dim_info['name']}, ejercicio {year}.",
                "org": "Ministerio de Economía — DGSIAF",
                "portal": portal,
                "url": f"{DGSIAF_URL}/{year}/d-{dim_info['slug']}-{year}.zip",
                "dl": f"{DGSIAF_URL}/{year}/d-{dim_info['slug']}-{year}.zip",
                "cols": columns_json,
                "tags": f"presupuesto,clasificador,{dim_key},{year}",
                "now": now,
                "rows": len(df),
            },
        )
        dataset_row = conn.execute(
            text(
                "SELECT CAST(id AS text) FROM datasets WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": source_id, "portal": portal},
        ).fetchone()
        dataset_id = dataset_row[0] if dataset_row else None

    # Persist the cached_datasets row through the canonical state
    # machine instead of the previous direct INSERT. Sprint 1.2:
    # 55 dimension tables now get layout_profile / header_quality /
    # error_category / parser_version like every other dataset, so
    # /data/tables and the cleanup_invariants sweep treat them
    # consistently with the rest of the corpus.
    if dataset_id:
        try:
            from app.infrastructure.celery.tasks._db import (
                register_via_b_with_state,
            )

            register_via_b_with_state(
                engine,
                dataset_id=dataset_id,
                table_name=table_name,
                columns=list(df.columns),
                row_count=len(df),
            )
        except Exception:
            logger.exception(
                "register_via_b_with_state failed for %s; refusing legacy cached_datasets bypass",
                table_name,
            )
            raise

    return dataset_id


@celery_app.task(
    name="openarg.ingest_presupuesto_dimensiones",
    bind=True,
    max_retries=3,
    soft_time_limit=600,
    time_limit=720,
)
def ingest_presupuesto_dimensiones(self):
    """Download budget dimension tables (clasificadores) from DGSIAF and cache in PG.

    Downloads ZIP files containing CSVs from the public DGSIAF repository.
    No auth token required. Covers 2016-2026 × 5 classifiers = up to 55 tables.
    """
    import httpx

    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}

    try:
        for dim_key, dim_info in DIMENSION_TABLES.items():
            for year in DIMENSION_YEARS:
                table_name = _dimension_table_name(dim_key, year)

                url = f"{DGSIAF_URL}/{year}/d-{dim_info['slug']}-{year}.zip"
                try:
                    with httpx.Client(timeout=60.0) as client:
                        resp = client.get(url, follow_redirects=True)
                        if resp.status_code == 404:
                            logger.debug("Dimension %s %d: 404 — skipping", dim_key, year)
                            results["skipped"] += 1
                            continue
                        resp.raise_for_status()

                    # Extract CSV from ZIP
                    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                        if not csv_files:
                            logger.warning("Dimension %s %d: ZIP has no CSV", dim_key, year)
                            results["skipped"] += 1
                            continue
                        df = None
                        with zf.open(csv_files[0]) as f:
                            try:
                                df = pd.read_csv(f, encoding="utf-8", on_bad_lines="skip")
                            except UnicodeDecodeError:
                                pass
                        if df is None:
                            with zf.open(csv_files[0]) as f2:
                                df = pd.read_csv(f2, encoding="latin-1", on_bad_lines="skip")

                    if df.empty:
                        results["skipped"] += 1
                        continue

                    if len(df) > MAX_ROWS:
                        df = df.head(MAX_ROWS)

                    # Same DependentObjectsStillExist guard as in
                    # ingest_presupuesto: TRUNCATE + append when a mart
                    # already depends on this dimension table.
                    try:
                        df.to_sql(table_name, engine, if_exists="replace", index=False)
                    except Exception as exc:
                        if "DependentObjectsStillExist" not in type(exc).__name__ \
                                and "depend on it" not in str(exc):
                            raise
                        logger.info(
                            "Presupuesto dim %s %d: replace blocked by dependent view; "
                            "falling back to TRUNCATE + append",
                            dim_key, year,
                        )
                        from app.infrastructure.celery.tasks._db import safe_truncate_table
                        safe_truncate_table(engine, table_name)
                        df.to_sql(table_name, engine, if_exists="append", index=False)

                    dataset_id = _register_dimension(
                        engine, dim_key, dim_info, year, table_name, df
                    )
                    if dataset_id:
                        from app.infrastructure.celery.tasks.scraper_tasks import (
                            index_dataset_embedding,
                        )

                        index_dataset_embedding.delay(dataset_id)

                    from app.infrastructure.celery.tasks._db import register_via_b_table

                    register_via_b_table(
                        engine,
                        resource_identity=f"presupuesto_abierto::{dim_key}::{year}",
                        table_name=table_name,
                        schema_name="public",
                        row_count=len(df),
                    )

                    results["ingested"] += 1
                    logger.info(
                        "Dimension %s %d: %d rows cached → %s",
                        dim_key,
                        year,
                        len(df),
                        table_name,
                    )

                except Exception:
                    results["errors"] += 1
                    logger.warning(
                        "Failed to ingest dimension %s %d",
                        dim_key,
                        year,
                        exc_info=True,
                    )

        return results

    except SoftTimeLimitExceeded:
        logger.error("Dimension ingestion timed out")
        raise
    except Exception as exc:
        logger.exception("Dimension ingestion failed")
        raise self.retry(exc=exc, countdown=120)
