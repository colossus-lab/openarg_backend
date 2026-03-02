"""
Presupuesto Abierto — ETL de datos presupuestarios nacionales.

Descarga ZIPs con CSVs del repositorio DGSIAF/MECON, los parsea con pandas
y los cachea en PostgreSQL para consultas SQL (NL2SQL).
"""
from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from datetime import UTC, datetime

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

BASE_URL = "https://dgsiaf-repo.mecon.gob.ar/repository/pa/datasets"

PRIORITY_PREFIXES = [
    "credito-presupuestario",
    "ejecucion-presupuestaria",
    "recursos-recaudacion",
    "deuda-publica",
    "planta-de-personal",
    "transferencias",
]

ALL_PREFIXES = [
    *PRIORITY_PREFIXES,
    "gastos-por-finalidad",
    "gastos-por-funcion",
    "gastos-por-jurisdiccion",
    "gastos-por-objeto",
    "gastos-por-programa",
    "gastos-por-fuente",
    "gastos-por-ubicacion-geografica",
    "inversiones-reales",
    "resultado-financiero",
    "resultado-primario",
    "servicio-de-la-deuda",
    "intereses-de-la-deuda",
    "esquema-ahorro-inversion",
    "coparticipacion-federal",
    "contribuciones-patronales",
    "otros-recursos",
    "ingresos-tributarios",
    "ingresos-no-tributarios",
]

YEARS = list(range(2010, 2026))

MAX_ROWS = 500_000
MAX_DOWNLOAD_BYTES = 500 * 1024 * 1024


def _sanitize_table_name(prefix: str, year: int) -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", prefix.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    return f"cache_presupuesto_{clean}_{year}"


def _register_dataset(engine, prefix: str, year: int, table_name: str, df: pd.DataFrame):
    """Upsert into datasets and cached_datasets tables."""
    source_id = f"{prefix}-{year}"
    portal = "presupuesto_abierto"
    title = f"Presupuesto - {prefix.replace('-', ' ').title()} {year}"
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
                "sid": source_id, "title": title,
                "desc": f"Datos de {prefix.replace('-', ' ')} del presupuesto nacional, año {year}.",
                "org": "Ministerio de Economía - DGSIAF",
                "portal": portal,
                "url": f"{BASE_URL}/{year}/{prefix}-{year}.zip",
                "dl": f"{BASE_URL}/{year}/{prefix}-{year}.zip",
                "cols": columns_json,
                "tags": f"presupuesto,{prefix},{year},finanzas públicas",
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
def ingest_presupuesto(self, prefixes: list[str] | None = None, years: list[int] | None = None):
    """Download and cache Presupuesto Abierto datasets."""
    import httpx

    target_prefixes = prefixes or PRIORITY_PREFIXES
    target_years = years or YEARS
    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}

    try:
        for prefix in target_prefixes:
            for year in target_years:
                table_name = _sanitize_table_name(prefix, year)

                # Check if already cached
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

                url = f"{BASE_URL}/{year}/{prefix}-{year}.zip"
                try:
                    with httpx.Client(timeout=120.0) as client:
                        # HEAD check
                        try:
                            head = client.head(url, follow_redirects=True)
                            if head.status_code == 404:
                                continue
                            cl = int(head.headers.get("content-length", 0))
                            if cl > MAX_DOWNLOAD_BYTES:
                                logger.warning("Presupuesto %s-%d too large: %d", prefix, year, cl)
                                continue
                        except Exception:
                            pass

                        resp = client.get(url, follow_redirects=True)
                        if resp.status_code == 404:
                            continue
                        resp.raise_for_status()

                    # Extract CSVs from ZIP
                    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                        if not csv_names:
                            logger.warning("No CSVs in %s", url)
                            continue

                        # Concatenate all CSVs in the ZIP
                        frames = []
                        for csv_name in csv_names:
                            with zf.open(csv_name) as f:
                                try:
                                    df = pd.read_csv(f, encoding="utf-8", on_bad_lines="skip")
                                except Exception:
                                    try:
                                        with zf.open(csv_name) as f2:
                                            df = pd.read_csv(f2, encoding="latin-1", on_bad_lines="skip")
                                    except Exception:
                                        logger.warning("Cannot parse %s in %s", csv_name, url)
                                        continue
                                frames.append(df)

                        if not frames:
                            continue
                        df = pd.concat(frames, ignore_index=True)

                    # Truncate
                    if len(df) > MAX_ROWS:
                        df = df.head(MAX_ROWS)
                        logger.warning("Presupuesto %s-%d truncated to %d rows", prefix, year, MAX_ROWS)

                    # Write to PG
                    df.to_sql(table_name, engine, if_exists="replace", index=False)

                    # Register and dispatch embeddings
                    dataset_id = _register_dataset(engine, prefix, year, table_name, df)
                    if dataset_id:
                        from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding
                        index_dataset_embedding.delay(dataset_id)

                    results["ingested"] += 1
                    logger.info("Ingested presupuesto %s-%d: %d rows", prefix, year, len(df))

                except Exception:
                    results["errors"] += 1
                    logger.warning("Failed to ingest presupuesto %s-%d", prefix, year, exc_info=True)

        return results

    except SoftTimeLimitExceeded:
        logger.error("Presupuesto ingestion timed out")
        raise
    except Exception as exc:
        logger.exception("Presupuesto ingestion failed")
        raise self.retry(exc=exc, countdown=120)
    finally:
        engine.dispose()
