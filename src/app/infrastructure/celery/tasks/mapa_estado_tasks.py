"""
Mapa del Estado — ETL de autoridades del Poder Ejecutivo Nacional.

Descarga el CSV actualizado de autoridades PEN desde
mapadelestado.jefatura.gob.ar (Jefatura de Gabinete de Ministros)
y lo cachea en PostgreSQL para consultas NL2SQL.

Incluye: Presidente, Vicepresidente, Ministros, Secretarios, etc.
"""
from __future__ import annotations

import csv
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

CSV_URL = "https://mapadelestado.jefatura.gob.ar/back/api/datos.php?db=m&id=9&fi=csv"


def _register_dataset(engine, source_id: str, title: str, table_name: str, df: pd.DataFrame):
    """Upsert into datasets and cached_datasets tables."""
    portal = "mapa_estado"
    columns_json = json.dumps(list(df.columns))
    now = datetime.now(UTC)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO datasets
                    (source_id, title, description, organization, portal, url,
                     download_url, format, columns, tags, last_updated_at, is_cached, row_count)
                VALUES
                    (:sid, :title, :desc, :org, :portal, :url, '', 'csv', :cols, :tags,
                     :now, true, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = true, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, last_updated_at = :now, updated_at = :now
            """),
            {
                "sid": source_id, "title": title,
                "desc": (
                    "Estructura orgánica y autoridades del Poder Ejecutivo Nacional. "
                    "Fuente: Mapa del Estado (Jefatura de Gabinete de Ministros)."
                ),
                "org": "Jefatura de Gabinete de Ministros — Mapa del Estado",
                "portal": portal,
                "url": "https://mapadelestado.jefatura.gob.ar/",
                "cols": columns_json,
                "tags": "autoridades,pen,presidente,ministros,gobierno,poder ejecutivo",
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
    name="openarg.scrape_mapa_estado", bind=True, max_retries=3,
    soft_time_limit=300, time_limit=360,
)
def scrape_mapa_estado(self):
    """Scrape PEN authorities from Mapa del Estado (Jefatura de Gabinete).

    Downloads the full CSV of 6000+ authorities and caches two tables:
    - cache_autoridades_pen: All authorities (full dataset)
    - cache_autoridades_pen_principales: Top officials only (Autoridad Superior)
    """
    import httpx

    engine = get_sync_engine()

    try:
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            resp = client.get(CSV_URL)
            resp.raise_for_status()

        raw = resp.text.strip().lstrip("\t ")
        reader = csv.DictReader(io.StringIO(raw))
        rows = list(reader)

        if not rows:
            logger.warning("Mapa del Estado: empty CSV response")
            return {"error": "empty_response"}

        df = pd.DataFrame(rows)

        # Clean column names (strip BOM/whitespace)
        df.columns = [c.strip().strip("\ufeff") for c in df.columns]

        # Full dataset
        table_full = "cache_autoridades_pen"
        df.to_sql(table_full, engine, if_exists="replace", index=False)
        did_full = _register_dataset(
            engine, "mapa-estado-pen", "Autoridades del Poder Ejecutivo Nacional",
            table_full, df,
        )
        logger.info("Mapa del Estado: %d autoridades cached → %s", len(df), table_full)

        # Filtered: only top officials with names (Autoridad Superior + named)
        mask = (
            df["autoridad_nombre"].notna()
            & (df["autoridad_nombre"].str.strip() != "")
        )
        df_top = df[mask][
            ["jurisdiccion", "cargo", "car_nivel", "autoridad_tratamiento",
             "autoridad_nombre", "autoridad_apellido", "autoridad_sexo",
             "autoridad_norma_designacion", "web"]
        ].copy()

        table_top = "cache_autoridades_pen_principales"
        df_top.to_sql(table_top, engine, if_exists="replace", index=False)
        did_top = _register_dataset(
            engine, "mapa-estado-pen-principales",
            "Principales Autoridades del PEN (Presidente, Ministros, Secretarios)",
            table_top, df_top,
        )
        logger.info("Mapa del Estado: %d principales cached → %s", len(df_top), table_top)

        # Dispatch embeddings
        from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding
        if did_full:
            index_dataset_embedding.delay(did_full)
        if did_top:
            index_dataset_embedding.delay(did_top)

        return {
            "tables": [
                {"table": table_full, "rows": len(df)},
                {"table": table_top, "rows": len(df_top)},
            ],
        }

    except SoftTimeLimitExceeded:
        logger.error("Mapa del Estado scrape timed out")
        raise
    except Exception as exc:
        logger.exception("Mapa del Estado scrape failed")
        raise self.retry(exc=exc, countdown=120)
    finally:
        engine.dispose()
