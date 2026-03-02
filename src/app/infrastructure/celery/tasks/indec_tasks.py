"""
INDEC — ETL selectivo de datasets de alto valor.

INDEC no tiene CKAN. Se predefinen URLs directas a CSV/XLS de alto valor
(IPC, EMAE, PIB, comercio exterior, empleo, etc.) y se descargan mensualmente.
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

INDEC_DATASETS = [
    {"id": "ipc", "name": "IPC mensual",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_aperturas.xls"},
    {"id": "emae", "name": "EMAE mensual",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls"},
    {"id": "pib", "name": "PIB trimestral",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_oferta_demanda_12_24.xls"},
    {"id": "comercio_exterior", "name": "Comercio Exterior",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_comext.xls"},
    {"id": "empleo", "name": "EPH - Empleo",
     "url": "https://www.indec.gob.ar/ftp/cuadros/menusuperior/eph/EPH_usu_1er_Trim_2024_txt.zip"},
    {"id": "canasta_basica", "name": "Canasta Básica Total y Alimentaria",
     "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/serie_CBT_CBA.xls"},
    {"id": "salarios", "name": "Índice de Salarios",
     "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/serie_salarios.xls"},
    {"id": "pobreza", "name": "Pobreza e Indigencia",
     "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/serie_pobreza.xls"},
    {"id": "construccion", "name": "ISAC - Construcción",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ISAC_mensual.xls"},
    {"id": "industria", "name": "IPI Manufacturero",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_IPI_Manufacturero.xls"},
    {"id": "supermercados", "name": "Supermercados",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_super_mensual.xls"},
    {"id": "turismo", "name": "Turismo Internacional",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_turismo_internacional.xls"},
    {"id": "uso_tiempo", "name": "Uso del Tiempo",
     "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/enut_cuadros.xls"},
    {"id": "pib_provincial", "name": "PIB Provincial",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_PIB_provincial.xls"},
    {"id": "ipc_regiones", "name": "IPC por Regiones",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_regiones.xls"},
]

MAX_ROWS = 500_000
MAX_DOWNLOAD_BYTES = 500 * 1024 * 1024


def _sanitize_table_name(topic: str) -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", topic.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    return f"cache_indec_{clean}"


def _register_dataset(engine, ds_info: dict, table_name: str, df: pd.DataFrame):
    """Upsert into datasets and cached_datasets tables."""
    source_id = f"indec-{ds_info['id']}"
    portal = "indec"
    columns_json = json.dumps(list(df.columns))
    now = datetime.now(UTC)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO datasets
                    (source_id, title, description, organization, portal, url,
                     download_url, format, columns, tags, last_updated_at, is_cached, row_count)
                VALUES
                    (:sid, :title, :desc, :org, :portal, :url, :dl, :fmt, :cols, :tags,
                     :now, true, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = true, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, last_updated_at = :now, updated_at = :now
            """),
            {
                "sid": source_id, "title": f"INDEC - {ds_info['name']}",
                "desc": f"Datos oficiales del INDEC: {ds_info['name']}.",
                "org": "Instituto Nacional de Estadística y Censos",
                "portal": portal,
                "url": ds_info["url"],
                "dl": ds_info["url"],
                "fmt": "xls" if ds_info["url"].endswith(".xls") else "csv",
                "cols": columns_json,
                "tags": f"indec,estadísticas,{ds_info['id']}",
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


def _download_and_parse(url: str) -> pd.DataFrame | None:
    """Download and parse a file (XLS, CSV, or ZIP containing CSVs)."""
    import httpx

    with httpx.Client(timeout=120.0) as client:
        try:
            head = client.head(url, follow_redirects=True)
            if head.status_code == 404:
                return None
            cl = int(head.headers.get("content-length", 0))
            if cl > MAX_DOWNLOAD_BYTES:
                logger.warning("INDEC file too large: %d bytes — %s", cl, url)
                return None
        except Exception:
            pass

        resp = client.get(url, follow_redirects=True)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()

    content = resp.content
    lower_url = url.lower()

    if lower_url.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            # Find CSV or TXT files inside ZIP
            data_files = [n for n in zf.namelist()
                          if n.lower().endswith((".csv", ".txt"))]
            if not data_files:
                return None
            with zf.open(data_files[0]) as f:
                try:
                    return pd.read_csv(f, encoding="utf-8", on_bad_lines="skip", sep=";")
                except Exception:
                    with zf.open(data_files[0]) as f2:
                        return pd.read_csv(f2, encoding="latin-1", on_bad_lines="skip", sep=";")

    if lower_url.endswith((".xls", ".xlsx")):
        try:
            return pd.read_excel(io.BytesIO(content), sheet_name=0)
        except Exception:
            # Some INDEC XLS files have multiple sheets — try all
            try:
                dfs = pd.read_excel(io.BytesIO(content), sheet_name=None)
                # Return the largest sheet
                return max(dfs.values(), key=len) if dfs else None
            except Exception:
                return None

    # Default: CSV
    try:
        return pd.read_csv(io.BytesIO(content), encoding="utf-8", on_bad_lines="skip")
    except Exception:
        return pd.read_csv(io.BytesIO(content), encoding="latin-1", on_bad_lines="skip")


@celery_app.task(
    name="openarg.ingest_indec", bind=True, max_retries=3,
    soft_time_limit=600, time_limit=720,
)
def ingest_indec(self):
    """Download and cache INDEC high-value datasets."""
    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}

    try:
        for ds_info in INDEC_DATASETS:
            table_name = _sanitize_table_name(ds_info["id"])

            try:
                df = _download_and_parse(ds_info["url"])
                if df is None or df.empty:
                    results["skipped"] += 1
                    logger.warning("INDEC %s: no data or 404", ds_info["id"])
                    continue

                # Truncate
                if len(df) > MAX_ROWS:
                    df = df.head(MAX_ROWS)

                # Write to PG
                df.to_sql(table_name, engine, if_exists="replace", index=False)

                # Register and dispatch embeddings
                dataset_id = _register_dataset(engine, ds_info, table_name, df)
                if dataset_id:
                    from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding
                    index_dataset_embedding.delay(dataset_id)

                results["ingested"] += 1
                logger.info("INDEC %s: %d rows cached", ds_info["id"], len(df))

            except Exception:
                results["errors"] += 1
                logger.warning("Failed to ingest INDEC %s", ds_info["id"], exc_info=True)

        return results

    except SoftTimeLimitExceeded:
        logger.error("INDEC ingestion timed out")
        raise
    except Exception as exc:
        logger.exception("INDEC ingestion failed")
        raise self.retry(exc=exc, countdown=120)
    finally:
        engine.dispose()
