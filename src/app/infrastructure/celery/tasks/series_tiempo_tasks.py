"""
Series de Tiempo — ETL de series temporales del gobierno argentino.

Consulta la API de Series de Tiempo (apis.datos.gob.ar/series/api)
y cachea los resultados en PostgreSQL para consultas NL2SQL.
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
from app.infrastructure.celery.tasks.collector_tasks import _finalize_cached_dataset

logger = logging.getLogger(__name__)

API_URL = "https://apis.datos.gob.ar/series/api/series"

SERIES_CATALOG = {
    "inflacion_ipc": {
        "ids": "148.3_INIVELNAL_DICI_M_26",
        "title": "IPC Nacional - Indice de Precios al Consumidor",
    },
    "tipo_cambio": {"ids": "92.2_TIPO_CAMBIION_0_0_21_24", "title": "Tipo de Cambio Peso/Dolar"},
    "emae": {
        "ids": "143.3_NO_PR_2004_A_21",
        "title": "EMAE - Estimador Mensual de Actividad Economica",
    },
    "desempleo": {"ids": "45.2_ECTDT_0_T_33", "title": "Tasa de Desempleo"},
    "gasto_publico": {"ids": "451.3_GPNGPN_0_0_3_30", "title": "Gasto Publico Nacional"},
    "reservas_internacionales": {
        "ids": "174.1_RRVAS_IDOS_0_0_36",
        "title": "Reservas Internacionales",
    },
    "base_monetaria": {"ids": "331.1_SALDO_BASERIA__15", "title": "Base Monetaria"},
    "salarios": {"ids": "149.1_TL_INDIIOS_OCTU_0_21", "title": "Indice de Salarios"},
    "canasta_basica": {"ids": "150.1_LA_POBREZA_0_D_13", "title": "Canasta Basica Total"},
    "exportaciones": {"ids": "74.3_IET_0_M_16", "title": "Exportaciones"},
    "importaciones": {"ids": "74.3_IIT_0_M_25", "title": "Importaciones"},
    "actividad_industrial": {"ids": "11.3_AGCS_2004_M_41", "title": "Actividad Industrial"},
}


def _register_dataset(engine, key: str, table_name: str, df: pd.DataFrame):
    """Upsert into datasets and cached_datasets tables."""
    entry = SERIES_CATALOG[key]
    source_id = f"series-tiempo-{key}"
    portal = "series_tiempo"
    title = f"Series de Tiempo — {entry['title']}"
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
                     :now, false, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = false, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, last_updated_at = :now, updated_at = :now
            """),
            {
                "sid": source_id,
                "title": title,
                "desc": f"Serie temporal: {entry['title']}. Fuente: APIs de Datos Argentina.",
                "org": "Ministerio de Economia — Series de Tiempo",
                "portal": portal,
                "url": f"{API_URL}?ids={entry['ids']}",
                "cols": columns_json,
                "tags": f"series de tiempo,{key.replace('_', ' ')},economia,indicadores",
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

    # Out of the begin() block: cached_datasets INSERT inside
    # _finalize_cached_dataset opens its own tx and would otherwise race
    # the uncommitted datasets row, hitting fk_cached_datasets_dataset_id.
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
            download_url=f"{API_URL}?ids={entry['ids']}",
            now=now,
        )
        if not finalized["ok"]:
            return None

    return dataset_id


@celery_app.task(
    name="openarg.ingest_series_tiempo",
    bind=True,
    max_retries=3,
    soft_time_limit=300,
    time_limit=360,
)
def ingest_series_tiempo(self):
    """Fetch time series data from the Series de Tiempo API and cache it."""
    import httpx

    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}

    try:
        for key, entry in SERIES_CATALOG.items():
            table_name = f"cache_series_{key}"

            # Skip if already cached
            with engine.begin() as conn:
                cached = conn.execute(
                    text(
                        "SELECT id FROM cached_datasets WHERE table_name = :tn AND status = 'ready'"
                    ),
                    {"tn": table_name},
                ).fetchone()
            if cached:
                results["skipped"] += 1
                continue

            try:
                with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                    resp = client.get(
                        API_URL,
                        params={
                            "ids": entry["ids"],
                            "limit": 1000,
                            "format": "json",
                        },
                    )
                    resp.raise_for_status()

                payload = resp.json()
                data_rows = payload.get("data", [])
                meta = payload.get("meta", [])

                if not data_rows:
                    results["skipped"] += 1
                    logger.info("Series %s: no data returned", key)
                    continue

                # Build column names: first column is always "fecha",
                # remaining come from meta field descriptors (skip meta[0] which
                # is frequency/date-range info, not a field descriptor)
                col_names = ["fecha"]
                for field_meta in meta[1:]:
                    field_title = field_meta.get("field", {}).get("description", key)
                    col_names.append(field_title)

                # Ensure column count matches data width
                data_width = len(data_rows[0]) if data_rows else 0
                if len(col_names) < data_width:
                    for i in range(len(col_names), data_width):
                        col_names.append(f"valor_{i}")
                col_names = col_names[:data_width]

                df = pd.DataFrame(data_rows, columns=col_names)
                df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

                df.to_sql(table_name, engine, if_exists="replace", index=False)

                dataset_id = _register_dataset(engine, key, table_name, df)
                if dataset_id:
                    from app.infrastructure.celery.tasks.scraper_tasks import (
                        index_dataset_embedding,
                    )

                    index_dataset_embedding.delay(dataset_id)

                results["ingested"] += 1
                logger.info("Series %s: %d rows cached", key, len(df))

            except Exception:
                results["errors"] += 1
                logger.warning(
                    "Failed to ingest series %s",
                    key,
                    exc_info=True,
                )

        return results

    except SoftTimeLimitExceeded:
        logger.error("Series de Tiempo ingestion timed out")
        raise
    except Exception as exc:
        logger.exception("Series de Tiempo ingestion failed")
        raise self.retry(exc=exc, countdown=120)
    finally:
        engine.dispose()
