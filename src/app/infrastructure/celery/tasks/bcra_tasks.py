"""
BCRA Snapshot — Daily snapshot of exchange rates and monetary variables.

Downloads cotizaciones and principales variables from BCRA API,
caches them in PostgreSQL for NL2SQL queries.
"""

from __future__ import annotations

import asyncio
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


def _register_dataset(engine, source_id: str, title: str, table_name: str, df: pd.DataFrame):
    """Upsert into datasets and cached_datasets tables."""
    portal = "bcra"
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
                "desc": f"Datos del BCRA: {title}",
                "org": "Banco Central de la República Argentina",
                "portal": portal,
                "url": "https://www.bcra.gob.ar/Estadisticas/Datos_Abiertos.asp",
                "cols": columns_json,
                "tags": "bcra,monetario,cambiario,finanzas",
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
                download_url="https://www.bcra.gob.ar/Estadisticas/Datos_Abiertos.asp",
                now=now,
            )
            if not finalized["ok"]:
                return None

    return dataset_id


def _fetch_bcra_data():
    """Fetch BCRA cotizaciones synchronously using async adapter."""
    from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter

    adapter = BCRAAdapter()

    async def _run():
        return await adapter.get_cotizaciones()

    return asyncio.run(_run())


@celery_app.task(
    name="openarg.snapshot_bcra",
    bind=True,
    max_retries=3,
    soft_time_limit=300,
    time_limit=360,
)
def snapshot_bcra(self):
    """Daily snapshot of BCRA exchange rates and monetary variables."""
    engine = get_sync_engine()

    try:
        cotizaciones = _fetch_bcra_data()

        results = {"tables": []}

        if cotizaciones.records:
            df = pd.DataFrame(cotizaciones.records)
            if not df.empty:
                table_name = "cache_bcra_cotizaciones"
                df.to_sql(table_name, engine, if_exists="replace", index=False)
                dataset_id = _register_dataset(
                    engine,
                    "bcra-cotizaciones",
                    "Cotizaciones Cambiarias BCRA",
                    table_name,
                    df,
                )
                if dataset_id:
                    from app.infrastructure.celery.tasks.scraper_tasks import (
                        index_dataset_embedding,
                    )

                    index_dataset_embedding.delay(dataset_id)
                results["tables"].append({"table": table_name, "rows": len(df)})
                logger.info("BCRA cotizaciones: %d records cached", len(df))

        return results

    except SoftTimeLimitExceeded:
        logger.error("BCRA snapshot timed out")
        raise
    except Exception as exc:
        logger.exception("BCRA snapshot failed")
        raise self.retry(exc=exc, countdown=60)
    finally:
        engine.dispose()
