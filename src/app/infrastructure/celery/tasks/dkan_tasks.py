"""
DKAN — Scraper de datos abiertos para portales DKAN (Rosario, Jujuy).

Consulta la API DKAN, descarga CSVs y cachea en PostgreSQL.
"""

from __future__ import annotations

import io
import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

MAX_ROWS = 500_000
MAX_DOWNLOAD_BYTES = 500 * 1024 * 1024


@dataclass(frozen=True)
class DKANPortal:
    api_url: str
    portal_key: str  # e.g. "rosario_dkan"
    source_prefix: str  # e.g. "rosario"
    table_prefix: str  # e.g. "cache_rosario"
    organization: str
    base_url: str  # for building dataset URLs
    tags: str


DKAN_PORTALS = {
    "rosario": DKANPortal(
        api_url="https://datosabiertos.rosario.gob.ar/api/1/metastore/schemas/dataset/items",
        portal_key="rosario_dkan",
        source_prefix="rosario",
        table_prefix="cache_rosario",
        organization="Municipalidad de Rosario",
        base_url="https://datosabiertos.rosario.gob.ar/dataset",
        tags="rosario,municipio,datos abiertos",
    ),
    "jujuy": DKANPortal(
        api_url="https://datos.gajujuy.gob.ar/api/1/metastore/schemas/dataset/items",
        portal_key="jujuy_dkan",
        source_prefix="jujuy",
        table_prefix="cache_jujuy",
        organization="Gobierno de Jujuy",
        base_url="https://datos.gajujuy.gob.ar/dataset",
        tags="jujuy,provincia,datos abiertos",
    ),
}


def _sanitize_name(name: str) -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", name.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    return clean[:50]


def _register_dataset(
    engine, portal: DKANPortal, ds_id: str, title: str, table_name: str, df: pd.DataFrame, url: str
):
    """Upsert into datasets and cached_datasets tables."""
    source_id = f"{portal.source_prefix}-{ds_id}"
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
                "sid": source_id,
                "title": title,
                "desc": f"Dataset de {portal.organization}: {title}",
                "org": portal.organization,
                "portal": portal.portal_key,
                "url": f"{portal.base_url}/{ds_id}",
                "dl": url,
                "cols": columns_json,
                "tags": portal.tags,
                "now": now,
                "rows": len(df),
            },
        )
        dataset_row = conn.execute(
            text(
                "SELECT CAST(id AS text) FROM datasets WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": source_id, "portal": portal.portal_key},
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
                {
                    "did": dataset_id,
                    "tn": table_name,
                    "rows": len(df),
                    "cols": columns_json,
                    "now": now,
                },
            )

    return dataset_id


def _scrape_dkan_portal(portal: DKANPortal) -> dict:
    """Generic DKAN scraper — fetches catalog and caches CSV datasets."""
    import httpx

    engine = get_sync_engine()
    results = {"portal": portal.portal_key, "ingested": 0, "skipped": 0, "errors": 0}

    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.get(portal.api_url)
            resp.raise_for_status()
            catalog = resp.json()

        if not isinstance(catalog, list):
            logger.warning("DKAN %s: unexpected response format", portal.portal_key)
            return {"error": "unexpected_format", "portal": portal.portal_key}

        for dataset in catalog:
            try:
                ds_id = dataset.get("identifier", "")
                title = dataset.get("title", ds_id)
                distributions = dataset.get("distribution", [])

                csv_dists = [
                    d
                    for d in distributions
                    if d.get("format", "").lower() == "csv"
                    or d.get("mediaType", "").lower() == "text/csv"
                ]
                if not csv_dists:
                    results["skipped"] += 1
                    continue

                download_url = csv_dists[0].get("downloadURL", "")
                if not download_url:
                    results["skipped"] += 1
                    continue

                table_name = f"{portal.table_prefix}_{_sanitize_name(ds_id)}"

                with httpx.Client(timeout=120.0) as client:
                    try:
                        head = client.head(download_url, follow_redirects=True)
                        cl = int(head.headers.get("content-length", 0))
                        if cl > MAX_DOWNLOAD_BYTES:
                            continue
                    except Exception:
                        pass

                    resp = client.get(download_url, follow_redirects=True)
                    resp.raise_for_status()

                try:
                    df = pd.read_csv(
                        io.BytesIO(resp.content),
                        encoding="utf-8",
                        on_bad_lines="skip",
                    )
                except Exception:
                    df = pd.read_csv(
                        io.BytesIO(resp.content),
                        encoding="latin-1",
                        on_bad_lines="skip",
                    )

                if df.empty:
                    results["skipped"] += 1
                    continue

                if len(df) > MAX_ROWS:
                    df = df.head(MAX_ROWS)

                df.to_sql(table_name, engine, if_exists="replace", index=False)

                dataset_id = _register_dataset(
                    engine, portal, ds_id, title, table_name, df, download_url
                )
                if dataset_id:
                    from app.infrastructure.celery.tasks.scraper_tasks import (
                        index_dataset_embedding,
                    )

                    index_dataset_embedding.delay(dataset_id)

                results["ingested"] += 1
                logger.info("DKAN %s %s: %d rows", portal.portal_key, ds_id, len(df))

            except Exception:
                results["errors"] += 1
                logger.warning(
                    "DKAN %s: failed %s",
                    portal.portal_key,
                    dataset.get("identifier", "?"),
                    exc_info=True,
                )

        return results

    finally:
        engine.dispose()


@celery_app.task(
    name="openarg.scrape_dkan_rosario",
    bind=True,
    max_retries=3,
    soft_time_limit=600,
    time_limit=720,
)
def scrape_dkan_rosario(self):
    """Scrape Rosario DKAN catalog and cache CSV datasets."""
    try:
        return _scrape_dkan_portal(DKAN_PORTALS["rosario"])
    except SoftTimeLimitExceeded:
        logger.error("DKAN Rosario scrape timed out")
        raise
    except Exception as exc:
        logger.exception("DKAN Rosario scrape failed")
        raise self.retry(exc=exc, countdown=120)


@celery_app.task(
    name="openarg.scrape_dkan_jujuy",
    bind=True,
    max_retries=3,
    soft_time_limit=600,
    time_limit=720,
)
def scrape_dkan_jujuy(self):
    """Scrape Jujuy DKAN catalog and cache CSV datasets."""
    try:
        return _scrape_dkan_portal(DKAN_PORTALS["jujuy"])
    except SoftTimeLimitExceeded:
        logger.error("DKAN Jujuy scrape timed out")
        raise
    except Exception as exc:
        logger.exception("DKAN Jujuy scrape failed")
        raise self.retry(exc=exc, countdown=120)
