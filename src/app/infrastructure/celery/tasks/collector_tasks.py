"""
Collector Worker — Agente recolector de datos.

Dado un dataset_id, descarga el archivo real (CSV/JSON/XLSX),
lo parsea con pandas y lo cachea en PostgreSQL para consultas SQL.
"""
from __future__ import annotations

import io
import json
import logging
import os
import re

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


def _sanitize_table_name(name: str) -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", name.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    return f"cache_{clean[:50]}"


_CONTENT_TYPES = {
    "csv": "text/csv",
    "json": "application/json",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.ms-excel",
}


def _upload_to_s3(content: bytes, portal: str, dataset_id: str, filename: str) -> str:
    """Sube archivo crudo a S3, retorna la key."""
    import boto3

    bucket = os.getenv("S3_BUCKET", "openarg-datasets")
    region = os.getenv("AWS_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name=region)
    key = f"datasets/{portal}/{dataset_id}/{filename}"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    content_type = _CONTENT_TYPES.get(ext, "application/octet-stream")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=content,
        ContentType=content_type,
        ContentDisposition=f'attachment; filename="{filename}"',
    )
    return key


@celery_app.task(name="openarg.collect_data", bind=True, max_retries=3, soft_time_limit=300, time_limit=360)
def collect_dataset(self, dataset_id: str):
    """
    Descarga un dataset real, lo parsea y lo guarda como tabla SQL.
    Esto permite al Analyst hacer queries SQL reales sobre los datos.
    """
    import httpx

    logger.info(f"Collecting dataset: {dataset_id}")
    engine = get_sync_engine()

    try:
        # Get dataset info
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT title, download_url, format, portal, source_id "
                    "FROM datasets WHERE id = CAST(:id AS uuid)"
                ),
                {"id": dataset_id},
            ).fetchone()

        if not row:
            logger.warning(f"Dataset {dataset_id} not found")
            return {"error": "not_found"}

        title, download_url, fmt = row.title, row.download_url, row.format
        portal, source_id = row.portal, row.source_id

        if not download_url:
            logger.warning(f"Dataset {dataset_id} has no download URL")
            return {"error": "no_download_url"}

        # Check if already cached
        table_name = _sanitize_table_name(title)
        with engine.begin() as conn:
            cached = conn.execute(
                text(
                    "SELECT id FROM cached_datasets WHERE dataset_id = CAST(:did AS uuid) AND status = 'ready'"
                ),
                {"did": dataset_id},
            ).fetchone()

        if cached:
            logger.info(f"Dataset {dataset_id} already cached as {table_name}")
            return {"dataset_id": dataset_id, "table_name": table_name, "status": "already_cached"}

        # Update status
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO cached_datasets (dataset_id, table_name, status, updated_at)
                    VALUES (CAST(:did AS uuid), :tn, 'downloading', NOW())
                    ON CONFLICT (table_name) DO UPDATE SET status = 'downloading', updated_at = NOW()
                """),
                {"did": dataset_id, "tn": table_name},
            )

        # Download (with size guard: reject files > 500MB)
        max_download_bytes = 500 * 1024 * 1024  # 500 MB
        with httpx.Client(timeout=120.0) as client:
            # HEAD to check Content-Length before downloading
            try:
                head = client.head(download_url, follow_redirects=True)
                content_length = int(head.headers.get("content-length", 0))
                if content_length > max_download_bytes:
                    logger.warning(f"Dataset {dataset_id} too large: {content_length} bytes")
                    return {"error": f"file_too_large: {content_length} bytes"}
            except Exception:
                pass  # HEAD may fail; proceed with GET

            resp = client.get(download_url, follow_redirects=True)
            resp.raise_for_status()

            if len(resp.content) > max_download_bytes:
                logger.warning(f"Dataset {dataset_id} download too large: {len(resp.content)} bytes")
                return {"error": f"file_too_large: {len(resp.content)} bytes"}

        # Upload raw file to S3
        s3_key = None
        try:
            filename = f"{source_id}.{fmt}"
            s3_key = _upload_to_s3(resp.content, portal, dataset_id, filename)
            logger.info(f"Uploaded to S3: {s3_key}")
        except Exception:
            logger.warning(f"S3 upload failed for {dataset_id}, continuing without S3", exc_info=True)

        # Parse with pandas
        df: pd.DataFrame
        if fmt == "csv":
            df = pd.read_csv(io.BytesIO(resp.content), encoding="utf-8", on_bad_lines="skip")
        elif fmt == "json":
            df = pd.read_json(io.BytesIO(resp.content))
        elif fmt in ("xlsx", "xls"):
            df = pd.read_excel(io.BytesIO(resp.content))
        else:
            logger.warning(f"Unsupported format: {fmt}")
            return {"error": f"unsupported_format: {fmt}"}

        # Limit size
        if len(df) > 500_000:
            df = df.head(500_000)
            logger.warning(f"Dataset {dataset_id} truncated to 500k rows")

        # Write to PostgreSQL
        df.to_sql(table_name, engine, if_exists="replace", index=False)

        # Update cache status
        columns_json = json.dumps(list(df.columns))
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE cached_datasets SET
                        status = 'ready', row_count = :rows,
                        columns_json = :cols, size_bytes = :size,
                        s3_key = :s3key
                    WHERE dataset_id = CAST(:did AS uuid)
                """),
                {
                    "rows": len(df),
                    "cols": columns_json,
                    "size": len(resp.content),
                    "s3key": s3_key,
                    "did": dataset_id,
                },
            )

        # Update dataset — track whether this is a new cache for conditional re-embedding
        with engine.begin() as conn:
            prev = conn.execute(
                text("SELECT is_cached FROM datasets WHERE id = CAST(:id AS uuid)"),
                {"id": dataset_id},
            ).fetchone()
            was_cached = prev.is_cached if prev else False
            conn.execute(
                text("UPDATE datasets SET is_cached = true, row_count = :rows WHERE id = CAST(:id AS uuid)"),
                {"rows": len(df), "id": dataset_id},
            )

        # Re-embed only on first cache (enriches with sample rows); skip if already embedded
        if not was_cached:
            from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

            index_dataset_embedding.delay(dataset_id)

        logger.info(f"Dataset {dataset_id} cached: {len(df)} rows in table {table_name}")
        return {
            "dataset_id": dataset_id,
            "table_name": table_name,
            "rows": len(df),
            "columns": list(df.columns),
            "s3_key": s3_key,
        }

    except SoftTimeLimitExceeded:
        logger.error(f"Task timed out for dataset {dataset_id}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE cached_datasets SET status = 'error', error_message = :msg "
                    "WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"msg": "Task timed out", "did": dataset_id},
            )
        raise
    except Exception as exc:
        logger.exception(f"Collection failed for dataset {dataset_id}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE cached_datasets SET status = 'error', error_message = :msg "
                    "WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"msg": str(exc), "did": dataset_id},
            )
        raise self.retry(exc=exc, countdown=60)
    finally:
        engine.dispose()


@celery_app.task(name="openarg.bulk_collect_all", bind=True, soft_time_limit=60, time_limit=120)
def bulk_collect_all(self, portal: str | None = None):
    """Descarga y cachea TODOS los datasets no cacheados."""
    engine = get_sync_engine()

    try:
        query = "SELECT CAST(id AS text) FROM datasets WHERE is_cached = false"
        params: dict = {}
        if portal:
            query += " AND portal = :portal"
            params["portal"] = portal
        query += " ORDER BY portal, title LIMIT 5000"

        with engine.connect() as conn:
            rows = conn.execute(text(query), params).fetchall()

        logger.info(f"Bulk collect: {len(rows)} uncached datasets to process")
        from celery import group
        tasks = [collect_dataset.s(row[0]) for row in rows]
        if tasks:
            group(tasks).apply_async()

        return {"dispatched": len(rows)}
    finally:
        engine.dispose()


@celery_app.task(name="openarg.recover_stuck_tasks", bind=True, soft_time_limit=60, time_limit=120)
def recover_stuck_tasks(self):
    """
    Recover tasks stuck in intermediate states for more than 30 minutes.
    - cached_datasets stuck in 'downloading'
    - user_queries stuck in intermediate states (planning, collecting, analyzing)
    Re-dispatches collect_dataset for stuck downloads.
    """
    engine = get_sync_engine()
    try:
        recovered_downloads = 0
        recovered_queries = 0

        # 1. Recover stuck cached_datasets
        with engine.begin() as conn:
            stuck_downloads = conn.execute(
                text("""
                    SELECT CAST(dataset_id AS text) AS dataset_id, table_name
                    FROM cached_datasets
                    WHERE status = 'downloading'
                      AND updated_at < NOW() - INTERVAL '30 minutes'
                """),
            ).fetchall()

            for row in stuck_downloads:
                conn.execute(
                    text("""
                        UPDATE cached_datasets
                        SET status = 'error', error_message = 'Recovered: stuck in downloading state', updated_at = NOW()
                        WHERE dataset_id = CAST(:did AS uuid) AND status = 'downloading'
                    """),
                    {"did": row.dataset_id},
                )
                collect_dataset.delay(row.dataset_id)
                recovered_downloads += 1

        # 2. Recover stuck user_queries
        with engine.begin() as conn:
            stuck_queries = conn.execute(
                text("""
                    SELECT CAST(id AS text) AS id, status
                    FROM user_queries
                    WHERE status IN ('planning', 'collecting', 'analyzing')
                      AND updated_at < NOW() - INTERVAL '30 minutes'
                """),
            ).fetchall()

            for row in stuck_queries:
                conn.execute(
                    text("""
                        UPDATE user_queries
                        SET status = 'error', error_message = 'Recovered: stuck in intermediate state', updated_at = NOW()
                        WHERE id = CAST(:id AS uuid)
                    """),
                    {"id": row.id},
                )
                recovered_queries += 1

        if recovered_downloads or recovered_queries:
            logger.warning(
                "Recovered stuck tasks: %d downloads, %d queries",
                recovered_downloads, recovered_queries,
            )
        else:
            logger.debug("No stuck tasks found")

        return {
            "recovered_downloads": recovered_downloads,
            "recovered_queries": recovered_queries,
        }
    finally:
        engine.dispose()
