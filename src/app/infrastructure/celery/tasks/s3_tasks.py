"""
S3 Worker — Manejo dedicado de uploads a S3.

Reintenta uploads fallidos y permite subir datasets individualmente.
"""
from __future__ import annotations

import logging
import os

from sqlalchemy import create_engine, text

from app.infrastructure.celery.app import celery_app

logger = logging.getLogger(__name__)


def _get_sync_engine():
    url = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db")
    return create_engine(url)


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


@celery_app.task(name="openarg.retry_s3_uploads", bind=True)
def retry_s3_uploads(self):
    """Reintenta subir a S3 datasets cacheados que no tienen s3_key."""
    engine = _get_sync_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT cd.id, cd.dataset_id, cd.table_name, "
                "       d.portal, d.source_id, d.format, d.download_url "
                "FROM cached_datasets cd "
                "JOIN datasets d ON d.id = cd.dataset_id "
                "WHERE cd.status = 'ready' AND (cd.s3_key IS NULL OR cd.s3_key = '') "
                "LIMIT 50"
            )).fetchall()

        logger.info(f"S3 retry: {len(rows)} datasets missing s3_key")
        for row in rows:
            upload_dataset_to_s3.delay(str(row.dataset_id))

        return {"dispatched": len(rows)}
    finally:
        engine.dispose()


@celery_app.task(name="openarg.upload_to_s3", bind=True, max_retries=3)
def upload_dataset_to_s3(self, dataset_id: str):
    """Descarga y sube un dataset a S3."""
    import httpx

    engine = _get_sync_engine()
    try:
        # 1. Leer info del dataset
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT d.download_url, d.portal, d.source_id, d.format, cd.s3_key "
                    "FROM datasets d "
                    "JOIN cached_datasets cd ON cd.dataset_id = d.id "
                    "WHERE d.id = CAST(:did AS uuid) AND cd.status = 'ready'"
                ),
                {"did": dataset_id},
            ).fetchone()

        if not row:
            logger.warning(f"Dataset {dataset_id} not found or not ready")
            return {"error": "not_found_or_not_ready"}

        # 2. Verificar que no tenga ya s3_key
        if row.s3_key:
            logger.info(f"Dataset {dataset_id} already has s3_key: {row.s3_key}")
            return {"status": "already_uploaded", "s3_key": row.s3_key}

        if not row.download_url:
            logger.warning(f"Dataset {dataset_id} has no download URL")
            return {"error": "no_download_url"}

        # 3. Descargar via httpx
        client = httpx.Client(timeout=120.0)
        resp = client.get(row.download_url, follow_redirects=True)
        resp.raise_for_status()
        client.close()

        # 4. Subir a S3
        filename = f"{row.source_id}.{row.format}"
        s3_key = _upload_to_s3(resp.content, row.portal, dataset_id, filename)
        logger.info(f"Uploaded to S3: {s3_key}")

        # 5. Actualizar s3_key en cached_datasets
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE cached_datasets SET s3_key = :s3key "
                    "WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"s3key": s3_key, "did": dataset_id},
            )

        return {"dataset_id": dataset_id, "s3_key": s3_key}

    except Exception as exc:
        logger.exception(f"S3 upload failed for dataset {dataset_id}")
        raise self.retry(exc=exc, countdown=60)
    finally:
        engine.dispose()
