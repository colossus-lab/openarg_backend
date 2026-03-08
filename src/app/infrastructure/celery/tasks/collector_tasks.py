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
import tempfile
from urllib.parse import urlparse

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

# Domains with known SSL certificate issues (self-signed, missing intermediates).
_DOMAINS_SKIP_SSL = frozenset({
    "datos.salud.gob.ar",
    "www.mecon.gob.ar",
    "datos.yvera.gob.ar",
    "datosabiertos.desarrollosocial.gob.ar",
    "ide.transporte.gob.ar",
    "datos.energia.gob.ar",
    "www.energia.gob.ar",
    "sgda.energia.gob.ar",
    "datos.ambiente.gob.ar",
    "datos.mininterior.gob.ar",
    "datos.cultura.gob.ar",
    "ciam.ambiente.gob.ar",
})


def _should_verify_ssl(url: str) -> bool:
    """Return False if the URL domain has known SSL cert issues."""
    try:
        host = urlparse(url).hostname or ""
        return host not in _DOMAINS_SKIP_SSL
    except Exception:
        return True

logger = logging.getLogger(__name__)

# After this many total attempts (across Celery retries AND external
# re-dispatches) the task is marked permanently_failed and never retried.
MAX_TOTAL_ATTEMPTS = 5


def _sanitize_table_name(name: str) -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", name.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    return f"cache_{clean[:50]}"


_CONTENT_TYPES = {
    "csv": "text/csv",
    "json": "application/json",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.ms-excel",
    "geojson": "application/geo+json",
    "txt": "text/plain",
    "ods": "application/vnd.oasis.opendocument.spreadsheet",
    "zip": "application/zip",
    "xml": "application/xml",
}


def _detect_format_from_url(url: str, metadata_fmt: str) -> str:
    """Override metadata format if URL extension clearly indicates a different format."""
    if not url:
        return metadata_fmt
    path = url.split("?")[0].split("#")[0].lower()
    # Extract extension using rsplit to avoid .geojson matching .json
    dot_pos = path.rfind(".")
    if dot_pos == -1:
        return metadata_fmt
    ext = path[dot_pos:]
    _ext_map = {
        ".geojson": "geojson",
        ".zip": "zip",
        ".ods": "ods",
        ".txt": "txt",
        ".xml": "xml",
        ".csv": "csv",
        ".json": "json",
        ".xlsx": "xlsx",
        ".xls": "xls",
    }
    return _ext_map.get(ext, metadata_fmt)


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


def _upload_file_to_s3(file_path: str, portal: str, dataset_id: str, filename: str) -> str:
    """Upload a file from disk to S3, retorna la key."""
    import boto3

    bucket = os.getenv("S3_BUCKET", "openarg-datasets")
    region = os.getenv("AWS_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name=region)
    key = f"datasets/{portal}/{dataset_id}/{filename}"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    content_type = _CONTENT_TYPES.get(ext, "application/octet-stream")
    s3.upload_file(
        file_path,
        bucket,
        key,
        ExtraArgs={
            "ContentType": content_type,
            "ContentDisposition": f'attachment; filename="{filename}"',
        },
    )
    return key


def _stream_download(url: str, dest_path: str, verify_ssl: bool = True, max_bytes: int = 500 * 1024 * 1024) -> int:
    """Stream-download a URL to a file on disk. Returns total bytes written.

    Raises ValueError if the download exceeds max_bytes.
    """
    import httpx

    total = 0
    with httpx.Client(timeout=180.0, verify=verify_ssl) as client:
        with client.stream("GET", url, follow_redirects=True) as resp:
            resp.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=256 * 1024):
                    total += len(chunk)
                    if total > max_bytes:
                        raise ValueError(f"file_too_large: {total}+ bytes (limit {max_bytes})")
                    f.write(chunk)
    return total


def _detect_csv_params(file_path: str) -> dict:
    """Read first few lines to detect encoding and separator."""
    params: dict = {"on_bad_lines": "skip"}

    # Try UTF-8 first
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            header = f.readline()
        params["encoding"] = "utf-8"
    except UnicodeDecodeError:
        params["encoding"] = "latin-1"
        with open(file_path, "r", encoding="latin-1") as f:
            header = f.readline()

    # Auto-detect separator
    if header.count(";") > header.count(","):
        params["sep"] = ";"

    return params


def _csv_load_inner(file_path: str, table_name: str, engine,
                    csv_params: dict, chunk_size: int) -> tuple[int, list[str]]:
    """Inner CSV loading loop."""
    total_rows = 0
    columns: list[str] = []

    reader = pd.read_csv(file_path, chunksize=chunk_size, **csv_params)
    for i, chunk_df in enumerate(reader):
        if i == 0 and chunk_df.shape[1] <= 1 and "sep" not in csv_params:
            reader.close()
            csv_params["sep"] = ";"
            reader = pd.read_csv(file_path, chunksize=chunk_size, **csv_params)
            for j, retry_df in enumerate(reader):
                mode = "replace" if j == 0 else "append"
                retry_df.to_sql(table_name, engine, if_exists=mode, index=False)
                total_rows += len(retry_df)
                if j == 0:
                    columns = list(retry_df.columns)
            return total_rows, columns

        mode = "replace" if i == 0 else "append"
        chunk_df.to_sql(table_name, engine, if_exists=mode, index=False)
        total_rows += len(chunk_df)
        if i == 0:
            columns = list(chunk_df.columns)

    return total_rows, columns


def _load_csv_chunked(file_path: str, table_name: str, engine) -> tuple[int, list[str]]:
    """Load a CSV file into PostgreSQL in chunks. Returns (total_rows, columns)."""
    csv_params = _detect_csv_params(file_path)
    chunk_size = 50_000
    try:
        return _csv_load_inner(file_path, table_name, engine, csv_params, chunk_size)
    except pd.errors.ParserError:
        logger.info("CSV C-parser failed, retrying with Python engine")
        csv_params["engine"] = "python"
        return _csv_load_inner(file_path, table_name, engine, csv_params, chunk_size)


def _set_error_status(engine, dataset_id: str, error_msg: str):
    """Mark cached_datasets as permanently_failed for deterministic errors.

    Deterministic errors (unsupported format, file too large, zip bomb, etc.)
    will never succeed on retry, so we mark them permanently_failed immediately
    to prevent infinite re-dispatch loops from bulk_collect_all.
    """
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE cached_datasets SET status = 'permanently_failed', "
                    "error_message = :msg, retry_count = retry_count + 1, "
                    "updated_at = NOW() "
                    "WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"msg": error_msg[:500], "did": dataset_id},
            )
    except Exception:
        logger.debug("Could not update error status for %s", dataset_id, exc_info=True)


@celery_app.task(name="openarg.collect_data", bind=True, max_retries=3, soft_time_limit=600, time_limit=720)
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
        fmt = _detect_format_from_url(download_url, fmt)
        portal, source_id = row.portal, row.source_id

        if not download_url:
            logger.warning(f"Dataset {dataset_id} has no download URL")
            return {"error": "no_download_url"}

        # Check retry_count — skip if permanently failed
        with engine.begin() as conn:
            cd_row = conn.execute(
                text(
                    "SELECT retry_count FROM cached_datasets "
                    "WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"did": dataset_id},
            ).fetchone()
            if cd_row and cd_row.retry_count >= MAX_TOTAL_ATTEMPTS:
                logger.info(
                    f"Dataset {dataset_id} permanently failed "
                    f"after {cd_row.retry_count} attempts, skipping"
                )
                return {"error": "permanently_failed"}

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

        # Stream download to temp file (memory-safe for large files)
        max_download_bytes = 500 * 1024 * 1024  # 500 MB
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{fmt}", dir="/tmp")
        tmp_path = tmp_file.name
        tmp_file.close()
        file_size = 0

        try:
            # HEAD to check Content-Length before downloading
            try:
                with httpx.Client(timeout=30.0, verify=_should_verify_ssl(download_url)) as client:
                    head = client.head(download_url, follow_redirects=True)
                    content_length = int(head.headers.get("content-length", 0))
                    if content_length > max_download_bytes:
                        logger.warning(f"Dataset {dataset_id} too large: {content_length} bytes")
                        _set_error_status(engine, dataset_id, f"file_too_large: {content_length} bytes")
                        return {"error": f"file_too_large: {content_length} bytes"}
            except Exception:
                pass  # HEAD may fail; proceed with stream download

            file_size = _stream_download(
                download_url, tmp_path,
                verify_ssl=_should_verify_ssl(download_url),
                max_bytes=max_download_bytes,
            )

            # Upload raw file to S3 (from disk, not memory)
            s3_key = None
            try:
                filename = f"{source_id}.{fmt}"
                s3_key = _upload_file_to_s3(tmp_path, portal, dataset_id, filename)
                logger.info(f"Uploaded to S3: {s3_key}")
            except Exception:
                logger.warning(f"S3 upload failed for {dataset_id}, continuing without S3", exc_info=True)

            # Parse and load into PostgreSQL
            row_count = 0
            columns: list[str] = []

            if fmt in ("csv", "txt"):
                # Chunked CSV loading — never loads full file into memory
                row_count, columns = _load_csv_chunked(tmp_path, table_name, engine)

            elif fmt == "zip":
                import zipfile
                max_decompressed = 500 * 1024 * 1024
                try:
                    zf = zipfile.ZipFile(tmp_path)
                except zipfile.BadZipFile:
                    logger.warning(f"ZIP {dataset_id}: not a valid ZIP file")
                    _set_error_status(engine, dataset_id, "bad_zip_file")
                    return {"error": "bad_zip_file"}
                total_size = sum(info.file_size for info in zf.infolist())
                if total_size > max_decompressed:
                    logger.warning(f"ZIP {dataset_id}: decompressed size {total_size} exceeds limit")
                    _set_error_status(engine, dataset_id, f"zip_too_large: {total_size} bytes decompressed")
                    return {"error": f"zip_too_large: {total_size} bytes"}

                parsed = False
                for name in zf.namelist():
                    lower = name.lower()
                    if lower.endswith((".csv", ".txt")):
                        # Extract CSV to temp file, then chunked load
                        csv_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", dir="/tmp")
                        csv_tmp_path = csv_tmp.name
                        csv_tmp.close()
                        try:
                            with zf.open(name) as zf_entry, open(csv_tmp_path, "wb") as out:
                                while True:
                                    block = zf_entry.read(256 * 1024)
                                    if not block:
                                        break
                                    out.write(block)
                            row_count, columns = _load_csv_chunked(csv_tmp_path, table_name, engine)
                        finally:
                            try:
                                os.unlink(csv_tmp_path)
                            except OSError:
                                pass
                        parsed = True
                        break
                    elif lower.endswith((".xlsx", ".xls")):
                        with zf.open(name) as f:
                            content = f.read()
                        df = pd.read_excel(io.BytesIO(content))
                        df.to_sql(table_name, engine, if_exists="replace", index=False)
                        row_count, columns = len(df), list(df.columns)
                        parsed = True
                        break
                    elif lower.endswith(".geojson"):
                        with zf.open(name) as f:
                            content = f.read()
                        raw_geo = json.loads(content)
                        features = raw_geo.get("features", []) if isinstance(raw_geo, dict) else []
                        if features:
                            records = []
                            for feat in features:
                                props = feat.get("properties", {}) or {}
                                geom = feat.get("geometry") or {}
                                if geom:
                                    props["geometry_type"] = geom.get("type")
                                records.append(props)
                            df = pd.DataFrame(records)
                        else:
                            df = pd.json_normalize(raw_geo if isinstance(raw_geo, list) else [raw_geo])
                        df.to_sql(table_name, engine, if_exists="replace", index=False)
                        row_count, columns = len(df), list(df.columns)
                        parsed = True
                        break
                    elif lower.endswith(".json"):
                        with zf.open(name) as f:
                            content = f.read()
                        try:
                            df = pd.read_json(io.BytesIO(content))
                        except ValueError:
                            raw_j = json.loads(content)
                            if isinstance(raw_j, list):
                                df = pd.json_normalize(raw_j)
                            elif isinstance(raw_j, dict):
                                for k in ("data", "results", "records", "rows"):
                                    if k in raw_j and isinstance(raw_j[k], list):
                                        df = pd.json_normalize(raw_j[k])
                                        break
                                else:
                                    df = pd.json_normalize([raw_j])
                            else:
                                continue
                        df.to_sql(table_name, engine, if_exists="replace", index=False)
                        row_count, columns = len(df), list(df.columns)
                        parsed = True
                        break
                if not parsed:
                    logger.warning(f"ZIP {dataset_id}: no parseable file found in {zf.namelist()}")
                    _set_error_status(engine, dataset_id, "zip_no_parseable_file")
                    return {"error": "zip_no_parseable_file"}

            elif fmt == "json":
                with open(tmp_path, "rb") as f:
                    content = f.read()
                try:
                    df = pd.read_json(io.BytesIO(content))
                except ValueError:
                    raw = json.loads(content)
                    if isinstance(raw, list):
                        df = pd.json_normalize(raw)
                    elif isinstance(raw, dict):
                        for key in ("data", "results", "records", "features", "rows"):
                            if key in raw and isinstance(raw[key], list):
                                df = pd.json_normalize(raw[key])
                                break
                        else:
                            df = pd.json_normalize([raw])
                    else:
                        raise
                df.to_sql(table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            elif fmt == "geojson":
                with open(tmp_path, "rb") as f:
                    raw = json.load(f)
                features = raw.get("features", []) if isinstance(raw, dict) else []
                if features:
                    records = []
                    for feat in features:
                        props = feat.get("properties", {}) or {}
                        geom = feat.get("geometry") or {}
                        if geom:
                            props["geometry_type"] = geom.get("type")
                            coords = geom.get("coordinates")
                            if coords:
                                coord_str = str(coords)
                                if len(coord_str) > 2000:
                                    coord_str = coord_str[:2000] + "..."
                                props["geometry_coordinates"] = coord_str
                        records.append(props)
                    df = pd.DataFrame(records)
                else:
                    logger.warning(f"GeoJSON {dataset_id}: no features found")
                    _set_error_status(engine, dataset_id, "geojson_no_features")
                    return {"error": "geojson_no_features"}
                df.to_sql(table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            elif fmt in ("xlsx", "xls"):
                df = pd.read_excel(tmp_path)
                df.to_sql(table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            elif fmt == "ods":
                df = pd.read_excel(tmp_path, engine="odf")
                df.to_sql(table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            elif fmt == "xml":
                try:
                    df = pd.read_xml(tmp_path)
                except Exception:
                    logger.warning(f"XML {dataset_id}: pd.read_xml failed, skipping")
                    _set_error_status(engine, dataset_id, "xml_parse_failed")
                    return {"error": "xml_parse_failed"}
                df.to_sql(table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            else:
                logger.warning(f"Unsupported format: {fmt}")
                _set_error_status(engine, dataset_id, f"unsupported_format: {fmt}")
                return {"error": f"unsupported_format: {fmt}"}

            # Update cache status
            columns_json = json.dumps([str(c) for c in columns])
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE cached_datasets SET
                            status = 'ready', row_count = :rows,
                            columns_json = :cols, size_bytes = :size,
                            s3_key = :s3key, updated_at = NOW()
                        WHERE dataset_id = CAST(:did AS uuid)
                    """),
                    {
                        "rows": row_count,
                        "cols": columns_json,
                        "size": file_size,
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
                    {"rows": row_count, "id": dataset_id},
                )

            # Re-embed only on first cache (enriches with sample rows); skip if already embedded
            if not was_cached:
                from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

                index_dataset_embedding.delay(dataset_id)

            logger.info(f"Dataset {dataset_id} cached: {row_count} rows in table {table_name}")
            return {
                "dataset_id": dataset_id,
                "table_name": table_name,
                "rows": row_count,
                "columns": columns,
                "s3_key": s3_key,
            }

        finally:
            # Always clean up temp file
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    except SoftTimeLimitExceeded:
        logger.error(f"Task timed out for dataset {dataset_id}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE cached_datasets "
                    "SET retry_count = retry_count + 1, "
                    "    error_message = :msg, "
                    "    updated_at = NOW(), "
                    "    status = CASE "
                    "      WHEN retry_count + 1 >= :max THEN 'permanently_failed' "
                    "      ELSE 'error' "
                    "    END "
                    "WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"msg": "Task timed out", "did": dataset_id, "max": MAX_TOTAL_ATTEMPTS},
            )
        raise
    except Exception as exc:
        exc_str = str(exc)
        # Non-retryable errors — mark permanently failed immediately
        non_retryable = (
            any(s in exc_str for s in (
                "403 Forbidden", "401 Unauthorized",
                "illegal status line", "Request Denied",
                "Name or service not known",
            ))
            or "TooManyRedirects" in type(exc).__name__
        )
        if non_retryable:
            logger.warning("Non-retryable error for %s: %s", dataset_id, exc_str[:200])
            _set_error_status(engine, dataset_id, exc_str[:500])
            return {"error": "non_retryable"}

        logger.exception(f"Collection failed for dataset {dataset_id}")
        with engine.begin() as conn:
            # Increment retry_count and check if we should give up
            conn.execute(
                text(
                    "UPDATE cached_datasets "
                    "SET retry_count = retry_count + 1, "
                    "    error_message = :msg, "
                    "    updated_at = NOW(), "
                    "    status = CASE "
                    "      WHEN retry_count + 1 >= :max THEN 'permanently_failed' "
                    "      ELSE 'error' "
                    "    END "
                    "WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {
                    "msg": str(exc)[:500],
                    "did": dataset_id,
                    "max": MAX_TOTAL_ATTEMPTS,
                },
            )
            current = conn.execute(
                text(
                    "SELECT retry_count FROM cached_datasets "
                    "WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"did": dataset_id},
            ).fetchone()

        attempts = current.retry_count if current else 0
        if attempts >= MAX_TOTAL_ATTEMPTS:
            logger.warning(
                f"Dataset {dataset_id} permanently failed "
                f"after {attempts} attempts: {exc}"
            )
            return {"error": "permanently_failed", "attempts": attempts}

        raise self.retry(exc=exc, countdown=60) from exc
    finally:
        engine.dispose()


@celery_app.task(name="openarg.bulk_collect_all", bind=True, soft_time_limit=60, time_limit=120)
def bulk_collect_all(self, portal: str | None = None):
    """Descarga y cachea TODOS los datasets no cacheados."""
    engine = get_sync_engine()

    try:
        # Skip datasets that have permanently failed collection
        query = (
            "SELECT CAST(d.id AS text) FROM datasets d "
            "LEFT JOIN cached_datasets cd ON cd.dataset_id = d.id "
            "WHERE d.is_cached = false "
            "AND (cd.status IS NULL OR cd.status != 'permanently_failed')"
        )
        params: dict = {}
        if portal:
            query += " AND d.portal = :portal"
            params["portal"] = portal
        query += " ORDER BY d.portal, d.title LIMIT 5000"

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

        # 1. Recover stuck cached_datasets (skip permanently failed)
        with engine.begin() as conn:
            stuck_downloads = conn.execute(
                text("""
                    SELECT CAST(dataset_id AS text) AS dataset_id,
                           table_name, retry_count
                    FROM cached_datasets
                    WHERE status = 'downloading'
                      AND updated_at < NOW() - INTERVAL '30 minutes'
                      AND retry_count < :max
                """),
                {"max": MAX_TOTAL_ATTEMPTS},
            ).fetchall()

            for row in stuck_downloads:
                new_count = (row.retry_count or 0) + 1
                if new_count >= MAX_TOTAL_ATTEMPTS:
                    conn.execute(
                        text("""
                            UPDATE cached_datasets
                            SET status = 'permanently_failed',
                                retry_count = :cnt,
                                error_message = 'Permanently failed: stuck in downloading too many times',
                                updated_at = NOW()
                            WHERE dataset_id = CAST(:did AS uuid)
                              AND status = 'downloading'
                        """),
                        {"did": row.dataset_id, "cnt": new_count},
                    )
                else:
                    conn.execute(
                        text("""
                            UPDATE cached_datasets
                            SET status = 'error',
                                retry_count = :cnt,
                                error_message = 'Recovered: stuck in downloading state',
                                updated_at = NOW()
                            WHERE dataset_id = CAST(:did AS uuid)
                              AND status = 'downloading'
                        """),
                        {"did": row.dataset_id, "cnt": new_count},
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


@celery_app.task(
    name="openarg.reset_failed_collectors",
    soft_time_limit=60,
    time_limit=120,
)
def reset_failed_collectors():
    """Weekly reset: give permanently_failed datasets another chance.

    Resets retry_count and status so the next bulk_collect cycle
    picks them up again.  If the underlying problem persists they
    will fail 5 more times and go back to permanently_failed.
    """
    engine = get_sync_engine()
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    UPDATE cached_datasets
                    SET status  = 'error',
                        retry_count = 0,
                        updated_at  = NOW()
                    WHERE status = 'permanently_failed'
                """),
            )
            count = result.rowcount
        if count:
            logger.info(
                "Reset %d permanently_failed datasets "
                "for retry", count,
            )
        else:
            logger.debug("No permanently_failed datasets to reset")
        return {"reset": count}
    finally:
        engine.dispose()
