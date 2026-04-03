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
import random
import re
import tempfile
from urllib.parse import urlparse

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

# Domains with known SSL certificate issues (self-signed, missing intermediates).
_DOMAINS_SKIP_SSL = frozenset(
    {
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
    }
)


logger = logging.getLogger(__name__)


def _should_verify_ssl(url: str) -> bool:
    """Return False if the URL domain has known SSL cert issues."""
    try:
        host = urlparse(url).hostname or ""
        return host not in _DOMAINS_SKIP_SSL
    except Exception:
        logger.debug("Failed to parse URL for SSL check: %s", url, exc_info=True)
        return True


# After this many total attempts (across Celery retries AND external
# re-dispatches) the task is marked permanently_failed and never retried.
MAX_TOTAL_ATTEMPTS = 5

# Maximum rows to store per dataset table.  Datasets exceeding this limit
# are truncated to the first MAX_TABLE_ROWS rows.
MAX_TABLE_ROWS = 500_000


def _sanitize_table_name(name: str, portal: str = "") -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", name.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    if portal:
        portal_clean = re.sub(r"[^a-z0-9_]", "_", portal.lower()).strip("_")
        # Enforce PostgreSQL 63-char identifier limit:
        # "cache_" (6) + portal (max 12) + "_" (1) + name (max 44) = 63
        return f"cache_{portal_clean[:12]}_{clean[:44]}"
    return f"cache_{clean[:57]}"


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


_MAX_GEOJSON_GEOMETRY_BYTES = 100 * 1024  # 100KB per feature geometry


def _geojson_features_to_df(features: list[dict]) -> pd.DataFrame:
    """Convert GeoJSON features to a DataFrame preserving full geometry.

    Each feature's ``geometry`` is serialised as compact JSON in the
    ``_geometry_geojson`` column.  Geometries larger than
    ``_MAX_GEOJSON_GEOMETRY_BYTES`` are replaced by their centroid (for
    Points) or dropped to avoid bloating the database.
    """
    records: list[dict] = []
    for feat in features:
        props = dict(feat.get("properties", {}) or {})
        geom = feat.get("geometry") or {}
        if geom:
            props["geometry_type"] = geom.get("type")
            geom_str = json.dumps(geom, separators=(",", ":"))
            if len(geom_str) > _MAX_GEOJSON_GEOMETRY_BYTES:
                # Oversized → store centroid for Point-like, skip otherwise
                coords = geom.get("coordinates")
                gtype = geom.get("type", "")
                if gtype == "Point" and coords:
                    props["_geometry_geojson"] = geom_str
                elif coords:
                    # Best-effort centroid: first coordinate of the first ring
                    try:
                        c = coords
                        while isinstance(c, list) and c and isinstance(c[0], list):
                            c = c[0]
                        if isinstance(c, list) and len(c) >= 2:
                            centroid = {"type": "Point", "coordinates": c[:2]}
                            props["_geometry_geojson"] = json.dumps(centroid, separators=(",", ":"))
                    except Exception:
                        pass  # skip geometry entirely
            else:
                props["_geometry_geojson"] = geom_str
        records.append(props)
    return pd.DataFrame(records)


def _read_shapefile_from_zip(zip_path: str) -> pd.DataFrame | None:
    """Read a shapefile from a ZIP archive using fiona, return DataFrame or None."""
    try:
        import fiona
    except ImportError:
        logger.warning("fiona not installed — cannot parse shapefiles")
        return None

    try:
        # fiona can read directly from zip:// paths
        layers = fiona.listlayers(f"zip://{zip_path}")
        if not layers:
            return None
        with fiona.open(f"zip://{zip_path}", layer=layers[0]) as src:
            features = []
            for i, feat in enumerate(src):
                if i >= MAX_TABLE_ROWS:
                    break
                # fiona features are dict-like; convert geometry to GeoJSON dict
                geom = dict(feat.get("geometry", {})) if feat.get("geometry") else {}
                props = dict(feat.get("properties", {})) if feat.get("properties") else {}
                features.append({"type": "Feature", "geometry": geom, "properties": props})
            if not features:
                return None
            return _geojson_features_to_df(features)
    except Exception:
        logger.warning("Failed to read shapefile from %s", zip_path, exc_info=True)
        return None


def _stream_download(
    url: str, dest_path: str, verify_ssl: bool = True, max_bytes: int = 500 * 1024 * 1024
) -> int:
    """Stream-download a URL to a file on disk. Returns total bytes written.

    Raises ValueError if the download exceeds max_bytes.
    """
    import httpx

    total = 0
    # connect=30s, read=60s per chunk (not total). Celery soft_time_limit handles total.
    dl_timeout = httpx.Timeout(connect=30.0, read=60.0, write=30.0, pool=30.0)
    with httpx.Client(timeout=dl_timeout, verify=verify_ssl) as client:
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
        with open(file_path, encoding="utf-8") as f:
            header = f.readline()
        params["encoding"] = "utf-8"
    except UnicodeDecodeError:
        params["encoding"] = "latin-1"
        with open(file_path, encoding="latin-1") as f:
            header = f.readline()

    # Auto-detect separator
    if header.count(";") > header.count(","):
        params["sep"] = ";"

    return params


def _csv_load_inner(
    file_path: str,
    table_name: str,
    engine,
    csv_params: dict,
    chunk_size: int,
    max_rows: int = 0,
    source_dataset_id: str | None = None,
    force_append: bool = False,
) -> tuple[int, list[str], bool]:
    """Inner CSV loading loop.

    Returns ``(total_rows_written, columns, was_truncated)``.
    When *max_rows* > 0 the loader stops after writing that many rows.
    When *source_dataset_id* is provided, adds ``_source_dataset_id`` column.
    When *force_append* is True, all chunks use ``if_exists='append'``.
    """
    total_rows = 0
    columns: list[str] = []
    truncated = False

    def _write_chunk(chunk_df: pd.DataFrame, is_first: bool) -> bool:
        """Write a single chunk, truncating if the cap would be exceeded.

        Returns True when the row cap has been reached.
        """
        nonlocal total_rows, columns, truncated
        if max_rows:
            remaining = max_rows - total_rows
            if remaining <= 0:
                truncated = True
                return True
            if len(chunk_df) > remaining:
                chunk_df = chunk_df.iloc[:remaining]
                truncated = True
        if source_dataset_id:
            chunk_df["_source_dataset_id"] = source_dataset_id
        if force_append:
            mode = "append"
        else:
            mode = "replace" if is_first else "append"
        _to_sql_safe(chunk_df, table_name, engine, if_exists=mode, index=False)
        total_rows += len(chunk_df)
        if is_first:
            columns = list(chunk_df.columns)
        return truncated

    reader = pd.read_csv(file_path, chunksize=chunk_size, **csv_params)
    for i, chunk_df in enumerate(reader):
        if i == 0 and chunk_df.shape[1] <= 1 and "sep" not in csv_params:
            reader.close()
            csv_params["sep"] = ";"
            reader = pd.read_csv(file_path, chunksize=chunk_size, **csv_params)
            for j, retry_df in enumerate(reader):
                if _write_chunk(retry_df, is_first=(j == 0)):
                    break
            return total_rows, columns, truncated

        if _write_chunk(chunk_df, is_first=(i == 0)):
            break

    return total_rows, columns, truncated


def _drop_table_if_exists(engine, table_name: str):
    """Explicitly DROP a cache table if it exists."""
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))  # noqa: S608
    logger.info("Dropped table %s for schema refresh", table_name)


def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Clean column names: strip whitespace, non-breaking spaces, normalize.

    If most columns are 'Unnamed: N', treat the first data row as the real header.
    """
    df.columns = [
        col.replace("\xa0", "").strip() if isinstance(col, str) else col for col in df.columns
    ]

    # Detect misplaced header: if >50% of columns are "Unnamed: N", promote first row
    unnamed_count = sum(1 for c in df.columns if str(c).startswith("Unnamed:"))
    if unnamed_count > len(df.columns) * 0.5 and len(df) > 1:
        new_header = [
            str(v).strip() if pd.notna(v) else f"col_{i}" for i, v in enumerate(df.iloc[0])
        ]
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = new_header

    return df


def _to_sql_safe(df: pd.DataFrame, table_name: str, engine, **kwargs):
    """Write DataFrame to SQL, retrying with DROP if schema mismatch occurs."""
    df = _sanitize_columns(df)
    try:
        df.to_sql(table_name, engine, **kwargs)
    except Exception as exc:
        exc_str = str(exc).lower()
        schema_keywords = (
            "column",
            "type mismatch",
            "incompatible",
            "does not exist",
            "undefined column",
            "schema",
            "relation",
        )
        if any(kw in exc_str for kw in schema_keywords):
            logger.warning(
                "Schema mismatch on table %s, dropping and retrying: %s",
                table_name,
                str(exc)[:200],
            )
            _drop_table_if_exists(engine, table_name)
            kwargs["if_exists"] = "replace"
            df.to_sql(table_name, engine, **kwargs)
        else:
            raise


def _get_table_row_count(engine, table_name: str) -> int:
    """Get current row count for a cache table. Returns 0 if table doesn't exist."""
    try:
        with engine.connect() as conn:
            count = conn.execute(
                text(f'SELECT COUNT(*) FROM "{table_name}"')  # noqa: S608
            ).scalar()
            conn.rollback()
            return count or 0
    except Exception:
        return 0


def _ensure_postgis_geom(engine, table_name: str, columns: list[str]) -> None:
    """Add a native PostGIS geom column if the table has _geometry_geojson.

    Idempotent — skips if geom column already exists or PostGIS unavailable.
    """
    if "_geometry_geojson" not in columns:
        return
    try:
        tbl = f'"{table_name}"'
        idx_name = f"idx_{table_name}_geom"[:63]
        with engine.begin() as conn:
            exists = conn.execute(
                text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = 'public' "
                    "AND table_name = :tn AND column_name = 'geom'"
                ),
                {"tn": table_name},
            ).fetchone()
            if exists:
                return
            # Ensure PostGIS is available
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            conn.execute(text(f"ALTER TABLE {tbl} ADD COLUMN geom geometry(Geometry, 4326)"))
            conn.execute(
                text(
                    f"UPDATE {tbl} SET geom = ST_SetSRID(ST_GeomFromGeoJSON(_geometry_geojson), 4326) "
                    f"WHERE _geometry_geojson IS NOT NULL AND _geometry_geojson != '' "
                    f"AND _geometry_geojson ~ '^\\s*\\{{' AND geom IS NULL"
                )
            )
            conn.execute(
                text(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON {tbl} USING gist (geom)')
            )
        logger.info("PostGIS geom column added to %s", table_name)
    except Exception:
        logger.debug("PostGIS geom column skipped for %s", table_name, exc_info=True)


def _load_csv_chunked(
    file_path: str,
    table_name: str,
    engine,
    source_dataset_id: str | None = None,
    force_append: bool = False,
) -> tuple[int, list[str], bool]:
    """Load a CSV file into PostgreSQL in chunks.

    Returns ``(total_rows, columns, was_truncated)``.
    Stops after ``MAX_TABLE_ROWS`` rows.
    """
    csv_params = _detect_csv_params(file_path)
    chunk_size = 50_000
    try:
        return _csv_load_inner(
            file_path,
            table_name,
            engine,
            csv_params,
            chunk_size,
            max_rows=MAX_TABLE_ROWS,
            source_dataset_id=source_dataset_id,
            force_append=force_append,
        )
    except pd.errors.ParserError:
        logger.info("CSV C-parser failed, retrying with Python engine")
        csv_params["engine"] = "python"
        return _csv_load_inner(
            file_path,
            table_name,
            engine,
            csv_params,
            chunk_size,
            max_rows=MAX_TABLE_ROWS,
            source_dataset_id=source_dataset_id,
            force_append=force_append,
        )


def _ensure_cached_entry(engine, dataset_id: str, table_name: str):
    """Ensure a cached_datasets row exists for this dataset (idempotent)."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO cached_datasets (dataset_id, table_name, status, updated_at) "
                    "VALUES (CAST(:did AS uuid), :tn, 'downloading', NOW()) "
                    "ON CONFLICT (table_name) DO NOTHING"
                ),
                {"did": dataset_id, "tn": table_name},
            )
    except Exception:
        logger.debug("Could not ensure cached entry for %s", dataset_id, exc_info=True)


def _set_error_status(engine, dataset_id: str, error_msg: str, *, table_name: str | None = None):
    """Mark cached_datasets as permanently_failed for deterministic errors.

    Deterministic errors (unsupported format, file too large, zip bomb, etc.)
    will never succeed on retry, so we mark them permanently_failed immediately
    to prevent infinite re-dispatch loops from bulk_collect_all.
    """
    try:
        with engine.begin() as conn:
            if table_name:
                # UPSERT: works even if no row exists yet
                conn.execute(
                    text(
                        "INSERT INTO cached_datasets "
                        "(dataset_id, table_name, status, error_message, retry_count, updated_at) "
                        "VALUES (CAST(:did AS uuid), :tn, 'permanently_failed', :msg, 1, NOW()) "
                        "ON CONFLICT (table_name) DO UPDATE SET "
                        "status = 'permanently_failed', "
                        "error_message = :msg, "
                        "retry_count = cached_datasets.retry_count + 1, "
                        "updated_at = NOW()"
                    ),
                    {"msg": error_msg[:500], "did": dataset_id, "tn": table_name},
                )
            else:
                # Fallback: plain UPDATE when table_name is unknown
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


def _check_schema_compat(engine, table_name: str, df: pd.DataFrame) -> bool:
    """Check if df columns are compatible with existing table for append.

    Returns True if schemas are compatible (subset in either direction), False otherwise.
    """
    try:
        with engine.connect() as conn:
            existing_cols = set(
                r.column_name
                for r in conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = :tn AND table_schema = 'public'"
                    ),
                    {"tn": table_name},
                ).fetchall()
            )
            conn.rollback()
        new_cols = set(df.columns) - {"_source_dataset_id"}
        existing_cols_clean = existing_cols - {"_source_dataset_id"}
        return new_cols.issubset(existing_cols_clean) or existing_cols_clean.issubset(new_cols)
    except Exception:
        return True  # If check fails, allow append


def _update_row_count_after_append(engine, table_name: str):
    """Update row_count in cached_datasets after appending rows."""
    try:
        with engine.begin() as conn:
            new_count = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()  # noqa: S608
            conn.execute(
                text(
                    "UPDATE cached_datasets SET row_count = :cnt, updated_at = NOW() "
                    "WHERE table_name = :tn"
                ),
                {"cnt": new_count, "tn": table_name},
            )
    except Exception:
        logger.debug("Could not update row_count after append for %s", table_name, exc_info=True)


@celery_app.task(
    name="openarg.collect_data", bind=True, max_retries=3, soft_time_limit=600, time_limit=720
)
def collect_dataset(self, dataset_id: str):
    """
    Descarga un dataset real, lo parsea y lo guarda como tabla SQL.
    Esto permite al Analyst hacer queries SQL reales sobre los datos.
    """
    import httpx

    logger.info(f"Collecting dataset: {dataset_id}")
    engine = get_sync_engine()
    table_name = None

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
        table_name = _sanitize_table_name(title, portal)

        # Ensure a cached_datasets row exists EARLY so failures are always tracked
        _ensure_cached_entry(engine, dataset_id, table_name)

        if not download_url:
            logger.warning(f"Dataset {dataset_id} has no download URL")
            _set_error_status(engine, dataset_id, "no_download_url", table_name=table_name)
            return {"error": "no_download_url"}

        # Check retry_count — skip if permanently failed
        with engine.begin() as conn:
            cd_row = conn.execute(
                text(
                    "SELECT retry_count FROM cached_datasets WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"did": dataset_id},
            ).fetchone()
            if cd_row and cd_row.retry_count >= MAX_TOTAL_ATTEMPTS:
                logger.info(
                    f"Dataset {dataset_id} permanently failed "
                    f"after {cd_row.retry_count} attempts, skipping"
                )
                return {"error": "permanently_failed"}

        # Check if already cached — distinguish same-resource vs sibling-resource
        append_mode = False
        with engine.begin() as conn:
            cached = conn.execute(
                text(
                    "SELECT id, dataset_id FROM cached_datasets "
                    "WHERE table_name = :tn AND status = 'ready'"
                ),
                {"tn": table_name},
            ).fetchone()

        if cached:
            cached_did = str(cached.dataset_id)
            if cached_did == dataset_id:
                # Same resource — truly already cached, skip
                logger.info(f"Dataset {dataset_id} already cached as {table_name}")
                with engine.begin() as conn:
                    conn.execute(
                        text("UPDATE datasets SET is_cached = true WHERE id = CAST(:id AS uuid)"),
                        {"id": dataset_id},
                    )
                return {
                    "dataset_id": dataset_id,
                    "table_name": table_name,
                    "status": "already_cached",
                }
            else:
                # Sibling resource (same title+portal, different dataset_id) — append
                logger.info(
                    f"Dataset {dataset_id} is sibling of {cached_did} for table {table_name}, "
                    "will attempt append"
                )
                # Check if this specific resource already contributed rows
                try:
                    with engine.connect() as conn:
                        already_appended = conn.execute(
                            text(
                                f'SELECT EXISTS(SELECT 1 FROM "{table_name}" WHERE _source_dataset_id = :did LIMIT 1)'
                            ),  # noqa: S608
                            {"did": dataset_id},
                        ).scalar()
                        conn.rollback()
                    if already_appended:
                        logger.info(
                            f"Dataset {dataset_id} already appended to {table_name}, skipping"
                        )
                        with engine.begin() as conn:
                            conn.execute(
                                text(
                                    "UPDATE datasets SET is_cached = true WHERE id = CAST(:id AS uuid)"
                                ),
                                {"id": dataset_id},
                            )
                        return {"dataset_id": dataset_id, "status": "already_appended"}
                except Exception:
                    pass  # Column might not exist yet (old tables), proceed with append
                # Check aggregate row cap before appending
                current_rows = _get_table_row_count(engine, table_name)
                if current_rows >= MAX_TABLE_ROWS:
                    logger.info(
                        f"Table {table_name} already has {current_rows} rows (cap {MAX_TABLE_ROWS}), "
                        f"skipping append of {dataset_id}"
                    )
                    # Mark as cached so bulk_collect_all doesn't re-dispatch
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                "UPDATE datasets SET is_cached = true WHERE id = CAST(:id AS uuid)"
                            ),
                            {"id": dataset_id},
                        )
                    return {"dataset_id": dataset_id, "status": "table_full"}
                append_mode = True

        # Update status (skip for append_mode — table is already 'ready')
        if not append_mode:
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

        verify_ssl = _should_verify_ssl(download_url)

        try:
            # HEAD to check Content-Length before downloading
            try:
                with httpx.Client(timeout=30.0, verify=verify_ssl) as client:
                    head = client.head(download_url, follow_redirects=True)
                    content_length = int(head.headers.get("content-length", 0))
                    if content_length > max_download_bytes:
                        logger.warning(f"Dataset {dataset_id} too large: {content_length} bytes")
                        _set_error_status(
                            engine,
                            dataset_id,
                            f"file_too_large: {content_length} bytes",
                            table_name=table_name,
                        )
                        return {"error": f"file_too_large: {content_length} bytes"}
            except Exception:
                logger.debug(
                    "HEAD request failed for %s, proceeding with stream download",
                    dataset_id,
                    exc_info=True,
                )

            try:
                file_size = _stream_download(
                    download_url,
                    tmp_path,
                    verify_ssl=verify_ssl,
                    max_bytes=max_download_bytes,
                )
            except Exception as ssl_exc:
                if verify_ssl and (
                    "SSL" in type(ssl_exc).__name__ or "CERTIFICATE_VERIFY_FAILED" in str(ssl_exc)
                ):
                    logger.warning(
                        "SSL error for %s, retrying without verification: %s",
                        dataset_id,
                        str(ssl_exc)[:200],
                    )
                    file_size = _stream_download(
                        download_url,
                        tmp_path,
                        verify_ssl=False,
                        max_bytes=max_download_bytes,
                    )
                else:
                    raise

            # Upload raw file to S3 (from disk, not memory)
            s3_key = None
            try:
                filename = f"{source_id}.{fmt}"
                s3_key = _upload_file_to_s3(tmp_path, portal, dataset_id, filename)
                logger.info(f"Uploaded to S3: {s3_key}")
            except Exception:
                logger.warning(
                    "S3 upload failed for %s, continuing without S3", dataset_id, exc_info=True
                )

            # Parse and load into PostgreSQL
            row_count = 0
            columns: list[str] = []
            sampled_note: str | None = None  # set when dataset is truncated

            if fmt in ("csv", "txt"):
                # Chunked CSV loading — never loads full file into memory
                # For append mode, do a quick schema check using first chunk
                if append_mode:
                    try:
                        _csv_params = _detect_csv_params(tmp_path)
                        preview_df = pd.read_csv(tmp_path, nrows=5, **_csv_params)
                        with engine.connect() as conn:
                            existing_cols = set(
                                r.column_name
                                for r in conn.execute(
                                    text(
                                        "SELECT column_name FROM information_schema.columns "
                                        "WHERE table_name = :tn AND table_schema = 'public'"
                                    ),
                                    {"tn": table_name},
                                ).fetchall()
                            )
                            conn.rollback()
                        new_cols = set(preview_df.columns) - {"_source_dataset_id"}
                        existing_cols_clean = existing_cols - {"_source_dataset_id"}
                        if not new_cols.issubset(
                            existing_cols_clean
                        ) and not existing_cols_clean.issubset(new_cols):
                            logger.info(
                                f"Schema mismatch for {dataset_id} vs {table_name}, skipping append"
                            )
                            with engine.begin() as conn:
                                conn.execute(
                                    text(
                                        "UPDATE cached_datasets SET status = 'schema_mismatch', "
                                        "error_message = 'Incompatible columns for append', "
                                        "updated_at = NOW() "
                                        "WHERE dataset_id = CAST(:did AS uuid)"
                                    ),
                                    {"did": dataset_id},
                                )
                            return {"dataset_id": dataset_id, "status": "schema_mismatch"}
                    except Exception:
                        logger.debug(
                            "Schema preview failed for %s, proceeding anyway",
                            dataset_id,
                            exc_info=True,
                        )

                row_count, columns, csv_truncated = _load_csv_chunked(
                    tmp_path,
                    table_name,
                    engine,
                    source_dataset_id=dataset_id,
                    force_append=append_mode,
                )
                if csv_truncated:
                    sampled_note = f"sampled: first {row_count} rows kept (limit {MAX_TABLE_ROWS})"
                    logger.warning(
                        "Dataset %s truncated at %d rows (CSV row cap)",
                        dataset_id,
                        row_count,
                    )

            elif fmt == "zip":
                import zipfile

                max_decompressed = 500 * 1024 * 1024
                try:
                    zf = zipfile.ZipFile(tmp_path)
                except zipfile.BadZipFile:
                    logger.warning(f"ZIP {dataset_id}: not a valid ZIP file")
                    _set_error_status(engine, dataset_id, "bad_zip_file", table_name=table_name)
                    return {"error": "bad_zip_file"}
                total_size = sum(info.file_size for info in zf.infolist())
                if total_size > max_decompressed:
                    logger.warning(
                        f"ZIP {dataset_id}: decompressed size {total_size} exceeds limit"
                    )
                    _set_error_status(
                        engine,
                        dataset_id,
                        f"zip_too_large: {total_size} bytes decompressed",
                        table_name=table_name,
                    )
                    return {"error": f"zip_too_large: {total_size} bytes"}

                parsed = False
                for name in zf.namelist():
                    lower = name.lower()
                    if lower.endswith((".csv", ".txt")):
                        # Extract CSV to temp file, then chunked load
                        csv_tmp = tempfile.NamedTemporaryFile(
                            delete=False, suffix=".csv", dir="/tmp"
                        )
                        csv_tmp_path = csv_tmp.name
                        csv_tmp.close()
                        try:
                            with zf.open(name) as zf_entry, open(csv_tmp_path, "wb") as out:
                                while True:
                                    block = zf_entry.read(256 * 1024)
                                    if not block:
                                        break
                                    out.write(block)
                            row_count, columns, csv_trunc = _load_csv_chunked(
                                csv_tmp_path,
                                table_name,
                                engine,
                                source_dataset_id=dataset_id,
                                force_append=append_mode,
                            )
                            if csv_trunc:
                                sampled_note = (
                                    f"sampled: first {row_count} rows kept (limit {MAX_TABLE_ROWS})"
                                )
                                logger.warning(
                                    "Dataset %s (zip/csv) truncated at %d rows",
                                    dataset_id,
                                    row_count,
                                )
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
                        df = pd.read_excel(io.BytesIO(content), nrows=MAX_TABLE_ROWS)
                        original_len = len(df)
                        if original_len >= MAX_TABLE_ROWS:
                            sampled_note = (
                                f"sampled: first {MAX_TABLE_ROWS} rows kept"
                                f" (excel in zip, file may have more)"
                            )
                            logger.warning(
                                "Dataset %s (zip/excel) truncated at %d rows",
                                dataset_id,
                                MAX_TABLE_ROWS,
                            )
                        df["_source_dataset_id"] = dataset_id
                        if append_mode:
                            if not _check_schema_compat(engine, table_name, df):
                                logger.info(
                                    f"Schema mismatch for {dataset_id} vs {table_name}, skipping append"
                                )
                                return {"dataset_id": dataset_id, "status": "schema_mismatch"}
                            _to_sql_safe(df, table_name, engine, if_exists="append", index=False)
                        else:
                            _to_sql_safe(df, table_name, engine, if_exists="replace", index=False)
                        row_count, columns = len(df), list(df.columns)
                        parsed = True
                        break
                    elif lower.endswith(".geojson"):
                        with zf.open(name) as f:
                            content = f.read()
                        raw_geo = json.loads(content)
                        features = raw_geo.get("features", []) if isinstance(raw_geo, dict) else []
                        if features:
                            df = _geojson_features_to_df(features)
                        else:
                            df = pd.json_normalize(
                                raw_geo if isinstance(raw_geo, list) else [raw_geo]
                            )
                        if len(df) > MAX_TABLE_ROWS:
                            sampled_note = (
                                f"sampled: first {MAX_TABLE_ROWS} of {len(df)} total rows"
                            )
                            logger.warning(
                                "Dataset %s (zip/geojson) truncated from %d to %d rows",
                                dataset_id,
                                len(df),
                                MAX_TABLE_ROWS,
                            )
                            df = df.iloc[:MAX_TABLE_ROWS]
                        df["_source_dataset_id"] = dataset_id
                        if append_mode:
                            if not _check_schema_compat(engine, table_name, df):
                                logger.info(
                                    f"Schema mismatch for {dataset_id} vs {table_name}, skipping append"
                                )
                                return {"dataset_id": dataset_id, "status": "schema_mismatch"}
                            _to_sql_safe(df, table_name, engine, if_exists="append", index=False)
                        else:
                            _to_sql_safe(df, table_name, engine, if_exists="replace", index=False)
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
                        if len(df) > MAX_TABLE_ROWS:
                            sampled_note = (
                                f"sampled: first {MAX_TABLE_ROWS} of {len(df)} total rows"
                            )
                            logger.warning(
                                "Dataset %s (zip/json) truncated from %d to %d rows",
                                dataset_id,
                                len(df),
                                MAX_TABLE_ROWS,
                            )
                            df = df.iloc[:MAX_TABLE_ROWS]
                        df["_source_dataset_id"] = dataset_id
                        if append_mode:
                            if not _check_schema_compat(engine, table_name, df):
                                logger.info(
                                    f"Schema mismatch for {dataset_id} vs {table_name}, skipping append"
                                )
                                return {"dataset_id": dataset_id, "status": "schema_mismatch"}
                            _to_sql_safe(df, table_name, engine, if_exists="append", index=False)
                        else:
                            _to_sql_safe(df, table_name, engine, if_exists="replace", index=False)
                        row_count, columns = len(df), list(df.columns)
                        parsed = True
                        break
                # Fallback: try reading as shapefile (ZIP may contain .shp/.dbf)
                if not parsed:
                    has_shp = any(n.lower().endswith(".shp") for n in zf.namelist())
                    if has_shp:
                        df = _read_shapefile_from_zip(tmp_path)
                        if df is not None and len(df) > 0:
                            if len(df) > MAX_TABLE_ROWS:
                                sampled_note = (
                                    f"sampled: first {MAX_TABLE_ROWS} of {len(df)} rows (shapefile)"
                                )
                                logger.warning(
                                    "Dataset %s (zip/shp) truncated from %d to %d rows",
                                    dataset_id,
                                    len(df),
                                    MAX_TABLE_ROWS,
                                )
                                df = df.iloc[:MAX_TABLE_ROWS]
                            df["_source_dataset_id"] = dataset_id
                            if append_mode:
                                if not _check_schema_compat(engine, table_name, df):
                                    logger.info(
                                        f"Schema mismatch for {dataset_id} vs {table_name}, skipping append"
                                    )
                                    return {"dataset_id": dataset_id, "status": "schema_mismatch"}
                                _to_sql_safe(
                                    df, table_name, engine, if_exists="append", index=False
                                )
                            else:
                                _to_sql_safe(
                                    df, table_name, engine, if_exists="replace", index=False
                                )
                            row_count, columns = len(df), list(df.columns)
                            parsed = True

                if not parsed:
                    logger.warning(f"ZIP {dataset_id}: no parseable file found in {zf.namelist()}")
                    _set_error_status(
                        engine, dataset_id, "zip_no_parseable_file", table_name=table_name
                    )
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
                if len(df) > MAX_TABLE_ROWS:
                    sampled_note = f"sampled: first {MAX_TABLE_ROWS} of {len(df)} total rows"
                    logger.warning(
                        "Dataset %s (json) truncated from %d to %d rows",
                        dataset_id,
                        len(df),
                        MAX_TABLE_ROWS,
                    )
                    df = df.iloc[:MAX_TABLE_ROWS]
                df["_source_dataset_id"] = dataset_id
                if append_mode:
                    if not _check_schema_compat(engine, table_name, df):
                        logger.info(
                            f"Schema mismatch for {dataset_id} vs {table_name}, skipping append"
                        )
                        return {"dataset_id": dataset_id, "status": "schema_mismatch"}
                    _to_sql_safe(df, table_name, engine, if_exists="append", index=False)
                else:
                    _to_sql_safe(df, table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            elif fmt == "geojson":
                with open(tmp_path, "rb") as f:
                    raw = json.load(f)
                features = raw.get("features", []) if isinstance(raw, dict) else []
                if features:
                    df = _geojson_features_to_df(features)
                else:
                    logger.warning(f"GeoJSON {dataset_id}: no features found")
                    _set_error_status(
                        engine, dataset_id, "geojson_no_features", table_name=table_name
                    )
                    return {"error": "geojson_no_features"}
                if len(df) > MAX_TABLE_ROWS:
                    sampled_note = f"sampled: first {MAX_TABLE_ROWS} of {len(df)} total rows"
                    logger.warning(
                        "Dataset %s (geojson) truncated from %d to %d rows",
                        dataset_id,
                        len(df),
                        MAX_TABLE_ROWS,
                    )
                    df = df.iloc[:MAX_TABLE_ROWS]
                df["_source_dataset_id"] = dataset_id
                if append_mode:
                    if not _check_schema_compat(engine, table_name, df):
                        logger.info(
                            f"Schema mismatch for {dataset_id} vs {table_name}, skipping append"
                        )
                        return {"dataset_id": dataset_id, "status": "schema_mismatch"}
                    _to_sql_safe(df, table_name, engine, if_exists="append", index=False)
                else:
                    _to_sql_safe(df, table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            elif fmt in ("xlsx", "xls"):
                df = pd.read_excel(tmp_path, nrows=MAX_TABLE_ROWS)
                if len(df) >= MAX_TABLE_ROWS:
                    sampled_note = (
                        f"sampled: first {MAX_TABLE_ROWS} rows kept (excel file may have more)"
                    )
                    logger.warning(
                        "Dataset %s (excel) truncated at %d rows",
                        dataset_id,
                        MAX_TABLE_ROWS,
                    )
                df["_source_dataset_id"] = dataset_id
                if append_mode:
                    if not _check_schema_compat(engine, table_name, df):
                        logger.info(
                            f"Schema mismatch for {dataset_id} vs {table_name}, skipping append"
                        )
                        return {"dataset_id": dataset_id, "status": "schema_mismatch"}
                    _to_sql_safe(df, table_name, engine, if_exists="append", index=False)
                else:
                    _to_sql_safe(df, table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            elif fmt == "ods":
                df = pd.read_excel(tmp_path, engine="odf", nrows=MAX_TABLE_ROWS)
                if len(df) >= MAX_TABLE_ROWS:
                    sampled_note = (
                        f"sampled: first {MAX_TABLE_ROWS} rows kept (ods file may have more)"
                    )
                    logger.warning(
                        "Dataset %s (ods) truncated at %d rows",
                        dataset_id,
                        MAX_TABLE_ROWS,
                    )
                df["_source_dataset_id"] = dataset_id
                if append_mode:
                    if not _check_schema_compat(engine, table_name, df):
                        logger.info(
                            f"Schema mismatch for {dataset_id} vs {table_name}, skipping append"
                        )
                        return {"dataset_id": dataset_id, "status": "schema_mismatch"}
                    _to_sql_safe(df, table_name, engine, if_exists="append", index=False)
                else:
                    _to_sql_safe(df, table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            elif fmt == "xml":
                try:
                    df = pd.read_xml(tmp_path)
                except Exception:
                    logger.warning(
                        "XML %s: pd.read_xml failed, skipping", dataset_id, exc_info=True
                    )
                    _set_error_status(engine, dataset_id, "xml_parse_failed", table_name=table_name)
                    return {"error": "xml_parse_failed"}
                if len(df) > MAX_TABLE_ROWS:
                    sampled_note = f"sampled: first {MAX_TABLE_ROWS} of {len(df)} total rows"
                    logger.warning(
                        "Dataset %s (xml) truncated from %d to %d rows",
                        dataset_id,
                        len(df),
                        MAX_TABLE_ROWS,
                    )
                    df = df.iloc[:MAX_TABLE_ROWS]
                df["_source_dataset_id"] = dataset_id
                if append_mode:
                    if not _check_schema_compat(engine, table_name, df):
                        logger.info(
                            f"Schema mismatch for {dataset_id} vs {table_name}, skipping append"
                        )
                        return {"dataset_id": dataset_id, "status": "schema_mismatch"}
                    _to_sql_safe(df, table_name, engine, if_exists="append", index=False)
                else:
                    _to_sql_safe(df, table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            else:
                logger.warning(f"Unsupported format: {fmt}")
                _set_error_status(
                    engine, dataset_id, f"unsupported_format: {fmt}", table_name=table_name
                )
                return {"error": f"unsupported_format: {fmt}"}

            if append_mode:
                # Append mode: update row_count from actual table, keep status 'ready'
                _update_row_count_after_append(engine, table_name)
                # Also mark this dataset as cached in datasets table
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            "UPDATE datasets SET is_cached = true, row_count = :rows "
                            "WHERE id = CAST(:id AS uuid)"
                        ),
                        {"rows": row_count, "id": dataset_id},
                    )
                logger.info(f"Dataset {dataset_id} appended {row_count} rows to table {table_name}")
                return {
                    "dataset_id": dataset_id,
                    "table_name": table_name,
                    "rows": row_count,
                    "status": "appended",
                    "s3_key": s3_key,
                }

            # Update cache status (include sampling note when truncated)
            columns_json = json.dumps([str(c) for c in columns])
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE cached_datasets SET
                            status = 'ready', row_count = :rows,
                            columns_json = :cols, size_bytes = :size,
                            s3_key = :s3key,
                            error_message = :sampled,
                            updated_at = NOW()
                        WHERE dataset_id = CAST(:did AS uuid)
                    """),
                    {
                        "rows": row_count,
                        "cols": columns_json,
                        "size": file_size,
                        "s3key": s3_key,
                        "sampled": sampled_note,
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
                    text(
                        "UPDATE datasets SET is_cached = true, row_count = :rows WHERE id = CAST(:id AS uuid)"
                    ),
                    {"rows": row_count, "id": dataset_id},
                )

            # Add PostGIS native geometry column if this is a geo dataset
            _ensure_postgis_geom(engine, table_name, columns)

            # Re-embed only on first cache (enriches with sample rows); skip if already embedded
            if not was_cached:
                from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

                index_dataset_embedding.delay(dataset_id)

                # Auto-enrich with semantic catalog metadata
                from app.infrastructure.celery.tasks.catalog_enrichment_tasks import (
                    enrich_single_table,
                )

                enrich_single_table.delay(table_name)

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
        backoff = min(60 * (2**self.request.retries), 600)  # 60s, 120s, 240s, capped at 600s
        jitter = random.uniform(0, backoff * 0.3)
        raise self.retry(countdown=int(backoff + jitter))
    except Exception as exc:
        exc_str = str(exc)
        # Non-retryable errors — mark permanently failed immediately
        non_retryable = (
            any(
                s in exc_str
                for s in (
                    "403 Forbidden",
                    "401 Unauthorized",
                    "404 Not Found",
                    "410 Gone",
                    "illegal status line",
                    "Request Denied",
                    "Name or service not known",
                    "TooManyColumns",
                )
            )
            or "TooManyRedirects" in type(exc).__name__
        )
        if non_retryable:
            logger.warning("Non-retryable error for %s: %s", dataset_id, exc_str[:200])
            _set_error_status(engine, dataset_id, exc_str[:500], table_name=table_name)
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
                    "SELECT retry_count FROM cached_datasets WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"did": dataset_id},
            ).fetchone()

        attempts = current.retry_count if current else 0
        if attempts >= MAX_TOTAL_ATTEMPTS:
            logger.warning(
                f"Dataset {dataset_id} permanently failed after {attempts} attempts: {exc}"
            )
            return {"error": "permanently_failed", "attempts": attempts}

        backoff = min(60 * (2**self.request.retries), 600)  # 60s, 120s, 240s, capped at 600s
        jitter = random.uniform(0, backoff * 0.3)
        raise self.retry(exc=exc, countdown=int(backoff + jitter)) from exc
    finally:
        engine.dispose()


@celery_app.task(name="openarg.bulk_collect_all", bind=True, soft_time_limit=120, time_limit=180)
def bulk_collect_all(self, portal: str | None = None):
    """Descarga y cachea TODOS los datasets no cacheados.

    Phase 1: Dispatch individual collect_dataset for groups with <=50 siblings.
    Phase 2: Dispatch collect_large_group for groups with >50 siblings,
             which handles format dedup, schema probing, and row budgets.
    """
    engine = get_sync_engine()

    try:
        from celery import group as celery_group

        # ── Phase 1: individual datasets (groups <= 50 siblings) ──
        query = (
            "SELECT CAST(d.id AS text) FROM datasets d "
            "LEFT JOIN cached_datasets cd ON cd.dataset_id = d.id "
            "WHERE d.is_cached = false "
            "AND (cd.status IS NULL OR cd.status NOT IN ('ready', 'permanently_failed', 'schema_mismatch')) "
            "AND d.title NOT IN ("
            "  SELECT title FROM datasets "
            "  WHERE portal = d.portal AND is_cached = false "
            "  GROUP BY title, portal "
            "  HAVING COUNT(*) > 50"
            ")"
        )
        params: dict = {}
        if portal:
            query += " AND d.portal = :portal"
            params["portal"] = portal
        query += " ORDER BY d.portal, d.title LIMIT 5000"

        with engine.connect() as conn:
            rows = conn.execute(text(query), params).fetchall()

        phase1_count = len(rows)
        tasks = [collect_dataset.s(row[0]) for row in rows]
        if tasks:
            celery_group(tasks).apply_async()

        # ── Phase 2: large groups (>50 siblings) ──
        large_query = (
            "SELECT d.title, d.portal, COUNT(*) as cnt FROM datasets d "
            "WHERE d.is_cached = false "
            "GROUP BY d.title, d.portal "
            "HAVING COUNT(*) > 50 "
            "ORDER BY COUNT(*) ASC "
            "LIMIT 100"
        )
        with engine.connect() as conn:
            large_groups = conn.execute(text(large_query)).fetchall()

        phase2_count = len(large_groups)
        if large_groups:
            large_tasks = [collect_large_group.s(row.title, row.portal) for row in large_groups]
            celery_group(large_tasks).apply_async()

        logger.info(
            f"Bulk collect: phase1={phase1_count} individual, phase2={phase2_count} large groups"
        )
        return {"dispatched_individual": phase1_count, "dispatched_groups": phase2_count}
    finally:
        engine.dispose()


_FORMAT_PRIORITY = {
    "csv": 1,
    "txt": 2,
    "json": 3,
    "xlsx": 4,
    "xls": 5,
    "geojson": 6,
    "zip": 7,
    "ods": 8,
    "xml": 9,
}


@celery_app.task(
    name="openarg.collect_large_group",
    bind=True,
    soft_time_limit=1800,
    time_limit=2100,
    max_retries=1,
)
def collect_large_group(self, title: str, portal: str):
    """Collect a large multi-resource group (>50 siblings) as a single table.

    Strategy:
      1. Format-dedup: group by URL stem, keep highest-priority format per stem
      2. Schema-probe: download 5 rows from 3 sample resources to check compatibility
      3. Concatenate compatible resources until MAX_TABLE_ROWS, skip incompatible
      4. Mark all datasets in the group as is_cached=true
    """
    engine = get_sync_engine()
    try:
        table_name = _sanitize_table_name(title, portal)

        # Check if table already exists and is full
        current_rows = _get_table_row_count(engine, table_name)
        if current_rows >= MAX_TABLE_ROWS:
            logger.info(
                f"Large group '{title}' ({portal}): table already full ({current_rows} rows)"
            )
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE datasets SET is_cached = true "
                        "WHERE title = :title AND portal = :portal AND is_cached = false"
                    ),
                    {"title": title, "portal": portal},
                )
            return {"title": title, "status": "table_full", "rows": current_rows}

        # Get all uncached resources for this group
        with engine.connect() as conn:
            resources = conn.execute(
                text(
                    "SELECT CAST(d.id AS text) as id, d.format, d.download_url "
                    "FROM datasets d "
                    "WHERE d.title = :title AND d.portal = :portal AND d.is_cached = false "
                    "ORDER BY d.format, d.id"
                ),
                {"title": title, "portal": portal},
            ).fetchall()
            conn.rollback()

        if not resources:
            return {"title": title, "status": "nothing_to_collect"}

        # Format dedup: group by URL path (without extension), keep best format
        seen_stems: dict[str, tuple] = {}  # stem → (dataset_id, format, url)
        for r in resources:
            url = r.download_url or ""
            if not url:
                continue
            # Extract URL stem (without extension and query params)
            path = url.split("?")[0].split("#")[0]
            dot = path.rfind(".")
            stem = path[:dot] if dot > 0 else path
            fmt = (r.format or "").lower().strip()
            priority = _FORMAT_PRIORITY.get(fmt, 99)

            if stem not in seen_stems or priority < _FORMAT_PRIORITY.get(seen_stems[stem][1], 99):
                seen_stems[stem] = (r.id, fmt, url)

        deduped = list(seen_stems.values())
        logger.info(
            f"Large group '{title}' ({portal}): {len(resources)} resources → "
            f"{len(deduped)} after format dedup"
        )

        # Dispatch individual collect_dataset for each deduped resource (up to 200)
        # The append_mode + aggregate row cap in collect_dataset handles the rest
        from celery import group as celery_group

        tasks = [collect_dataset.s(did) for did, _, _ in deduped[:200]]
        if tasks:
            celery_group(tasks).apply_async()

        # Mark remaining (beyond 200) as cached to avoid re-dispatch
        if len(deduped) > 200:
            skipped_ids = [did for did, _, _ in deduped[200:]]
            with engine.begin() as conn:
                for sid in skipped_ids:
                    conn.execute(
                        text("UPDATE datasets SET is_cached = true WHERE id = CAST(:id AS uuid)"),
                        {"id": sid},
                    )

        # Mark format-dupes as cached
        deduped_ids = {did for did, _, _ in deduped}
        format_dupes = [r.id for r in resources if r.id not in deduped_ids]
        if format_dupes:
            with engine.begin() as conn:
                for fid in format_dupes:
                    conn.execute(
                        text("UPDATE datasets SET is_cached = true WHERE id = CAST(:id AS uuid)"),
                        {"id": fid},
                    )
            logger.info(f"Marked {len(format_dupes)} format-duplicate resources as cached")

        return {
            "title": title,
            "portal": portal,
            "total_resources": len(resources),
            "deduped": len(deduped),
            "dispatched": min(len(deduped), 200),
            "format_dupes_skipped": len(format_dupes),
        }
    except Exception as exc:
        logger.warning(f"collect_large_group failed for '{title}' ({portal})", exc_info=True)
        raise self.retry(exc=exc, countdown=60) from exc
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

        # 1a. Transition stuck downloads that exhausted retries → permanently_failed
        with engine.begin() as conn:
            exhausted = conn.execute(
                text("""
                    UPDATE cached_datasets
                    SET status = 'permanently_failed',
                        error_message = 'Exhausted retries while stuck in downloading',
                        updated_at = NOW()
                    WHERE status = 'downloading'
                      AND updated_at < NOW() - INTERVAL '30 minutes'
                      AND retry_count >= :max
                """),
                {"max": MAX_TOTAL_ATTEMPTS},
            )
            if exhausted.rowcount:
                logger.warning(
                    "Marked %d stuck downloads as permanently_failed (retry >= %d)",
                    exhausted.rowcount,
                    MAX_TOTAL_ATTEMPTS,
                )

        # 1b. Recover stuck cached_datasets that still have retries left
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
                # Check if the table exists via information_schema (safe, no transaction abort)
                table_exists = False
                if row.table_name:
                    exists_result = conn.execute(
                        text(
                            "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                            "WHERE table_name = :tn AND table_schema = 'public')"
                        ),
                        {"tn": row.table_name},
                    ).scalar()
                    table_exists = bool(exists_result)

                if table_exists:
                    # Table exists — check row count
                    row_count = (
                        conn.execute(
                            text(f'SELECT COUNT(*) FROM "{row.table_name}"')  # noqa: S608
                        ).scalar()
                        or 0
                    )
                    if row_count > 0:
                        col_result = conn.execute(
                            text(
                                "SELECT column_name FROM information_schema.columns "
                                "WHERE table_name = :tn ORDER BY ordinal_position"
                            ),
                            {"tn": row.table_name},
                        ).fetchall()
                        columns = [r.column_name for r in col_result]
                        columns_json = json.dumps(columns)
                        conn.execute(
                            text("""
                                UPDATE cached_datasets
                                SET status = 'ready',
                                    row_count = :rows,
                                    columns_json = :cols,
                                    error_message = NULL,
                                    updated_at = NOW()
                                WHERE dataset_id = CAST(:did AS uuid)
                                  AND status = 'downloading'
                            """),
                            {"did": row.dataset_id, "rows": row_count, "cols": columns_json},
                        )
                        conn.execute(
                            text(
                                "UPDATE datasets SET is_cached = true, row_count = :rows "
                                "WHERE id = CAST(:did AS uuid)"
                            ),
                            {"did": row.dataset_id, "rows": row_count},
                        )
                        logger.info(
                            "Recovered stuck download %s: table %s has %d rows, set to ready",
                            row.dataset_id,
                            row.table_name,
                            row_count,
                        )
                        recovered_downloads += 1
                        continue

                # Table doesn't exist or has no data — re-dispatch or mark failed
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

        # 3. Validate 'ready' datasets — check that the actual table still exists
        #    (catches orphaned records after DB restore, migration, or manual cleanup)
        orphaned_ready = 0
        with engine.begin() as conn:
            ready_datasets = conn.execute(
                text("""
                    SELECT CAST(dataset_id AS text) AS dataset_id, table_name
                    FROM cached_datasets
                    WHERE status = 'ready' AND table_name IS NOT NULL
                """),
            ).fetchall()

            for row in ready_datasets:
                table_exists = conn.execute(
                    text(
                        "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                        "WHERE table_name = :tn AND table_schema = 'public')"
                    ),
                    {"tn": row.table_name},
                ).scalar()

                if not table_exists:
                    conn.execute(
                        text("""
                            UPDATE cached_datasets
                            SET status = 'error',
                                retry_count = 0,
                                error_message = 'Table missing: marked for re-download',
                                updated_at = NOW()
                            WHERE dataset_id = CAST(:did AS uuid)
                              AND status = 'ready'
                        """),
                        {"did": row.dataset_id},
                    )
                    conn.execute(
                        text("UPDATE datasets SET is_cached = false WHERE id = CAST(:did AS uuid)"),
                        {"did": row.dataset_id},
                    )
                    collect_dataset.delay(row.dataset_id)
                    orphaned_ready += 1

            if orphaned_ready:
                logger.warning(
                    "Found %d 'ready' datasets with missing tables, re-dispatched for download",
                    orphaned_ready,
                )

        if recovered_downloads or recovered_queries or orphaned_ready:
            logger.warning(
                "Recovered: %d stuck downloads, %d stuck queries, %d orphaned ready",
                recovered_downloads,
                recovered_queries,
                orphaned_ready,
            )
        else:
            logger.debug("No stuck tasks found")

        return {
            "recovered_downloads": recovered_downloads,
            "recovered_queries": recovered_queries,
            "orphaned_ready": orphaned_ready,
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
                "Reset %d permanently_failed datasets for retry",
                count,
            )
        else:
            logger.debug("No permanently_failed datasets to reset")
        return {"reset": count}
    finally:
        engine.dispose()


@celery_app.task(
    name="openarg.retry_failed_shapefiles",
    bind=True,
    soft_time_limit=300,
    time_limit=360,
)
def retry_failed_shapefiles(self):
    """Re-dispatch datasets that failed with zip_no_parseable_file.

    Now that we support shapefiles via fiona, these may succeed on retry.
    Only retries datasets whose ZIP actually contains .shp files.
    """
    engine = get_sync_engine()
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    "SELECT cd.dataset_id, cd.table_name, d.download_url, d.portal "
                    "FROM cached_datasets cd "
                    "JOIN datasets d ON d.id = cd.dataset_id "
                    "WHERE cd.status = 'permanently_failed' "
                    "AND cd.error_message = 'zip_no_parseable_file' "
                    "LIMIT 500"
                )
            ).fetchall()

        if not rows:
            logger.info("No zip_no_parseable_file datasets to retry")
            return {"dispatched": 0}

        # Reset status so collect_data picks them up
        with engine.begin() as conn:
            for r in rows:
                conn.execute(
                    text(
                        "UPDATE cached_datasets "
                        "SET status = 'pending', retry_count = 0, "
                        "    error_message = NULL, updated_at = NOW() "
                        "WHERE dataset_id = CAST(:did AS uuid)"
                    ),
                    {"did": str(r.dataset_id)},
                )

        # Dispatch collect_data for each
        tasks = []
        for r in rows:
            tasks.append(collect_dataset.s(str(r.dataset_id)))
        if tasks:
            from celery import group

            group(tasks).apply_async()

        logger.info("Retrying %d shapefile datasets", len(rows))
        return {"dispatched": len(rows)}
    finally:
        engine.dispose()
