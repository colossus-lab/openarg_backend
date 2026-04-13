"""
Collector Worker — Agente recolector de datos.

Dado un dataset_id, descarga el archivo real (CSV/JSON/XLSX),
lo parsea con pandas y lo cachea en PostgreSQL para consultas SQL.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import random
import re
import shutil
import tempfile
import time
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
_MAX_SQL_COLUMNS = 1400
_TEMP_SPACE_RESERVE_BYTES = 256 * 1024 * 1024  # keep 256MB free for worker stability
_COLLECT_DISPATCH_BATCH_SIZE = int(os.getenv("OPENARG_COLLECT_DISPATCH_BATCH_SIZE", "10"))
_COLLECT_DISPATCH_STEP_SECONDS = int(os.getenv("OPENARG_COLLECT_DISPATCH_STEP_SECONDS", "30"))
_COLLECT_MAX_INFLIGHT = int(os.getenv("OPENARG_COLLECT_MAX_INFLIGHT", "100"))
_COLLECT_MAX_INFLIGHT_PER_PORTAL = int(os.getenv("OPENARG_COLLECT_MAX_INFLIGHT_PER_PORTAL", "10"))
_COLLECT_DATASET_LOCK_PREFIX = "collect_dataset"
_BULK_COLLECT_LOCK_KEY = 8_004_001


def _temp_dir() -> str:
    """Return the temp directory used by collector tasks."""
    return os.getenv("OPENARG_TEMP_DIR", "/tmp")


def _lock_key(*parts: str) -> int:
    """Build a stable advisory-lock key from string parts."""
    digest = hashlib.sha1("::".join(parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False) & 0x7FFF_FFFF_FFFF_FFFF


def _try_advisory_lock(engine, key: int) -> bool:
    """Try to acquire a PostgreSQL advisory lock for the current session."""
    with engine.connect() as conn:
        acquired = conn.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": key}).scalar()
        conn.rollback()
    return bool(acquired)


def _release_advisory_lock(engine, key: int) -> None:
    """Release a previously acquired PostgreSQL advisory lock."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
            conn.rollback()
    except Exception:
        logger.debug("Could not release advisory lock %s", key, exc_info=True)


def _has_temp_space(required_bytes: int, reserve_bytes: int = _TEMP_SPACE_RESERVE_BYTES) -> bool:
    """Check whether the temp filesystem has enough free space for a new task."""
    try:
        usage = shutil.disk_usage(_temp_dir())
    except OSError:
        logger.debug("Could not inspect temp dir space", exc_info=True)
        return True
    return usage.free >= (required_bytes + reserve_bytes)


def _dispatch_in_batches(signatures: list, *, batch_size: int, step_seconds: int) -> int:
    """Dispatch Celery signatures in staggered batches to reduce worker pressure."""
    from celery import group as celery_group

    dispatched_batches = 0
    for start in range(0, len(signatures), batch_size):
        batch = signatures[start : start + batch_size]
        if not batch:
            continue
        celery_group(batch).apply_async(countdown=dispatched_batches * step_seconds)
        dispatched_batches += 1
    return dispatched_batches


def _get_inflight_counts(engine) -> tuple[int, dict[str, int]]:
    """Return current collector inflight totals overall and by portal."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT d.portal, COUNT(*) "
                "FROM cached_datasets cd "
                "JOIN datasets d ON d.id = cd.dataset_id "
                "WHERE cd.status = 'downloading' "
                "GROUP BY d.portal"
            )
        ).fetchall()
        conn.rollback()
    by_portal = {str(row[0]): int(row[1]) for row in rows}
    return sum(by_portal.values()), by_portal


def _throttle_collect_rows(
    rows,
    *,
    inflight_total: int,
    inflight_by_portal: dict[str, int],
    max_total: int,
    max_per_portal: int,
):
    """Keep only rows that fit within inflight collector budgets."""
    selected = []
    deferred = []
    total = inflight_total
    per_portal = dict(inflight_by_portal)

    for row in rows:
        dataset_id = str(row[0])
        portal = str(row[1])
        if total >= max_total or per_portal.get(portal, 0) >= max_per_portal:
            deferred.append((dataset_id, portal))
            continue
        selected.append((dataset_id, portal))
        total += 1
        per_portal[portal] = per_portal.get(portal, 0) + 1

    return selected, deferred


def _recycle_stuck_downloads(
    engine,
    *,
    max_age_minutes: int = 30,
    redispatch: bool = False,
) -> dict[str, int]:
    """Recycle stale `downloading` rows so they stop consuming inflight slots.

    Old rows with materialized data are promoted to `ready`.
    Old rows without data are moved back to `error` (or `permanently_failed`
    if they already exhausted retries). Optionally re-dispatches datasets that
    are recycled back to `error`.
    """
    recovered_ready = 0
    recycled_error = 0
    recycled_failed = 0

    with engine.begin() as conn:
        exhausted = conn.execute(
            text(
                """
                UPDATE cached_datasets
                SET status = 'permanently_failed',
                    error_message = 'Exhausted retries while stuck in downloading',
                    updated_at = NOW()
                WHERE status = 'downloading'
                  AND updated_at < NOW() - (:age_minutes * INTERVAL '1 minute')
                  AND retry_count >= :max
                """
            ),
            {"age_minutes": max_age_minutes, "max": MAX_TOTAL_ATTEMPTS},
        )
        recycled_failed += exhausted.rowcount or 0

        stale_rows = conn.execute(
            text(
                """
                SELECT CAST(dataset_id AS text) AS dataset_id,
                       table_name,
                       retry_count
                FROM cached_datasets
                WHERE status = 'downloading'
                  AND updated_at < NOW() - (:age_minutes * INTERVAL '1 minute')
                  AND retry_count < :max
                """
            ),
            {"age_minutes": max_age_minutes, "max": MAX_TOTAL_ATTEMPTS},
        ).fetchall()

        to_redispatch: list[str] = []
        for row in stale_rows:
            table_exists = False
            if row.table_name:
                table_exists = bool(
                    conn.execute(
                        text(
                            "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                            "WHERE table_name = :tn AND table_schema = 'public')"
                        ),
                        {"tn": row.table_name},
                    ).scalar()
                )

            if table_exists:
                row_count = (
                    conn.execute(text(f'SELECT COUNT(*) FROM "{row.table_name}"')).scalar() or 0
                )  # noqa: S608
                if row_count > 0:
                    col_result = conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_name = :tn ORDER BY ordinal_position"
                        ),
                        {"tn": row.table_name},
                    ).fetchall()
                    columns = [r.column_name for r in col_result]
                    conn.execute(
                        text(
                            """
                            UPDATE cached_datasets
                            SET status = 'ready',
                                row_count = :rows,
                                columns_json = :cols,
                                error_message = NULL,
                                updated_at = NOW()
                            WHERE dataset_id = CAST(:did AS uuid)
                              AND status = 'downloading'
                            """
                        ),
                        {
                            "did": row.dataset_id,
                            "rows": row_count,
                            "cols": json.dumps(columns),
                        },
                    )
                    conn.execute(
                        text(
                            "UPDATE datasets SET is_cached = true, row_count = :rows "
                            "WHERE id = CAST(:did AS uuid)"
                        ),
                        {"did": row.dataset_id, "rows": row_count},
                    )
                    recovered_ready += 1
                    continue

            new_count = (row.retry_count or 0) + 1
            next_status = "permanently_failed" if new_count >= MAX_TOTAL_ATTEMPTS else "error"
            next_error = (
                "Permanently failed: stuck in downloading too many times"
                if next_status == "permanently_failed"
                else "Recovered: stuck in downloading state"
            )
            conn.execute(
                text(
                    """
                    UPDATE cached_datasets
                    SET status = :status,
                        retry_count = :cnt,
                        error_message = :msg,
                        updated_at = NOW()
                    WHERE dataset_id = CAST(:did AS uuid)
                      AND status = 'downloading'
                    """
                ),
                {
                    "did": row.dataset_id,
                    "status": next_status,
                    "cnt": new_count,
                    "msg": next_error,
                },
            )
            if next_status == "permanently_failed":
                recycled_failed += 1
            else:
                recycled_error += 1
                if redispatch:
                    to_redispatch.append(str(row.dataset_id))

    if redispatch:
        for dataset_id in to_redispatch:
            collect_dataset.delay(dataset_id)

    return {
        "recovered_ready": recovered_ready,
        "recycled_error": recycled_error,
        "recycled_failed": recycled_failed,
    }


def _reconcile_cache_coverage(
    engine,
    *,
    redispatch: bool = False,
    reindex_embeddings: bool = False,
) -> dict[str, int]:
    """Fix inconsistent cache flags and optionally requeue missing work."""
    orphaned_ready = 0
    fixed_cached_flags = 0
    reindexed_missing_chunks = 0

    with engine.begin() as conn:
        orphaned_rows = conn.execute(
            text(
                """
                SELECT CAST(cd.dataset_id AS text) AS dataset_id, cd.table_name
                FROM cached_datasets cd
                WHERE cd.status = 'ready'
                  AND cd.table_name IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM information_schema.tables t
                      WHERE t.table_schema = 'public'
                        AND t.table_name = cd.table_name
                  )
                """
            )
        ).fetchall()
        for row in orphaned_rows:
            conn.execute(
                text(
                    """
                    UPDATE cached_datasets
                    SET status = 'error',
                        retry_count = 0,
                        error_message = 'Table missing: marked for re-download',
                        updated_at = NOW()
                    WHERE dataset_id = CAST(:did AS uuid)
                      AND status = 'ready'
                    """
                ),
                {"did": row.dataset_id},
            )
            conn.execute(
                text("UPDATE datasets SET is_cached = false WHERE id = CAST(:did AS uuid)"),
                {"did": row.dataset_id},
            )
            orphaned_ready += 1

        inconsistent_rows = conn.execute(
            text(
                """
                SELECT CAST(d.id AS text) AS dataset_id
                FROM datasets d
                WHERE d.is_cached = true
                  AND NOT EXISTS (
                      SELECT 1
                      FROM cached_datasets cd
                      WHERE cd.dataset_id = d.id
                        AND cd.status = 'ready'
                  )
                """
            )
        ).fetchall()
        for row in inconsistent_rows:
            conn.execute(
                text("UPDATE datasets SET is_cached = false WHERE id = CAST(:did AS uuid)"),
                {"did": row.dataset_id},
            )
            fixed_cached_flags += 1

        missing_chunk_rows = conn.execute(
            text(
                """
                SELECT CAST(d.id AS text) AS dataset_id
                FROM datasets d
                WHERE d.is_cached = true
                  AND EXISTS (
                      SELECT 1
                      FROM cached_datasets cd
                      WHERE cd.dataset_id = d.id
                        AND cd.status = 'ready'
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM dataset_chunks dc
                      WHERE dc.dataset_id = d.id
                  )
                LIMIT 5000
                """
            )
        ).fetchall()

    if redispatch:
        for row in orphaned_rows:
            collect_dataset.delay(str(row.dataset_id))

    if reindex_embeddings and missing_chunk_rows:
        from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

        for row in missing_chunk_rows:
            index_dataset_embedding.delay(str(row.dataset_id))
            reindexed_missing_chunks += 1

    return {
        "orphaned_ready": orphaned_ready,
        "fixed_cached_flags": fixed_cached_flags,
        "reindexed_missing_chunks": reindexed_missing_chunks,
    }


def _revive_schema_mismatch(engine, *, redispatch: bool = False) -> dict[str, int]:
    """Move legacy schema_mismatch rows back into the retry path."""
    revived = 0

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT CAST(dataset_id AS text) AS dataset_id
                FROM cached_datasets
                WHERE status = 'schema_mismatch'
                  AND retry_count < :max
                """
            ),
            {"max": MAX_TOTAL_ATTEMPTS},
        ).fetchall()

        for row in rows:
            conn.execute(
                text(
                    """
                    UPDATE cached_datasets
                    SET status = 'error',
                        error_message = 'Recovered from schema_mismatch for retry',
                        updated_at = NOW()
                    WHERE dataset_id = CAST(:did AS uuid)
                      AND status = 'schema_mismatch'
                    """
                ),
                {"did": row.dataset_id},
            )
            conn.execute(
                text("UPDATE datasets SET is_cached = false WHERE id = CAST(:did AS uuid)"),
                {"did": row.dataset_id},
            )
            revived += 1

    if redispatch:
        for row in rows:
            collect_dataset.delay(str(row.dataset_id))

    return {"revived_schema_mismatch": revived}


def _sanitize_table_name(name: str, portal: str = "") -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", name.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    if portal:
        portal_clean = re.sub(r"[^a-z0-9_]", "_", portal.lower()).strip("_")
        # Enforce PostgreSQL 63-char identifier limit:
        # "cache_" (6) + portal (max 12) + "_" (1) + name (max 44) = 63
        return f"cache_{portal_clean[:12]}_{clean[:44]}"
    return f"cache_{clean[:57]}"


def _schema_columns(columns) -> tuple[str, ...]:
    """Return a canonical, ordered schema signature excluding source metadata."""
    return tuple(sorted(str(col) for col in columns if str(col) != "_source_dataset_id"))


def _schema_suffix(columns) -> str:
    signature = "|".join(_schema_columns(columns))
    return hashlib.sha1(signature.encode("utf-8")).hexdigest()[:8]


def _schema_table_name(base_table_name: str, columns) -> str:
    """Build a stable variant table name for a specific schema signature."""
    suffix = f"_s{_schema_suffix(columns)}"
    trimmed = base_table_name[: max(1, 63 - len(suffix))]
    return f"{trimmed}{suffix}"


def _resource_table_name(base_table_name: str, dataset_id: str) -> str:
    """Build a stable resource-specific fallback table name."""
    compact_id = str(dataset_id).replace("-", "")[:10]
    suffix = f"_r{compact_id}"
    trimmed = base_table_name[: max(1, 63 - len(suffix))]
    return f"{trimmed}{suffix}"


def _consolidated_table_name(base_table_name: str, columns) -> str:
    """Build a stable group-consolidated table name for a schema signature."""
    suffix = f"_g{_schema_suffix(columns)}"
    trimmed = base_table_name[: max(1, 63 - len(suffix))]
    return f"{trimmed}{suffix}"


def _create_alias_view(engine, alias_table_name: str, source_table_name: str) -> None:
    """Create a stable alias view so duplicate-format datasets remain queryable."""
    with engine.begin() as conn:
        conn.execute(text(f'DROP VIEW IF EXISTS "{alias_table_name}" CASCADE'))  # noqa: S608
        conn.execute(text(f'DROP TABLE IF EXISTS "{alias_table_name}" CASCADE'))  # noqa: S608
        conn.execute(
            text(
                f'CREATE VIEW "{alias_table_name}" AS SELECT * FROM "{source_table_name}"'
            )  # noqa: S608
        )


def _materialize_format_duplicate_aliases(
    engine,
    *,
    title: str,
    portal: str,
    group_table_name: str,
    stem_to_winner: dict[str, tuple[str, str, str]],
) -> dict[str, int]:
    """Create alias views for duplicate-format resources that share the same canonical source."""
    created = 0
    skipped_missing_source = 0

    with engine.connect() as conn:
        resources = conn.execute(
            text(
                "SELECT CAST(d.id AS text) as id, d.format, d.download_url "
                "FROM datasets d "
                "WHERE d.title = :title AND d.portal = :portal"
            ),
            {"title": title, "portal": portal},
        ).fetchall()
        conn.rollback()

    for resource in resources:
        url = resource.download_url or ""
        if not url:
            continue
        path = url.split("?")[0].split("#")[0]
        dot = path.rfind(".")
        stem = path[:dot] if dot > 0 else path
        winner = stem_to_winner.get(stem)
        if not winner:
            continue
        winner_id, _winner_fmt, _winner_url = winner
        if winner_id == resource.id:
            continue

        source_table = _resource_table_name(group_table_name, winner_id)
        alias_table = _resource_table_name(group_table_name, resource.id)
        if not _table_exists(engine, source_table):
            skipped_missing_source += 1
            continue

        with engine.begin() as conn:
            source_row = conn.execute(
                text(
                    """
                    SELECT row_count, columns_json, size_bytes, s3_key
                    FROM cached_datasets
                    WHERE dataset_id = CAST(:did AS uuid)
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """
                ),
                {"did": winner_id},
            ).fetchone()

        _create_alias_view(engine, alias_table, source_table)

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO cached_datasets
                        (dataset_id, table_name, status, row_count, columns_json, size_bytes, s3_key, error_message, updated_at)
                    VALUES
                        (CAST(:did AS uuid), :tn, 'ready', :rows, :cols, :size, :s3, :msg, NOW())
                    ON CONFLICT (table_name) DO UPDATE SET
                        dataset_id = CAST(:did AS uuid),
                        status = 'ready',
                        row_count = :rows,
                        columns_json = :cols,
                        size_bytes = :size,
                        s3_key = :s3,
                        error_message = :msg,
                        updated_at = NOW()
                    """
                ),
                {
                    "did": resource.id,
                    "tn": alias_table,
                    "rows": source_row.row_count if source_row else None,
                    "cols": source_row.columns_json if source_row else None,
                    "size": source_row.size_bytes if source_row else None,
                    "s3": source_row.s3_key if source_row else None,
                    "msg": f"format_duplicate_of:{winner_id}",
                },
            )
            conn.execute(
                text(
                    "UPDATE datasets SET is_cached = true, row_count = COALESCE(:rows, row_count) "
                    "WHERE id = CAST(:did AS uuid)"
                ),
                {
                    "did": resource.id,
                    "rows": source_row.row_count if source_row else None,
                },
            )
        created += 1

    return {
        "created_aliases": created,
        "skipped_missing_source": skipped_missing_source,
    }


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


def _parse_zip_archive(
    zf,
    *,
    zip_path: str,
    dataset_id: str,
    table_name: str,
    engine,
    append_mode: bool,
    nested_depth: int = 0,
) -> dict:
    """Parse a ZIP archive, recursing one level into nested ZIP members."""
    import zipfile

    sampled_note: str | None = None
    row_count = 0
    columns: list[str] = []

    for name in zf.namelist():
        lower = name.lower()
        if lower.endswith((".csv", ".txt")):
            csv_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", dir=_temp_dir())
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
                    sampled_note = f"sampled: first {row_count} rows kept (limit {MAX_TABLE_ROWS})"
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
            return {
                "parsed": True,
                "table_name": table_name,
                "append_mode": append_mode,
                "row_count": row_count,
                "columns": columns,
                "sampled_note": sampled_note,
                "result": None,
            }

        if lower.endswith((".xlsx", ".xls")):
            with zf.open(name) as f:
                content = f.read()
            try:
                df = _read_excel_frame(io.BytesIO(content), nrows=MAX_TABLE_ROWS)
            except ValueError as exc:
                if str(exc) == "excel_no_worksheets":
                    logger.warning(
                        "ZIP %s: skipping excel member %s with no worksheets",
                        dataset_id,
                        name,
                    )
                    continue
                raise
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
            table_name, append_mode, routed_status = _route_table_for_schema(
                engine,
                dataset_id,
                table_name,
                df.columns,
                append_mode=append_mode,
            )
            if routed_status:
                if routed_status == "resource_table_full":
                    return {"parsed": False, "result": {"dataset_id": dataset_id, "error": routed_status}}
                return {"parsed": False, "result": {"dataset_id": dataset_id, "status": routed_status}}
            _to_sql_safe(
                df,
                table_name,
                engine,
                if_exists="append" if append_mode else "replace",
                index=False,
            )
            return {
                "parsed": True,
                "table_name": table_name,
                "append_mode": append_mode,
                "row_count": len(df),
                "columns": list(df.columns),
                "sampled_note": sampled_note,
                "result": None,
            }

        if lower.endswith(".geojson"):
            with zf.open(name) as f:
                content = f.read()
            raw_geo = json.loads(content)
            features = raw_geo.get("features", []) if isinstance(raw_geo, dict) else []
            if features:
                df = _geojson_features_to_df(features)
            else:
                df = pd.json_normalize(raw_geo if isinstance(raw_geo, list) else [raw_geo])
            if len(df) > MAX_TABLE_ROWS:
                sampled_note = f"sampled: first {MAX_TABLE_ROWS} of {len(df)} total rows"
                logger.warning(
                    "Dataset %s (zip/geojson) truncated from %d to %d rows",
                    dataset_id,
                    len(df),
                    MAX_TABLE_ROWS,
                )
                df = df.iloc[:MAX_TABLE_ROWS]
            df["_source_dataset_id"] = dataset_id
            table_name, append_mode, routed_status = _route_table_for_schema(
                engine,
                dataset_id,
                table_name,
                df.columns,
                append_mode=append_mode,
            )
            if routed_status:
                if routed_status == "resource_table_full":
                    return {"parsed": False, "result": {"dataset_id": dataset_id, "error": routed_status}}
                return {"parsed": False, "result": {"dataset_id": dataset_id, "status": routed_status}}
            _to_sql_safe(
                df,
                table_name,
                engine,
                if_exists="append" if append_mode else "replace",
                index=False,
            )
            return {
                "parsed": True,
                "table_name": table_name,
                "append_mode": append_mode,
                "row_count": len(df),
                "columns": list(df.columns),
                "sampled_note": sampled_note,
                "result": None,
            }

        if lower.endswith(".json"):
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
                sampled_note = f"sampled: first {MAX_TABLE_ROWS} of {len(df)} total rows"
                logger.warning(
                    "Dataset %s (zip/json) truncated from %d to %d rows",
                    dataset_id,
                    len(df),
                    MAX_TABLE_ROWS,
                )
                df = df.iloc[:MAX_TABLE_ROWS]
            df["_source_dataset_id"] = dataset_id
            table_name, append_mode, routed_status = _route_table_for_schema(
                engine,
                dataset_id,
                table_name,
                df.columns,
                append_mode=append_mode,
            )
            if routed_status:
                if routed_status == "resource_table_full":
                    return {"parsed": False, "result": {"dataset_id": dataset_id, "error": routed_status}}
                return {"parsed": False, "result": {"dataset_id": dataset_id, "status": routed_status}}
            _to_sql_safe(
                df,
                table_name,
                engine,
                if_exists="append" if append_mode else "replace",
                index=False,
            )
            return {
                "parsed": True,
                "table_name": table_name,
                "append_mode": append_mode,
                "row_count": len(df),
                "columns": list(df.columns),
                "sampled_note": sampled_note,
                "result": None,
            }

        if nested_depth < 1 and lower.endswith(".zip"):
            nested_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip", dir=_temp_dir())
            nested_tmp_path = nested_tmp.name
            nested_tmp.close()
            try:
                with zf.open(name) as zf_entry, open(nested_tmp_path, "wb") as out:
                    while True:
                        block = zf_entry.read(256 * 1024)
                        if not block:
                            break
                        out.write(block)
                try:
                    with zipfile.ZipFile(nested_tmp_path) as nested_zf:
                        nested_result = _parse_zip_archive(
                            nested_zf,
                            zip_path=nested_tmp_path,
                            dataset_id=dataset_id,
                            table_name=table_name,
                            engine=engine,
                            append_mode=append_mode,
                            nested_depth=nested_depth + 1,
                        )
                except zipfile.BadZipFile:
                    logger.warning("ZIP %s: nested member %s is not a valid ZIP", dataset_id, name)
                    continue
                if nested_result.get("parsed") or nested_result.get("result") is not None:
                    return nested_result
            finally:
                try:
                    os.unlink(nested_tmp_path)
                except OSError:
                    pass

    has_shp = any(n.lower().endswith(".shp") for n in zf.namelist())
    if has_shp:
        df = _read_shapefile_from_zip(zip_path)
        if df is not None and len(df) > 0:
            if len(df) > MAX_TABLE_ROWS:
                sampled_note = f"sampled: first {MAX_TABLE_ROWS} of {len(df)} rows (shapefile)"
                logger.warning(
                    "Dataset %s (zip/shp) truncated from %d to %d rows",
                    dataset_id,
                    len(df),
                    MAX_TABLE_ROWS,
                )
                df = df.iloc[:MAX_TABLE_ROWS]
            df["_source_dataset_id"] = dataset_id
            table_name, append_mode, routed_status = _route_table_for_schema(
                engine,
                dataset_id,
                table_name,
                df.columns,
                append_mode=append_mode,
            )
            if routed_status:
                if routed_status == "resource_table_full":
                    return {"parsed": False, "result": {"dataset_id": dataset_id, "error": routed_status}}
                return {"parsed": False, "result": {"dataset_id": dataset_id, "status": routed_status}}
            _to_sql_safe(
                df,
                table_name,
                engine,
                if_exists="append" if append_mode else "replace",
                index=False,
            )
            return {
                "parsed": True,
                "table_name": table_name,
                "append_mode": append_mode,
                "row_count": len(df),
                "columns": list(df.columns),
                "sampled_note": sampled_note,
                "result": None,
            }

    return {
        "parsed": False,
        "table_name": table_name,
        "append_mode": append_mode,
        "row_count": 0,
        "columns": [],
        "sampled_note": None,
        "result": None,
    }


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
            content_length = int(resp.headers.get("content-length", 0) or 0)
            if content_length > max_bytes:
                raise ValueError(f"file_too_large: {content_length} bytes (limit {max_bytes})")
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


def _make_unique_columns(columns) -> list[str]:
    """Normalize column names and guarantee uniqueness after cleanup."""
    used: set[str] = set()
    counters: dict[str, int] = {}
    normalized: list[str] = []

    for idx, raw in enumerate(columns):
        if pd.isna(raw):
            base = f"col_{idx}"
        else:
            base = str(raw).replace("\xa0", "").strip()
            if not base or base.lower() == "nan":
                base = f"col_{idx}"

        counters[base] = counters.get(base, 0) + 1
        candidate = base if counters[base] == 1 else f"{base}_{counters[base]}"
        while candidate in used:
            counters[base] += 1
            candidate = f"{base}_{counters[base]}"

        used.add(candidate)
        normalized.append(candidate)

    return normalized


def _serialize_nested_value(value):
    """Convert Python container values into stable JSON strings for SQL writes."""
    if isinstance(value, dict | list | tuple | set):
        try:
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
        except TypeError:
            return json.dumps(str(value), ensure_ascii=False)
    return value


def _serialize_structured_cells(df: pd.DataFrame) -> pd.DataFrame:
    """Stringify nested JSON-like cells that psycopg cannot adapt automatically."""
    if df.empty:
        return df

    normalized = df.copy()
    for column in normalized.columns:
        series = normalized[column]
        if getattr(series.dtype, "kind", None) != "O":
            continue
        normalized[column] = series.map(_serialize_nested_value)
    return normalized


def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Clean column names: strip whitespace, non-breaking spaces, normalize.

    If most columns are 'Unnamed: N', treat the first data row as the real header.
    """
    df.columns = _make_unique_columns(df.columns)

    # Detect misplaced header: if >50% of columns are "Unnamed: N", promote first row
    unnamed_count = sum(1 for c in df.columns if str(c).startswith("Unnamed:"))
    if unnamed_count > len(df.columns) * 0.5 and len(df) > 1:
        new_header = _make_unique_columns(df.iloc[0])
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = new_header

    df = _serialize_structured_cells(df)
    return _compact_wide_dataframe(df)


def _read_excel_frame(source, *, nrows: int, **kwargs) -> pd.DataFrame:
    """Read an Excel workbook and normalize empty/invalid-sheet errors."""
    try:
        return pd.read_excel(source, nrows=nrows, **kwargs)
    except ValueError as exc:
        message = str(exc)
        if "0 worksheets found" in message or "Worksheet index 0 is invalid" in message:
            raise ValueError("excel_no_worksheets") from exc
        raise


def _compact_wide_dataframe(df: pd.DataFrame, max_columns: int = _MAX_SQL_COLUMNS) -> pd.DataFrame:
    """Collapse overflow columns into `_overflow_json` before hitting Postgres column limits."""
    if len(df.columns) <= max_columns:
        return df

    keep_budget = max_columns - 1  # reserve one slot for _overflow_json
    if "_source_dataset_id" in df.columns:
        base_cols = [col for col in df.columns if col != "_source_dataset_id"]
        kept_base = base_cols[: max(0, keep_budget - 1)]
        keep_cols = kept_base + ["_source_dataset_id"]
    else:
        keep_cols = list(df.columns[:keep_budget])

    overflow_cols = [col for col in df.columns if col not in keep_cols]
    overflow_frame = df[overflow_cols].where(pd.notna(df[overflow_cols]), None)
    overflow_records = overflow_frame.to_dict(orient="records")

    compacted = df[keep_cols].copy()
    compacted["_overflow_json"] = [
        json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        if any(value is not None for value in record.values())
        else None
        for record in overflow_records
    ]
    logger.warning(
        "Compacted wide dataset from %d to %d SQL columns (%d overflow columns stored in _overflow_json)",
        len(df.columns),
        len(compacted.columns),
        len(overflow_cols),
    )
    return compacted


def _detect_schema_drift(
    engine, table_name: str, new_df: pd.DataFrame
) -> dict[str, list[str]] | None:
    """Compare the existing cache table schema to the incoming DataFrame.

    Returns a dict with ``added``, ``removed`` and ``type_changed`` column
    lists if drift is detected, or None if the table does not exist or the
    schemas match. Used to emit a structured warning BEFORE we replace the
    table — operators need visibility when an upstream schema shifts
    because downstream SQL sandbox queries and NL2SQL prompts assume the
    schema is stable. Read-only, non-blocking: if the inspection itself
    fails, we return None and let the write proceed.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT column_name, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = :tn"
                ),
                {"tn": table_name},
            ).fetchall()
            conn.rollback()
    except Exception:
        return None

    if not rows:
        return None  # new table, no drift

    existing = {r[0]: r[1] for r in rows}
    incoming = {str(c) for c in new_df.columns}

    added = sorted(incoming - existing.keys())
    removed = sorted(set(existing.keys()) - incoming)

    # Rough type drift check: compare pandas dtype kind to PG type family.
    type_changed: list[str] = []
    pg_numeric = {"integer", "bigint", "smallint", "numeric", "double precision", "real"}
    pg_text = {"text", "character varying", "character", "varchar"}
    pg_temporal = {"date", "timestamp without time zone", "timestamp with time zone", "time"}
    for col in sorted(incoming & existing.keys()):
        pg_type = existing[col].lower()
        pd_dtype = str(new_df[col].dtype)
        pd_kind = new_df[col].dtype.kind  # i,u,f,O,M,b
        if pd_kind in ("i", "u", "f") and pg_type not in pg_numeric:
            type_changed.append(f"{col}: {pg_type} -> {pd_dtype}")
        elif pd_kind == "O" and pg_type not in pg_text:
            type_changed.append(f"{col}: {pg_type} -> {pd_dtype}")
        elif pd_kind == "M" and pg_type not in pg_temporal:
            type_changed.append(f"{col}: {pg_type} -> {pd_dtype}")

    if not (added or removed or type_changed):
        return None
    return {"added": added, "removed": removed, "type_changed": type_changed}


def _to_sql_safe(df: pd.DataFrame, table_name: str, engine, **kwargs):
    """Write DataFrame to SQL, retrying with DROP if schema mismatch occurs.

    Before the write, inspect the existing table schema (if any) for
    drift — column additions, removals, or type shifts — and emit a
    structured warning so operators know when an upstream feed changed.
    This does not block the write; downstream SQL sandbox/NL2SQL consumers
    may still break on the new schema, but at least the drift is visible
    in logs and metrics instead of surfacing as a mysterious "column does
    not exist" error hours later.
    """
    df = _sanitize_columns(df)

    if kwargs.get("if_exists") == "replace":
        drift = _detect_schema_drift(engine, table_name, df)
        if drift is not None:
            logger.warning(
                "schema_drift_detected table=%s added=%s removed=%s type_changed=%s",
                table_name,
                drift["added"],
                drift["removed"],
                drift["type_changed"],
            )
            try:
                from app.infrastructure.monitoring.metrics import MetricsCollector

                MetricsCollector().record_connector_call(
                    f"schema_drift:{table_name}",
                    latency_ms=0,
                    error=True,
                )
            except Exception:
                logger.debug("Could not record schema_drift metric", exc_info=True)

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


def _table_exists(engine, table_name: str) -> bool:
    try:
        with engine.connect() as conn:
            exists = conn.execute(
                text(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = :tn AND table_schema = 'public')"
                ),
                {"tn": table_name},
            ).scalar()
            conn.rollback()
        return bool(exists)
    except Exception:
        return False


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


def _check_schema_compat_columns(engine, table_name: str, columns) -> bool:
    """Check if columns are compatible with an existing table for append.

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
        new_cols = set(_schema_columns(columns))
        existing_cols_clean = existing_cols - {"_source_dataset_id"}
        return new_cols.issubset(existing_cols_clean) or existing_cols_clean.issubset(new_cols)
    except Exception:
        return True  # If check fails, allow append


def _check_schema_compat(engine, table_name: str, df: pd.DataFrame) -> bool:
    """Check if df columns are compatible with existing table for append."""
    return _check_schema_compat_columns(engine, table_name, df.columns)


def _route_table_for_schema(
    engine,
    dataset_id: str,
    table_name: str,
    columns,
    *,
    append_mode: bool,
) -> tuple[str, bool, str | None]:
    """Route mismatched siblings into a stable schema-specific table instead of dropping them."""
    if not append_mode:
        return table_name, False, None

    target_table = table_name
    if _table_exists(engine, table_name):
        if not _check_schema_compat_columns(engine, table_name, columns):
            target_table = _schema_table_name(table_name, columns)
            logger.info(
                "Dataset %s schema differs from %s, routing to %s",
                dataset_id,
                table_name,
                target_table,
            )

    if target_table != table_name:
        _ensure_cached_entry(engine, dataset_id, target_table)

    if not _table_exists(engine, target_table):
        return target_table, False, None

    try:
        with engine.connect() as conn:
            already_appended = conn.execute(
                text(
                    f'SELECT EXISTS(SELECT 1 FROM "{target_table}" WHERE _source_dataset_id = :did LIMIT 1)'
                ),  # noqa: S608
                {"did": dataset_id},
            ).scalar()
            conn.rollback()
        if already_appended:
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE datasets SET is_cached = true WHERE id = CAST(:id AS uuid)"),
                    {"id": dataset_id},
                )
            return target_table, True, "already_appended"
    except Exception:
        logger.debug("Could not inspect source_dataset_id on %s", target_table, exc_info=True)

    current_rows = _get_table_row_count(engine, target_table)
    if current_rows >= MAX_TABLE_ROWS:
        resource_table = _resource_table_name(target_table, dataset_id)
        logger.info(
            "Dataset %s target table %s is full (%d rows), routing to resource table %s",
            dataset_id,
            target_table,
            current_rows,
            resource_table,
        )
        _ensure_cached_entry(engine, dataset_id, resource_table)
        if not _table_exists(engine, resource_table):
            return resource_table, False, None

        resource_rows = _get_table_row_count(engine, resource_table)
        if resource_rows >= MAX_TABLE_ROWS:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE cached_datasets
                        SET status = 'error',
                            error_message = 'resource_table_full',
                            updated_at = NOW()
                        WHERE dataset_id = CAST(:id AS uuid)
                        """
                    ),
                    {"id": dataset_id},
                )
                conn.execute(
                    text("UPDATE datasets SET is_cached = false WHERE id = CAST(:id AS uuid)"),
                    {"id": dataset_id},
                )
            return resource_table, False, "resource_table_full"

        try:
            with engine.connect() as conn:
                already_appended = conn.execute(
                    text(
                        f'SELECT EXISTS(SELECT 1 FROM "{resource_table}" WHERE _source_dataset_id = :did LIMIT 1)'
                    ),  # noqa: S608
                    {"did": dataset_id},
                ).scalar()
                conn.rollback()
            if already_appended:
                with engine.begin() as conn:
                    conn.execute(
                        text("UPDATE datasets SET is_cached = true WHERE id = CAST(:id AS uuid)"),
                        {"id": dataset_id},
                    )
                return resource_table, True, "already_appended"
        except Exception:
            logger.debug(
                "Could not inspect resource fallback table %s", resource_table, exc_info=True
            )

        return resource_table, True, None

    return target_table, True, None


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
    name="openarg.collect_data", bind=True, max_retries=3, soft_time_limit=1200, time_limit=1380
)
def collect_dataset(self, dataset_id: str):
    """
    Descarga un dataset real, lo parsea y lo guarda como tabla SQL.
    Esto permite al Analyst hacer queries SQL reales sobre los datos.
    """

    logger.info(f"Collecting dataset: {dataset_id}")
    engine = get_sync_engine()
    table_name = None
    started_at = time.monotonic()
    download_ms = 0
    parse_ms = 0
    upload_ms = 0
    lock_key = _lock_key(_COLLECT_DATASET_LOCK_PREFIX, dataset_id)
    lock_acquired = False

    try:
        lock_acquired = _try_advisory_lock(engine, lock_key)
        if not lock_acquired:
            logger.info("Dataset %s is already being collected, skipping duplicate run", dataset_id)
            return {"dataset_id": dataset_id, "status": "already_collecting"}

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
        group_table_name = _sanitize_table_name(title, portal)
        table_name = _resource_table_name(group_table_name, dataset_id)

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

        # Resource staging is the primary materialization path: each dataset gets its own table.
        append_mode = False
        previous_cache_state = None
        with engine.begin() as conn:
            cached = conn.execute(
                text(
                    "SELECT id, dataset_id FROM cached_datasets "
                    "WHERE table_name = :tn AND status = 'ready'"
                ),
                {"tn": table_name},
            ).fetchone()
            previous_cache_state = conn.execute(
                text(
                    """
                    SELECT d.is_cached,
                           cd.table_name,
                           cd.row_count AS cached_row_count,
                           cd.columns_json,
                           cd.s3_key,
                           cd.error_message
                    FROM datasets d
                    LEFT JOIN cached_datasets cd
                      ON cd.dataset_id = d.id
                    WHERE d.id = CAST(:id AS uuid)
                    ORDER BY CASE WHEN cd.status = 'ready' THEN 0 ELSE 1 END,
                             cd.updated_at DESC NULLS LAST
                    LIMIT 1
                    """
                ),
                {"id": dataset_id},
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
        if not _has_temp_space(max_download_bytes):
            logger.warning("Insufficient temp space before downloading dataset %s", dataset_id)
            # Temp space is transient — retry later instead of marking permanently failed
            backoff = min(120 * (2**self.request.retries), 600)
            raise self.retry(
                exc=RuntimeError("insufficient_temp_space"),
                countdown=int(backoff + random.uniform(0, backoff * 0.3)),
            )

        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{fmt}", dir=_temp_dir())
        tmp_path = tmp_file.name
        tmp_file.close()
        file_size = 0

        verify_ssl = _should_verify_ssl(download_url)

        try:
            try:
                download_started_at = time.monotonic()
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
            download_ms = int((time.monotonic() - download_started_at) * 1000)

            # Upload raw file to S3 (from disk, not memory)
            s3_key = None
            try:
                upload_started_at = time.monotonic()
                filename = f"{source_id}.{fmt}"
                s3_key = _upload_file_to_s3(tmp_path, portal, dataset_id, filename)
                upload_ms = int((time.monotonic() - upload_started_at) * 1000)
                logger.info(f"Uploaded to S3: {s3_key}")
            except Exception:
                logger.warning(
                    "S3 upload failed for %s, continuing without S3", dataset_id, exc_info=True
                )

            # Parse and load into PostgreSQL
            row_count = 0
            columns: list[str] = []
            sampled_note: str | None = None  # set when dataset is truncated
            parse_started_at = time.monotonic()

            if fmt in ("csv", "txt"):
                # Chunked CSV loading — never loads full file into memory
                # For append mode, do a quick schema check using first chunk
                if append_mode:
                    try:
                        _csv_params = _detect_csv_params(tmp_path)
                        preview_df = pd.read_csv(tmp_path, nrows=5, **_csv_params)
                        table_name, append_mode, routed_status = _route_table_for_schema(
                            engine,
                            dataset_id,
                            table_name,
                            preview_df.columns,
                            append_mode=append_mode,
                        )
                        if routed_status:
                            if routed_status == "resource_table_full":
                                return {"dataset_id": dataset_id, "error": routed_status}
                            logger.info(
                                "Dataset %s resolved early status %s on %s",
                                dataset_id,
                                routed_status,
                                table_name,
                            )
                            return {"dataset_id": dataset_id, "status": routed_status}
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
                if not _has_temp_space(total_size):
                    logger.warning(
                        "ZIP %s: insufficient temp space for %d decompressed bytes",
                        dataset_id,
                        total_size,
                    )
                    backoff = min(120 * (2**self.request.retries), 600)
                    raise self.retry(
                        exc=RuntimeError("insufficient_temp_space (zip decompression)"),
                        countdown=int(backoff + random.uniform(0, backoff * 0.3)),
                    )

                zip_result = _parse_zip_archive(
                    zf,
                    zip_path=tmp_path,
                    dataset_id=dataset_id,
                    table_name=table_name,
                    engine=engine,
                    append_mode=append_mode,
                )
                if zip_result.get("result") is not None:
                    return zip_result["result"]

                if zip_result.get("parsed"):
                    table_name = zip_result["table_name"]
                    append_mode = zip_result["append_mode"]
                    row_count = zip_result["row_count"]
                    columns = zip_result["columns"]
                    sampled_note = zip_result["sampled_note"]
                else:
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
                table_name, append_mode, routed_status = _route_table_for_schema(
                    engine,
                    dataset_id,
                    table_name,
                    df.columns,
                    append_mode=append_mode,
                )
                if routed_status:
                    if routed_status == "resource_table_full":
                        return {"dataset_id": dataset_id, "error": routed_status}
                    return {"dataset_id": dataset_id, "status": routed_status}
                if append_mode:
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
                table_name, append_mode, routed_status = _route_table_for_schema(
                    engine,
                    dataset_id,
                    table_name,
                    df.columns,
                    append_mode=append_mode,
                )
                if routed_status:
                    if routed_status == "resource_table_full":
                        return {"dataset_id": dataset_id, "error": routed_status}
                    return {"dataset_id": dataset_id, "status": routed_status}
                if append_mode:
                    _to_sql_safe(df, table_name, engine, if_exists="append", index=False)
                else:
                    _to_sql_safe(df, table_name, engine, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            elif fmt in ("xlsx", "xls"):
                try:
                    df = _read_excel_frame(tmp_path, nrows=MAX_TABLE_ROWS)
                except ValueError as exc:
                    if str(exc) == "excel_no_worksheets":
                        logger.warning("Dataset %s: Excel workbook has no worksheets", dataset_id)
                        _set_error_status(
                            engine,
                            dataset_id,
                            "excel_no_worksheets",
                            table_name=table_name,
                        )
                        return {"error": "excel_no_worksheets"}
                    raise
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
                table_name, append_mode, routed_status = _route_table_for_schema(
                    engine,
                    dataset_id,
                    table_name,
                    df.columns,
                    append_mode=append_mode,
                )
                if routed_status:
                    if routed_status == "resource_table_full":
                        return {"dataset_id": dataset_id, "error": routed_status}
                    return {"dataset_id": dataset_id, "status": routed_status}
                if append_mode:
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
                table_name, append_mode, routed_status = _route_table_for_schema(
                    engine,
                    dataset_id,
                    table_name,
                    df.columns,
                    append_mode=append_mode,
                )
                if routed_status:
                    if routed_status == "resource_table_full":
                        return {"dataset_id": dataset_id, "error": routed_status}
                    return {"dataset_id": dataset_id, "status": routed_status}
                if append_mode:
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
                table_name, append_mode, routed_status = _route_table_for_schema(
                    engine,
                    dataset_id,
                    table_name,
                    df.columns,
                    append_mode=append_mode,
                )
                if routed_status:
                    if routed_status == "resource_table_full":
                        return {"dataset_id": dataset_id, "error": routed_status}
                    return {"dataset_id": dataset_id, "status": routed_status}
                if append_mode:
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
                from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

                index_dataset_embedding.delay(dataset_id)

                from app.infrastructure.celery.tasks.catalog_enrichment_tasks import (
                    enrich_single_table,
                )

                enrich_single_table.delay(table_name)
                parse_ms = int((time.monotonic() - parse_started_at) * 1000)
                total_ms = int((time.monotonic() - started_at) * 1000)
                logger.info(
                    "Collector timings dataset=%s portal=%s fmt=%s rows=%s size_bytes=%s "
                    "download_ms=%s upload_ms=%s parse_ms=%s total_ms=%s mode=append",
                    dataset_id,
                    portal,
                    fmt,
                    row_count,
                    file_size,
                    download_ms,
                    upload_ms,
                    parse_ms,
                    total_ms,
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
                was_cached = previous_cache_state.is_cached if previous_cache_state else False
                conn.execute(
                    text(
                        "UPDATE datasets SET is_cached = true, row_count = :rows WHERE id = CAST(:id AS uuid)"
                    ),
                    {"rows": row_count, "id": dataset_id},
                )

            # Add PostGIS native geometry column if this is a geo dataset
            _ensure_postgis_geom(engine, table_name, columns)

            previous_table_name = previous_cache_state.table_name if previous_cache_state else None
            previous_row_count = previous_cache_state.cached_row_count if previous_cache_state else None
            previous_columns_json = (
                previous_cache_state.columns_json if previous_cache_state else None
            )
            previous_s3_key = previous_cache_state.s3_key if previous_cache_state else None
            previous_sampled_note = (
                previous_cache_state.error_message if previous_cache_state else None
            )
            should_refresh_embeddings = (
                (not was_cached)
                or previous_table_name != table_name
                or previous_row_count != row_count
                or previous_columns_json != columns_json
                or previous_s3_key != s3_key
                or previous_sampled_note != sampled_note
            )

            if should_refresh_embeddings:
                from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

                index_dataset_embedding.delay(dataset_id)

                # Auto-enrich with semantic catalog metadata
                from app.infrastructure.celery.tasks.catalog_enrichment_tasks import (
                    enrich_single_table,
                )

                enrich_single_table.delay(table_name)

            parse_ms = int((time.monotonic() - parse_started_at) * 1000)
            total_ms = int((time.monotonic() - started_at) * 1000)
            logger.info(
                "Collector timings dataset=%s portal=%s fmt=%s rows=%s size_bytes=%s "
                "download_ms=%s upload_ms=%s parse_ms=%s total_ms=%s",
                dataset_id,
                portal,
                fmt,
                row_count,
                file_size,
                download_ms,
                upload_ms,
                parse_ms,
                total_ms,
            )
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
        total_ms = int((time.monotonic() - started_at) * 1000)
        logger.error("Task timed out for dataset %s after %dms", dataset_id, total_ms)
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
                    "file_too_large",
                    "No columns to parse from file",
                    "Excel file format cannot be determined",
                )
            )
            or "TooManyRedirects" in type(exc).__name__
        )
        if non_retryable:
            total_ms = int((time.monotonic() - started_at) * 1000)
            logger.warning(
                "Non-retryable error for %s after %dms: %s",
                dataset_id,
                total_ms,
                exc_str[:200],
            )
            _set_error_status(engine, dataset_id, exc_str[:500], table_name=table_name)
            return {"error": "non_retryable"}

        total_ms = int((time.monotonic() - started_at) * 1000)
        logger.exception("Collection failed for dataset %s after %dms", dataset_id, total_ms)
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
        if lock_acquired:
            _release_advisory_lock(engine, lock_key)
        engine.dispose()


@celery_app.task(name="openarg.bulk_collect_all", bind=True, soft_time_limit=300, time_limit=420)
def bulk_collect_all(self, portal: str | None = None):
    """Descarga y cachea TODOS los datasets no cacheados.

    Phase 1: Dispatch individual collect_dataset for groups with <=50 siblings.
    Phase 2: Dispatch collect_large_group for groups with >50 siblings,
             which handles format dedup, schema probing, and row budgets.
    """
    engine = get_sync_engine()
    lock_acquired = False

    try:
        lock_acquired = _try_advisory_lock(engine, _BULK_COLLECT_LOCK_KEY)
        if not lock_acquired:
            logger.info("bulk_collect_all skipped because another run already holds the lock")
            return {"status": "skipped_already_running"}

        reconciled = _reconcile_cache_coverage(
            engine,
            redispatch=False,
            reindex_embeddings=True,
        )
        if any(reconciled.values()):
            logger.warning(
                "Bulk collect reconciled cache coverage before dispatch: orphaned_ready=%s fixed_cached_flags=%s reindexed_missing_chunks=%s",
                reconciled["orphaned_ready"],
                reconciled["fixed_cached_flags"],
                reconciled["reindexed_missing_chunks"],
            )

        revived_schema = _revive_schema_mismatch(engine, redispatch=False)
        if revived_schema["revived_schema_mismatch"]:
            logger.warning(
                "Bulk collect revived %s schema_mismatch rows back into retry",
                revived_schema["revived_schema_mismatch"],
            )

        recycled = _recycle_stuck_downloads(engine, redispatch=False)
        if any(recycled.values()):
            logger.warning(
                "Bulk collect recycled stale downloads before dispatch: ready=%s error=%s failed=%s",
                recycled["recovered_ready"],
                recycled["recycled_error"],
                recycled["recycled_failed"],
            )

        # ── Phase 1: individual datasets (groups <= 50 siblings) ──
        query = (
            "SELECT CAST(d.id AS text), d.portal FROM datasets d "
            "LEFT JOIN cached_datasets cd ON cd.dataset_id = d.id "
            "WHERE d.is_cached = false "
            "AND (cd.status IS NULL OR cd.status NOT IN ('ready', 'permanently_failed')) "
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

        inflight_total, inflight_by_portal = _get_inflight_counts(engine)
        selected_rows, deferred_rows = _throttle_collect_rows(
            rows,
            inflight_total=inflight_total,
            inflight_by_portal=inflight_by_portal,
            max_total=_COLLECT_MAX_INFLIGHT,
            max_per_portal=_COLLECT_MAX_INFLIGHT_PER_PORTAL,
        )

        phase1_count = len(selected_rows)
        deferred_individual = len(deferred_rows)
        tasks = [collect_dataset.s(dataset_id) for dataset_id, _portal in selected_rows]
        phase1_batches = 0
        if tasks:
            phase1_batches = _dispatch_in_batches(
                tasks,
                batch_size=_COLLECT_DISPATCH_BATCH_SIZE,
                step_seconds=_COLLECT_DISPATCH_STEP_SECONDS,
            )

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

        remaining_total_slots = max(_COLLECT_MAX_INFLIGHT - inflight_total - phase1_count, 0)
        remaining_portal_slots = {
            portal_key: max(
                _COLLECT_MAX_INFLIGHT_PER_PORTAL
                - inflight_by_portal.get(portal_key, 0)
                - sum(1 for _did, p in selected_rows if p == portal_key),
                0,
            )
            for portal_key in {row.portal for row in large_groups}
        }
        selected_groups = []
        deferred_groups = 0
        for row in large_groups:
            if remaining_total_slots <= 0 or remaining_portal_slots.get(row.portal, 0) <= 0:
                deferred_groups += 1
                continue
            selected_groups.append(row)
            remaining_total_slots -= 1
            remaining_portal_slots[row.portal] = remaining_portal_slots.get(row.portal, 0) - 1

        phase2_count = len(selected_groups)
        phase2_batches = 0
        if selected_groups:
            large_tasks = [collect_large_group.s(row.title, row.portal) for row in selected_groups]
            phase2_batches = _dispatch_in_batches(
                large_tasks,
                batch_size=_COLLECT_DISPATCH_BATCH_SIZE,
                step_seconds=_COLLECT_DISPATCH_STEP_SECONDS,
            )

        logger.info(
            "Bulk collect: inflight_total=%s phase1=%s individual (%s batches, %s deferred) "
            "phase2=%s large groups (%s batches, %s deferred)",
            inflight_total,
            phase1_count,
            phase1_batches,
            deferred_individual,
            phase2_count,
            phase2_batches,
            deferred_groups,
        )
        return {
            "dispatched_individual": phase1_count,
            "dispatched_groups": phase2_count,
            "phase1_batches": phase1_batches,
            "phase2_batches": phase2_batches,
            "deferred_individual": deferred_individual,
            "deferred_groups": deferred_groups,
            "inflight_total": inflight_total,
            "reconciled_orphaned_ready": reconciled["orphaned_ready"],
            "reconciled_cached_flags": reconciled["fixed_cached_flags"],
            "reindexed_missing_chunks": reconciled["reindexed_missing_chunks"],
            "revived_schema_mismatch": revived_schema["revived_schema_mismatch"],
            "recycled_stale_ready": recycled["recovered_ready"],
            "recycled_stale_error": recycled["recycled_error"],
            "recycled_stale_failed": recycled["recycled_failed"],
        }
    finally:
        if lock_acquired:
            _release_advisory_lock(engine, _BULK_COLLECT_LOCK_KEY)
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
    """Collect a large multi-resource group (>50 siblings) by staging each resource safely.

    Strategy:
      1. Format-dedup: group by URL stem, keep highest-priority format per stem
      2. Dispatch each surviving resource to its own primary cache table
      3. Leave overflow resources pending for a future pass instead of marking them cached
    """
    engine = get_sync_engine()
    try:
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
        tasks = [collect_dataset.s(did) for did, _, _ in deduped[:200]]
        dispatched_batches = 0
        if tasks:
            dispatched_batches = _dispatch_in_batches(
                tasks,
                batch_size=_COLLECT_DISPATCH_BATCH_SIZE,
                step_seconds=_COLLECT_DISPATCH_STEP_SECONDS,
            )

        consolidation_scheduled = False
        alias_materialization_scheduled = False
        if deduped:
            consolidate_group_tables.apply_async(
                args=[title, portal],
                countdown=(dispatched_batches * _COLLECT_DISPATCH_STEP_SECONDS) + 60,
            )
            consolidation_scheduled = True
            materialize_format_duplicate_aliases.apply_async(
                args=[title, portal],
                countdown=(dispatched_batches * _COLLECT_DISPATCH_STEP_SECONDS) + 30,
            )
            alias_materialization_scheduled = True

        # Leave remaining (beyond 200) pending for a later pass
        deferred_count = 0
        if len(deduped) > 200:
            deferred_count = len(deduped) - 200
            logger.warning(
                "Large group '%s' (%s): deferring %d resources beyond dispatch cap",
                title,
                portal,
                deferred_count,
            )

        deduped_ids = {did for did, _, _ in deduped}
        format_dupes = [r.id for r in resources if r.id not in deduped_ids]

        return {
            "title": title,
            "portal": portal,
            "total_resources": len(resources),
            "deduped": len(deduped),
            "dispatched": min(len(deduped), 200),
            "dispatch_batches": dispatched_batches,
            "deferred": deferred_count,
            "format_dupes_pending_alias": len(format_dupes),
            "consolidation_scheduled": consolidation_scheduled,
            "alias_materialization_scheduled": alias_materialization_scheduled,
        }
    except Exception as exc:
        logger.warning(f"collect_large_group failed for '{title}' ({portal})", exc_info=True)
        raise self.retry(exc=exc, countdown=60) from exc
    finally:
        engine.dispose()


@celery_app.task(
    name="openarg.materialize_format_duplicate_aliases",
    bind=True,
    soft_time_limit=300,
    time_limit=420,
)
def materialize_format_duplicate_aliases(self, title: str, portal: str):
    """Create alias views for lower-priority format duplicates after canonical resources land."""
    engine = get_sync_engine()
    try:
        group_table_name = _sanitize_table_name(title, portal)
        with engine.connect() as conn:
            resources = conn.execute(
                text(
                    "SELECT CAST(d.id AS text) as id, d.format, d.download_url "
                    "FROM datasets d "
                    "WHERE d.title = :title AND d.portal = :portal"
                ),
                {"title": title, "portal": portal},
            ).fetchall()
            conn.rollback()

        stem_to_winner: dict[str, tuple[str, str, str]] = {}
        for resource in resources:
            url = resource.download_url or ""
            if not url:
                continue
            path = url.split("?")[0].split("#")[0]
            dot = path.rfind(".")
            stem = path[:dot] if dot > 0 else path
            fmt = (resource.format or "").lower().strip()
            priority = _FORMAT_PRIORITY.get(fmt, 99)
            if stem not in stem_to_winner or priority < _FORMAT_PRIORITY.get(
                stem_to_winner[stem][1],
                99,
            ):
                stem_to_winner[stem] = (resource.id, fmt, url)

        stats = _materialize_format_duplicate_aliases(
            engine,
            title=title,
            portal=portal,
            group_table_name=group_table_name,
            stem_to_winner=stem_to_winner,
        )
        return {
            "title": title,
            "portal": portal,
            **stats,
        }
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
        recycled = _recycle_stuck_downloads(engine, redispatch=True)
        recovered_downloads = sum(recycled.values())

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
    name="openarg.consolidate_group_tables",
    bind=True,
    soft_time_limit=600,
    time_limit=900,
)
def consolidate_group_tables(self, title: str, portal: str):
    """Best-effort consolidation of ready resource tables for a title+portal group."""
    engine = get_sync_engine()
    try:
        base_table_name = _sanitize_table_name(title, portal)
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT CAST(d.id AS text) AS dataset_id,
                           cd.table_name,
                           cd.columns_json
                    FROM datasets d
                    JOIN cached_datasets cd ON cd.dataset_id = d.id
                    WHERE d.title = :title
                      AND d.portal = :portal
                      AND cd.status = 'ready'
                      AND cd.table_name LIKE :resource_pattern
                    ORDER BY cd.table_name
                    """
                ),
                {
                    "title": title,
                    "portal": portal,
                    "resource_pattern": f"{base_table_name}_r%",
                },
            ).fetchall()
            conn.rollback()

        groups: dict[tuple[str, ...], list[tuple[str, str]]] = {}
        for row in rows:
            try:
                columns = json.loads(row.columns_json or "[]")
            except (json.JSONDecodeError, TypeError):
                columns = []
            signature = _schema_columns(columns)
            if not signature:
                continue
            groups.setdefault(signature, []).append((str(row.dataset_id), row.table_name))

        consolidated_tables = 0
        consolidated_sources = 0
        for signature, members in groups.items():
            if len(members) < 2:
                continue

            target_table = _consolidated_table_name(base_table_name, signature)
            source_tables = [table_name for _dataset_id, table_name in members]

            with engine.begin() as conn:
                if not _table_exists(engine, target_table):
                    conn.execute(
                        text(
                            f'CREATE TABLE "{target_table}" AS SELECT * FROM "{source_tables[0]}" WITH NO DATA'
                        )  # noqa: S608
                    )

                loaded_ids = set(
                    row[0]
                    for row in conn.execute(
                        text(
                            f'SELECT DISTINCT _source_dataset_id FROM "{target_table}" '
                            "WHERE _source_dataset_id IS NOT NULL"
                        )  # noqa: S608
                    ).fetchall()
                )

                for dataset_id, source_table in members:
                    if dataset_id in loaded_ids:
                        continue
                    conn.execute(
                        text(f'INSERT INTO "{target_table}" SELECT * FROM "{source_table}"')  # noqa: S608
                    )
                    loaded_ids.add(dataset_id)
                    consolidated_sources += 1

            consolidated_tables += 1

        return {
            "title": title,
            "portal": portal,
            "schemas_seen": len(groups),
            "consolidated_tables": consolidated_tables,
            "consolidated_sources": consolidated_sources,
        }
    finally:
        engine.dispose()


@celery_app.task(name="openarg.audit_cache_coverage", soft_time_limit=60, time_limit=120)
def audit_cache_coverage():
    """Audit cache completeness and return actionable counts."""
    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            total_datasets = conn.execute(text("SELECT COUNT(*) FROM datasets")).scalar() or 0
            non_cached = (
                conn.execute(text("SELECT COUNT(*) FROM datasets WHERE is_cached = false")).scalar()
                or 0
            )
            stale_downloading = (
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM cached_datasets "
                        "WHERE status = 'downloading' "
                        "AND updated_at < NOW() - INTERVAL '30 minutes'"
                    )
                ).scalar()
                or 0
            )
            recoverable_errors = (
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM cached_datasets "
                        "WHERE status IN ('error', 'schema_mismatch')"
                    )
                ).scalar()
                or 0
            )
            permanently_failed = (
                conn.execute(
                    text("SELECT COUNT(*) FROM cached_datasets WHERE status = 'permanently_failed'")
                ).scalar()
                or 0
            )
            ready_missing_table = (
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM cached_datasets cd
                        WHERE cd.status = 'ready'
                          AND cd.table_name IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1
                              FROM information_schema.tables t
                              WHERE t.table_schema = 'public'
                                AND t.table_name = cd.table_name
                          )
                        """
                    )
                ).scalar()
                or 0
            )
            cached_flag_inconsistent = (
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM datasets d
                        WHERE d.is_cached = true
                          AND NOT EXISTS (
                              SELECT 1
                              FROM cached_datasets cd
                              WHERE cd.dataset_id = d.id
                                AND cd.status = 'ready'
                          )
                        """
                    )
                ).scalar()
                or 0
            )
            ready_missing_chunks = (
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM datasets d
                        WHERE d.is_cached = true
                          AND EXISTS (
                              SELECT 1
                              FROM cached_datasets cd
                              WHERE cd.dataset_id = d.id
                                AND cd.status = 'ready'
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM dataset_chunks dc
                              WHERE dc.dataset_id = d.id
                          )
                        """
                    )
                ).scalar()
                or 0
            )
            conn.rollback()

        return {
            "total_datasets": int(total_datasets),
            "non_cached": int(non_cached),
            "stale_downloading": int(stale_downloading),
            "recoverable_errors": int(recoverable_errors),
            "permanently_failed": int(permanently_failed),
            "ready_missing_table": int(ready_missing_table),
            "cached_flag_inconsistent": int(cached_flag_inconsistent),
            "ready_missing_chunks": int(ready_missing_chunks),
        }
    finally:
        engine.dispose()


@celery_app.task(
    name="openarg.reconcile_cache_coverage",
    bind=True,
    soft_time_limit=120,
    time_limit=180,
)
def reconcile_cache_coverage(self):
    """Fix inconsistent cache state and requeue missing collector/embedding work."""
    engine = get_sync_engine()
    try:
        reconciled = _reconcile_cache_coverage(
            engine,
            redispatch=True,
            reindex_embeddings=True,
        )
        logger.warning(
            "Reconciled cache coverage: orphaned_ready=%s fixed_cached_flags=%s reindexed_missing_chunks=%s",
            reconciled["orphaned_ready"],
            reconciled["fixed_cached_flags"],
            reconciled["reindexed_missing_chunks"],
        )
        return reconciled
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
