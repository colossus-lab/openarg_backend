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
from collections import Counter
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlparse

import httpx
import pandas as pd
import psycopg
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from app.application.catalog.physical_namer import RawPhysicalName, RawPhysicalNamer
from app.application.expander import MultiFileExpander
from app.application.validation.collector_hooks import (
    is_critical as _ws0_is_critical,
)
from app.application.validation.collector_hooks import (
    validate_post_parse as _ws0_validate_post_parse,
)
from app.application.validation.collector_hooks import (
    validate_pre_parse as _ws0_validate_pre_parse,
)
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine


def _read_byte_sample(path: str, n: int = 4096) -> bytes:
    """Read up to n bytes from disk for ingestion validators."""
    try:
        with open(path, "rb") as f:
            return f.read(n)
    except Exception:
        return b""

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

_LAYOUT_SIMPLE = "simple_tabular"
_LAYOUT_PRESENTATION = "presentation_sheet"
_LAYOUT_MULTILINE = "header_multiline"
_LAYOUT_SPARSE = "header_sparse"
_LAYOUT_WIDE = "wide_csv"

_HEADER_GOOD = "good"
_HEADER_DEGRADED = "degraded"
_HEADER_INVALID = "invalid"

_OUTCOME_MATERIALIZED_READY = "materialized_ready"
_OUTCOME_MATERIALIZED_DEGRADED = "materialized_degraded"
_OUTCOME_TERMINAL_NON_TABULAR = "terminal_non_tabular"
_OUTCOME_TERMINAL_UPSTREAM = "terminal_upstream"
_OUTCOME_RETRYABLE_UPSTREAM = "retryable_upstream"
_OUTCOME_RETRYABLE_MATERIALIZATION = "retryable_materialization"
_OUTCOME_PARSER_INVALID = "parser_invalid"

_PARSER_VERSION = "phase4-v1"
_NORMALIZATION_VERSION = "phase4-v1"

_ZIP_PARSEABLE_SUFFIXES = frozenset(
    {
        ".csv",
        ".txt",
        ".xlsx",
        ".xls",
        ".geojson",
        ".json",
        ".kml",
        ".kmz",
        ".shp",
        ".dbf",
        ".shx",
        ".prj",
        ".cpg",
        ".zip",
    }
)
_ZIP_DOCUMENT_SUFFIXES = frozenset(
    {
        ".pdf",
        ".doc",
        ".docx",
        ".ppt",
        ".pptx",
        ".odt",
        ".ods",
        ".odp",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".tif",
        ".tiff",
        ".webp",
    }
)


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
_ZIP_CSV_PREVIEW_ROWS = int(os.getenv("OPENARG_ZIP_CSV_PREVIEW_ROWS", "250"))
_MAX_SQL_COLUMNS = 1400
_TEMP_SPACE_RESERVE_BYTES = 256 * 1024 * 1024  # keep 256MB free for worker stability
_CSV_TARGET_CELLS_PER_CHUNK = int(os.getenv("OPENARG_CSV_TARGET_CELLS_PER_CHUNK", "250000"))
_CSV_MIN_CHUNK_SIZE = int(os.getenv("OPENARG_CSV_MIN_CHUNK_SIZE", "500"))
_CSV_MAX_CHUNK_SIZE = int(os.getenv("OPENARG_CSV_MAX_CHUNK_SIZE", "50000"))
_JSON_RECORD_MAP_STREAM_THRESHOLD_BYTES = int(
    os.getenv("OPENARG_JSON_RECORD_MAP_STREAM_THRESHOLD_BYTES", str(16 * 1024 * 1024))
)
_JSON_RECORD_MAP_CHUNK_SIZE = int(os.getenv("OPENARG_JSON_RECORD_MAP_CHUNK_SIZE", "5000"))
_COLLECT_DISPATCH_BATCH_SIZE = int(os.getenv("OPENARG_COLLECT_DISPATCH_BATCH_SIZE", "10"))
_COLLECT_DISPATCH_STEP_SECONDS = int(os.getenv("OPENARG_COLLECT_DISPATCH_STEP_SECONDS", "30"))
_COLLECT_MAX_INFLIGHT = int(os.getenv("OPENARG_COLLECT_MAX_INFLIGHT", "100"))
_COLLECT_MAX_INFLIGHT_PER_PORTAL = int(os.getenv("OPENARG_COLLECT_MAX_INFLIGHT_PER_PORTAL", "10"))
# When the collector queue's inflight count crosses this threshold, route
# part of the new dispatch wave to the heavy worker so the spare CPU on
# `worker_collector_heavy` (~0.2% baseline) absorbs the overflow.
_COLLECT_HEAVY_OVERFLOW_THRESHOLD = int(
    os.getenv("OPENARG_COLLECT_HEAVY_OVERFLOW_THRESHOLD", "70")
)
_BULK_COLLECT_RETRY_DELAY_SECONDS = int(os.getenv("OPENARG_BULK_COLLECT_RETRY_DELAY_SECONDS", "300"))
_BULK_COLLECT_MAX_CHAIN_DEPTH = int(os.getenv("OPENARG_BULK_COLLECT_MAX_CHAIN_DEPTH", "48"))
_BULK_COLLECT_SOFT_TIME_LIMIT = int(os.getenv("OPENARG_BULK_COLLECT_SOFT_TIME_LIMIT", "600"))
_BULK_COLLECT_TIME_LIMIT = int(os.getenv("OPENARG_BULK_COLLECT_TIME_LIMIT", "720"))
_COLLECT_DATASET_LOCK_PREFIX = "collect_dataset"
_BULK_COLLECT_LOCK_KEY = 8_004_001
_HEAVY_COLLECT_QUEUE = os.getenv("OPENARG_HEAVY_COLLECT_QUEUE", "collector-heavy")
_HEAVY_RETRY_QUEUE = os.getenv("OPENARG_HEAVY_RETRY_QUEUE", "collector-heavy-retry")
_HEAVY_WIDTH_COLUMN_THRESHOLD = int(os.getenv("OPENARG_HEAVY_WIDTH_COLUMN_THRESHOLD", "600"))
_HEAVY_METADATA_PORTAL_FORMATS: dict[str, frozenset[str]] = {
    "datos_gob_ar": frozenset({"zip"}),
    "diputados": frozenset({"json"}),
    # Vaca Muerta + producción de pozos: el dataset enero-actualidad
    # llega al cap de 500K rows y tarda 100-180s por archivo,
    # monopolizando 1 fork del worker normal. Routing a heavy libera
    # los slots normales para datasets más rápidos.
    "energia": frozenset({"csv"}),
}
_HEAVY_METADATA_PORTAL_KEYWORDS: dict[str, frozenset[str]] = {
    "datos_gob_ar": frozenset(
        {
            "encuesta",
            "salud",
            "emse",
            "ennys",
            "alimentos",
            "suplementos",
            "bebidas",
        }
    ),
    "energia": frozenset(
        {
            # Slugs/títulos típicos del feed de Secretaría de Energía
            # que disparan archivos > 100MB.
            "produccion de petroleo",
            "produccion de gas",
            "produccion-de-petroleo",
            "produccion-de-gas",
            "pozo",
        }
    ),
    "caba": frozenset(
        {
            "3d",
            "edificios publicos",
            "edificios públicos",
            "legislatura-portena-zip",
            "congreso-de-la-nacion-argentina-zip",
        }
    ),
}


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
            if conn.in_transaction():
                conn.rollback()
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


def _row_value(row, attr: str, index: int):
    if hasattr(row, attr):
        return getattr(row, attr)
    return row[index]


def _should_route_to_heavy_from_metadata(
    *,
    portal: str | None,
    fmt: str | None,
    title: str | None = None,
    download_url: str | None = None,
) -> bool:
    normalized_portal = (portal or "").strip().lower()
    normalized_fmt = (fmt or "").strip().lower()

    heavy_formats = _HEAVY_METADATA_PORTAL_FORMATS.get(normalized_portal, frozenset())
    portal_keywords = _HEAVY_METADATA_PORTAL_KEYWORDS.get(normalized_portal, frozenset())
    haystack = " ".join(filter(None, [title, download_url])).lower()

    if normalized_fmt in heavy_formats and (
        not portal_keywords or any(keyword in haystack for keyword in portal_keywords)
    ):
        return True

    if not portal_keywords:
        return False

    if normalized_portal == "datos_gob_ar" and normalized_fmt not in {"csv", "txt", "zip"}:
        return False

    if normalized_portal == "caba" and normalized_fmt != "zip":
        return False

    return any(keyword in haystack for keyword in portal_keywords)


def _should_route_to_heavy_by_width(column_count: int) -> bool:
    return column_count >= _HEAVY_WIDTH_COLUMN_THRESHOLD


def _is_transient_collect_error(exc: Exception) -> bool:
    if isinstance(
        exc,
        httpx.RemoteProtocolError
        | httpx.ReadTimeout
        | httpx.ConnectTimeout
        | httpx.ReadError
        | httpx.ConnectError
        | httpx.NetworkError,
    ):
        return True

    exc_str = str(exc)
    transient_markers = (
        "peer closed connection without sending complete message body",
        "Connection reset by peer",
        "Server disconnected without sending a response",
        "temporarily unavailable",
        "timed out",
        "timeout",
        "Connection refused",
        "RemoteProtocolError",
    )
    return any(marker in exc_str for marker in transient_markers)


def _collect_dataset_signature(
    dataset_id: str,
    *,
    portal: str | None = None,
    fmt: str | None = None,
    title: str | None = None,
    heavy_overflow: bool = False,
):
    if _should_route_to_heavy_from_metadata(portal=portal, fmt=fmt, title=title):
        return collect_dataset.s(dataset_id, force_heavy=True).set(queue=_HEAVY_COLLECT_QUEUE)
    if heavy_overflow:
        # Normal collector queue is saturated → drain part of the wave to
        # the heavy worker. The heavy worker is otherwise idle most of the
        # time (rare datasets with metadata signaling heavy). `force_heavy`
        # is False so the task uses the normal pandas/limit configs; the
        # only difference is which worker picks it up.
        return collect_dataset.s(dataset_id).set(queue=_HEAVY_COLLECT_QUEUE)
    return collect_dataset.s(dataset_id)


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
        portal = str(_row_value(row, "portal", 1))
        if total >= max_total or per_portal.get(portal, 0) >= max_per_portal:
            deferred.append(row)
            continue
        selected.append(row)
        total += 1
        per_portal[portal] = per_portal.get(portal, 0) + 1

    return selected, deferred


def _count_bulk_collect_remaining(engine, portal: str | None = None) -> dict[str, int]:
    """Count remaining eligible work for bulk collection.

    Excludes datasets already marked `ready` or terminally
    `permanently_failed`, so callers can use this as a convergence signal.
    """
    params: dict[str, object] = {}
    portal_clause = ""
    if portal:
        portal_clause = " AND d.portal = :portal"
        params["portal"] = portal

    eligible_filter = (
        "d.is_cached = false "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM cached_datasets cd "
        "  WHERE cd.dataset_id = d.id "
        "    AND cd.status IN ('ready', 'permanently_failed')"
        ")"
    )
    eligible_filter_d2 = (
        "d2.is_cached = false "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM cached_datasets cd "
        "  WHERE cd.dataset_id = d2.id "
        "    AND cd.status IN ('ready', 'permanently_failed')"
        ")"
    )

    individual_sql = text(
        f"""
        SELECT COUNT(*)
        FROM datasets d
        WHERE {eligible_filter}
          {portal_clause}
          AND d.title NOT IN (
            SELECT d2.title
            FROM datasets d2
            WHERE d2.portal = d.portal
              AND {eligible_filter_d2}
            GROUP BY d2.title, d2.portal
            HAVING COUNT(*) > 50
          )
        """
    )

    group_sql = text(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT d.title, d.portal
            FROM datasets d
            WHERE {eligible_filter}
              {portal_clause}
            GROUP BY d.title, d.portal
            HAVING COUNT(*) > 50
        ) large_groups
        """
    )

    with engine.connect() as conn:
        individual = int(conn.execute(individual_sql, params).scalar() or 0)
        groups = int(conn.execute(group_sql, params).scalar() or 0)
        conn.rollback()

    return {
        "eligible_individual": individual,
        "eligible_groups": groups,
    }


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
                    error_category = 'orchestration_recovery_loop',
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
    revived_missing_table = 0

    with engine.begin() as conn:
        # Reverse of the orphaned_ready path below: rows that were marked
        # `error` with "Table missing" by a *previous* sweep but whose
        # physical table is back. This happens after schema-recovery
        # operations (alembic downgrade/upgrade, manual restore) where the
        # raw schema is briefly absent — the sweep at that moment marks
        # the rows as missing, then the schema returns but the rows stay
        # stuck in `error`. Without this branch, every wipe-and-rebuild
        # leaves a permanent debris of false-missing rows.
        revived = conn.execute(
            text(
                """
                UPDATE cached_datasets cd
                SET status = 'ready',
                    retry_count = 0,
                    error_message = NULL,
                    error_category = 'unknown',
                    updated_at = NOW()
                WHERE cd.status = 'error'
                  AND cd.error_message = 'Table missing: marked for re-download'
                  AND EXISTS (
                      SELECT 1
                      FROM information_schema.tables t
                      WHERE t.table_schema IN ('public', 'raw', 'staging', 'mart')
                        AND t.table_name = cd.table_name
                  )
                RETURNING dataset_id
                """
            )
        ).fetchall()
        revived_missing_table = len(revived)
        for row in revived:
            conn.execute(
                text(
                    "UPDATE datasets SET is_cached = true "
                    "WHERE id = CAST(:did AS uuid) AND is_cached = false"
                ),
                {"did": str(row.dataset_id)},
            )

        orphaned_rows = conn.execute(
            text(
                """
                SELECT CAST(cd.dataset_id AS text) AS dataset_id, cd.table_name
                FROM cached_datasets cd
                WHERE cd.status = 'ready'
                  AND cd.table_name IS NOT NULL
                  AND NOT EXISTS (
                      -- The table can live in `public` (legacy `cache_*`),
                      -- `raw` (Phase 1 medallion), `staging` (Phase 2), or
                      -- `mart` (Phase 3). Any of those is "exists".
                      SELECT 1
                      FROM information_schema.tables t
                      WHERE t.table_schema IN ('public', 'raw', 'staging', 'mart')
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
        "revived_missing_table": revived_missing_table,
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
    _record_cache_drop(
        engine,
        table_name=alias_table_name,
        reason="alias_view_replace",
        actor="collector._create_alias_view",
        extra={"source_table_name": source_table_name},
    )
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

        # If the winner row was inserted with row_count=0 (race window where
        # the alias was created before the winner's count was finalized),
        # fall back to COUNT(*) on the source table so the alias reflects
        # the real data volume — otherwise the catalog reports a "ready"
        # alias with 0 rows, which the analyst skips.
        winner_rows = source_row.row_count if source_row else None
        if not winner_rows:
            try:
                with engine.connect() as conn:
                    cnt = conn.execute(
                        text(f'SELECT COUNT(*) FROM "{source_table}"')  # noqa: S608
                    ).scalar()
                    conn.rollback()
                winner_rows = int(cnt) if cnt is not None else None
            except Exception:
                logger.debug(
                    "Could not COUNT source table %s for alias row_count fallback",
                    source_table,
                    exc_info=True,
                )

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
                    "rows": winner_rows,
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
                    "rows": winner_rows,
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
    import zipfile

    try:
        import fiona
    except ImportError:
        logger.warning("fiona not installed — cannot parse shapefiles")
        return None

    def _features_to_df(src) -> pd.DataFrame | None:
        features = []
        for i, feat in enumerate(src):
            if i >= MAX_TABLE_ROWS:
                break
            geom = dict(feat.get("geometry", {})) if feat.get("geometry") else {}
            props = dict(feat.get("properties", {})) if feat.get("properties") else {}
            features.append({"type": "Feature", "geometry": geom, "properties": props})
        if not features:
            return None
        return _geojson_features_to_df(features)

    try:
        # fiona can read directly from zip:// paths
        layers = fiona.listlayers(f"zip://{zip_path}")
        if not layers:
            return None
        with fiona.open(f"zip://{zip_path}", layer=layers[0]) as src:
            return _features_to_df(src)
    except Exception as exc:
        # Corrupt-zip / unsupported-format fails are logged at the outcome
        # layer (`bad_zip_file`); a stack trace here just adds noise.
        msg = str(exc)
        is_known = (
            "not recognized as being in a supported file format" in msg
            or "Failed to open dataset" in msg
            or "BadZipFile" in type(exc).__name__
        )
        logger.warning(
            "Direct zip shapefile read failed for %s: %s",
            zip_path,
            msg if is_known else "see traceback",
            exc_info=not is_known,
        )

    try:
        with zipfile.ZipFile(zip_path) as zf:
            shp_members = [name for name in zf.namelist() if name.lower().endswith(".shp")]
            if not shp_members:
                return None

            with tempfile.TemporaryDirectory(dir=_temp_dir()) as extract_dir:
                for shp_member in shp_members:
                    base_name = os.path.splitext(shp_member)[0]
                    sibling_prefix = f"{base_name}."
                    members_to_extract = [
                        name
                        for name in zf.namelist()
                        if name.startswith(sibling_prefix)
                        or name == shp_member
                    ]
                    for member in members_to_extract:
                        if member.endswith("/"):
                            continue
                        target_path = os.path.join(extract_dir, member)
                        os.makedirs(os.path.dirname(target_path), exist_ok=True)
                        with zf.open(member) as src, open(target_path, "wb") as dst:
                            shutil.copyfileobj(src, dst)

                    local_shp_path = os.path.join(extract_dir, shp_member)
                    try:
                        with fiona.open(local_shp_path) as src:
                            df = _features_to_df(src)
                            if df is not None:
                                return df
                    except Exception:
                        logger.warning(
                            "Extracted shapefile read failed for %s member %s",
                            zip_path,
                            shp_member,
                            exc_info=True,
                        )
    except Exception:
        logger.warning("Failed to read shapefile from %s", zip_path, exc_info=True)
        return None

    return None


def _read_vector_file_with_fiona(path: str) -> pd.DataFrame | None:
    """Read a local vector file with fiona and normalize to a DataFrame."""
    try:
        import fiona
    except ImportError:
        logger.warning("fiona not installed — cannot parse vector file %s", path)
        return None

    # Enable KML/LIBKML drivers if the underlying GDAL build supports them.
    # Best-effort: if the driver isn't compiled in, the open() will still
    # raise DriverError and we fall through to the bottom of the function.
    for driver in ("KML", "LIBKML"):
        try:
            fiona.supported_drivers[driver] = "rw"
        except Exception:
            pass

    features = []
    try:
        with fiona.open(path) as src:
            for i, feat in enumerate(src):
                if i >= MAX_TABLE_ROWS:
                    break
                geom = dict(feat.get("geometry", {})) if feat.get("geometry") else {}
                props = dict(feat.get("properties", {})) if feat.get("properties") else {}
                features.append({"type": "Feature", "geometry": geom, "properties": props})
    except Exception as exc:
        msg = str(exc)
        # `unsupported driver: 'KML'` means the GDAL build lacks KML support;
        # the dataset is then routed to `policy_non_tabular` upstream.
        is_known_driver = "unsupported driver" in msg
        logger.warning(
            "Vector file read failed for %s: %s",
            path,
            msg if is_known_driver else "see traceback",
            exc_info=not is_known_driver,
        )
        return None

    if not features:
        return None
    return _geojson_features_to_df(features)


def _read_kml_from_zip(zip_path: str) -> pd.DataFrame | None:
    """Read a KML/KMZ member from a ZIP archive using fiona."""
    import zipfile

    try:
        with zipfile.ZipFile(zip_path) as zf:
            kml_members = [
                name
                for name in zf.namelist()
                if name.lower().endswith((".kml", ".kmz")) and not name.endswith("/")
            ]
            if not kml_members:
                return None

            with tempfile.TemporaryDirectory(dir=_temp_dir()) as extract_dir:
                for member in kml_members:
                    target_path = os.path.join(extract_dir, member)
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    with zf.open(member) as src, open(target_path, "wb") as dst:
                        shutil.copyfileobj(src, dst)
                    df = _read_vector_file_with_fiona(target_path)
                    if df is not None:
                        return df
    except Exception:
        logger.warning("Failed to read KML from %s", zip_path, exc_info=True)
        return None

    return None


def _zip_member_suffix(name: str) -> str:
    basename = os.path.basename(name.rstrip("/"))
    return os.path.splitext(basename)[1].lower()


def _zip_structure_summary(member_names: list[str]) -> dict[str, object]:
    file_members = [name for name in member_names if not name.endswith("/")]
    suffix_counter = Counter(_zip_member_suffix(name) or "<noext>" for name in file_members)
    parseable_count = sum(1 for name in file_members if _zip_member_suffix(name) in _ZIP_PARSEABLE_SUFFIXES)
    document_count = sum(1 for name in file_members if _zip_member_suffix(name) in _ZIP_DOCUMENT_SUFFIXES)
    directory_depths = [
        max(0, len([part for part in name.split("/") if part]) - 1)
        for name in file_members
    ]
    max_depth = max(directory_depths, default=0)
    return {
        "file_count": len(file_members),
        "directory_count": sum(1 for name in member_names if name.endswith("/")),
        "max_depth": max_depth,
        "extensions": dict(sorted(suffix_counter.items())),
        "parseable_count": parseable_count,
        "document_count": document_count,
        "sample_members": file_members[:10],
    }


def _zip_only_documents(summary: dict[str, object]) -> bool:
    file_count = int(summary.get("file_count", 0) or 0)
    parseable_count = int(summary.get("parseable_count", 0) or 0)
    document_count = int(summary.get("document_count", 0) or 0)
    extensions = set((summary.get("extensions") or {}).keys())
    if file_count == 0:
        return False
    if parseable_count > 0:
        return False
    if document_count != file_count:
        return False
    return all(ext in _ZIP_DOCUMENT_SUFFIXES for ext in extensions if ext != "<noext>")


def _append_parsed_zip_member(parsed_members: list[dict[str, object]], member_result: dict) -> None:
    """Accumulate parsed ZIP-member results by target table."""
    table_name = str(member_result["table_name"])
    row_count = int(member_result.get("row_count", 0) or 0)
    columns = [str(c) for c in member_result.get("columns", [])]
    sampled_note = member_result.get("sampled_note")

    for existing in parsed_members:
        if existing["table_name"] != table_name:
            continue
        existing["row_count"] = int(existing.get("row_count", 0) or 0) + row_count
        existing["columns"] = columns or existing.get("columns", [])
        if sampled_note:
            existing["sampled_note"] = sampled_note
        return

    parsed_members.append(
        {
            "table_name": table_name,
            "row_count": row_count,
            "columns": columns,
            "sampled_note": sampled_note,
        }
    )


def _snapshot_member_tables(parsed_members: list[dict[str, object]]) -> list[dict[str, object]]:
    """Return a detached copy of parsed-member progress for callbacks/results."""
    return [
        {
            "table_name": str(member["table_name"]),
            "row_count": int(member.get("row_count", 0) or 0),
            "columns": [str(c) for c in member.get("columns", [])],
            "sampled_note": member.get("sampled_note"),
        }
        for member in parsed_members
    ]


def _parse_zip_archive(
    zf,
    *,
    zip_path: str,
    dataset_id: str,
    table_name: str,
    engine,
    append_mode: bool,
    nested_depth: int = 0,
    existing_member_tables: set[str] | None = None,
    member_names_override: list[str] | None = None,
    progress_callback=None,
    force_text_for_wide_csv: bool = False,
) -> dict:
    """Parse a ZIP archive, recursing one level into nested ZIP members."""
    import zipfile

    def _nested_zip_priority(name: str) -> tuple[int, str]:
        lower_name = name.lower()
        if lower_name.endswith(".kmz"):
            return (2, lower_name)
        try:
            with zf.open(name) as nested_entry:
                with zipfile.ZipFile(io.BytesIO(nested_entry.read())) as nested_inner:
                    nested_summary = _zip_structure_summary(nested_inner.namelist())
        except Exception:
            return (9, lower_name)

        nested_exts = set((nested_summary.get("extensions") or {}).keys())
        if {".geojson", ".json"} & nested_exts:
            return (0, lower_name)
        if ".shp" in nested_exts:
            return (1, lower_name)
        if {".kml", ".kmz"} & nested_exts:
            return (2, lower_name)
        if nested_summary.get("parseable_count"):
            return (3, lower_name)
        return (9, lower_name)

    known_member_tables = set(existing_member_tables or set())

    def _record_dataframe(df: pd.DataFrame, *, sampled: str | None = None) -> dict:
        nonlocal table_name, current_append_mode
        df = df.copy()
        df["_source_dataset_id"] = dataset_id
        # Route using the same normalized schema that will be written to SQL.
        # Otherwise append-mode decisions can be made against raw upstream
        # headers while `_to_sql_safe()` later renames/deduplicates columns,
        # which is exactly how long SEPA runs end up hitting destructive
        # drop/recreate retries mid-bundle.
        df = _sanitize_columns(df)
        table_name, current_append_mode, routed_status = _route_table_for_schema(
            engine,
            dataset_id,
            table_name,
            df.columns,
            append_mode=current_append_mode,
        )
        if routed_status == "already_appended" and any(
            member["table_name"] == table_name for member in parsed_members
        ):
            routed_status = None
        if routed_status == "already_appended" and table_name in known_member_tables:
            routed_status = None
        if routed_status:
            if routed_status == "resource_table_full":
                return {"parsed": False, "result": {"dataset_id": dataset_id, "error": routed_status}}
            return {"parsed": False, "result": {"dataset_id": dataset_id, "status": routed_status}}

        _to_sql_safe(
            df,
            table_name,
            engine,
            if_exists="append" if current_append_mode else "replace",
            index=False,
        )

        member_result = {
            "parsed": True,
            "table_name": table_name,
            "append_mode": True,
            "row_count": len(df),
            "columns": list(df.columns),
            "sampled_note": sampled,
            "result": None,
        }
        _append_parsed_zip_member(parsed_members, member_result)
        known_member_tables.add(str(table_name))
        if progress_callback:
            progress_callback(_snapshot_member_tables(parsed_members))
        current_append_mode = True
        return member_result

    def _record_csv_file(file_path: str, *, sampled: str | None = None) -> dict:
        nonlocal table_name, current_append_mode
        csv_params = _detect_csv_params(file_path)
        preview_df = pd.read_csv(file_path, nrows=_ZIP_CSV_PREVIEW_ROWS, **csv_params)
        preview_df = preview_df.assign(_source_dataset_id=dataset_id)
        preview_df = _sanitize_columns(preview_df)
        csv_params = _stabilize_csv_params_for_width(
            csv_params,
            column_count=len(preview_df.columns),
            force_text=force_text_for_wide_csv,
        )
        chunk_size = _csv_chunk_size_for_columns(len(preview_df.columns))
        table_name, current_append_mode, routed_status = _route_table_for_schema(
            engine,
            dataset_id,
            table_name,
            preview_df.columns,
            append_mode=current_append_mode,
        )
        if routed_status == "already_appended" and any(
            member["table_name"] == table_name for member in parsed_members
        ):
            routed_status = None
        if routed_status == "already_appended" and table_name in known_member_tables:
            routed_status = None
        if routed_status:
            if routed_status == "resource_table_full":
                return {"parsed": False, "result": {"dataset_id": dataset_id, "error": routed_status}}
            return {"parsed": False, "result": {"dataset_id": dataset_id, "status": routed_status}}

        row_count, columns, truncated = _load_csv_chunked(
            file_path,
            table_name,
            engine,
            chunk_size=chunk_size,
            source_dataset_id=dataset_id,
            force_append=current_append_mode,
            csv_params_override=csv_params,
        )
        sampled_note = sampled
        if truncated:
            sampled_note = f"sampled: first {row_count} rows kept (limit {MAX_TABLE_ROWS})"
            logger.warning(
                "Dataset %s (zip/csv) truncated at %d rows",
                dataset_id,
                row_count,
            )

        member_result = {
            "parsed": True,
            "table_name": table_name,
            "append_mode": True,
            "row_count": row_count,
            "columns": list(columns),
            "sampled_note": sampled_note,
            "result": None,
        }
        _append_parsed_zip_member(parsed_members, member_result)
        known_member_tables.add(str(table_name))
        if progress_callback:
            progress_callback(_snapshot_member_tables(parsed_members))
        current_append_mode = True
        return member_result

    current_append_mode = append_mode
    sampled_note: str | None = None
    parsed_members: list[dict[str, object]] = []
    member_names = list(member_names_override or zf.namelist())
    summary = _zip_structure_summary(member_names)

    logger.info(
        "ZIP structure dataset=%s depth=%s files=%s dirs=%s parseable=%s documents=%s extensions=%s sample=%s",
        dataset_id,
        summary["max_depth"],
        summary["file_count"],
        summary["directory_count"],
        summary["parseable_count"],
        summary["document_count"],
        summary["extensions"],
        summary["sample_members"],
    )

    if _zip_only_documents(summary):
        logger.warning(
            "ZIP %s classified as non-tabular document bundle: extensions=%s sample=%s",
            dataset_id,
            summary["extensions"],
            summary["sample_members"],
        )
        return {
            "parsed": False,
            "table_name": table_name,
            "append_mode": append_mode,
            "row_count": 0,
            "columns": [],
            "sampled_note": None,
            "result": {"error": "zip_document_bundle"},
        }

    ordered_member_names = list(member_names)
    nested_zip_members = [
        name
        for name in member_names
        if name.lower().endswith((".zip", ".kmz")) and not name.endswith("/")
    ]
    if nested_zip_members and nested_depth < 1:
        prioritized_nested = sorted(nested_zip_members, key=_nested_zip_priority)
        non_nested = [name for name in member_names if name not in nested_zip_members]
        ordered_member_names = non_nested + prioritized_nested

    for name in ordered_member_names:
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
                record_result = _record_csv_file(csv_tmp_path)
                if record_result.get("result") is not None:
                    return record_result
            finally:
                try:
                    os.unlink(csv_tmp_path)
                except OSError:
                    pass
            continue

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
            record_result = _record_dataframe(df, sampled=sampled_note)
            if record_result.get("result") is not None:
                return record_result
            continue

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
            record_result = _record_dataframe(df, sampled=sampled_note)
            if record_result.get("result") is not None:
                return record_result
            continue

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
            record_result = _record_dataframe(df, sampled=sampled_note)
            if record_result.get("result") is not None:
                return record_result
            continue

        if lower.endswith(".kml"):
            kml_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".kml", dir=_temp_dir())
            kml_tmp_path = kml_tmp.name
            kml_tmp.close()
            try:
                with zf.open(name) as zf_entry, open(kml_tmp_path, "wb") as out:
                    while True:
                        block = zf_entry.read(256 * 1024)
                        if not block:
                            break
                        out.write(block)
                df = _read_vector_file_with_fiona(kml_tmp_path)
                if df is None:
                    continue
                if len(df) > MAX_TABLE_ROWS:
                    sampled_note = f"sampled: first {MAX_TABLE_ROWS} of {len(df)} rows (kml)"
                    logger.warning(
                        "Dataset %s (zip/kml) truncated from %d to %d rows",
                        dataset_id,
                        len(df),
                        MAX_TABLE_ROWS,
                    )
                    df = df.iloc[:MAX_TABLE_ROWS]
                record_result = _record_dataframe(df, sampled=sampled_note)
                if record_result.get("result") is not None:
                    return record_result
            finally:
                try:
                    os.unlink(kml_tmp_path)
                except OSError:
                    pass
            continue

        if nested_depth < 1 and lower.endswith((".zip", ".kmz")):
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
                            append_mode=current_append_mode,
                            nested_depth=nested_depth + 1,
                            existing_member_tables=known_member_tables,
                            progress_callback=None,
                        )
                except zipfile.BadZipFile:
                    logger.warning("ZIP %s: nested member %s is not a valid ZIP", dataset_id, name)
                    continue
                if nested_result.get("parsed"):
                    nested_members = nested_result.get("member_tables") or [
                        {
                            "table_name": nested_result["table_name"],
                            "row_count": nested_result["row_count"],
                            "columns": nested_result["columns"],
                            "sampled_note": nested_result["sampled_note"],
                        }
                    ]
                    for member in nested_members:
                        _append_parsed_zip_member(parsed_members, member)
                        known_member_tables.add(str(member["table_name"]))
                    if progress_callback:
                        progress_callback(_snapshot_member_tables(parsed_members))
                    current_append_mode = True
                    continue
                if nested_result.get("result") is not None:
                    if nested_result["result"].get("error") == "zip_document_bundle":
                        continue
                    return nested_result
            finally:
                try:
                    os.unlink(nested_tmp_path)
                except OSError:
                    pass

    has_shp = any(n.lower().endswith(".shp") for n in member_names)
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
            record_result = _record_dataframe(df, sampled=sampled_note)
            if record_result.get("result") is not None:
                return record_result

    has_kml = any(n.lower().endswith((".kml", ".kmz")) for n in member_names)
    if has_kml:
        df = _read_kml_from_zip(zip_path)
        if df is not None and len(df) > 0:
            if len(df) > MAX_TABLE_ROWS:
                sampled_note = f"sampled: first {MAX_TABLE_ROWS} of {len(df)} rows (kml)"
                logger.warning(
                    "Dataset %s (zip/kml) truncated from %d to %d rows",
                    dataset_id,
                    len(df),
                    MAX_TABLE_ROWS,
                )
                df = df.iloc[:MAX_TABLE_ROWS]
            record_result = _record_dataframe(df, sampled=sampled_note)
            if record_result.get("result") is not None:
                return record_result

    if parsed_members:
        primary = parsed_members[0]
        total_rows = sum(int(member.get("row_count", 0) or 0) for member in parsed_members)
        latest_sampled = next(
            (member.get("sampled_note") for member in reversed(parsed_members) if member.get("sampled_note")),
            None,
        )
        return {
            "parsed": True,
            "table_name": str(primary["table_name"]),
            "append_mode": True,
            "row_count": total_rows,
            "columns": list(primary.get("columns", [])),
            "sampled_note": latest_sampled,
            "member_tables": parsed_members,
            "result": None,
        }

    return {
        "parsed": False,
        "table_name": table_name,
        "append_mode": current_append_mode,
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

    # Try UTF-8 with BOM stripping first
    try:
        with open(file_path, encoding="utf-8-sig") as f:
            header = f.readline()
        params["encoding"] = "utf-8-sig"
    except UnicodeDecodeError:
        params["encoding"] = "latin-1"
        with open(file_path, encoding="latin-1") as f:
            header = f.readline()

    # Auto-detect separator
    candidate_counts = {
        ",": header.count(","),
        ";": header.count(";"),
        "|": header.count("|"),
        "\t": header.count("\t"),
    }
    best_sep, best_count = max(candidate_counts.items(), key=lambda item: item[1])
    if best_count > 0:
        params["sep"] = best_sep

    return params


def _read_csv_preview(file_path: str, *, nrows: int, csv_params: dict | None = None) -> pd.DataFrame:
    params = dict(csv_params) if csv_params is not None else _detect_csv_params(file_path)
    try:
        return pd.read_csv(file_path, nrows=nrows, **params)
    except UnicodeDecodeError:
        if str(params.get("encoding") or "").lower() != "latin-1":
            fallback_params = dict(params)
            fallback_params["encoding"] = "latin-1"
            logger.info(
                "CSV preview decode failed for %s with encoding=%s, retrying with latin-1",
                file_path,
                params.get("encoding"),
            )
            return pd.read_csv(file_path, nrows=nrows, **fallback_params)
        raise


def _csv_load_inner(
    file_path: str,
    table_name: str,
    engine,
    csv_params: dict,
    chunk_size: int,
    max_rows: int = 0,
    source_dataset_id: str | None = None,
    force_append: bool = False,
    *,
    write_schema: str | None = None,
) -> tuple[int, list[str], bool]:
    """Inner CSV loading loop.

    Returns ``(total_rows_written, columns, was_truncated)``.
    When *max_rows* > 0 the loader stops after writing that many rows.
    When *source_dataset_id* is provided, adds ``_source_dataset_id`` column.
    When *force_append* is True, all chunks use ``if_exists='append'``.
    `write_schema` selects which Postgres schema to materialize into; when
    None the legacy `public` default applies.
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
            chunk_df = chunk_df.assign(_source_dataset_id=source_dataset_id)
        if force_append:
            mode = "append"
        else:
            mode = "replace" if is_first else "append"
        _to_sql_safe(
            chunk_df, table_name, engine, schema=write_schema, if_exists=mode, index=False
        )
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
    _record_cache_drop(
        engine,
        table_name=table_name,
        reason="schema_refresh",
        actor="collector._drop_table_if_exists",
    )
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

    normalized = df
    for column in normalized.columns:
        series = normalized[column]
        if getattr(series.dtype, "kind", None) != "O":
            continue
        sample = next(
            (
                value
                for value in series.head(25)
                if value is not None and not (isinstance(value, float) and pd.isna(value))
            ),
            None,
        )
        if not isinstance(sample, dict | list | tuple | set):
            continue
        normalized[column] = series.map(_serialize_nested_value)
    return normalized


@dataclass(frozen=True)
class _LayoutCandidate:
    profile: str
    columns: list[str]
    rows_consumed: int


@dataclass(frozen=True)
class _CollectorRunOutcome:
    result_kind: str
    cached_status: str
    error_message: str | None = None
    retry_increment: int = 0
    should_prune_open: bool = False


def _is_placeholder_column_name(value: object) -> bool:
    text_value = str(value or "").strip()
    if not text_value:
        return True
    lowered = text_value.lower()
    if lowered.startswith("unnamed:"):
        return True
    if re.fullmatch(r"col_[0-9]+", lowered):
        return True
    if re.fullmatch(r"[0-9]+", lowered):
        return True
    return False


def _header_quality(columns) -> tuple[int, int, int]:
    normalized = [str(col or "").strip() for col in columns]
    placeholder_count = sum(1 for col in normalized if _is_placeholder_column_name(col))
    alpha_count = sum(1 for col in normalized if re.search(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]", col or ""))
    nonempty_count = sum(1 for col in normalized if col)
    return placeholder_count, alpha_count, nonempty_count


def _header_numeric_count(columns) -> int:
    return sum(
        1
        for col in (str(col or "").strip() for col in columns)
        if re.fullmatch(r"[0-9]+", col or "")
    )


def _header_quality_label(columns) -> str:
    total_columns = max(len(columns), 1)
    placeholder_count, alpha_count, nonempty_count = _header_quality(columns)
    numeric_count = _header_numeric_count(columns)
    placeholder_ratio = placeholder_count / total_columns
    numeric_ratio = numeric_count / total_columns
    min_nonempty = 1 if total_columns <= 2 else max(3, int(total_columns * 0.4))

    if placeholder_ratio >= 0.35 or nonempty_count < min_nonempty:
        return _HEADER_INVALID
    if (
        placeholder_count > 0
        or numeric_ratio >= 0.25
        or alpha_count < max(2, int(total_columns * 0.2))
    ):
        return _HEADER_DEGRADED
    return _HEADER_GOOD


def _normalize_header_token(value: object) -> str:
    if pd.isna(value):
        return ""
    text_value = str(value).replace("\xa0", " ").strip()
    if not text_value or text_value.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", text_value)


def _forward_fill_header_tokens(values) -> list[str]:
    filled: list[str] = []
    last_seen = ""
    for value in values:
        token = _normalize_header_token(value)
        if token and not _is_placeholder_column_name(token):
            last_seen = token
            filled.append(token)
        else:
            filled.append(last_seen if last_seen else token)
    return filled


def _token_looks_like_data(token: str) -> bool:
    normalized = token.strip()
    if not normalized:
        return False
    if re.fullmatch(r"[-+]?[\d.,/%]+", normalized):
        return True
    if re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", normalized):
        return True
    return False


def _row_looks_like_header(row: list[object]) -> bool:
    tokens = [_normalize_header_token(value) for value in row]
    nonempty = [token for token in tokens if token]
    if not nonempty:
        return False

    placeholder_count = sum(1 for token in nonempty if _is_placeholder_column_name(token))
    alpha_count = sum(1 for token in nonempty if re.search(r"[A-Za-zÁ-ÿ]", token))
    dataish_count = sum(1 for token in nonempty if _token_looks_like_data(token))

    if placeholder_count >= max(1, int(len(nonempty) * 0.6)):
        return False
    if alpha_count < max(1, int(len(nonempty) * 0.4)):
        return False
    if dataish_count > max(1, int(len(nonempty) * 0.5)):
        return False
    return True


def _row_has_hierarchy_signal(row: list[object]) -> bool:
    tokens = [
        token
        for token in (_normalize_header_token(value) for value in row)
        if token and not _is_placeholder_column_name(token)
    ]
    if not tokens:
        return False
    if len(tokens) != len(row):
        return True
    return len(set(tokens)) < len(tokens)


def _combine_header_rows(rows: list[list[object]], *, sparse: bool = False) -> list[str]:
    prepared_rows: list[list[str]] = []
    for row in rows:
        tokens = [_normalize_header_token(value) for value in row]
        if sparse:
            tokens = _forward_fill_header_tokens(tokens)
        prepared_rows.append(tokens)

    combined: list[str | None] = []
    for col_values in zip(*prepared_rows, strict=False):
        parts: list[str] = []
        for token in col_values:
            if not token or _is_placeholder_column_name(token):
                continue
            if parts and token == parts[-1]:
                continue
            parts.append(token)
        combined.append(" / ".join(parts) if parts else None)
    return _make_unique_columns(combined)


def _candidate_is_meaningfully_better(
    current_columns: list[str],
    candidate_columns: list[str],
    *,
    rows_consumed: int,
) -> bool:
    total_columns = max(len(current_columns), 1)
    current_placeholder, current_alpha, current_nonempty = _header_quality(current_columns)
    candidate_placeholder, candidate_alpha, candidate_nonempty = _header_quality(candidate_columns)

    if candidate_nonempty < max(3, int(total_columns * 0.3)):
        return False

    if candidate_placeholder <= max(0, current_placeholder - 2) and candidate_alpha >= max(
        3, current_alpha
    ):
        return True

    if (
        current_placeholder >= max(2, int(total_columns * 0.35))
        and candidate_placeholder <= max(1, int(total_columns * 0.15))
        and candidate_alpha >= max(2, current_alpha - 1)
    ):
        return True

    if (
        rows_consumed >= 2
        and candidate_placeholder < current_placeholder
        and candidate_alpha > current_alpha
    ):
        return True

    return False


def _infer_layout_profile(
    df: pd.DataFrame,
    *,
    max_candidate_rows: int = 5,
) -> _LayoutCandidate:
    current_columns = _make_unique_columns(df.columns)
    best = _LayoutCandidate(profile=_LAYOUT_SIMPLE, columns=current_columns, rows_consumed=0)

    candidate_rows = min(max_candidate_rows, len(df))
    if df.empty or candidate_rows == 0:
        return best

    for idx in range(candidate_rows):
        row_values = list(df.iloc[idx])
        if not _row_looks_like_header(row_values):
            break
        if idx > 0:
            break
        promoted_columns = _make_unique_columns(df.iloc[idx])
        if _candidate_is_meaningfully_better(
            current_columns,
            promoted_columns,
            rows_consumed=idx + 1,
        ):
            best = _LayoutCandidate(
                profile=_LAYOUT_PRESENTATION,
                columns=promoted_columns,
                rows_consumed=idx + 1,
            )

    header_like_prefix = 0
    for idx in range(candidate_rows):
        if _row_looks_like_header(list(df.iloc[idx])):
            header_like_prefix += 1
        else:
            break

    for row_count in (2,):
        if candidate_rows < row_count:
            continue
        if header_like_prefix < row_count:
            continue
        header_rows = [list(df.iloc[idx]) for idx in range(row_count)]
        if _row_has_hierarchy_signal(header_rows[0]):
            combined_columns = _combine_header_rows(header_rows, sparse=False)
            if _candidate_is_meaningfully_better(
                current_columns,
                combined_columns,
                rows_consumed=row_count,
            ):
                best = _LayoutCandidate(
                    profile=_LAYOUT_MULTILINE,
                    columns=combined_columns,
                    rows_consumed=row_count,
                )
        if any(_normalize_header_token(value) == "" for row in header_rows for value in row):
            sparse_columns = _combine_header_rows(header_rows, sparse=True)
            if _candidate_is_meaningfully_better(
                current_columns,
                sparse_columns,
                rows_consumed=row_count,
            ):
                best = _LayoutCandidate(
                    profile=_LAYOUT_SPARSE,
                    columns=sparse_columns,
                    rows_consumed=row_count,
                )

    return best


def _maybe_promote_header_row(df: pd.DataFrame, *, max_candidate_rows: int = 5) -> pd.DataFrame:
    if df.empty:
        return df

    candidate = _infer_layout_profile(df, max_candidate_rows=max_candidate_rows)
    if candidate.rows_consumed == 0:
        return df

    current_columns = _make_unique_columns(df.columns)
    current_placeholder, current_alpha, _ = _header_quality(current_columns)
    best_placeholder, best_alpha, _ = _header_quality(candidate.columns)

    promoted = df.iloc[candidate.rows_consumed :].reset_index(drop=True).copy()
    promoted.columns = candidate.columns
    promoted.attrs["layout_profile"] = candidate.profile
    promoted.attrs["header_quality"] = _header_quality_label(candidate.columns)
    logger.info(
        "Applied layout_profile=%s consuming %s header rows (placeholders %s -> %s, alpha %s -> %s)",
        candidate.profile,
        candidate.rows_consumed,
        current_placeholder,
        best_placeholder,
        current_alpha,
        best_alpha,
    )
    return promoted


def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns using a profile-driven header strategy before SQL writes."""
    df.columns = _make_unique_columns(df.columns)

    df = _maybe_promote_header_row(df)
    df.columns = _make_unique_columns(df.columns)
    df.attrs.setdefault("layout_profile", _infer_layout_profile(df).profile if not df.empty else _LAYOUT_SIMPLE)
    df.attrs["header_quality"] = _header_quality_label(df.columns)
    if len(df.columns) > _HEAVY_WIDTH_COLUMN_THRESHOLD:
        df.attrs["layout_profile"] = _LAYOUT_WIDE

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
    compacted = df[keep_cols].copy()
    if overflow_cols:
        overflow_json: list[str | None] = []
        overflow_frame = df[overflow_cols]
        for row in overflow_frame.itertuples(index=False, name=None):
            record = {
                col: value
                for col, value in zip(overflow_cols, row, strict=False)
                if value is not None and not pd.isna(value)
            }
            overflow_json.append(
                json.dumps(record, ensure_ascii=False, separators=(",", ":")) if record else None
            )
        compacted["_overflow_json"] = overflow_json
    else:
        compacted["_overflow_json"] = None
    logger.warning(
        "Compacted wide dataset from %d to %d SQL columns (%d overflow columns stored in _overflow_json)",
        len(df.columns),
        len(compacted.columns),
        len(overflow_cols),
    )
    return compacted


def _detect_schema_drift(
    engine, table_name: str, new_df: pd.DataFrame, *, schema: str | None = None
) -> dict[str, list[str]] | None:
    """Compare the existing cache table schema to the incoming DataFrame.

    Returns a dict with ``added``, ``removed`` and ``type_changed`` column
    lists if drift is detected, or None if the table does not exist or the
    schemas match. Used to emit a structured warning BEFORE we replace the
    table — operators need visibility when an upstream schema shifts
    because downstream SQL sandbox queries and NL2SQL prompts assume the
    schema is stable. Read-only, non-blocking: if the inspection itself
    fails, we return None and let the write proceed.

    `schema` selects which Postgres schema to inspect; default `public`.
    """
    target_schema = schema or "public"
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT column_name, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_schema = :sch AND table_name = :tn"
                ),
                {"sch": target_schema, "tn": table_name},
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


def _to_sql_safe(df: pd.DataFrame, table_name: str, engine, *, schema: str | None = None, **kwargs):
    """Write DataFrame to SQL, retrying with DROP if schema mismatch occurs.

    Before the write, inspect the existing table schema (if any) for
    drift — column additions, removals, or type shifts — and emit a
    structured warning so operators know when an upstream feed changed.
    This does not block the write; downstream SQL sandbox/NL2SQL consumers
    may still break on the new schema, but at least the drift is visible
    in logs and metrics instead of surfacing as a mysterious "column does
    not exist" error hours later.

    `schema` selects the target Postgres schema. Explicit `schema=` always
    wins; when None, falls back to the `_current_write_schema` ContextVar
    (set by `collect_dataset` for the duration of the task). When that is
    also None, the legacy `public` default applies.
    """
    if schema is None:
        schema = _current_write_schema.get()
    df = _sanitize_columns(df)

    if kwargs.get("if_exists") == "replace":
        drift = _detect_schema_drift(engine, table_name, df, schema=schema)
        if drift is not None:
            logger.warning(
                "schema_drift_detected schema=%s table=%s added=%s removed=%s type_changed=%s",
                schema or "public",
                table_name,
                drift["added"],
                drift["removed"],
                drift["type_changed"],
            )
            try:
                from app.infrastructure.monitoring.metrics import MetricsCollector

                MetricsCollector().record_connector_call(
                    f"schema_drift:{schema or 'public'}.{table_name}",
                    latency_ms=0,
                    error=True,
                )
            except Exception:
                logger.debug("Could not record schema_drift metric", exc_info=True)

    lock_namespace = f"materialize_table:{schema or 'public'}"
    table_lock_key = _lock_key(lock_namespace, table_name)
    with engine.connect() as write_conn:
        write_conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": table_lock_key})
        write_conn.commit()
        try:
            tx = write_conn.begin()
            try:
                if schema is not None:
                    df.to_sql(table_name, write_conn, schema=schema, **kwargs)
                else:
                    df.to_sql(table_name, write_conn, **kwargs)
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
                    if tx.is_active:
                        tx.rollback()
                    drop_qualified = (
                        f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                    )
                    _record_cache_drop(
                        engine,
                        table_name=f"{schema}.{table_name}" if schema else table_name,
                        reason="schema_mismatch_recreate",
                        actor="collector._to_sql_safe",
                        extra={"exc": str(exc)[:300]},
                    )
                    with engine.begin() as reset_conn:
                        reset_conn.execute(text(f'DROP TABLE IF EXISTS {drop_qualified} CASCADE'))  # noqa: S608
                    retry_kwargs = dict(kwargs)
                    retry_kwargs["if_exists"] = "replace"
                    # Force a hard dedup before the retry: `_sanitize_columns`
                    # already runs `_make_unique_columns`, but post-sanitize
                    # transforms (`_compact_wide_dataframe`, structured-cell
                    # serialization, raw-metadata injection) can re-introduce
                    # collisions when an upstream feed has near-identical
                    # column labels. `loc[:, ~duplicated()]` keeps the first
                    # occurrence of each name so CREATE TABLE never sees
                    # `specified more than once`.
                    df = df.loc[:, ~df.columns.duplicated()]
                    retry_tx = write_conn.begin()
                    try:
                        # Forward `schema` so the retry creates the table in
                        # the same medallion layer the caller asked for —
                        # otherwise pandas falls back to the connection's
                        # default search_path (typically `public`).
                        if schema is not None:
                            df.to_sql(table_name, write_conn, schema=schema, **retry_kwargs)
                        else:
                            df.to_sql(table_name, write_conn, **retry_kwargs)
                    except Exception as retry_exc:
                        if retry_tx.is_active:
                            retry_tx.rollback()
                        # Demote the retry-failure log so we don't print the
                        # second (already-known) traceback when the upstream
                        # is structurally broken — the outer caller still
                        # marks the dataset failed via `_apply_cached_outcome`.
                        logger.warning(
                            "Retry write also failed for %s after schema dedup: %s",
                            table_name,
                            str(retry_exc)[:200],
                        )
                        raise
                    else:
                        retry_tx.commit()
                else:
                    raise
            else:
                tx.commit()
            finally:
                if tx.is_active:
                    tx.rollback()
        finally:
            try:
                if write_conn.in_transaction():
                    write_conn.rollback()
            except Exception:
                logger.debug(
                    "Could not roll back write connection before unlock for %s",
                    table_name,
                    exc_info=True,
                )
            try:
                write_conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": table_lock_key})
                write_conn.rollback()
            except Exception:
                logger.debug("Could not release materialization lock for %s", table_name, exc_info=True)


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
    chunk_size: int | None = None,
    source_dataset_id: str | None = None,
    force_append: bool = False,
    csv_params_override: dict | None = None,
    *,
    write_schema: str | None = None,
) -> tuple[int, list[str], bool]:
    """Load a CSV file into PostgreSQL in chunks.

    Returns ``(total_rows, columns, was_truncated)``.
    Stops after ``MAX_TABLE_ROWS`` rows.
    `write_schema` selects medallion layer; defaults to legacy `public`.
    """
    csv_params = dict(csv_params_override) if csv_params_override is not None else _detect_csv_params(file_path)
    effective_chunk_size = chunk_size or _CSV_MAX_CHUNK_SIZE
    try:
        return _csv_load_inner(
            file_path,
            table_name,
            engine,
            csv_params,
            effective_chunk_size,
            max_rows=MAX_TABLE_ROWS,
            source_dataset_id=source_dataset_id,
            force_append=force_append,
            write_schema=write_schema,
        )
    except UnicodeDecodeError:
        fallback_encoding = csv_params.get("encoding")
        if fallback_encoding != "latin-1":
            logger.warning(
                "CSV decode failed for %s with encoding=%s, retrying with latin-1",
                file_path,
                fallback_encoding,
            )
            fallback_params = dict(csv_params)
            fallback_params["encoding"] = "latin-1"
            return _csv_load_inner(
                file_path,
                table_name,
                engine,
                fallback_params,
                effective_chunk_size,
                max_rows=MAX_TABLE_ROWS,
                source_dataset_id=source_dataset_id,
                force_append=force_append,
                write_schema=write_schema,
            )
        raise
    except pd.errors.ParserError:
        logger.info("CSV C-parser failed, retrying with Python engine")
        csv_params["engine"] = "python"
        return _csv_load_inner(
            file_path,
            table_name,
            engine,
            csv_params,
            effective_chunk_size,
            max_rows=MAX_TABLE_ROWS,
            source_dataset_id=source_dataset_id,
            force_append=force_append,
            write_schema=write_schema,
        )


def _csv_chunk_size_for_columns(column_count: int) -> int:
    """Derive a safer CSV chunk size for wide datasets.

    Target a rough upper bound in "cells per chunk" so 1,800-column CSVs
    don't try to allocate 50k-row DataFrames in one shot.
    """
    safe_columns = max(column_count, 1)
    derived = _CSV_TARGET_CELLS_PER_CHUNK // safe_columns
    return max(_CSV_MIN_CHUNK_SIZE, min(_CSV_MAX_CHUNK_SIZE, derived))


def _stabilize_csv_params_for_width(
    csv_params: dict,
    *,
    column_count: int,
    force_text: bool = False,
) -> dict:
    """Freeze dtype inference for very wide CSVs so chunks keep one schema.

    On pathological survey/health datasets, pandas may infer different dtypes
    across chunks for the same column, which later turns into append-time
    schema churn (`text -> float64`, `double precision -> str`, etc.). For
    very wide datasets we prefer stable all-text ingestion over repeated
    drop/recreate loops in the heavy lane.
    """
    stabilized = dict(csv_params)
    if force_text or _should_route_to_heavy_by_width(column_count):
        stabilized["dtype"] = str
        stabilized["keep_default_na"] = False
        stabilized["low_memory"] = False
    return stabilized


def _looks_like_json_record_map(file_path: str) -> bool:
    """Detect giant JSON payloads shaped like {"1": {...}, "2": {...}}."""
    try:
        with open(file_path, encoding="utf-8-sig", errors="ignore") as fh:
            sample = fh.read(2048).lstrip()
    except OSError:
        return False
    return bool(re.match(r'^\{\s*"\d+"\s*:\s*\{', sample))


def _iter_json_record_map_values(file_path: str, *, read_size: int = 1 << 20):
    """Yield top-level object values without loading the whole JSON blob."""
    decoder = json.JSONDecoder()
    with open(file_path, encoding="utf-8-sig") as fh:
        buf = ""
        idx = 0
        eof = False

        def _fill() -> bool:
            nonlocal buf, idx, eof
            if eof:
                return False
            chunk = fh.read(read_size)
            if chunk == "":
                eof = True
                return False
            if idx:
                buf = buf[idx:]
                idx = 0
            buf += chunk
            return True

        def _skip_ws() -> None:
            nonlocal idx
            while True:
                while idx < len(buf) and buf[idx].isspace():
                    idx += 1
                if idx < len(buf) or not _fill():
                    return

        _fill()
        _skip_ws()
        if idx >= len(buf) or buf[idx] != "{":
            raise ValueError("json_record_map_expected_object")
        idx += 1

        while True:
            _skip_ws()
            if idx >= len(buf):
                raise ValueError("json_record_map_unexpected_eof")
            if buf[idx] == "}":
                return
            if buf[idx] == ",":
                idx += 1
                continue

            while True:
                try:
                    key, next_idx = decoder.raw_decode(buf, idx)
                    break
                except ValueError:
                    if not _fill():
                        raise
            if not isinstance(key, str):
                raise ValueError("json_record_map_non_string_key")
            idx = next_idx

            _skip_ws()
            if idx >= len(buf) or buf[idx] != ":":
                raise ValueError("json_record_map_expected_colon")
            idx += 1

            _skip_ws()
            while True:
                try:
                    value, next_idx = decoder.raw_decode(buf, idx)
                    break
                except ValueError:
                    if not _fill():
                        raise
            idx = next_idx
            yield value

            if idx > read_size:
                buf = buf[idx:]
                idx = 0


def _load_json_record_map_chunked(
    file_path: str,
    table_name: str,
    engine,
    dataset_id: str,
) -> tuple[int, list[str], str, bool, str | None]:
    """Load keyed-record JSON blobs in bounded chunks."""
    total_rows = 0
    columns: list[str] = []
    sampled_note: str | None = None
    current_table = table_name
    append_mode = False
    first_chunk = True
    chunk: list[dict] = []

    def _flush(rows: list[dict]) -> None:
        nonlocal total_rows, columns, current_table, append_mode, first_chunk
        if not rows:
            return
        df = pd.json_normalize(rows)
        df["_source_dataset_id"] = dataset_id
        if first_chunk:
            current_table, append_mode, routed_status = _route_table_for_schema(
                engine,
                dataset_id,
                current_table,
                df.columns,
                append_mode=False,
            )
            if routed_status:
                raise RuntimeError(f"json_record_map_routed:{routed_status}")
            columns = list(df.columns)
            _to_sql_safe(
                df,
                current_table,
                engine,
                if_exists="append" if append_mode else "replace",
                index=False,
            )
            first_chunk = False
        else:
            df = df.reindex(columns=columns, fill_value=None)
            _to_sql_safe(df, current_table, engine, if_exists="append", index=False)
        total_rows += len(df)

    for value in _iter_json_record_map_values(file_path):
        chunk.append(value if isinstance(value, dict) else {"value": value})
        if total_rows + len(chunk) >= MAX_TABLE_ROWS:
            remaining = MAX_TABLE_ROWS - total_rows
            _flush(chunk[:remaining])
            sampled_note = f"sampled: first {MAX_TABLE_ROWS} rows kept (large json record map)"
            chunk = []
            break
        if len(chunk) >= _JSON_RECORD_MAP_CHUNK_SIZE:
            _flush(chunk)
            chunk = []

    if chunk and total_rows < MAX_TABLE_ROWS:
        _flush(chunk)

    if first_chunk:
        raise ValueError("json_record_map_no_rows")
    return total_rows, columns, current_table, append_mode, sampled_note


def _prune_open_cached_entries(engine, dataset_id: str, *, keep_table_name: str | None = None) -> None:
    """Delete duplicate open cached_datasets rows for one dataset.

    Keep terminal history and ready rows untouched; only collapse the noisy
    `downloading`/`pending`/`error` duplicates that inflate the open-work view
    and cause redundant collector churn.
    """
    try:
        with engine.begin() as conn:
            params: dict[str, object] = {"did": dataset_id}
            sql = (
                "DELETE FROM cached_datasets "
                "WHERE dataset_id = CAST(:did AS uuid) "
                "  AND status IN ('downloading', 'pending', 'error')"
            )
            if keep_table_name is not None:
                sql += " AND table_name <> :tn"
                params["tn"] = keep_table_name
            conn.execute(text(sql), params)
    except Exception:
        logger.debug("Could not prune open cached entries for %s", dataset_id, exc_info=True)


def _mark_dataset_rerouted_pending(
    engine,
    dataset_id: str,
    table_name: str,
    *,
    reason: str,
) -> None:
    """Mark exactly one cached row as rerouted/pending, then collapse open duplicates."""
    try:
        with engine.begin() as conn:
            existing = conn.execute(
                text(
                    """
                    SELECT id
                    FROM cached_datasets
                    WHERE dataset_id = CAST(:did AS uuid)
                      AND table_name = :tn
                    ORDER BY updated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """
                ),
                {"did": dataset_id, "tn": table_name},
            ).fetchone()
            if existing:
                conn.execute(
                    text(
                        """
                        UPDATE cached_datasets
                        SET status = 'pending',
                            error_message = :msg,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {"msg": f"rerouted_heavy:{reason}"[:500], "id": existing.id},
                )
            else:
                recycled = conn.execute(
                    text(
                        """
                        SELECT id
                        FROM cached_datasets
                        WHERE dataset_id = CAST(:did AS uuid)
                          AND status IN ('downloading', 'pending', 'error')
                        ORDER BY updated_at DESC NULLS LAST, id DESC
                        LIMIT 1
                        """
                    ),
                    {"did": dataset_id},
                ).fetchone()
                if recycled:
                    conn.execute(
                        text(
                            """
                            UPDATE cached_datasets
                            SET table_name = :tn,
                                status = 'pending',
                                error_message = :msg,
                                updated_at = NOW()
                            WHERE id = :id
                            """
                        ),
                        {
                            "tn": table_name,
                            "msg": f"rerouted_heavy:{reason}"[:500],
                            "id": recycled.id,
                        },
                    )
                else:
                    conn.execute(
                        text(
                            """
                            INSERT INTO cached_datasets (
                                dataset_id, table_name, status, error_message, updated_at
                            )
                            VALUES (CAST(:did AS uuid), :tn, 'pending', :msg, NOW())
                            ON CONFLICT (table_name) DO UPDATE SET
                                status = 'pending',
                                error_message = :msg,
                                updated_at = NOW()
                            """
                        ),
                        {
                            "did": dataset_id,
                            "tn": table_name,
                            "msg": f"rerouted_heavy:{reason}"[:500],
                        },
                    )
        _prune_open_cached_entries(engine, dataset_id, keep_table_name=table_name)
    except Exception:
        logger.debug("Could not mark dataset %s as rerouted pending", dataset_id, exc_info=True)


def _ensure_cached_entry(engine, dataset_id: str, table_name: str):
    """Ensure a cached_datasets row exists for this dataset (idempotent)."""
    try:
        with engine.begin() as conn:
            existing = conn.execute(
                text(
                    """
                    SELECT id
                    FROM cached_datasets
                    WHERE dataset_id = CAST(:did AS uuid)
                      AND table_name = :tn
                    ORDER BY updated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """
                ),
                {"did": dataset_id, "tn": table_name},
            ).fetchone()
            if existing:
                conn.execute(
                    text(
                        """
                        UPDATE cached_datasets
                        SET status = 'downloading',
                            error_message = NULL,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {"id": existing.id},
                )
            else:
                recycled = conn.execute(
                    text(
                        """
                        SELECT id
                        FROM cached_datasets
                        WHERE dataset_id = CAST(:did AS uuid)
                          AND status IN ('downloading', 'pending', 'error')
                        ORDER BY updated_at DESC NULLS LAST, id DESC
                        LIMIT 1
                        """
                    ),
                    {"did": dataset_id},
                ).fetchone()
                if recycled:
                    conn.execute(
                        text(
                            """
                            UPDATE cached_datasets
                            SET table_name = :tn,
                                status = 'downloading',
                                error_message = NULL,
                                updated_at = NOW()
                            WHERE id = :id
                            """
                        ),
                        {"tn": table_name, "id": recycled.id},
                    )
                else:
                    conn.execute(
                        text(
                            "INSERT INTO cached_datasets (dataset_id, table_name, status, updated_at) "
                            "VALUES (CAST(:did AS uuid), :tn, 'downloading', NOW()) "
                            "ON CONFLICT (table_name) DO NOTHING"
                        ),
                        {"did": dataset_id, "tn": table_name},
                    )
            conn.execute(
                text(
                    """
                    DELETE FROM cached_datasets
                    WHERE dataset_id = CAST(:did AS uuid)
                      AND table_name <> :tn
                      AND status IN ('downloading', 'pending', 'error')
                    """
                ),
                {"did": dataset_id, "tn": table_name},
            )
    except Exception:
        logger.debug("Could not ensure cached entry for %s", dataset_id, exc_info=True)


def _set_error_status(engine, dataset_id: str, error_msg: str, *, table_name: str | None = None):
    """Mark cached_datasets as permanently_failed for deterministic errors.

    Wrapper around `_apply_cached_outcome` that synthesizes a `_CollectorRunOutcome`
    for legacy callers that knew about deterministic failures before the outcome
    model existed. Per constitution §0.7, every terminal-status write must flow
    through the canonical `_apply_cached_outcome` path so `error_category` is
    classified consistently.

    Deterministic errors (unsupported format, file too large, zip bomb, etc.)
    will never succeed on retry, so we mark them permanently_failed immediately
    to prevent infinite re-dispatch loops from bulk_collect_all.
    """
    outcome = _CollectorRunOutcome(
        result_kind=_OUTCOME_TERMINAL_NON_TABULAR
        if _is_non_tabular_error_message(error_msg)
        else _OUTCOME_TERMINAL_UPSTREAM,
        cached_status="permanently_failed",
        error_message=error_msg[:500],
        retry_increment=1,
        should_prune_open=True,
    )
    try:
        # _apply_cached_outcome handles classification + persistence + pruning
        # (the canonical path per constitution §0.7).
        _apply_cached_outcome(
            engine,
            dataset_id=dataset_id,
            outcome=outcome,
            table_name=table_name,
        )
    except Exception:
        # Was logger.debug — silently swallowed status-update failures left
        # orphaned datasets in inconsistent states. Surface at warning so the
        # operator can correlate with worker errors.
        logger.warning(
            "Could not update error status for dataset %s (msg=%r)",
            dataset_id,
            error_msg[:120],
            exc_info=True,
        )


def _degraded_headers_allowed(*, portal: str, layout_profile: str, declared_format: str) -> bool:
    """Explicit policy gate for degraded-yet-acceptable materializations.

    Phase 3 keeps backward compatibility by allowing degraded outputs through
    an explicit policy function instead of accidental implicit acceptance.
    """
    if os.getenv("OPENARG_ALLOW_DEGRADED_HEADERS", "1") == "1":
        return True
    if layout_profile == _LAYOUT_WIDE and declared_format.lower() in {"csv", "txt"}:
        return True
    return portal in set(filter(None, os.getenv("OPENARG_DEGRADED_HEADER_PORTALS", "").split(",")))


def _is_non_tabular_error_message(error_message: str | None) -> bool:
    msg = (error_message or "").lower()
    markers = (
        "zip_document_bundle",
        "zip_no_parseable_file",
        "unsupported_format",
        "bad_zip_file",
        "file_too_large",
        "zip_entry_too_large",
    )
    return any(marker in msg for marker in markers)


def _materialization_outcome_for_ready(
    *,
    portal: str,
    declared_format: str,
    table_name: str,
    row_count: int,
    columns: list[str],
    layout_profile: str,
    header_quality: str,
    ws0_finding,
) -> _CollectorRunOutcome:
    if _ws0_is_critical(ws0_finding):
        return _CollectorRunOutcome(
            result_kind=_OUTCOME_PARSER_INVALID,
            cached_status="permanently_failed",
            error_message=f"ingestion_validation_failed:{ws0_finding.detector_name}",
            retry_increment=1,
            should_prune_open=True,
        )

    if row_count <= 0:
        return _CollectorRunOutcome(
            result_kind=_OUTCOME_PARSER_INVALID,
            cached_status="permanently_failed",
            error_message="parser_invalid:no_rows_materialized",
            retry_increment=1,
            should_prune_open=True,
        )

    if header_quality == _HEADER_INVALID:
        return _CollectorRunOutcome(
            result_kind=_OUTCOME_PARSER_INVALID,
            cached_status="permanently_failed",
            error_message="parser_invalid:header_quality_invalid",
            retry_increment=1,
            should_prune_open=True,
        )

    if header_quality == _HEADER_DEGRADED:
        if not _degraded_headers_allowed(
            portal=portal,
            layout_profile=layout_profile,
            declared_format=declared_format,
        ):
            return _CollectorRunOutcome(
                result_kind=_OUTCOME_PARSER_INVALID,
                cached_status="permanently_failed",
                error_message="parser_invalid:degraded_headers_disallowed",
                retry_increment=1,
                should_prune_open=True,
            )
        return _CollectorRunOutcome(
            result_kind=_OUTCOME_MATERIALIZED_DEGRADED,
            cached_status="ready",
            error_message=f"header_quality:{header_quality};layout_profile:{layout_profile}",
            should_prune_open=True,
        )

    return _CollectorRunOutcome(
        result_kind=_OUTCOME_MATERIALIZED_READY,
        cached_status="ready",
        should_prune_open=True,
    )


def _classify_collect_exception(exc: Exception, *, heavy_execution: bool, current_queue: str) -> _CollectorRunOutcome:
    exc_str = str(exc)
    lowered = exc_str.lower()

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
                # Structural DataFrame issues — retrying without changing
                # the upstream payload reproduces the exact same failure.
                # Treat as terminal so the dataset goes to
                # `permanently_failed` with `parse_schema_mismatch`
                # immediately instead of cycling through retries that all
                # log full tracebacks.
                "DuplicateColumn",
                "specified more than once",
            )
        )
        or "TooManyRedirects" in type(exc).__name__
    )
    if non_retryable:
        result_kind = (
            _OUTCOME_TERMINAL_NON_TABULAR if _is_non_tabular_error_message(exc_str) else _OUTCOME_TERMINAL_UPSTREAM
        )
        return _CollectorRunOutcome(
            result_kind=result_kind,
            cached_status="permanently_failed",
            error_message=exc_str[:500],
            retry_increment=1,
            should_prune_open=True,
        )

    # Belt-and-suspenders: catch InFailedSqlTransaction by type (raw or DBAPI-wrapped).
    # The marker-string match below also catches it via .lower(), but a typed
    # check avoids regressions if the message format ever changes.
    raw_inflight_failure = isinstance(exc, psycopg.errors.InFailedSqlTransaction)
    wrapped_inflight_failure = (
        isinstance(exc, DBAPIError)
        and exc.orig is not None
        and isinstance(exc.orig, psycopg.errors.InFailedSqlTransaction)
    )
    if raw_inflight_failure or wrapped_inflight_failure:
        return _CollectorRunOutcome(
            result_kind=_OUTCOME_RETRYABLE_MATERIALIZATION,
            cached_status="error",
            error_message=f"InFailedSqlTransaction (caught by type): {exc_str[:400]}",
            retry_increment=1,
        )

    materialization_markers = (
        "schema mismatch",
        "schema_drift_detected",
        "drop table if exists",
        "pg_type_typname_nsp_index",
        "infailedsqltransaction",
        "another command is already in progress",
    )
    if any(marker in lowered for marker in materialization_markers):
        return _CollectorRunOutcome(
            result_kind=_OUTCOME_RETRYABLE_MATERIALIZATION,
            cached_status="error",
            error_message=exc_str[:500],
            retry_increment=1,
        )

    if heavy_execution and current_queue != _HEAVY_RETRY_QUEUE and _is_transient_collect_error(exc):
        return _CollectorRunOutcome(
            result_kind=_OUTCOME_RETRYABLE_UPSTREAM,
            cached_status="pending",
            error_message=exc_str[:500],
            retry_increment=1,
        )

    return _CollectorRunOutcome(
        result_kind=_OUTCOME_RETRYABLE_UPSTREAM,
        cached_status="error",
        error_message=exc_str[:500],
        retry_increment=1,
    )


def _classify_error_category(
    error_message: str | None,
    *,
    exc: Exception | None = None,
) -> str:
    """Map free-text error messages + optional exception type to closed taxonomy.

    Used by `_apply_cached_outcome` to populate `cached_datasets.error_category`
    so operational dashboards can `GROUP BY error_category` instead of doing
    `LIKE` over free text. Buckets align with spec 014 §3.
    """
    if exc is not None and isinstance(exc, psycopg.errors.InFailedSqlTransaction):
        return "materialize_table_collision"
    if exc is not None and isinstance(exc, DBAPIError) and exc.orig is not None:
        if isinstance(exc.orig, psycopg.errors.InFailedSqlTransaction):
            return "materialize_table_collision"

    msg = (error_message or "").lower()
    if not msg:
        return "unknown"

    # Validation outputs (WS0)
    if "ingestion_validation_failed:html_as_data" in msg:
        return "validation_failed"
    if "ingestion_validation_failed:row_count" in msg:
        return "validation_failed"
    if "ingestion_validation_failed:" in msg:
        return "validation_failed"

    # Parser-side
    if msg.startswith("parser_invalid:"):
        return "validation_failed"
    if "no columns to parse from file" in msg:
        return "parse_format"
    if "excel file format cannot be determined" in msg:
        return "parse_format"
    if "schema mismatch" in msg or "schema_drift_detected" in msg:
        return "parse_schema_mismatch"
    if "unicodedecodeerror" in msg or "codec can't decode" in msg:
        return "parse_encoding"

    # Materialization-side
    if "infailedsqltransaction" in msg or "current transaction is aborted" in msg:
        return "materialize_table_collision"
    if "duplicate key value violates unique constraint" in msg:
        return "materialize_table_collision"
    if "pg_type_typname_nsp_index" in msg:
        return "materialize_table_collision"
    if "another command is already in progress" in msg:
        return "materialize_table_collision"
    if "execution failed on sql 'insert into" in msg:
        # pandas-side INSERT failures normally mean the materialized columns
        # the worker tried to write don't match the existing physical schema —
        # most often because the parser produced suffixed columns like
        # "1.foo", "2.foo" from a multi-sheet/multi-csv concat.
        return "parse_schema_mismatch"
    if "execution failed on sql" in msg:
        return "parse_schema_mismatch"
    if "duplicatecolumn" in msg or "specified more than once" in msg:
        return "parse_schema_mismatch"
    if "no space left on device" in msg or "disk full" in msg:
        return "materialize_disk_full"
    if "resource_table_full" in msg:
        return "materialize_disk_full"

    # Archive
    if "bad_zip_file" in msg or "zip_no_parseable_file" in msg:
        return "policy_non_tabular"
    if "zip_document_bundle" in msg:
        return "policy_non_tabular"

    # Policy
    if "file_too_large" in msg or "zip_entry_too_large" in msg:
        return "policy_too_large"

    # Network / HTTP
    if "403 forbidden" in msg or "401 unauthorized" in msg:
        return "download_http_error"
    if "404 not found" in msg or "410 gone" in msg:
        return "download_http_error"
    if "exceeded maximum allowed redirects" in msg or "toomanyredirects" in msg:
        return "download_http_error"
    if "5" in msg and ("server error" in msg or " 500" in msg or " 502" in msg or " 503" in msg or " 504" in msg):
        return "download_http_error"
    if "name or service not known" in msg or "no route to host" in msg:
        return "download_network"
    if "connection refused" in msg or "connection reset" in msg:
        return "download_network"
    if "timed out" in msg or "read operation timed out" in msg or "timeout" in msg:
        return "download_timeout"

    # Metadata
    if "no_download_url" in msg or "missing_download_url" in msg:
        return "metadata_no_url"

    # Orchestration recovery
    if "exhausted retries" in msg or "stuck in" in msg:
        return "orchestration_recovery_loop"
    if "table missing" in msg or "marked for re-download" in msg:
        return "orchestration_table_missing"

    return "unknown"


def _record_cache_drop(
    engine,
    *,
    table_name: str,
    reason: str,
    actor: str = "collector",
    extra: dict | None = None,
) -> None:
    """Application-side audit of DROP TABLE on cache_* tables.

    AWS RDS does not expose SUPERUSER, so the original mig 0036 plan to use
    a pg_event_trigger is unavailable. Instead we INSERT here before each
    DROP TABLE so we still capture the operationally-driven drops. Drops
    issued manually (psql admin) are not captured here — fall back to RDS
    audit logs / CloudTrail for those.
    """
    if not table_name:
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO cache_drop_audit (object_name, reason, actor, extra)
                    VALUES (:obj, :reason, :actor, CAST(:extra AS jsonb))
                    """
                ),
                {
                    "obj": table_name,
                    "reason": reason[:500],
                    "actor": actor,
                    "extra": json.dumps(extra) if extra else None,
                },
            )
    except Exception:
        # Don't let audit failures break the actual drop path.
        logger.warning(
            "Could not record cache_drop_audit for %s (reason=%s)",
            table_name,
            reason[:60],
            exc_info=True,
        )


# ─── Phase 1/1.5 (MASTERPLAN) — raw schema helpers ──────────────────────────
#
# `_resolve_collect_destination` is the entry point: callers in `collect_dataset`
# call it instead of `_sanitize_table_name` to decide where the materialized
# table lives (raw vs. legacy public). The choice is gated by the
# `OPENARG_USE_RAW_LAYER` env flag so rollback is `unset env, restart workers`.

_RAW_NAMER = RawPhysicalNamer()

# Whitelist of allowed `raw_schema` literals when `_apply_cached_outcome`
# composes a qualified name. Defends against any future caller that might
# pass an attacker-controlled schema string into the SQL composition.
_RAW_SCHEMA_ALLOWED = frozenset({"raw"})

# Per-task context variable that propagates the medallion target schema
# from `collect_dataset` (where `_resolve_collect_destination` runs) down
# to every `_to_sql_safe` call site — including the deeply-nested CSV
# chunk loaders, JSON record map loaders, and ZIP parsers that don't
# receive `write_schema` as an explicit kwarg.
#
# When the ContextVar is set, `_to_sql_safe(schema=None, ...)` falls back
# to it. Explicit `schema=` kwargs always win.
_current_write_schema: ContextVar[str | None] = ContextVar(
    "_current_write_schema", default=None
)


@dataclass(frozen=True)
class _CollectDestination:
    """Where a single `collect_dataset` run will materialize its table.

    `qualified_name` is what `cached_datasets.table_name` should store and what
    `catalog_resources.materialized_table_name` should point to: `<schema>.<bare>`
    when schema is non-public, otherwise the bare name (preserving the legacy
    `cache_*` look so existing callers keep working).
    """

    schema: str  # "raw" or "public"
    bare_name: str  # `<portal>__<slug>__<discrim>__v<N>` for raw, `cache_*` for legacy
    version: int  # 1+ for raw; 0 for legacy (no versioning)
    resource_identity: str  # `{portal}::{source_id}`

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.bare_name}" if self.schema != "public" else self.bare_name


def _use_raw_layer() -> bool:
    """Feature flag: write to schema `raw` with versioning.

    Default OFF so the first deploy is a no-op and operators flip the flag
    when ready for cutover.
    """
    return os.getenv("OPENARG_USE_RAW_LAYER", "0").strip().lower() in ("1", "true", "yes")


def _collect_write_chunk(
    df: pd.DataFrame,
    table_name: str,
    engine,
    *,
    write_schema: str | None,
    source_url: str | None = None,
    source_file_hash: str | None = None,
    parser_version: str | None = None,
    **kwargs,
) -> None:
    """Wrap `_to_sql_safe` so each call site does not repeat the metadata
    injection. When `write_schema == 'raw'` the DataFrame gets the lineage
    columns appended; for legacy `public` writes the call passes through.
    """
    if write_schema == "raw":
        df = _inject_raw_metadata_columns(
            df,
            source_url=source_url,
            source_file_hash=source_file_hash,
            parser_version=parser_version,
            collector_version=os.getenv("OPENARG_COLLECTOR_VERSION") or None,
        )
    _to_sql_safe(df, table_name, engine, schema=write_schema, **kwargs)


def _resolve_collect_destination(
    engine,
    dataset_id: str,
    *,
    portal: str,
    source_id: str,
    title: str,
) -> _CollectDestination:
    """Decide where the next materialization for `dataset_id` should land.

    Raw path (`OPENARG_USE_RAW_LAYER=1`): bumps `raw_table_versions` and returns
    a versioned name in schema `raw`.

    Legacy path (default): returns the historical `cache_<portal>_<slug>__<hash>`
    name in schema `public` so the codebase keeps working unchanged until
    operators flip the flag.
    """
    resource_identity = _resource_identity(portal, source_id)
    if _use_raw_layer():
        raw = _resolve_raw_table_for_dataset(
            engine,
            dataset_id,
            portal=portal,
            source_id=source_id,
            slug_hint=title,
        )
        return _CollectDestination(
            schema="raw",
            bare_name=raw.bare_name,
            version=raw.version,
            resource_identity=resource_identity,
        )
    # Legacy path — keep current behavior bit-for-bit.
    group_table_name = _sanitize_table_name(title, portal)
    legacy_name = _resource_table_name(group_table_name, dataset_id)
    return _CollectDestination(
        schema="public",
        bare_name=legacy_name,
        version=0,
        resource_identity=resource_identity,
    )


def _resolve_raw_table_for_dataset(
    engine,
    dataset_id: str,
    *,
    portal: str | None = None,
    source_id: str | None = None,
    slug_hint: str = "",
) -> RawPhysicalName:
    """Resolve the raw-schema table name for the next ingest of `dataset_id`.

    Reads `raw_table_versions` to find the previous max version; bumps by 1.
    `portal` and `source_id` may be passed explicitly; if omitted, they are
    looked up from the `datasets` row.

    Concurrency: the read-then-bump uses `pg_advisory_xact_lock` keyed by
    `resource_identity` so two collectors running the same dataset
    simultaneously serialize on this section. The lock is transaction-scoped
    so it auto-releases at COMMIT — no leak risk if the function raises.
    """
    if not portal or not source_id:
        with engine.connect() as conn:
            ds = conn.execute(
                text(
                    "SELECT portal, source_id, COALESCE(title, '') AS title "
                    "FROM datasets WHERE id = CAST(:did AS uuid)"
                ),
                {"did": dataset_id},
            ).fetchone()
            conn.rollback()
        if ds is None:
            raise ValueError(f"dataset {dataset_id} not found")
        portal = portal or ds.portal
        source_id = source_id or ds.source_id
        slug_hint = slug_hint or ds.title

    resource_identity = _resource_identity(portal, source_id)
    lock_key = _lock_key("raw_table_version_bump", resource_identity)
    with engine.begin() as conn:
        # Transaction-scoped advisory lock: two callers with the same
        # resource_identity serialize here. The lock releases at COMMIT.
        conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_key})
        prev = conn.execute(
            text(
                "SELECT COALESCE(MAX(version), 0) AS v "
                "FROM raw_table_versions WHERE resource_identity = :rid"
            ),
            {"rid": resource_identity},
        ).fetchone()
        next_version = int(prev.v) + 1 if prev else 1
    return _RAW_NAMER.build(portal, source_id, version=next_version, slug_hint=slug_hint)


def _resource_identity(portal: str, source_id: str, sub_path: str | None = None) -> str:
    """Deterministic key for `catalog_resources.resource_identity`.

    Mirrors the contract in spec 015 §2: `{portal}::{source_id}[::{sub_path}]`.
    """
    base = f"{portal}::{source_id}"
    return f"{base}::{sub_path}" if sub_path else base


def _register_raw_version(
    engine,
    *,
    resource_identity: str,
    version: int,
    table_name: str,
    schema_name: str = "raw",
    row_count: int | None = None,
    size_bytes: int | None = None,
    source_url: str | None = None,
    source_file_hash: str | None = None,
    parser_version: str | None = None,
    collector_version: str | None = None,
    is_truncated: bool = False,
) -> None:
    """Insert a new entry into `raw_table_versions` and mark prior versions
    of the same `resource_identity` as superseded.

    Idempotent on `(resource_identity, version)` — re-running with the same
    version is a no-op (`ON CONFLICT DO NOTHING`).
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO raw_table_versions (
                    resource_identity, version, schema_name, table_name,
                    row_count, size_bytes, source_url, source_file_hash,
                    parser_version, collector_version, is_truncated
                ) VALUES (
                    :rid, :v, :sch, :tn,
                    :rc, :sz, :url, :hash,
                    :pv, :cv, :trunc
                )
                ON CONFLICT (resource_identity, version) DO NOTHING
                """
            ),
            {
                "rid": resource_identity,
                "v": version,
                "sch": schema_name,
                "tn": table_name,
                "rc": row_count,
                "sz": size_bytes,
                "url": source_url,
                "hash": source_file_hash,
                "pv": parser_version,
                "cv": collector_version,
                "trunc": is_truncated,
            },
        )
        conn.execute(
            text(
                """
                UPDATE raw_table_versions
                SET superseded_at = NOW()
                WHERE resource_identity = :rid
                  AND version < :v
                  AND superseded_at IS NULL
                """
            ),
            {"rid": resource_identity, "v": version},
        )


def _inject_raw_metadata_columns(
    df: pd.DataFrame,
    *,
    source_url: str | None = None,
    source_file_hash: str | None = None,
    parser_version: str | None = None,
    collector_version: str | None = None,
) -> pd.DataFrame:
    """Add per-row lineage columns expected on every raw table.

    `_ingested_at` is a server-side default in the table; not set here so
    the database supplies it consistently. `_ingest_row_id` is a BIGSERIAL
    PK added by the caller via ALTER TABLE after the bulk INSERT, since
    `df.to_sql` does not declare PKs.
    """
    df = df.copy()
    df["_source_url"] = source_url
    df["_source_file_hash"] = source_file_hash
    df["_parser_version"] = parser_version
    df["_collector_version"] = collector_version
    return df


def _apply_cached_outcome(
    engine,
    *,
    dataset_id: str,
    outcome: _CollectorRunOutcome,
    table_name: str | None = None,
    row_count: int | None = None,
    columns: list[str] | None = None,
    size_bytes: int | None = None,
    s3_key: str | None = None,
    layout_profile: str | None = None,
    header_quality: str | None = None,
    now: datetime | None = None,
    # MASTERPLAN Fase 1.5 — raw-layer destination metadata. When `raw_schema`
    # is "raw" and the outcome is `ready`, this function ALSO registers the
    # new version in `raw_table_versions` and updates
    # `catalog_resources.materialized_table_name` to the qualified `raw.<name>`
    # form. All four fields default to None so legacy callers are unchanged.
    raw_schema: str | None = None,
    raw_version: int | None = None,
    resource_identity: str | None = None,
    source_url: str | None = None,
    is_truncated: bool = False,
) -> int:
    """Persist one explicit collector outcome and return the resulting retry_count."""
    finalized_at = now or datetime.now(UTC)
    columns_json = json.dumps([str(c) for c in columns]) if columns is not None else None
    error_category = _classify_error_category(outcome.error_message)

    with engine.begin() as conn:
        if outcome.cached_status == "ready" and table_name:
            conn.execute(
                text(
                    """
                    INSERT INTO cached_datasets (
                        dataset_id, table_name, status, row_count,
                        columns_json, size_bytes, s3_key, error_message,
                        error_category, layout_profile, header_quality, updated_at
                    ) VALUES (
                        CAST(:did AS uuid), :tn, 'ready', :rows,
                        :cols, :size, :s3, :msg,
                        :error_category, :layout_profile, :header_quality, :now
                    )
                    ON CONFLICT (table_name) DO UPDATE SET
                        dataset_id = CAST(:did AS uuid),
                        status = 'ready',
                        row_count = EXCLUDED.row_count,
                        columns_json = EXCLUDED.columns_json,
                        size_bytes = EXCLUDED.size_bytes,
                        s3_key = EXCLUDED.s3_key,
                        error_message = EXCLUDED.error_message,
                        error_category = EXCLUDED.error_category,
                        layout_profile = EXCLUDED.layout_profile,
                        header_quality = EXCLUDED.header_quality,
                        updated_at = :now
                    """
                ),
                {
                    "did": dataset_id,
                    "tn": table_name,
                    "rows": row_count,
                    "cols": columns_json,
                    "size": size_bytes,
                    "s3": s3_key,
                    "msg": outcome.error_message,
                    "error_category": error_category,
                    "layout_profile": layout_profile,
                    "header_quality": header_quality,
                    "now": finalized_at,
                },
            )
            retry_count = 0
        elif table_name:
            conn.execute(
                text(
                    """
                    INSERT INTO cached_datasets (
                        dataset_id, table_name, status, row_count, columns_json,
                        size_bytes, s3_key, error_message, error_category,
                        retry_count, layout_profile, header_quality, updated_at
                    ) VALUES (
                        CAST(:did AS uuid), :tn, :status, :rows, :cols,
                        :size, :s3, :msg, :error_category,
                        :retry, :layout_profile, :header_quality, :now
                    )
                    ON CONFLICT (table_name) DO UPDATE SET
                        dataset_id = CAST(:did AS uuid),
                        status = CASE
                            WHEN :status = 'permanently_failed' THEN 'permanently_failed'
                            WHEN cached_datasets.retry_count + :retry >= :max THEN 'permanently_failed'
                            ELSE :status
                        END,
                        row_count = COALESCE(:rows, cached_datasets.row_count),
                        columns_json = COALESCE(:cols, cached_datasets.columns_json),
                        size_bytes = COALESCE(:size, cached_datasets.size_bytes),
                        s3_key = COALESCE(:s3, cached_datasets.s3_key),
                        error_message = :msg,
                        error_category = :error_category,
                        layout_profile = COALESCE(:layout_profile, cached_datasets.layout_profile),
                        header_quality = COALESCE(:header_quality, cached_datasets.header_quality),
                        retry_count = cached_datasets.retry_count + :retry,
                        updated_at = :now
                    """
                ),
                {
                    "did": dataset_id,
                    "tn": table_name,
                    "status": outcome.cached_status,
                    "rows": row_count,
                    "cols": columns_json,
                    "size": size_bytes,
                    "s3": s3_key,
                    "msg": outcome.error_message,
                    "error_category": error_category,
                    "retry": outcome.retry_increment,
                    "layout_profile": layout_profile,
                    "header_quality": header_quality,
                    "max": MAX_TOTAL_ATTEMPTS,
                    "now": finalized_at,
                },
            )
            current = conn.execute(
                text("SELECT retry_count FROM cached_datasets WHERE table_name = :tn"),
                {"tn": table_name},
            ).fetchone()
            retry_count = int(current.retry_count) if current else 0
        else:
            conn.execute(
                text(
                    """
                    UPDATE cached_datasets
                    SET status = CASE
                            WHEN :status = 'permanently_failed' THEN 'permanently_failed'
                            WHEN retry_count + :retry >= :max THEN 'permanently_failed'
                            ELSE :status
                        END,
                        error_message = :msg,
                        error_category = :error_category,
                        layout_profile = COALESCE(:layout_profile, layout_profile),
                        header_quality = COALESCE(:header_quality, header_quality),
                        retry_count = retry_count + :retry,
                        updated_at = :now
                    WHERE dataset_id = CAST(:did AS uuid)
                    """
                ),
                {
                    "did": dataset_id,
                    "status": outcome.cached_status,
                    "msg": outcome.error_message,
                    "error_category": error_category,
                    "retry": outcome.retry_increment,
                    "layout_profile": layout_profile,
                    "header_quality": header_quality,
                    "max": MAX_TOTAL_ATTEMPTS,
                    "now": finalized_at,
                },
            )
            current = conn.execute(
                text("SELECT COALESCE(MAX(retry_count), 0) AS retry_count FROM cached_datasets WHERE dataset_id = CAST(:did AS uuid)"),
                {"did": dataset_id},
            ).fetchone()
            retry_count = int(current.retry_count) if current else 0

    if outcome.should_prune_open:
        _prune_open_cached_entries(engine, dataset_id, keep_table_name=table_name)

    # MASTERPLAN Fase 1.5 — when a raw-layer materialization landed `ready`,
    # register the version in `raw_table_versions` and bubble the qualified
    # name up to `catalog_resources.materialized_table_name` so the Serving
    # Port adapter (and any downstream reader) can resolve the new physical
    # location. The registration + catalog UPDATE happen in a single
    # transaction so the catalog never points at a version that is not in
    # the registry. Failure aborts the auto-promote — staging running on a
    # missing version would always fail.
    if (
        outcome.cached_status == "ready"
        and table_name
        and raw_schema in _RAW_SCHEMA_ALLOWED  # validate raw_schema literal
        and raw_version is not None
        and resource_identity
    ):
        raw_finalized = False
        try:
            _register_raw_version(
                engine,
                resource_identity=resource_identity,
                version=raw_version,
                table_name=table_name,
                schema_name=raw_schema,
                row_count=row_count,
                size_bytes=size_bytes,
                source_url=source_url,
                parser_version=os.getenv("OPENARG_PARSER_VERSION") or None,
                collector_version=os.getenv("OPENARG_COLLECTOR_VERSION") or None,
                is_truncated=is_truncated,
            )
            qualified = f'{raw_schema}."{table_name}"'
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE catalog_resources
                        SET materialized_table_name = :qn,
                            materialization_status = 'ready',
                            parser_version = COALESCE(:pv, parser_version),
                            updated_at = NOW()
                        WHERE resource_identity = :rid
                        """
                    ),
                    {
                        "qn": qualified,
                        "pv": os.getenv("OPENARG_PARSER_VERSION") or None,
                        "rid": resource_identity,
                    },
                )
            # Purge stale `ready` rows of the SAME dataset that point to
            # an older version of THIS resource. `_prune_open_cached_entries`
            # only purges `downloading/pending/error` by design, so when v2
            # lands the v1 row stays as `ready` and shows up as an orphan
            # in the catalog (raw_table_versions live ≠ cached_datasets
            # row's table_name). This DELETE is scoped to rows that are
            # provably superseded — never touches legacy public.cache_*
            # `ready` rows that have no raw_table_versions entry.
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        DELETE FROM cached_datasets cd
                        USING raw_table_versions rtv
                        WHERE cd.dataset_id = CAST(:did AS uuid)
                          AND cd.status = 'ready'
                          AND cd.table_name <> :current_table
                          AND rtv.table_name = cd.table_name
                          AND rtv.resource_identity = :rid
                          AND rtv.superseded_at IS NOT NULL
                        """
                    ),
                    {
                        "did": dataset_id,
                        "current_table": table_name,
                        "rid": resource_identity,
                    },
                )
            raw_finalized = True
        except Exception:
            logger.warning(
                "Could not finalize raw-layer registration for %s (resource=%s, v=%s); "
                "skipping auto-promote",
                table_name,
                resource_identity,
                raw_version,
                exc_info=True,
            )

        # Auto-refresh marts that consume this portal. We dispatch one
        # `refresh_mart` task per matching mart, but with a `task_id` that
        # buckets timestamps into 120s windows: Celery rejects duplicate
        # task_ids, so 50 raw landings within the same 2-min window
        # produce ONE refresh per mart, not 50. Trade: a refresh can be
        # delayed up to 120s after the last landing — acceptable for a
        # mart layer.
        if (
            raw_finalized
            and os.getenv("OPENARG_AUTO_REFRESH_MARTS", "0").lower()
            in ("1", "true", "yes")
        ):
            try:
                # Import inside the branch to avoid the circular dep at
                # module load (mart_tasks imports collector_tasks helpers).
                from app.infrastructure.celery.tasks.mart_tasks import (
                    find_marts_for_portal,
                    refresh_mart,
                )

                # Debounce: pin `task_id` per mart_id (no time bucket).
                # Celery rejects duplicate task_ids while the prior task is
                # still in queue or running, so 50 raw landings within one
                # mart's refresh window collapse to ONE refresh. Once that
                # refresh finishes, the next landing kicks off a new one.
                # The previous bucket-based approach had a boundary-race
                # bug where landings on either side of `now / 120s` ended
                # up in different buckets and ran back-to-back.
                portal_for_dispatch = resource_identity.split("::", 1)[0]
                for mart_id in find_marts_for_portal(portal_for_dispatch):
                    refresh_mart.apply_async(
                        args=[mart_id],
                        queue="ingest",
                        task_id=f"refresh_mart:{mart_id}",
                        expires=120,
                    )
            except Exception:
                logger.warning(
                    "Could not enqueue refresh_mart for portal of %s",
                    resource_identity,
                    exc_info=True,
                )

    return retry_count


def _finalize_cached_dataset(
    engine,
    *,
    dataset_id: str,
    portal: str,
    source_id: str,
    table_name: str,
    row_count: int,
    columns: list[str],
    declared_format: str,
    download_url: str | None = None,
    declared_size_bytes: int | None = None,
    s3_key: str | None = None,
    error_message: str | None = None,
    layout_profile: str | None = None,
    header_quality: str | None = None,
    now: datetime | None = None,
    # MASTERPLAN Fase 1.5 — see `_apply_cached_outcome` for semantics.
    raw_schema: str | None = None,
    raw_version: int | None = None,
    resource_identity: str | None = None,
    is_truncated: bool = False,
) -> dict[str, object]:
    """Apply the WS0 post-parse gate, then persist the final cached state."""
    finalized_at = now or datetime.now(UTC)
    normalized_columns = [str(c) for c in columns]
    columns_json = json.dumps(normalized_columns)

    finding = _ws0_validate_post_parse(
        engine,
        dataset_id=dataset_id,
        portal=portal,
        source_id=source_id,
        download_url=download_url,
        declared_format=declared_format,
        table_name=table_name,
        materialized_columns=normalized_columns,
        materialized_row_count=row_count,
        declared_size_bytes=declared_size_bytes,
        columns_json=columns_json,
    )
    resolved_layout_profile = (
        layout_profile
        or (_LAYOUT_WIDE if len(normalized_columns) > _HEAVY_WIDTH_COLUMN_THRESHOLD else _LAYOUT_SIMPLE)
    )
    resolved_header_quality = header_quality or _header_quality_label(normalized_columns)
    outcome = _materialization_outcome_for_ready(
        portal=portal,
        declared_format=declared_format,
        table_name=table_name,
        row_count=row_count,
        columns=normalized_columns,
        layout_profile=resolved_layout_profile,
        header_quality=resolved_header_quality,
        ws0_finding=finding,
    )

    if outcome.cached_status != "ready":
        logger.warning(
            "Dataset %s finalized as %s (%s)",
            dataset_id,
            outcome.result_kind,
            outcome.error_message,
        )
        _apply_cached_outcome(
            engine,
            dataset_id=dataset_id,
            table_name=table_name,
            outcome=outcome,
            row_count=row_count,
            columns=normalized_columns,
            size_bytes=declared_size_bytes,
            s3_key=s3_key,
            layout_profile=resolved_layout_profile,
            header_quality=resolved_header_quality,
            now=finalized_at,
            raw_schema=raw_schema,
            raw_version=raw_version,
            resource_identity=resource_identity,
            source_url=download_url,
            is_truncated=is_truncated,
        )
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE datasets "
                        "SET is_cached = false, updated_at = :now "
                        "WHERE id = CAST(:did AS uuid)"
                    ),
                    {"did": dataset_id, "now": finalized_at},
                )
        except Exception:
            logger.debug(
                "Could not clear datasets.is_cached after WS0 rejection for %s",
                dataset_id,
                exc_info=True,
            )
        return {"ok": False, "status": outcome.cached_status, "error": outcome.error_message}

    _apply_cached_outcome(
        engine,
        dataset_id=dataset_id,
        table_name=table_name,
        outcome=outcome,
        row_count=row_count,
        columns=normalized_columns,
        size_bytes=declared_size_bytes,
        s3_key=s3_key,
        layout_profile=resolved_layout_profile,
        header_quality=resolved_header_quality,
        now=finalized_at,
        raw_schema=raw_schema,
        raw_version=raw_version,
        resource_identity=resource_identity,
        source_url=download_url,
        is_truncated=is_truncated,
    )
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE datasets "
                "SET is_cached = true, row_count = :rows, updated_at = :now "
                "WHERE id = CAST(:did AS uuid)"
            ),
            {
                "did": dataset_id,
                "rows": row_count,
                "now": finalized_at,
            },
        )
    return {
        "ok": True,
        "status": outcome.cached_status,
        "result_kind": outcome.result_kind,
        "columns_json": columns_json,
    }


def _check_schema_compat_columns(engine, table_name: str, columns) -> bool:
    """Check if columns are compatible with an existing table for append.

    Returns True only when the normalized SQL-visible column set matches exactly.

    Subset/superset matching looked attractive for noisy upstream feeds, but it
    causes an operationally worse failure mode for long-running bundle ingestion:
    pandas `to_sql(..., if_exists="append")` will still try to insert the
    incoming columns, and PostgreSQL will reject the write if the table is
    missing any of them. At that point `_to_sql_safe()` has to drop/recreate the
    table mid-run, which destroys already materialized sibling progress. Exact
    equality is a safer routing boundary: genuinely different shapes get their
    own schema-variant table up front instead of colliding during append.
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
        return new_cols == existing_cols_clean
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
def collect_dataset(self, dataset_id: str, force_heavy: bool = False):
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

    def _update_task_state_safe(*, state: str, meta: dict) -> None:
        task_id = getattr(getattr(self, "request", None), "id", None)
        if not task_id:
            return
        self.update_state(state=state, meta=meta)

    def _is_heavy_execution() -> bool:
        delivery = getattr(getattr(self, "request", None), "delivery_info", None) or {}
        return force_heavy or delivery.get("queue") in {
            _HEAVY_COLLECT_QUEUE,
            _HEAVY_RETRY_QUEUE,
        }

    def _current_queue() -> str | None:
        delivery = getattr(getattr(self, "request", None), "delivery_info", None) or {}
        return delivery.get("queue")

    def _reroute_to_queue(
        reason: str,
        *,
        queue: str,
        countdown: int | None = None,
    ) -> dict[str, object]:
        logger.warning(
            "Rerouting dataset %s to %s (%s)",
            dataset_id,
            queue,
            reason,
        )
        _mark_dataset_rerouted_pending(
            engine,
            dataset_id,
            table_name,
            reason=reason,
        )
        apply_kwargs = {
            "args": [dataset_id],
            "kwargs": {"force_heavy": True},
            "queue": queue,
        }
        if countdown is not None:
            apply_kwargs["countdown"] = countdown
        collect_dataset.apply_async(**apply_kwargs)
        return {
            "dataset_id": dataset_id,
            "status": "rerouted_heavy" if queue == _HEAVY_COLLECT_QUEUE else "rerouted_heavy_retry",
            "reason": reason,
            "queue": queue,
        }

    def _reroute_to_heavy(reason: str) -> dict[str, object]:
        return _reroute_to_queue(reason, queue=_HEAVY_COLLECT_QUEUE)

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

        # MASTERPLAN Fase 1.5: route through the destination resolver so the
        # raw-layer feature flag controls where the table lands. When
        # `OPENARG_USE_RAW_LAYER` is unset/0 the resolver returns the legacy
        # `cache_<portal>_<slug>__<hash>` name in schema `public`, so this
        # call is a no-op for current production behavior.
        destination = _resolve_collect_destination(
            engine,
            dataset_id,
            portal=portal,
            source_id=source_id,
            title=title,
        )
        write_schema: str | None = destination.schema if destination.schema != "public" else None
        table_name = destination.bare_name

        # MASTERPLAN Fase 1.5: bind the destination schema to the task
        # ContextVar so every internal `_to_sql_safe` call (including the
        # ones inside chunked CSV loaders, JSON record map loaders and ZIP
        # parsers that don't receive `write_schema` as a kwarg) honors the
        # medallion target. The token is reset in the `finally` below.
        _write_schema_token = _current_write_schema.set(write_schema)

        # Ensure a cached_datasets row exists EARLY so failures are always tracked
        _ensure_cached_entry(engine, dataset_id, table_name)
        _update_task_state_safe(
            state="STARTED", meta={"dataset_id": dataset_id, "stage": "collecting"}
        )

        if not _is_heavy_execution() and _should_route_to_heavy_from_metadata(
            portal=portal,
            fmt=fmt,
            title=title,
            download_url=download_url,
        ):
            return _reroute_to_heavy(f"metadata:{portal}:{fmt}")

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

            # WS0 Modo 1 — pre-parse guard: reject HTML-as-data, GDrive
            # warnings, .rar disguised as .zip, etc. before pandas touches it.
            # Findings are persisted FIRST (so the audit trail captures the
            # rejection) and then the upload is short-circuited — we never
            # push HTML/.rar garbage to S3.
            _ws0_pre_finding = _ws0_validate_pre_parse(
                engine,
                dataset_id=dataset_id,
                portal=portal,
                source_id=source_id,
                download_url=download_url,
                declared_format=fmt,
                raw_byte_sample=_read_byte_sample(tmp_path),
                declared_size_bytes=file_size,
                table_name=table_name,
            )
            if _ws0_is_critical(_ws0_pre_finding):
                msg = f"ingestion_validation_failed:{_ws0_pre_finding.detector_name}"
                logger.warning(
                    "Dataset %s rejected pre-parse by %s: %s",
                    dataset_id,
                    _ws0_pre_finding.detector_name,
                    _ws0_pre_finding.message,
                )
                _set_error_status(engine, dataset_id, msg, table_name=table_name)
                return {"error": msg}

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
            member_tables: list[dict[str, object]] = []
            parse_started_at = time.monotonic()

            if fmt in ("csv", "txt"):
                # Chunked CSV loading — never loads full file into memory
                # For append mode, do a quick schema check using first chunk
                csv_chunk_size: int | None = None
                csv_preview_column_count: int | None = None
                if append_mode:
                    try:
                        _csv_params = _detect_csv_params(tmp_path)
                        preview_df = _read_csv_preview(tmp_path, nrows=5, csv_params=_csv_params)
                        csv_preview_column_count = len(preview_df.columns) + 1
                        if not _is_heavy_execution() and _should_route_to_heavy_by_width(
                            len(preview_df.columns)
                        ):
                            return _reroute_to_heavy(
                                f"width:{len(preview_df.columns)}:{portal}:{fmt}"
                            )
                        csv_chunk_size = _csv_chunk_size_for_columns(csv_preview_column_count)
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

                if csv_chunk_size is None:
                    try:
                        _csv_params = _detect_csv_params(tmp_path)
                        preview_df = _read_csv_preview(tmp_path, nrows=5, csv_params=_csv_params)
                        csv_preview_column_count = len(preview_df.columns) + 1
                        if not _is_heavy_execution() and _should_route_to_heavy_by_width(
                            len(preview_df.columns)
                        ):
                            return _reroute_to_heavy(
                                f"width:{len(preview_df.columns)}:{portal}:{fmt}"
                            )
                        csv_chunk_size = _csv_chunk_size_for_columns(csv_preview_column_count)
                    except Exception:
                        logger.debug(
                            "CSV chunk-size preview failed for %s, using default chunk size",
                            dataset_id,
                            exc_info=True,
                        )

                row_count, columns, csv_truncated = _load_csv_chunked(
                    tmp_path,
                    table_name,
                    engine,
                    chunk_size=csv_chunk_size,
                    source_dataset_id=dataset_id,
                    force_append=append_mode,
                    write_schema=write_schema,
                    csv_params_override=_stabilize_csv_params_for_width(
                        _detect_csv_params(tmp_path),
                        column_count=csv_preview_column_count or 1,
                        force_text=_is_heavy_execution(),
                    )
                    if csv_preview_column_count is not None
                    else None,
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

                try:
                    zf = zipfile.ZipFile(tmp_path)
                except zipfile.BadZipFile:
                    logger.warning(f"ZIP {dataset_id}: not a valid ZIP file")
                    _set_error_status(engine, dataset_id, "bad_zip_file", table_name=table_name)
                    return {"error": "bad_zip_file"}

                expansion = MultiFileExpander().expand(tmp_path)
                if expansion.skipped_oversized:
                    largest = expansion.skipped_oversized[0]
                    logger.warning(
                        "ZIP %s: oversized entry rejected by multi-file expander: %s",
                        dataset_id,
                        largest,
                    )
                    _set_error_status(
                        engine,
                        dataset_id,
                        f"zip_entry_too_large: {largest}",
                        table_name=table_name,
                    )
                    return {"error": f"zip_entry_too_large: {largest}"}

                expanded_member_names = [entry.name for entry in expansion.expanded]
                expanded_total_size = sum(entry.size_bytes for entry in expansion.expanded)
                if not expanded_member_names and expansion.document_bundle:
                    logger.warning(
                        "ZIP %s classified as document bundle by multi-file expander",
                        dataset_id,
                    )
                    _set_error_status(
                        engine,
                        dataset_id,
                        "zip_document_bundle",
                        table_name=table_name,
                    )
                    return {"error": "zip_document_bundle"}

                if not expanded_member_names:
                    logger.warning(
                        "ZIP %s: no parseable entry selected by multi-file expander (unknown=%s)",
                        dataset_id,
                        expansion.unknown,
                    )
                    _set_error_status(
                        engine,
                        dataset_id,
                        "zip_no_parseable_file",
                        table_name=table_name,
                    )
                    return {"error": "zip_no_parseable_file"}

                if not _has_temp_space(expanded_total_size):
                    logger.warning(
                        "ZIP %s: insufficient temp space for %d decompressed bytes",
                        dataset_id,
                        expanded_total_size,
                    )
                    backoff = min(120 * (2**self.request.retries), 600)
                    raise self.retry(
                        exc=RuntimeError("insufficient_temp_space (zip decompression)"),
                        countdown=int(backoff + random.uniform(0, backoff * 0.3)),
                    )

                def _update_partial_member_progress(member_snapshot: list[dict[str, object]]) -> None:
                    with engine.begin() as conn:
                        for member in member_snapshot:
                            conn.execute(
                                text(
                                    """
                                    UPDATE cached_datasets SET
                                        status = 'downloading',
                                        row_count = :rows,
                                        columns_json = :cols,
                                        size_bytes = :size,
                                        s3_key = :s3key,
                                        updated_at = NOW()
                                    WHERE table_name = :tn
                                    """
                                ),
                                {
                                    "rows": int(member.get("row_count", 0) or 0),
                                    "cols": json.dumps([str(c) for c in member.get("columns", [])]),
                                    "size": file_size,
                                    "s3key": s3_key,
                                    "tn": str(member["table_name"]),
                                },
                            )
                    _update_task_state_safe(
                        state="STARTED",
                        meta={
                            "dataset_id": dataset_id,
                            "stage": "materializing_members",
                            "member_tables": member_snapshot,
                        },
                    )

                zip_result = _parse_zip_archive(
                    zf,
                    zip_path=tmp_path,
                    dataset_id=dataset_id,
                    table_name=table_name,
                    engine=engine,
                    append_mode=append_mode,
                    member_names_override=expanded_member_names,
                    progress_callback=_update_partial_member_progress,
                    force_text_for_wide_csv=_is_heavy_execution(),
                )
                if zip_result.get("result") is not None:
                    result_payload = zip_result["result"]
                    if result_payload.get("error") == "zip_document_bundle":
                        _set_error_status(
                            engine,
                            dataset_id,
                            "zip_document_bundle",
                            table_name=table_name,
                        )
                    return result_payload

                if zip_result.get("parsed"):
                    table_name = zip_result["table_name"]
                    append_mode = zip_result["append_mode"]
                    row_count = zip_result["row_count"]
                    columns = zip_result["columns"]
                    sampled_note = zip_result["sampled_note"]
                    member_tables = list(zip_result.get("member_tables") or [])
                    if member_tables:
                        _update_task_state_safe(
                            state="STARTED",
                            meta={
                                "dataset_id": dataset_id,
                                "stage": "materialized_members",
                                "member_tables": member_tables,
                            },
                        )
                else:
                    logger.warning(
                        "ZIP %s: no parseable file found in %s",
                        dataset_id,
                        zf.namelist(),
                    )
                    _set_error_status(
                        engine, dataset_id, "zip_no_parseable_file", table_name=table_name
                    )
                    return {"error": "zip_no_parseable_file"}

            elif fmt == "json":
                file_size = os.path.getsize(tmp_path)
                if file_size >= _JSON_RECORD_MAP_STREAM_THRESHOLD_BYTES and _looks_like_json_record_map(
                    tmp_path
                ):
                    try:
                        row_count, columns, table_name, append_mode, streamed_sampled = (
                            _load_json_record_map_chunked(tmp_path, table_name, engine, dataset_id)
                        )
                    except RuntimeError as exc:
                        routed_status = str(exc).removeprefix("json_record_map_routed:")
                        if routed_status == "resource_table_full":
                            return {"dataset_id": dataset_id, "error": routed_status}
                        return {"dataset_id": dataset_id, "status": routed_status}
                    sampled_note = streamed_sampled or sampled_note
                else:
                    with open(tmp_path, encoding="utf-8-sig") as f:
                        try:
                            raw = json.load(f)
                        except json.JSONDecodeError:
                            f.seek(0)
                            df = pd.read_json(f)
                        else:
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
                        _collect_write_chunk(df, table_name, engine, write_schema=write_schema, source_url=download_url, if_exists="append", index=False)
                    else:
                        _collect_write_chunk(df, table_name, engine, write_schema=write_schema, source_url=download_url, if_exists="replace", index=False)
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
                    _collect_write_chunk(df, table_name, engine, write_schema=write_schema, source_url=download_url, if_exists="append", index=False)
                else:
                    _collect_write_chunk(df, table_name, engine, write_schema=write_schema, source_url=download_url, if_exists="replace", index=False)
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
                    _collect_write_chunk(df, table_name, engine, write_schema=write_schema, source_url=download_url, if_exists="append", index=False)
                else:
                    _collect_write_chunk(df, table_name, engine, write_schema=write_schema, source_url=download_url, if_exists="replace", index=False)
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
                    _collect_write_chunk(df, table_name, engine, write_schema=write_schema, source_url=download_url, if_exists="append", index=False)
                else:
                    _collect_write_chunk(df, table_name, engine, write_schema=write_schema, source_url=download_url, if_exists="replace", index=False)
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
                    _collect_write_chunk(df, table_name, engine, write_schema=write_schema, source_url=download_url, if_exists="append", index=False)
                else:
                    _collect_write_chunk(df, table_name, engine, write_schema=write_schema, source_url=download_url, if_exists="replace", index=False)
                row_count, columns = len(df), list(df.columns)

            else:
                logger.warning(f"Unsupported format: {fmt}")
                _set_error_status(
                    engine, dataset_id, f"unsupported_format: {fmt}", table_name=table_name
                )
                return {"error": f"unsupported_format: {fmt}"}

            if append_mode:
                # Append mode: update row_count from actual table, keep status 'ready'
                if member_tables:
                    # WS0 Modo 2 (per-member) — gate every ZIP child the same
                    # way as a single-file ingest. Members rejected by the
                    # post-parse detectors are marked permanently_failed
                    # instead of ready so the planner never sees a corrupt
                    # member table even when its sibling members succeeded.
                    rejected_members: dict[str, str] = {}
                    for _member in member_tables:
                        _member_table = str(_member["table_name"])
                        _member_columns = [str(c) for c in _member.get("columns", [])]
                        _member_rows = int(_member.get("row_count", 0) or 0)
                        _member_finding = _ws0_validate_post_parse(
                            engine,
                            dataset_id=dataset_id,
                            portal=portal,
                            source_id=source_id,
                            download_url=download_url,
                            declared_format=fmt,
                            table_name=_member_table,
                            materialized_columns=_member_columns,
                            materialized_row_count=_member_rows,
                            declared_size_bytes=file_size,
                            columns_json=json.dumps(_member_columns),
                        )
                        if _ws0_is_critical(_member_finding):
                            rejected_members[_member_table] = (
                                f"ingestion_validation_failed:{_member_finding.detector_name}"
                            )
                            logger.warning(
                                "ZIP member %s rejected post-parse by %s: %s",
                                _member_table,
                                _member_finding.detector_name,
                                _member_finding.message,
                            )
                    with engine.begin() as conn:
                        for member in member_tables:
                            member_table_name = str(member["table_name"])
                            if member_table_name in rejected_members:
                                conn.execute(
                                    text(
                                        "UPDATE cached_datasets "
                                        "SET status = 'permanently_failed', "
                                        "    error_message = :msg, "
                                        "    error_category = 'validation_failed', "
                                        "    updated_at = NOW() "
                                        "WHERE table_name = :tn"
                                    ),
                                    {
                                        "tn": member_table_name,
                                        "msg": rejected_members[member_table_name][:500],
                                    },
                                )
                                continue
                            member_row_count = int(member.get("row_count", 0) or 0)
                            member_columns_json = json.dumps(
                                [str(c) for c in member.get("columns", [])]
                            )
                            conn.execute(
                                text(
                                    """
                                    UPDATE cached_datasets SET
                                        status = 'ready',
                                        row_count = :rows,
                                        columns_json = :cols,
                                        size_bytes = :size,
                                        s3_key = :s3key,
                                        error_message = :sampled,
                                        updated_at = NOW()
                                    WHERE table_name = :tn
                                    """
                                ),
                                {
                                    "rows": member_row_count,
                                    "cols": member_columns_json,
                                    "size": file_size,
                                    "s3key": s3_key,
                                    "sampled": member.get("sampled_note"),
                                    "tn": member_table_name,
                                },
                            )
                    # Register each ZIP member in `raw_table_versions` so the
                    # Serving Port and mart auto-refresh can discover them.
                    # Each member becomes its own identity (sub-path = member
                    # table_name) so version stays at 1 — idempotent on
                    # `(resource_identity, version)`. Skip legacy paths
                    # (member tables in schema `public`, e.g. `cache_*`).
                    if _use_raw_layer():
                        for member in member_tables:
                            member_table_name = str(member["table_name"])
                            if member_table_name in rejected_members:
                                continue
                            if member_table_name.startswith("cache_"):
                                # Legacy public path; raw layer not in play.
                                continue
                            try:
                                _register_raw_version(
                                    engine,
                                    resource_identity=_resource_identity(
                                        portal, source_id, sub_path=member_table_name
                                    ),
                                    version=1,
                                    table_name=member_table_name,
                                    schema_name="raw",
                                    row_count=int(member.get("row_count", 0) or 0),
                                    size_bytes=file_size,
                                    source_url=download_url,
                                    parser_version=os.getenv("OPENARG_PARSER_VERSION") or None,
                                    collector_version=os.getenv("OPENARG_COLLECTOR_VERSION") or None,
                                )
                            except Exception:
                                logger.warning(
                                    "Could not register raw version for ZIP member %s "
                                    "(dataset=%s); table will be invisible to Serving Port",
                                    member_table_name,
                                    dataset_id,
                                    exc_info=True,
                                )
                    row_count = sum(int(member.get("row_count", 0) or 0) for member in member_tables)
                    sampled_note = next(
                        (
                            member.get("sampled_note")
                            for member in reversed(member_tables)
                            if member.get("sampled_note")
                        ),
                        sampled_note,
                    )
                else:
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
                    "member_tables": member_tables or None,
                }

            finalize_layout_profile = (
                (getattr(df, "attrs", {}) or {}).get("layout_profile")
                if "df" in locals()
                else (_LAYOUT_WIDE if len(columns) > _HEAVY_WIDTH_COLUMN_THRESHOLD else _LAYOUT_SIMPLE)
            )
            finalize_header_quality = (
                (getattr(df, "attrs", {}) or {}).get("header_quality")
                if "df" in locals()
                else _header_quality_label(columns)
            )
            finalize_result = _finalize_cached_dataset(
                engine,
                dataset_id=dataset_id,
                portal=portal,
                source_id=source_id,
                table_name=table_name,
                row_count=row_count,
                columns=[str(c) for c in columns],
                declared_format=fmt,
                download_url=download_url,
                declared_size_bytes=file_size,
                s3_key=s3_key,
                error_message=sampled_note,
                layout_profile=finalize_layout_profile,
                header_quality=finalize_header_quality,
                raw_schema=destination.schema if destination.schema != "public" else None,
                raw_version=destination.version if destination.version > 0 else None,
                resource_identity=destination.resource_identity,
                is_truncated=bool(sampled_note),
            )
            if not finalize_result["ok"]:
                return {"error": finalize_result["error"]}

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
            current_columns_json = json.dumps([str(c) for c in columns])
            should_refresh_embeddings = (
                (not was_cached)
                or previous_table_name != table_name
                or previous_row_count != row_count
                or previous_columns_json != current_columns_json
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
                "member_tables": member_tables or None,
            }

        finally:
            # Always clean up temp file
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            # Reset the medallion-target ContextVar so the NEXT task in
            # this worker process doesn't inherit the schema we set.
            try:
                _current_write_schema.reset(_write_schema_token)
            except (LookupError, NameError):
                pass

    except SoftTimeLimitExceeded:
        total_ms = int((time.monotonic() - started_at) * 1000)
        logger.error("Task timed out for dataset %s after %dms", dataset_id, total_ms)
        timeout_outcome = _CollectorRunOutcome(
            result_kind=_OUTCOME_RETRYABLE_UPSTREAM,
            cached_status="error",
            error_message="Task timed out",
            retry_increment=1,
        )
        attempts = _apply_cached_outcome(
            engine,
            dataset_id=dataset_id,
            table_name=table_name,
            outcome=timeout_outcome,
        )
        if attempts >= MAX_TOTAL_ATTEMPTS:
            return {"error": "permanently_failed", "attempts": attempts}
        backoff = min(60 * (2**self.request.retries), 600)  # 60s, 120s, 240s, capped at 600s
        jitter = random.uniform(0, backoff * 0.3)
        raise self.retry(countdown=int(backoff + jitter))
    except Exception as exc:
        current_queue = _current_queue()
        outcome = _classify_collect_exception(
            exc,
            heavy_execution=_is_heavy_execution(),
            current_queue=current_queue,
        )
        if outcome.cached_status == "permanently_failed":
            total_ms = int((time.monotonic() - started_at) * 1000)
            logger.warning(
                "Non-retryable error for %s after %dms: %s",
                dataset_id,
                total_ms,
                str(exc)[:200],
            )
            _set_error_status(engine, dataset_id, outcome.error_message or str(exc)[:500], table_name=table_name)
            return {"error": "non_retryable"}

        if outcome.cached_status == "pending" and outcome.result_kind == _OUTCOME_RETRYABLE_UPSTREAM:
            backoff = min(60 * (2**self.request.retries), 600)
            jitter = random.uniform(0, backoff * 0.3)
            countdown = int(backoff + jitter)
            attempts = _apply_cached_outcome(
                engine,
                dataset_id=dataset_id,
                table_name=table_name,
                outcome=outcome,
            )
            if attempts >= MAX_TOTAL_ATTEMPTS:
                logger.warning(
                    "Dataset %s permanently failed after %s attempts: %s",
                    dataset_id,
                    attempts,
                    exc,
                )
                return {"error": "permanently_failed", "attempts": attempts}

            return _reroute_to_queue(
                f"transient_retry:{type(exc).__name__}",
                queue=_HEAVY_RETRY_QUEUE,
                countdown=countdown,
            )

        total_ms = int((time.monotonic() - started_at) * 1000)
        logger.exception("Collection failed for dataset %s after %dms", dataset_id, total_ms)
        attempts = _apply_cached_outcome(
            engine,
            dataset_id=dataset_id,
            table_name=table_name,
            outcome=outcome,
        )
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


@celery_app.task(
    name="openarg.bulk_collect_all",
    bind=True,
    soft_time_limit=_BULK_COLLECT_SOFT_TIME_LIMIT,
    time_limit=_BULK_COLLECT_TIME_LIMIT,
)
def bulk_collect_all(self, portal: str | None = None, chain_depth: int = 0):
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
                "Bulk collect reconciled cache coverage before dispatch: "
                "orphaned_ready=%s fixed_cached_flags=%s "
                "reindexed_missing_chunks=%s revived_missing_table=%s",
                reconciled["orphaned_ready"],
                reconciled["fixed_cached_flags"],
                reconciled["reindexed_missing_chunks"],
                reconciled.get("revived_missing_table", 0),
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
        # Excludes `downloading` so the dispatcher does NOT re-dispatch a
        # dataset whose previous task is still in flight. Stuck downloads
        # are unstuck by `_recycle_stuck_downloads` (30-min stale sweep),
        # which moves them to `error`/`permanently_failed` — at that point
        # the row becomes eligible again.
        #
        # Fairness: the outer `ORDER BY rn, portal` interleaves rows so the
        # LIMIT N batch always carries representation from every portal.
        # The previous `ORDER BY portal, title` starved alphabetically-late
        # portals (mendoza, tucumán, etc.) when alphabetically-early portals
        # had thousands of pending datasets — they never reached the LIMIT
        # window. With round-robin, a 5000-row batch from 16 portals takes
        # ~313 datasets per portal off the top before going to a second pass.
        # Failure budget: portals whose 24h failure rate >= 50% are
        # excluded from the dispatch SELECT. Continuing to retry those
        # datasets wastes slots that could process portals that actually
        # complete. The exclusion is *self-resolving* — when the rate
        # drops back below 50% (because new attempts succeed or the
        # 24-hour window slides past the failures), the portal becomes
        # eligible again on the next bulk run, no manual intervention.
        # Threshold is 2× failed >= total because the SQL does
        # `failed * 2 >= total`, which avoids a division and the
        # zero-divide edge case for portals with 0 history.
        query = (
            "WITH burned_portals AS ( "
            "  SELECT d2.portal "
            "  FROM datasets d2 "
            "  JOIN cached_datasets cd2 ON cd2.dataset_id = d2.id "
            "  WHERE cd2.updated_at > now() - interval '24 hour' "
            "  GROUP BY d2.portal "
            "  HAVING COUNT(*) >= 20 "
            "    AND COUNT(*) FILTER (WHERE cd2.status='permanently_failed') * 2 >= COUNT(*) "
            "), "
            "eligible AS ( "
            "  SELECT CAST(d.id AS text) AS id, d.portal, d.format, d.title "
            "  FROM datasets d "
            "  WHERE d.is_cached = false "
            "    AND d.portal NOT IN (SELECT portal FROM burned_portals) "
            "    AND NOT EXISTS ( "
            "      SELECT 1 FROM cached_datasets cd "
            "      WHERE cd.dataset_id = d.id "
            "        AND cd.status IN ('ready', 'permanently_failed', 'downloading') "
            "    ) "
            "    AND d.title NOT IN ( "
            "      SELECT d2.title FROM datasets d2 "
            "      WHERE d2.portal = d.portal "
            "        AND d2.is_cached = false "
            "        AND NOT EXISTS ( "
            "          SELECT 1 FROM cached_datasets cd2 "
            "          WHERE cd2.dataset_id = d2.id "
            "            AND cd2.status IN ('ready', 'permanently_failed', 'downloading') "
            "        ) "
            "      GROUP BY d2.title, d2.portal "
            "      HAVING COUNT(*) > 50 "
            "    ) "
            "{portal_clause}"
            ") "
            "SELECT id, portal, format, title FROM ( "
            "  SELECT id, portal, format, title, "
            "         ROW_NUMBER() OVER (PARTITION BY portal ORDER BY title) AS rn "
            "  FROM eligible "
            ") sub "
            "ORDER BY rn, portal "
            "LIMIT 5000"
        )
        params: dict = {}
        portal_clause = ""
        if portal:
            portal_clause = "    AND d.portal = :portal "
            params["portal"] = portal
        query = query.replace("{portal_clause}", portal_clause)

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
        # Heavy overflow: if the collector queue is already loaded, half of
        # the new wave is routed to the heavy worker. The cutoff threshold
        # keeps the heavy worker reserved for genuinely heavy datasets when
        # the normal queue is healthy; it only kicks in once the normal
        # queue is past `_COLLECT_HEAVY_OVERFLOW_THRESHOLD` inflight tasks.
        _heavy_overflow_active = (
            inflight_total >= _COLLECT_HEAVY_OVERFLOW_THRESHOLD
        )
        tasks = [
            _collect_dataset_signature(
                str(_row_value(row, "id", 0)),
                portal=str(_row_value(row, "portal", 1)),
                fmt=str(_row_value(row, "format", 2) or ""),
                title=str(_row_value(row, "title", 3) or ""),
                # Stripe overflow across both queues so neither stays idle.
                heavy_overflow=(_heavy_overflow_active and (idx % 2 == 1)),
            )
            for idx, row in enumerate(selected_rows)
        ]
        if _heavy_overflow_active:
            logger.info(
                "Heavy overflow active: collector inflight=%d ≥ threshold=%d; "
                "striping %d/%d new tasks to %s",
                inflight_total,
                _COLLECT_HEAVY_OVERFLOW_THRESHOLD,
                len(selected_rows) // 2,
                len(selected_rows),
                _HEAVY_COLLECT_QUEUE,
            )
        phase1_batches = 0
        if tasks:
            phase1_batches = _dispatch_in_batches(
                tasks,
                batch_size=_COLLECT_DISPATCH_BATCH_SIZE,
                step_seconds=_COLLECT_DISPATCH_STEP_SECONDS,
            )

        # ── Phase 2: large groups (>50 siblings) ──
        # Round-robin by portal mirrors Phase 1 fairness; failure budget
        # mirrors Phase 1 too — burned portals are excluded so we don't
        # consume slots on groups that systematically fail.
        large_query = (
            "WITH burned_portals AS ( "
            "  SELECT d2.portal "
            "  FROM datasets d2 "
            "  JOIN cached_datasets cd2 ON cd2.dataset_id = d2.id "
            "  WHERE cd2.updated_at > now() - interval '24 hour' "
            "  GROUP BY d2.portal "
            "  HAVING COUNT(*) >= 20 "
            "    AND COUNT(*) FILTER (WHERE cd2.status='permanently_failed') * 2 >= COUNT(*) "
            "), "
            "grouped AS ( "
            "  SELECT d.title, d.portal, COUNT(*) AS cnt FROM datasets d "
            "  WHERE d.is_cached = false "
            "    AND d.portal NOT IN (SELECT portal FROM burned_portals) "
            "    AND NOT EXISTS ( "
            "      SELECT 1 FROM cached_datasets cd "
            "      WHERE cd.dataset_id = d.id "
            "        AND cd.status IN ('ready', 'permanently_failed', 'downloading') "
            "    ) "
            "{portal_clause}"
            "  GROUP BY d.title, d.portal "
            "  HAVING COUNT(*) > 50 "
            ") "
            "SELECT title, portal, cnt FROM ( "
            "  SELECT title, portal, cnt, "
            "         ROW_NUMBER() OVER (PARTITION BY portal ORDER BY cnt ASC) AS rn "
            "  FROM grouped "
            ") sub "
            "ORDER BY rn, portal "
            "LIMIT 100"
        )
        large_params: dict[str, object] = {}
        large_portal_clause = ""
        if portal:
            large_portal_clause = "    AND d.portal = :portal "
            large_params["portal"] = portal
        large_query = large_query.replace("{portal_clause}", large_portal_clause)
        with engine.connect() as conn:
            large_groups = conn.execute(text(large_query), large_params).fetchall()

        remaining_total_slots = max(_COLLECT_MAX_INFLIGHT - inflight_total - phase1_count, 0)
        remaining_portal_slots = {
            portal_key: max(
                _COLLECT_MAX_INFLIGHT_PER_PORTAL
                - inflight_by_portal.get(portal_key, 0)
                - sum(
                    1
                    for row in selected_rows
                    if str(_row_value(row, "portal", 1)) == portal_key
                ),
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

        immediate_continue = (
            phase1_count > 0
            or phase2_count > 0
            or deferred_individual > 0
            or deferred_groups > 0
            or inflight_total > 0
        )
        remaining = {"eligible_individual": 0, "eligible_groups": 0}
        should_continue = immediate_continue
        if not immediate_continue:
            remaining = _count_bulk_collect_remaining(engine, portal=portal)
            should_continue = (
                remaining["eligible_individual"] > 0
                or remaining["eligible_groups"] > 0
            )

        followup_scheduled = False
        converged = False
        if should_continue and chain_depth < _BULK_COLLECT_MAX_CHAIN_DEPTH:
            bulk_collect_all.apply_async(
                kwargs={"portal": portal, "chain_depth": chain_depth + 1},
                countdown=_BULK_COLLECT_RETRY_DELAY_SECONDS,
            )
            followup_scheduled = True
        elif should_continue and chain_depth >= _BULK_COLLECT_MAX_CHAIN_DEPTH:
            # The previous behaviour silently dropped the remaining work
            # (just logged a warning). Under load the chain hits 48 with
            # thousands of pending datasets — those would never be picked
            # up unless an operator manually re-triggered. Restart the
            # chain at depth 0 with a longer countdown so the loop is
            # guaranteed to converge eventually.
            logger.warning(
                "Bulk collect reached max chain depth=%s with work remaining "
                "(eligible_individual=%s eligible_groups=%s deferred_individual=%s "
                "deferred_groups=%s inflight_total=%s); resetting chain to 0",
                chain_depth,
                remaining["eligible_individual"],
                remaining["eligible_groups"],
                deferred_individual,
                deferred_groups,
                inflight_total,
            )
            bulk_collect_all.apply_async(
                kwargs={"portal": portal, "chain_depth": 0},
                countdown=300,  # 5min cooldown before resetting
            )
            followup_scheduled = True
        else:
            reconcile_cache_coverage.apply_async(countdown=60)
            from app.infrastructure.celery.tasks.catalog_backfill import catalog_backfill_task

            catalog_backfill_task.apply_async(countdown=180)
            converged = True

        logger.info(
            "Bulk collect: inflight_total=%s phase1=%s individual (%s batches, %s deferred) "
            "phase2=%s large groups (%s batches, %s deferred) remaining_individual=%s "
            "remaining_groups=%s followup_scheduled=%s converged=%s chain_depth=%s",
            inflight_total,
            phase1_count,
            phase1_batches,
            deferred_individual,
            phase2_count,
            phase2_batches,
            deferred_groups,
            remaining["eligible_individual"],
            remaining["eligible_groups"],
            followup_scheduled,
            converged,
            chain_depth,
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
            "remaining_eligible_individual": remaining["eligible_individual"],
            "remaining_eligible_groups": remaining["eligible_groups"],
            "followup_scheduled": followup_scheduled,
            "converged": converged,
            "chain_depth": chain_depth,
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
                    "SELECT CAST(d.id AS text) as id, d.format, d.download_url, d.title "
                    "FROM datasets d "
                    "WHERE d.title = :title AND d.portal = :portal AND d.is_cached = false "
                    "AND NOT EXISTS ("
                    "  SELECT 1 FROM cached_datasets cd "
                    "  WHERE cd.dataset_id = d.id "
                    "    AND cd.status IN ('ready', 'permanently_failed', 'downloading')"
                    ") "
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
        tasks = [
            _collect_dataset_signature(
                did,
                portal=portal,
                fmt=fmt,
                title=title,
            )
            for did, fmt, _url in deduped[:200]
        ]
        dispatched_batches = 0
        if tasks:
            dispatched_batches = _dispatch_in_batches(
                tasks,
                batch_size=_COLLECT_DISPATCH_BATCH_SIZE,
                step_seconds=_COLLECT_DISPATCH_STEP_SECONDS,
            )

        deduped_ids = {did for did, _, _ in deduped}
        format_dupes = [r.id for r in resources if r.id not in deduped_ids]

        consolidation_scheduled = False
        alias_materialization_scheduled = False
        dispatched_count = min(len(deduped), 200)
        if dispatched_count > 1:
            consolidate_group_tables.apply_async(
                args=[title, portal],
                countdown=(dispatched_batches * _COLLECT_DISPATCH_STEP_SECONDS) + 60,
            )
            consolidation_scheduled = True
        if format_dupes:
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
                        "WHERE table_name = :tn "
                        "AND table_schema IN ('public', 'raw', 'staging', 'mart'))"
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
