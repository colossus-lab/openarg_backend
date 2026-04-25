"""Modo 3 of WS0 — retrospective sweep.

Walks `cached_datasets` in batches and runs the full detector suite against
each row using only the metadata that's already available (no re-download).
Persists findings + (optionally, behind feature flag) flips
`materialization_status` to `materialization_corrupted`.

Default cadence: every 6h (see celery_app.beat_schedule).
Soft-mode default for first week in prod: register findings, do NOT auto-flip.
"""

from __future__ import annotations

import json
import logging
import os

from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.application.validation.collector_hooks import (
    soft_flip_enabled,
    validate_retrospective,
)
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


def _batch_size() -> int:
    try:
        return int(os.getenv("OPENARG_SWEEP_BATCH_SIZE", "500"))
    except ValueError:
        return 500


def _portal_filter() -> list[str] | None:
    raw = os.getenv("OPENARG_SWEEP_PORTALS", "").strip()
    if not raw:
        return None
    return [p.strip() for p in raw.split(",") if p.strip()]


def _load_batch(engine, *, offset: int, limit: int, portals: list[str] | None) -> list[dict]:
    sql = (
        "SELECT cd.dataset_id::text AS dataset_id, "
        "       cd.table_name, "
        "       cd.row_count, "
        "       cd.size_bytes, "
        "       cd.columns_json, "
        "       cd.status, "
        "       d.portal, "
        "       d.source_id, "
        "       d.download_url, "
        "       d.format "
        "FROM cached_datasets cd "
        "JOIN datasets d ON d.id = cd.dataset_id "
        "WHERE cd.status IN ('ready','error') "
    )
    params: dict[str, object] = {"limit": limit, "offset": offset}
    if portals:
        sql += "  AND d.portal = ANY(:portals) "
        params["portals"] = portals
    sql += "ORDER BY cd.updated_at DESC NULLS LAST LIMIT :limit OFFSET :offset"
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()]


def _materialized_columns(engine, table_name: str) -> list[str]:
    if not table_name:
        return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = :tn AND table_schema = 'public' "
                    "ORDER BY ordinal_position"
                ),
                {"tn": table_name},
            ).fetchall()
            return [r.column_name for r in rows]
    except Exception:
        logger.debug("Could not introspect columns for %s", table_name, exc_info=True)
        return []


def _materialized_row_count(engine, table_name: str) -> int | None:
    if not table_name:
        return None
    try:
        with engine.connect() as conn:
            res = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))  # noqa: S608
            return int(res.scalar() or 0)
    except Exception:
        return None


def _maybe_flip_status(engine, dataset_id: str, table_name: str, has_critical: bool) -> None:
    """When auto-flip is enabled and there's a critical finding, flag the row.

    We don't have `materialization_status` on `cached_datasets` (it lives on
    the future `catalog_resources`). For now we mark `error_message` so the
    discovery side can deprioritize the resource. Once WS2 lands, this
    becomes an UPDATE of `catalog_resources.materialization_status` to
    `materialization_corrupted`.
    """
    if not has_critical or not soft_flip_enabled():
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE cached_datasets "
                    "SET error_message = COALESCE(error_message,'') || "
                    "    CASE WHEN POSITION('materialization_corrupted' IN COALESCE(error_message,'')) > 0 "
                    "         THEN '' ELSE ' | materialization_corrupted' END, "
                    "    updated_at = NOW() "
                    "WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"did": dataset_id},
            )
    except Exception:
        logger.exception(
            "Failed to flip materialization_status for %s (%s)", dataset_id, table_name
        )


@celery_app.task(
    name="openarg.ws0_retrospective_sweep",
    bind=True,
    soft_time_limit=600,
    time_limit=720,
)
def retrospective_sweep(self, *, max_batches: int | None = None) -> dict:
    """Sweep through cached_datasets and persist findings.

    `max_batches` lets ad-hoc dispatchers cap the run; when omitted, the
    sweep runs to completion (or hits the soft time limit).
    """
    engine = get_sync_engine()
    portals = _portal_filter()
    batch_size = _batch_size()

    total_scanned = 0
    total_findings = 0
    total_critical = 0
    batch_idx = 0
    offset = 0

    try:
        while True:
            batch = _load_batch(engine, offset=offset, limit=batch_size, portals=portals)
            if not batch:
                break
            for row in batch:
                cols_real = _materialized_columns(engine, row["table_name"])
                rows_real = _materialized_row_count(engine, row["table_name"]) if cols_real else None
                findings = validate_retrospective(
                    engine,
                    dataset_id=row["dataset_id"],
                    portal=row["portal"],
                    source_id=row["source_id"],
                    download_url=row["download_url"],
                    declared_format=row["format"],
                    table_name=row["table_name"],
                    materialized_columns=cols_real or None,
                    materialized_row_count=rows_real,
                    declared_size_bytes=row["size_bytes"] or 0,
                    declared_row_count=row["row_count"] or 0,
                    columns_json=row["columns_json"],
                )
                total_scanned += 1
                total_findings += len(findings)
                has_critical = any(f.severity.value == "critical" for f in findings)
                if has_critical:
                    total_critical += 1
                _maybe_flip_status(engine, row["dataset_id"], row["table_name"], has_critical)
            offset += batch_size
            batch_idx += 1
            if max_batches is not None and batch_idx >= max_batches:
                break
    except SoftTimeLimitExceeded:
        logger.warning(
            "ws0 sweep hit soft time limit at batch %d (scanned=%d)", batch_idx, total_scanned
        )

    summary = {
        "scanned": total_scanned,
        "findings_persisted": total_findings,
        "critical_resources": total_critical,
        "batches": batch_idx,
        "auto_flip_enabled": soft_flip_enabled(),
        "portals_filter": portals,
    }
    logger.info("ws0 retrospective sweep done: %s", json.dumps(summary))
    return summary
