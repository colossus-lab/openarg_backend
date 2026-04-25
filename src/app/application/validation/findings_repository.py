"""Persistence for ingestion findings.

Idempotent: dos corridas consecutivas del mismo detector + version + mode
contra el mismo resource y mismo input_hash NO crean filas duplicadas —
actualizan `found_at`.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.application.validation.detector import Finding, Mode, ResourceContext

logger = logging.getLogger(__name__)


_UPSERT_SQL = text(
    """
    INSERT INTO ingestion_findings (
        resource_id, detector_name, detector_version, severity, mode,
        payload, should_redownload, message, input_hash, found_at
    ) VALUES (
        :resource_id, :detector_name, :detector_version, :severity, :mode,
        CAST(:payload AS JSONB), :should_redownload, :message, :input_hash, NOW()
    )
    ON CONFLICT (resource_id, detector_name, detector_version, mode, input_hash)
    DO UPDATE SET
        severity = EXCLUDED.severity,
        payload = EXCLUDED.payload,
        should_redownload = EXCLUDED.should_redownload,
        message = EXCLUDED.message,
        found_at = NOW(),
        resolved_at = NULL
    """
)


def persist_findings(
    engine: Engine,
    ctx: ResourceContext,
    findings: Iterable[Finding],
    *,
    input_hash: str,
) -> int:
    """Upsert findings. Returns count persisted."""
    rows = [
        {
            "resource_id": ctx.resource_id,
            "detector_name": f.detector_name,
            "detector_version": f.detector_version,
            "severity": f.severity.value,
            "mode": f.mode.value,
            "payload": json.dumps(f.payload, default=str),
            "should_redownload": f.should_redownload,
            "message": f.message[:1000] if f.message else "",
            "input_hash": input_hash,
        }
        for f in findings
    ]
    if not rows:
        return 0
    try:
        with engine.begin() as conn:
            for row in rows:
                conn.execute(_UPSERT_SQL, row)
    except Exception:
        logger.exception(
            "Failed to persist %d findings for resource %s", len(rows), ctx.resource_id
        )
        return 0
    return len(rows)


def mark_resolved(engine: Engine, resource_id: str, *, detector_name: str | None = None) -> int:
    """Mark findings as resolved (e.g. after a successful re-materialization).

    If `detector_name` is given, only that detector's open findings are
    closed; otherwise all open findings for the resource are closed.
    """
    sql = (
        "UPDATE ingestion_findings SET resolved_at = NOW() "
        "WHERE resource_id = :rid AND resolved_at IS NULL"
    )
    params: dict[str, object] = {"rid": resource_id}
    if detector_name:
        sql += " AND detector_name = :dn"
        params["dn"] = detector_name
    try:
        with engine.begin() as conn:
            res = conn.execute(text(sql), params)
            return int(res.rowcount or 0)
    except Exception:
        logger.exception("Failed to mark findings resolved for %s", resource_id)
        return 0


def open_findings_for(
    engine: Engine,
    resource_id: str,
    *,
    severity: str | None = None,
    mode: Mode | None = None,
) -> list[dict]:
    """List currently-open findings for a resource (resolved_at IS NULL)."""
    sql = (
        "SELECT detector_name, detector_version, severity, mode, payload, "
        "       should_redownload, message, found_at "
        "FROM ingestion_findings "
        "WHERE resource_id = :rid AND resolved_at IS NULL"
    )
    params: dict[str, object] = {"rid": resource_id}
    if severity:
        sql += " AND severity = :sev"
        params["sev"] = severity
    if mode:
        sql += " AND mode = :mode"
        params["mode"] = mode.value
    sql += " ORDER BY found_at DESC"
    try:
        with engine.connect() as conn:
            return [dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()]
    except Exception:
        logger.exception("Failed to list findings for %s", resource_id)
        return []
