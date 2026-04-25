from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text

from app.infrastructure.celery.tasks._db import get_sync_engine
from app.presentation.http.controllers.admin.tasks_router import verify_admin_key

router = APIRouter(prefix="/admin", tags=["admin-monitoring"])


def _table_exists(table_name: str) -> bool:
    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            return bool(
                conn.execute(
                    text(
                        "SELECT EXISTS ("
                        "  SELECT 1 FROM information_schema.tables "
                        "  WHERE table_schema = 'public' AND table_name = :tn"
                        ")"
                    ),
                    {"tn": table_name},
                ).scalar()
            )
    finally:
        engine.dispose()


@router.get("/findings", dependencies=[Depends(verify_admin_key)])
async def get_validation_findings(limit: int = 50):
    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT f.resource_id,
                           f.detector_name,
                           f.detector_version,
                           f.severity,
                           f.mode,
                           f.should_redownload,
                           f.message,
                           f.found_at,
                           f.resolved_at,
                           d.portal,
                           d.source_id
                    FROM ingestion_findings f
                    LEFT JOIN datasets d ON CAST(d.id AS text) = f.resource_id
                    ORDER BY f.found_at DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).fetchall()
        return {
            "items": [dict(row._mapping) for row in rows],
            "limit": limit,
        }
    finally:
        engine.dispose()


@router.get("/state-violations", dependencies=[Depends(verify_admin_key)])
async def get_state_violations(limit: int = 50):
    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT f.resource_id,
                           f.detector_name AS violation_kind,
                           f.severity,
                           f.message,
                           f.found_at,
                           f.resolved_at,
                           d.portal,
                           d.source_id
                    FROM ingestion_findings f
                    LEFT JOIN datasets d ON CAST(d.id AS text) = f.resource_id
                    WHERE f.mode = 'state_invariant'
                    ORDER BY f.found_at DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).fetchall()
        return {
            "items": [dict(row._mapping) for row in rows],
            "limit": limit,
        }
    finally:
        engine.dispose()


@router.get("/cache-drops", dependencies=[Depends(verify_admin_key)])
@router.get("/cache_drops", dependencies=[Depends(verify_admin_key)])
async def get_cache_drops(limit: int = 50):
    if not _table_exists("cache_drop_audit"):
        return {"items": [], "available": False, "limit": limit}

    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT object_name,
                           session_user_name,
                           current_user_name,
                           client_addr,
                           application_name,
                           query,
                           dropped_at
                    FROM cache_drop_audit
                    ORDER BY dropped_at DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).fetchall()
        return {
            "items": [dict(row._mapping) for row in rows],
            "available": True,
            "limit": limit,
        }
    finally:
        engine.dispose()


@router.get("/portal-health", dependencies=[Depends(verify_admin_key)])
@router.get("/portal_health", dependencies=[Depends(verify_admin_key)])
async def get_portal_health(limit: int = 100):
    if not _table_exists("portals"):
        return {"items": [], "available": False, "limit": limit}

    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT portal,
                           host,
                           is_down,
                           last_status,
                           last_check,
                           consecutive_failures,
                           last_error
                    FROM portals
                    ORDER BY is_down DESC, consecutive_failures DESC, portal ASC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).fetchall()
        return {
            "items": [dict(row._mapping) for row in rows],
            "available": True,
            "limit": limit,
        }
    finally:
        engine.dispose()
