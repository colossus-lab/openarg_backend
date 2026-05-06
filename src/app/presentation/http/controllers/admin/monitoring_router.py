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


@router.get("/throughput", dependencies=[Depends(verify_admin_key)])
@router.get("/throughput/", dependencies=[Depends(verify_admin_key)])
async def get_throughput(hours: int = 14):
    """Hourly ingest throughput for the last N hours.

    Each row reports:
      - `hour`         — bucket start (UTC)
      - `ready`        — datasets that landed `ready` in that hour
      - `failed`       — datasets that landed `permanently_failed`
      - `rate_per_min` — `ready / 60` (sanity check)
    Plus a `summary` block with rolling 5/15/60min and 24h totals so a
    single GET is enough for an operational dashboard.
    """
    hours = max(1, min(int(hours), 72))
    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            hourly = conn.execute(
                text(
                    """
                    SELECT date_trunc('hour', updated_at) AS hour,
                           COUNT(*) FILTER (WHERE status='ready')               AS ready,
                           COUNT(*) FILTER (WHERE status='permanently_failed')  AS failed
                    FROM cached_datasets
                    WHERE updated_at > now() - make_interval(hours => :hours)
                    GROUP BY 1
                    ORDER BY 1 DESC
                    """
                ),
                {"hours": hours},
            ).fetchall()
            summary = conn.execute(
                text(
                    """
                    SELECT
                      (SELECT COUNT(*) FROM cached_datasets WHERE status='ready' AND updated_at > now() - interval '5 min')  AS rate_5min,
                      (SELECT COUNT(*) FROM cached_datasets WHERE status='ready' AND updated_at > now() - interval '15 min') AS rate_15min,
                      (SELECT COUNT(*) FROM cached_datasets WHERE status='ready' AND updated_at > now() - interval '60 min') AS rate_1h,
                      (SELECT COUNT(*) FROM cached_datasets WHERE status='ready' AND updated_at > now() - interval '24 hour') AS rate_24h,
                      (SELECT COUNT(*) FROM cached_datasets WHERE status='ready')                                            AS cd_ready,
                      (SELECT COUNT(*) FROM cached_datasets WHERE status='downloading')                                      AS cd_dling,
                      (SELECT COUNT(*) FROM cached_datasets WHERE status='permanently_failed')                               AS cd_perm_failed,
                      (SELECT COUNT(*) FROM datasets d
                         WHERE d.is_cached=false AND NOT EXISTS (
                             SELECT 1 FROM cached_datasets cd
                             WHERE cd.dataset_id=d.id AND cd.status IN ('ready','permanently_failed','downloading')
                         ))                                                                                                  AS eligible_pending
                    """
                )
            ).first()
        return {
            "hourly": [dict(row._mapping) for row in hourly],
            "summary": dict(summary._mapping) if summary else {},
            "horizon_hours": hours,
        }
    finally:
        engine.dispose()


@router.get("/throughput/health", dependencies=[Depends(verify_admin_key)])
async def get_throughput_health():
    """Cheap healthcheck for ingest velocity. Returns:
      - `status`: "ok" | "degraded" | "stalled"
      - `rate_15min`: datasets/min sustained over last 15 min
      - `last_ready_seconds_ago`: time since the most recent ready

    Thresholds (tunable later):
      - rate_15min < 30 over 15min   → degraded
      - last_ready > 300s            → stalled
    """
    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                      (SELECT COUNT(*) FROM cached_datasets WHERE status='ready' AND updated_at > now() - interval '15 min') AS rate_15min,
                      (SELECT EXTRACT(epoch FROM (now() - MAX(updated_at))) FROM cached_datasets WHERE status='ready') AS seconds_since_last
                    """
                )
            ).first()
        rate_15min = int(row.rate_15min or 0)
        secs = int(row.seconds_since_last or 0)
        status = "ok"
        if secs > 300:
            status = "stalled"
        elif rate_15min < 30:
            status = "degraded"
        return {
            "status": status,
            "rate_15min": rate_15min,
            "last_ready_seconds_ago": secs,
        }
    finally:
        engine.dispose()
