from __future__ import annotations

import json

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Query
from pydantic import BaseModel
from sqlalchemy import text

from app.infrastructure.persistence_sqla.provider import MainAsyncSession

router = APIRouter(prefix="/transparency", tags=["transparency"])


def _safe_json_loads(raw: str | None) -> dict[str, int]:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class PortalHealthSummary(BaseModel):
    portal: str
    dataset_count: int
    avg_score: float
    fresh_count: int
    stale_count: int
    abandoned_count: int
    unknown_count: int = 0


class DatasetHealthDetail(BaseModel):
    dataset_id: str
    title: str
    portal: str
    format: str | None
    overall_score: float
    freshness_score: float
    freshness_status: str
    accessibility_score: float
    machine_readability_score: float
    completeness_score: float
    contactability_score: float


class GhostDataset(BaseModel):
    dataset_id: str
    title: str
    portal: str
    organization: str | None
    format: str | None
    freshness_status: str
    overall_score: float
    last_updated: str | None


class DdjjAnomaly(BaseModel):
    nombre: str
    cuit: str
    anio_declaracion: str
    patrimonio_cierre: float
    bienes_inicio: float
    bienes_cierre: float
    ingresos_trabajo_neto: float
    variacion_patrimonial: float
    brecha_inexplicable: float
    ratio_crecimiento: float
    anomaly_type: str | None
    severity: str | None


class DdjjAnomalySummary(BaseModel):
    total_analyzed: int
    total_flagged: int
    by_severity: dict[str, int]
    by_type: dict[str, int]
    anomalies: list[DdjjAnomaly]


class SessionTopicEntry(BaseModel):
    periodo: int
    reunion: int
    fecha: str | None
    tipo_sesion: str | None
    topic_name: str
    topic_category: str
    mention_count: int
    relevance_score: float
    top_speakers: dict[str, int]


class SessionTopicSummary(BaseModel):
    sessions_analyzed: int
    topic_distribution: dict[str, int]
    topics_by_session: list[SessionTopicEntry]


# ---------------------------------------------------------------------------
# Feature 1: Portal Health Index
# ---------------------------------------------------------------------------

@router.get("/health", response_model=list[PortalHealthSummary])
@inject
async def get_portal_health(
    session: FromDishka[MainAsyncSession],
) -> list[PortalHealthSummary]:
    """Resumen de salud por portal: score promedio, datasets frescos/rancios/abandonados."""
    result = await session.execute(text("""
        SELECT
            portal,
            COUNT(*) AS dataset_count,
            COALESCE(ROUND(AVG(overall_score)::numeric, 3), 0) AS avg_score,
            COALESCE(SUM(CASE WHEN freshness_status = 'FRESH' THEN 1 ELSE 0 END), 0) AS fresh_count,
            COALESCE(SUM(CASE WHEN freshness_status = 'STALE' THEN 1 ELSE 0 END), 0) AS stale_count,
            COALESCE(SUM(CASE WHEN freshness_status = 'ABANDONED' THEN 1 ELSE 0 END), 0) AS abandoned_count,
            COALESCE(SUM(CASE WHEN freshness_status = 'unknown' THEN 1 ELSE 0 END), 0) AS unknown_count
        FROM dataset_health_scores
        GROUP BY portal
        ORDER BY avg_score DESC
    """))
    return [
        PortalHealthSummary(
            portal=row.portal,
            dataset_count=row.dataset_count,
            avg_score=float(row.avg_score),
            fresh_count=row.fresh_count,
            stale_count=row.stale_count,
            abandoned_count=row.abandoned_count,
            unknown_count=row.unknown_count,
        )
        for row in result.fetchall()
    ]


@router.get("/health/{portal}", response_model=list[DatasetHealthDetail])
@inject
async def get_portal_datasets_health(
    portal: str,
    session: FromDishka[MainAsyncSession],
    sort_by: str = Query(default="overall_score", pattern="^(overall_score|freshness_score|completeness_score)$"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[DatasetHealthDetail]:
    """Detalle de salud por dataset dentro de un portal."""
    _sort_queries = {
        "overall_score": (
            "SELECT CAST(h.dataset_id AS text) AS dataset_id, d.title, d.portal, d.format,"
            " h.overall_score, h.freshness_score, h.freshness_status,"
            " h.accessibility_score, h.machine_readability_score,"
            " h.completeness_score, h.contactability_score"
            " FROM dataset_health_scores h"
            " JOIN datasets d ON d.id = h.dataset_id"
            " WHERE h.portal = :portal"
            " ORDER BY h.overall_score DESC"
            " LIMIT :limit OFFSET :offset"
        ),
        "freshness_score": (
            "SELECT CAST(h.dataset_id AS text) AS dataset_id, d.title, d.portal, d.format,"
            " h.overall_score, h.freshness_score, h.freshness_status,"
            " h.accessibility_score, h.machine_readability_score,"
            " h.completeness_score, h.contactability_score"
            " FROM dataset_health_scores h"
            " JOIN datasets d ON d.id = h.dataset_id"
            " WHERE h.portal = :portal"
            " ORDER BY h.freshness_score DESC"
            " LIMIT :limit OFFSET :offset"
        ),
        "completeness_score": (
            "SELECT CAST(h.dataset_id AS text) AS dataset_id, d.title, d.portal, d.format,"
            " h.overall_score, h.freshness_score, h.freshness_status,"
            " h.accessibility_score, h.machine_readability_score,"
            " h.completeness_score, h.contactability_score"
            " FROM dataset_health_scores h"
            " JOIN datasets d ON d.id = h.dataset_id"
            " WHERE h.portal = :portal"
            " ORDER BY h.completeness_score DESC"
            " LIMIT :limit OFFSET :offset"
        ),
    }
    safe_sort = sort_by if sort_by in _sort_queries else "overall_score"
    result = await session.execute(
        text(_sort_queries[safe_sort]),
        {"portal": portal, "limit": limit, "offset": offset},
    )
    return [
        DatasetHealthDetail(
            dataset_id=row.dataset_id,
            title=row.title,
            portal=row.portal,
            format=row.format,
            overall_score=row.overall_score,
            freshness_score=row.freshness_score,
            freshness_status=row.freshness_status,
            accessibility_score=row.accessibility_score,
            machine_readability_score=row.machine_readability_score,
            completeness_score=row.completeness_score,
            contactability_score=row.contactability_score,
        )
        for row in result.fetchall()
    ]


# ---------------------------------------------------------------------------
# Feature 2: Ghost Dataset Monitor
# ---------------------------------------------------------------------------

@router.get("/ghost-datasets", response_model=list[GhostDataset])
@inject
async def get_ghost_datasets(
    session: FromDishka[MainAsyncSession],
    portal: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> list[GhostDataset]:
    """Datasets abandonados o rancios: no actualizados en mucho tiempo."""
    query = """
        SELECT
            CAST(h.dataset_id AS text) AS dataset_id,
            d.title, d.portal, d.organization, d.format,
            h.freshness_status, h.overall_score,
            CAST(d.updated_at AS text) AS last_updated
        FROM dataset_health_scores h
        JOIN datasets d ON d.id = h.dataset_id
        WHERE h.freshness_status IN ('STALE', 'ABANDONED')
    """
    params: dict = {"limit": limit}
    if portal:
        query += " AND h.portal = :portal"
        params["portal"] = portal
    query += " ORDER BY h.overall_score ASC LIMIT :limit"

    result = await session.execute(text(query), params)
    return [
        GhostDataset(
            dataset_id=row.dataset_id,
            title=row.title,
            portal=row.portal,
            organization=row.organization,
            format=row.format,
            freshness_status=row.freshness_status,
            overall_score=row.overall_score,
            last_updated=row.last_updated,
        )
        for row in result.fetchall()
    ]


# ---------------------------------------------------------------------------
# Feature 3: DDJJ Anomalies
# ---------------------------------------------------------------------------

@router.get("/ddjj/anomalies", response_model=DdjjAnomalySummary)
@inject
async def get_ddjj_anomalies(
    session: FromDishka[MainAsyncSession],
    severity: str | None = Query(default=None, pattern="^(low|medium|high)$"),
    limit: int = Query(default=50, ge=1, le=200),
) -> DdjjAnomalySummary:
    """Anomalías patrimoniales detectadas en DDJJ de diputados."""
    # Summary counts
    totals = await session.execute(text("SELECT COUNT(*) FROM ddjj_anomalies"))
    total_analyzed = totals.scalar() or 0

    flagged = await session.execute(
        text("SELECT COUNT(*) FROM ddjj_anomalies WHERE is_anomalous = true")
    )
    total_flagged = flagged.scalar() or 0

    sev_rows = await session.execute(text(
        "SELECT severity, COUNT(*) AS c FROM ddjj_anomalies "
        "WHERE is_anomalous = true AND severity IS NOT NULL GROUP BY severity"
    ))
    by_severity = {row.severity: row.c for row in sev_rows.fetchall()}

    type_rows = await session.execute(text(
        "SELECT anomaly_type, COUNT(*) AS c FROM ddjj_anomalies "
        "WHERE is_anomalous = true AND anomaly_type IS NOT NULL GROUP BY anomaly_type"
    ))
    by_type = {row.anomaly_type: row.c for row in type_rows.fetchall()}

    # Detail list (only anomalous, sorted by ratio desc)
    query = """
        SELECT nombre, cuit, anio_declaracion, patrimonio_cierre,
               bienes_inicio, bienes_cierre, ingresos_trabajo_neto,
               variacion_patrimonial, brecha_inexplicable, ratio_crecimiento,
               anomaly_type, severity
        FROM ddjj_anomalies
        WHERE is_anomalous = true
    """
    params: dict = {"limit": limit}
    if severity:
        query += " AND severity = :severity"
        params["severity"] = severity
    query += " ORDER BY ratio_crecimiento DESC LIMIT :limit"

    detail_rows = await session.execute(text(query), params)
    anomalies = [
        DdjjAnomaly(
            nombre=row.nombre,
            cuit=row.cuit,
            anio_declaracion=row.anio_declaracion,
            patrimonio_cierre=row.patrimonio_cierre,
            bienes_inicio=row.bienes_inicio,
            bienes_cierre=row.bienes_cierre,
            ingresos_trabajo_neto=row.ingresos_trabajo_neto,
            variacion_patrimonial=row.variacion_patrimonial,
            brecha_inexplicable=row.brecha_inexplicable,
            ratio_crecimiento=row.ratio_crecimiento,
            anomaly_type=row.anomaly_type,
            severity=row.severity,
        )
        for row in detail_rows.fetchall()
    ]

    return DdjjAnomalySummary(
        total_analyzed=total_analyzed,
        total_flagged=total_flagged,
        by_severity=by_severity,
        by_type=by_type,
        anomalies=anomalies,
    )


# ---------------------------------------------------------------------------
# Feature 4: Session Topics
# ---------------------------------------------------------------------------

@router.get("/sessions/topics", response_model=SessionTopicSummary)
@inject
async def get_session_topics(
    session: FromDishka[MainAsyncSession],
    periodo: int | None = None,
    category: str | None = None,
) -> SessionTopicSummary:
    """Distribución temática de sesiones parlamentarias."""
    # Count distinct sessions
    sess_q = "SELECT COUNT(DISTINCT (periodo, reunion)) FROM session_topics"
    sess_params: dict = {}
    if periodo:
        sess_q += " WHERE periodo = :periodo"
        sess_params["periodo"] = periodo
    sessions_result = await session.execute(text(sess_q), sess_params)
    sessions_analyzed = sessions_result.scalar() or 0

    # Global topic distribution
    dist_q = """
        SELECT topic_category, SUM(mention_count) AS total
        FROM session_topics
    """
    dist_params: dict = {}
    conditions = []
    if periodo:
        conditions.append("periodo = :periodo")
        dist_params["periodo"] = periodo
    if category:
        conditions.append("topic_category = :category")
        dist_params["category"] = category
    if conditions:
        dist_q += " WHERE " + " AND ".join(conditions)
    dist_q += " GROUP BY topic_category ORDER BY total DESC"

    dist_rows = await session.execute(text(dist_q), dist_params)
    topic_distribution = {row.topic_category: row.total for row in dist_rows.fetchall()}

    # Per-session topics
    detail_q = """
        SELECT periodo, reunion, fecha, tipo_sesion, topic_name,
               topic_category, mention_count, relevance_score, top_speakers_json
        FROM session_topics
    """
    detail_params: dict = {}
    conditions = []
    if periodo:
        conditions.append("periodo = :periodo")
        detail_params["periodo"] = periodo
    if category:
        conditions.append("topic_category = :category")
        detail_params["category"] = category
    if conditions:
        detail_q += " WHERE " + " AND ".join(conditions)
    detail_q += " ORDER BY periodo DESC, reunion DESC, mention_count DESC LIMIT 500"

    detail_rows = await session.execute(text(detail_q), detail_params)
    topics_by_session = [
        SessionTopicEntry(
            periodo=row.periodo,
            reunion=row.reunion,
            fecha=row.fecha,
            tipo_sesion=row.tipo_sesion,
            topic_name=row.topic_name,
            topic_category=row.topic_category,
            mention_count=row.mention_count,
            relevance_score=row.relevance_score,
            top_speakers=_safe_json_loads(row.top_speakers_json),
        )
        for row in detail_rows.fetchall()
    ]

    return SessionTopicSummary(
        sessions_analyzed=sessions_analyzed,
        topic_distribution=topic_distribution,
        topics_by_session=topics_by_session,
    )


# ---------------------------------------------------------------------------
# Admin: trigger tasks manually
# ---------------------------------------------------------------------------

@router.post("/rescore")
async def trigger_rescore(portal: str | None = None):
    """Trigger health scoring task (re-scrape must happen first for last_updated_at)."""
    from app.infrastructure.celery.tasks.transparency_tasks import score_portal_health

    if portal:
        score_portal_health.delay(portal)
        return {"status": "dispatched", "task": "score_portal_health", "portal": portal}
    score_portal_health.delay()
    return {"status": "dispatched", "task": "score_portal_health", "portal": "all"}


@router.post("/rescrape")
async def trigger_rescrape(portal: str | None = None):
    """Trigger catalog scrape + health rescore (with delay)."""
    from app.infrastructure.celery.tasks.scraper_tasks import scrape_catalog
    from app.infrastructure.celery.tasks.transparency_tasks import score_portal_health

    portals = [portal] if portal else [
        "datos_gob_ar", "caba", "diputados", "justicia",
        "buenos_aires_prov", "cordoba_prov", "santa_fe",
        "mendoza", "entre_rios", "neuquen_legislatura",
    ]
    for p in portals:
        scrape_catalog.delay(p)
    # Rescore after scrapers finish (5 min delay)
    score_portal_health.apply_async(countdown=300)
    return {"status": "dispatched", "scrape_portals": portals, "rescore_delay": "5min"}


@router.post("/detect-anomalies")
async def trigger_detect_anomalies():
    """Trigger DDJJ anomaly detection (inflation-adjusted analysis)."""
    from app.infrastructure.celery.tasks.transparency_tasks import detect_ddjj_anomalies

    detect_ddjj_anomalies.delay()
    return {"status": "dispatched", "task": "detect_ddjj_anomalies"}


@router.post("/snapshot-staff")
async def trigger_staff_snapshot():
    """Trigger HCDN staff snapshot (download nómina + diff against previous)."""
    from app.infrastructure.celery.tasks.staff_tasks import snapshot_staff

    snapshot_staff.delay()
    return {"status": "dispatched", "task": "snapshot_staff"}


@router.get("/staff/status")
@inject
async def get_staff_status(
    session: FromDishka[MainAsyncSession],
):
    """Check staff snapshot status: latest snapshot date, total employees, recent changes."""
    snap = await session.execute(text(
        "SELECT snapshot_date, COUNT(*) AS total "
        "FROM staff_snapshots GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 5"
    ))
    snapshots = [{"date": str(r.snapshot_date), "total": r.total} for r in snap.fetchall()]

    changes = await session.execute(text(
        "SELECT tipo, COUNT(*) AS c FROM staff_changes GROUP BY tipo"
    ))
    changes_summary = {r.tipo: r.c for r in changes.fetchall()}

    return {
        "snapshots": snapshots,
        "changes": changes_summary,
        "has_data": len(snapshots) > 0,
    }


@router.post("/flush-cache")
@inject
async def flush_query_cache(
    session: FromDishka[MainAsyncSession],
):
    """Flush Redis query cache + pgvector semantic cache."""
    from app.domain.ports.cache.cache_port import ICacheService
    from dishka import AsyncContainer

    # Clear semantic cache (pgvector)
    result = await session.execute(text("DELETE FROM query_cache"))
    await session.commit()
    sem_deleted = result.rowcount

    return {
        "status": "flushed",
        "semantic_cache_deleted": sem_deleted,
        "note": "Redis cache entries will expire by TTL (max 2h for DDJJ)",
    }
