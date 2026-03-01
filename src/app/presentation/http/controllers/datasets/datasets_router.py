from __future__ import annotations

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text

from app.infrastructure.celery.tasks.scraper_tasks import scrape_catalog
from app.infrastructure.persistence_sqla.provider import MainAsyncSession

router = APIRouter(prefix="/datasets", tags=["datasets"])
limiter = Limiter(key_func=get_remote_address)


class DatasetSummary(BaseModel):
    id: str
    title: str
    description: str | None
    organization: str | None
    portal: str
    format: str | None
    is_cached: bool
    row_count: int | None


class PortalStats(BaseModel):
    portal: str
    count: int


@router.get("/", response_model=list[DatasetSummary])
@inject
async def list_datasets(
    session: FromDishka[MainAsyncSession],
    portal: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[DatasetSummary]:
    """List indexed datasets, optionally filtered by portal."""
    query = "SELECT CAST(id AS text), title, description, organization, portal, format, is_cached, row_count FROM datasets"
    params: dict = {"limit": limit, "offset": offset}

    if portal:
        query += " WHERE portal = :portal"
        params["portal"] = portal

    query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"

    result = await session.execute(text(query), params)
    rows = result.fetchall()

    return [
        DatasetSummary(
            id=row.id,
            title=row.title,
            description=row.description,
            organization=row.organization,
            portal=row.portal,
            format=row.format,
            is_cached=row.is_cached,
            row_count=row.row_count,
        )
        for row in rows
    ]


@router.get("/stats", response_model=list[PortalStats])
@inject
async def get_portal_stats(
    session: FromDishka[MainAsyncSession],
) -> list[PortalStats]:
    """Get dataset counts per portal."""
    result = await session.execute(
        text("SELECT portal, COUNT(*) as count FROM datasets GROUP BY portal ORDER BY count DESC")
    )
    return [PortalStats(portal=row.portal, count=row.count) for row in result.fetchall()]


@router.post("/scrape/{portal}")
@limiter.limit("2/hour")
async def trigger_scrape(request: Request, portal: str) -> dict:
    """Trigger a catalog scrape for a specific portal."""
    valid_portals = ["datos_gob_ar", "caba"]
    if portal not in valid_portals:
        raise HTTPException(status_code=400, detail=f"Invalid portal. Valid: {valid_portals}")

    task = scrape_catalog.delay(portal)
    return {"task_id": task.id, "portal": portal, "status": "scraping_started"}
