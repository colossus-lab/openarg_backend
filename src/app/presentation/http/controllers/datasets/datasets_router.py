from __future__ import annotations

import os
from typing import Any

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from app.infrastructure.persistence_sqla.provider import MainAsyncSession  # noqa: TC001

router = APIRouter(prefix="/datasets", tags=["datasets"])


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


class DownloadResponse(BaseModel):
    url: str
    source: str  # "s3" | "portal"
    filename: str
    expires_in: int  # seconds


@router.get("/", response_model=list[DatasetSummary])
@inject  # type: ignore[untyped-decorator]
async def list_datasets(
    session: FromDishka[MainAsyncSession],
    portal: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[DatasetSummary]:
    """List indexed datasets, optionally filtered by portal."""
    query = (
        "SELECT CAST(id AS text), title, description, "
        "organization, portal, format, is_cached, row_count "
        "FROM datasets"
    )
    params: dict[str, Any] = {"limit": limit, "offset": offset}

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
@inject  # type: ignore[untyped-decorator]
async def get_portal_stats(
    session: FromDishka[MainAsyncSession],
) -> list[PortalStats]:
    """Get dataset counts per portal."""
    result = await session.execute(
        text("SELECT portal, COUNT(*) as count FROM datasets GROUP BY portal ORDER BY count DESC")
    )
    return [PortalStats(portal=row.portal, count=row.count) for row in result.fetchall()]


@router.get("/{dataset_id}/download", response_model=DownloadResponse)
@inject  # type: ignore[untyped-decorator]
async def download_dataset(
    dataset_id: str,
    session: FromDishka[MainAsyncSession],
) -> DownloadResponse:
    """Descarga el archivo original de un dataset (presigned S3 URL o redirect al portal)."""
    result = await session.execute(
        text(
            "SELECT d.title, d.download_url, d.format, d.source_id, cd.s3_key "
            "FROM datasets d "
            "LEFT JOIN cached_datasets cd ON cd.dataset_id = d.id AND cd.status = 'ready' "
            "WHERE d.id = CAST(:did AS uuid)"
        ),
        {"did": dataset_id},
    )
    row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Dataset not found")

    filename = f"{row.source_id}.{row.format}" if row.source_id and row.format else row.title
    expires_in = 3600  # 1 hour

    # Si tiene s3_key: generar presigned URL
    if row.s3_key:
        import boto3

        bucket = os.getenv("S3_BUCKET", "openarg-datasets")
        region = os.getenv("AWS_REGION", "us-east-1")
        s3 = boto3.client("s3", region_name=region)
        presigned_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": row.s3_key},
            ExpiresIn=expires_in,
        )
        return DownloadResponse(
            url=presigned_url,
            source="s3",
            filename=filename,
            expires_in=expires_in,
        )

    # Si tiene download_url: redirect al portal
    if row.download_url:
        return DownloadResponse(
            url=row.download_url,
            source="portal",
            filename=filename,
            expires_in=0,
        )

    raise HTTPException(status_code=404, detail="No download URL available")
