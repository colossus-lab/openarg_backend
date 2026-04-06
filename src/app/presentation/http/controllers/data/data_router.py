"""
Data API Router — internal service-to-service endpoints.

Protected by DATA_SERVICE_TOKEN (env var).
Header: Authorization: Bearer svc_xxx

No public rate limiting by design: intended for trusted internal products
running under the same platform boundary.
"""

from __future__ import annotations

import logging
import os
import secrets
from typing import Any

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.application.pipeline.connectors.cache_table_selection import prefer_consolidated_table
from app.domain.ports.llm.llm_provider import IEmbeddingProvider
from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox
from app.domain.ports.search.vector_search import IVectorSearch

logger = logging.getLogger(__name__)

_MIN_SIMILARITY = 0.40

router = APIRouter(prefix="/data", tags=["data-api"])


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------


def _get_service_token() -> str:
    return os.getenv("DATA_SERVICE_TOKEN", "")


def verify_service_token(request: Request) -> str:
    """Validate service token from Authorization: Bearer header."""
    expected = _get_service_token()
    if not expected:
        raise HTTPException(status_code=503, detail="Service token not configured")

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    token = auth_header[7:].strip()
    if not token or not secrets.compare_digest(token, expected):
        logger.warning("Data API auth failed: invalid service token")
        raise HTTPException(status_code=401, detail="Unauthorized")

    return token


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------


class DataQueryRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=5000)


class DataQueryResponse(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool
    error: str | None = None


class TableInfoResponse(BaseModel):
    table_name: str
    name: str
    dataset_id: str
    row_count: int | None
    columns: list[str]


class DataSearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    limit: int = Field(default=10, ge=1, le=50)


class DataSearchResult(BaseModel):
    table_name: str
    name: str
    title: str
    description: str
    relevance: float


class TableListResponse(BaseModel):
    tables: list[TableInfoResponse]


class DataSearchResponse(BaseModel):
    results: list[DataSearchResult]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/query",
    response_model=DataQueryResponse,
    dependencies=[Depends(verify_service_token)],
)
@inject
async def data_query(
    body: DataQueryRequest,
    sandbox: FromDishka[ISQLSandbox],
) -> DataQueryResponse:
    """Execute a read-only SQL SELECT against cached dataset tables.

    Reuses the sandbox's 3-layer SQL validation (regex, AST, execution-time).
    """
    result = await sandbox.execute_readonly(body.sql)
    return DataQueryResponse(
        columns=result.columns,
        rows=result.rows,
        row_count=result.row_count,
        truncated=result.truncated,
        error=result.error,
    )


@router.get(
    "/tables",
    response_model=TableListResponse,
    dependencies=[Depends(verify_service_token)],
)
@inject
async def data_tables(
    sandbox: FromDishka[ISQLSandbox],
) -> TableListResponse:
    """List all cached dataset tables with schema metadata."""
    tables = await sandbox.list_cached_tables()
    return TableListResponse(
        tables=[
            TableInfoResponse(
                table_name=t.table_name,
                name=t.table_name,
                dataset_id=str(t.dataset_id) if t.dataset_id else "",
                row_count=t.row_count,
                columns=[str(c) for c in t.columns],
            )
            for t in tables
        ]
    )


@router.post(
    "/search",
    response_model=DataSearchResponse,
    dependencies=[Depends(verify_service_token)],
)
@inject
async def data_search(
    body: DataSearchRequest,
    sandbox: FromDishka[ISQLSandbox],
    vector_search: FromDishka[IVectorSearch],
    embedding_provider: FromDishka[IEmbeddingProvider],
) -> DataSearchResponse:
    """Semantic search for relevant cached dataset tables.

    Generates an embedding for the query text and searches pgvector
    for datasets whose descriptions are semantically similar.
    Cross-references results with cached_datasets to return real table names.
    """
    try:
        query_embedding = await embedding_provider.embed(body.query)
    except Exception:
        logger.error("Failed to generate embedding for query: %s", body.query[:100])
        raise HTTPException(status_code=502, detail="Servicio de búsqueda no disponible")

    logger.info("Executing semantic search for query: %s", body.query[:100])
    results = await vector_search.search_datasets(
        query_embedding=query_embedding,
        limit=body.limit,
        min_similarity=_MIN_SIMILARITY,
    )

    # Build dataset_id → table_name mapping from cached tables
    cached_tables = await sandbox.list_cached_tables()
    available_names = [t.table_name for t in cached_tables]
    dataset_to_table = {
        t.dataset_id: prefer_consolidated_table(t.table_name, available_names)
        for t in cached_tables
        if t.dataset_id
    }

    out: list[DataSearchResult] = []
    seen_tables: set[str] = set()
    for r in results:
        table_name = dataset_to_table.get(r.dataset_id, "")
        if not table_name:
            continue  # skip datasets without a cached table
        if table_name in seen_tables:
            continue  # dedupe: same table matched multiple chunks (main/columns/contextual)
        seen_tables.add(table_name)
        out.append(
            DataSearchResult(
                table_name=table_name,
                name=table_name,
                title=r.title,
                description=r.description,
                relevance=round(r.score, 3),
            )
        )
        if len(out) >= body.limit:
            break

    logger.info("Search completed: %d results for query: %s", len(out), body.query[:100])
    return DataSearchResponse(results=out)
