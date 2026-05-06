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
    # `truncated` reports whether the RESPONSE itself was capped at
    # MAX_ROWS (sandbox-level pagination). It does NOT report whether
    # the underlying tables are sampled. See `sampled_tables` for that.
    truncated: bool
    # `sampled_tables` lists raw-layer tables touched by this query that
    # carry `is_truncated=TRUE` in `raw_table_versions` (Sprint 0.1).
    # Empty when the query only touches full materialisations or marts.
    # Consumers should warn the user that any aggregation over these
    # tables reflects only the first MAX_TABLE_ROWS rows of upstream.
    sampled_tables: list[str] = []
    error: str | None = None


class TableInfoResponse(BaseModel):
    table_name: str
    name: str
    dataset_id: str
    row_count: int | None
    columns: list[str]
    # `is_truncated` flags raw landings that hit the parser's MAX_TABLE_ROWS
    # cap (default 500_000). Mart and legacy cache_* tables always report
    # False because they are populated from full materialised flows that
    # don't apply the cap. Surfaces the Sprint 0.1 flag from
    # `raw_table_versions.is_truncated` so consumers can decide whether to
    # treat the data as a complete corpus or a sampled prefix.
    is_truncated: bool = False


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
    Returns `sampled_tables` listing any raw-layer table touched by the
    query that is flagged `is_truncated=TRUE` so callers can warn users
    about partial coverage.
    """
    result = await sandbox.execute_readonly(body.sql)

    # Best-effort sampled-tables scan: regex over the SQL to extract
    # `raw."<bare>"` references, then look them up against the truncated
    # set. If anything fails, return an empty list rather than blocking
    # the response.
    sampled: list[str] = []
    if result.error is None:
        try:
            import asyncio as _asyncio
            import re as _re

            from sqlalchemy import text as _sql_text

            referenced = {
                m.group(1)
                for m in _re.finditer(r'raw\."([^"]+)"', body.sql)
            }
            referenced.update(
                m.group(1)
                for m in _re.finditer(r'\braw\.(\w+)', body.sql)
            )

            if referenced:
                def _check_sampled() -> list[str]:
                    engine = getattr(sandbox, "_engine", None)
                    if engine is None:
                        getter = getattr(sandbox, "_get_engine", None)
                        if callable(getter):
                            engine = getter()
                    if engine is None:
                        return []
                    with engine.connect() as _conn:
                        rows = _conn.execute(
                            _sql_text(
                                "SELECT table_name FROM raw_table_versions "
                                "WHERE is_truncated = TRUE "
                                "  AND superseded_at IS NULL "
                                "  AND table_name = ANY(:names)"
                            ),
                            {"names": list(referenced)},
                        ).fetchall()
                    return [str(r.table_name) for r in rows]

                sampled = await _asyncio.to_thread(_check_sampled)
        except Exception:
            logger.debug("sampled_tables scan failed", exc_info=True)

    return DataQueryResponse(
        columns=result.columns,
        rows=result.rows,
        row_count=result.row_count,
        truncated=result.truncated,
        sampled_tables=sampled,
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
    """List all dataset tables: cached `cache_*`, raw layer, and curated marts.

    Marts are surfaced with their qualified name (`mart.<view>`) so callers
    (e.g. territoria_backend) can `SELECT FROM mart.<view>` directly.
    Without this, `/data/tables` was legacy-only and downstream consumers
    couldn't discover the curated layer.
    """
    tables = await sandbox.list_cached_tables()

    # Mart enrichment — best-effort. If `mart_definitions` doesn't exist
    # (early deploy) or the query fails, we degrade silently to legacy.
    #
    # Note: `PgSandboxAdapter._engine` is a SYNCHRONOUS Engine, so this
    # runs in a worker thread to avoid blocking the event loop. The
    # earlier version used `async with engine.connect()` which silently
    # raised AttributeError on `__aenter__` and dropped marts from the
    # response — exactly the same bug that hid marts from /data/search.
    def _query_marts_sync() -> list[Any]:
        from sqlalchemy import text as _sql_text

        engine_for_marts = getattr(sandbox, "_engine", None)
        if engine_for_marts is None:
            getter = getattr(sandbox, "_get_engine", None)
            if callable(getter):
                engine_for_marts = getter()
        if engine_for_marts is None:
            return []
        with engine_for_marts.connect() as _conn:
            return (
                _conn.execute(
                    _sql_text(
                        "SELECT mart_id, mart_schema, mart_view_name, "
                        "       last_row_count, canonical_columns_json "
                        "FROM mart_definitions "
                        "WHERE COALESCE(last_row_count, 0) > 0"
                    )
                )
            ).fetchall()

    mart_entries: list[TableInfoResponse] = []
    try:
        import asyncio as _asyncio

        rows = await _asyncio.to_thread(_query_marts_sync)
        for _r in rows:
            _columns: list[str] = []
            try:
                _cc = _r.canonical_columns_json
                if isinstance(_cc, list):
                    _columns = [
                        str(c.get("name", "")) for c in _cc if c.get("name")
                    ]
            except Exception:
                _columns = []
            _schema = str(_r.mart_schema or "mart")
            _view = str(_r.mart_view_name or _r.mart_id)
            mart_entries.append(
                TableInfoResponse(
                    table_name=f"{_schema}.{_view}",
                    name=str(_r.mart_id),
                    dataset_id="",
                    row_count=int(_r.last_row_count or 0),
                    columns=_columns,
                )
            )
    except Exception:
        logger.warning("Could not enrich /data/tables with marts", exc_info=True)

    # Surface the raw-layer truncation flag (Sprint 0.1) per table so
    # downstream consumers (territoria etc) know which rows are sampled
    # prefixes vs full datasets. Best-effort: a failure here just means
    # all rows report `is_truncated=False` (default), no consumer breakage.
    def _query_truncated_map() -> dict[str, bool]:
        from sqlalchemy import text as _sql_text

        engine_for_rtv = getattr(sandbox, "_engine", None)
        if engine_for_rtv is None:
            getter = getattr(sandbox, "_get_engine", None)
            if callable(getter):
                engine_for_rtv = getter()
        if engine_for_rtv is None:
            return {}
        with engine_for_rtv.connect() as _conn:
            return {
                row.table_name: True
                for row in _conn.execute(
                    _sql_text(
                        "SELECT table_name FROM raw_table_versions "
                        "WHERE is_truncated = TRUE AND superseded_at IS NULL"
                    )
                ).fetchall()
            }

    truncated_map: dict[str, bool] = {}
    try:
        import asyncio as _asyncio

        truncated_map = await _asyncio.to_thread(_query_truncated_map)
    except Exception:
        logger.warning("Could not load is_truncated map for /data/tables", exc_info=True)

    def _is_truncated(table_name: str) -> bool:
        # Compare both the qualified form (raw.<bare>) and bare so we
        # match regardless of how the sandbox surfaced the row.
        if table_name in truncated_map:
            return True
        if "." in table_name and table_name.split(".", 1)[1].strip('"') in truncated_map:
            return True
        return False

    legacy_entries = [
        TableInfoResponse(
            table_name=t.table_name,
            name=t.table_name,
            dataset_id=str(t.dataset_id) if t.dataset_id else "",
            row_count=t.row_count,
            columns=[str(c) for c in t.columns],
            is_truncated=_is_truncated(t.table_name),
        )
        for t in tables
    ]
    return TableListResponse(tables=mart_entries + legacy_entries)


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

    # Mart layer first — semantically curated views beat raw cached tables
    # for downstream consumers (territoria, etc). We score marts via the
    # same embedding similarity as datasets so they compete on equal footing.
    # Without this branch, /data/tables surfaces marts but /data/search
    # never finds them, leaving the API inconsistent.
    #
    # Note: `PgSandboxAdapter._engine` is a SYNCHRONOUS Engine (it backs
    # the sandbox's blocking SQL path), so we run the query through
    # `asyncio.to_thread`. Using `async with engine.connect()` here would
    # AttributeError on `__aenter__` and the whole branch would fall
    # silently into the `except` below — which is exactly what shipped
    # in the first attempt and made marts invisible to /data/search.
    def _query_marts_sync() -> list[Any]:
        from sqlalchemy import text as _sql_text

        engine_for_marts = getattr(sandbox, "_engine", None)
        if engine_for_marts is None:
            # The sandbox lazily creates its engine on first use; force it
            # here so we don't bail before the first real query lands.
            getter = getattr(sandbox, "_get_engine", None)
            if callable(getter):
                engine_for_marts = getter()
        if engine_for_marts is None:
            return []
        embedding_literal = "[" + ",".join(repr(float(v)) for v in query_embedding) + "]"
        with engine_for_marts.connect() as _conn:
            return (
                _conn.execute(
                    _sql_text(
                        """
                        SELECT mart_id, mart_schema, mart_view_name,
                               description, domain,
                               1 - (embedding <=> CAST(:emb AS vector)) AS score
                        FROM mart_definitions
                        WHERE embedding IS NOT NULL
                          AND COALESCE(last_row_count, 0) > 0
                          AND 1 - (embedding <=> CAST(:emb AS vector)) >= :min_sim
                        ORDER BY embedding <=> CAST(:emb AS vector)
                        LIMIT :lim
                        """
                    ),
                    {
                        "emb": embedding_literal,
                        "min_sim": _MIN_SIMILARITY,
                        "lim": body.limit,
                    },
                )
            ).fetchall()

    try:
        import asyncio as _asyncio

        mart_rows = await _asyncio.to_thread(_query_marts_sync)
        for mart_row in mart_rows:
            qualified = f"{mart_row.mart_schema}.{mart_row.mart_view_name}"
            if qualified in seen_tables:
                continue
            seen_tables.add(qualified)
            out.append(
                DataSearchResult(
                    table_name=qualified,
                    name=str(mart_row.mart_id),
                    title=str(mart_row.mart_id),
                    description=str(mart_row.description or mart_row.domain or ""),
                    relevance=round(float(mart_row.score), 3),
                )
            )
            if len(out) >= body.limit:
                break
    except Exception:
        logger.warning("Mart enrichment for /data/search failed", exc_info=True)

    # Raw layer semantic search for rows that exist in table_catalog but
    # don't have a `dataset_id` mapping through cached_datasets. Without
    # this branch, `/data/tables` can list live `raw.*` landings that
    # `/data/search` can never find.
    def _query_raw_semantic_sync() -> list[Any]:
        from sqlalchemy import text as _sql_text

        engine_for_catalog = getattr(sandbox, "_engine", None)
        if engine_for_catalog is None:
            getter = getattr(sandbox, "_get_engine", None)
            if callable(getter):
                engine_for_catalog = getter()
        if engine_for_catalog is None:
            return []
        embedding_literal = "[" + ",".join(repr(float(v)) for v in query_embedding) + "]"
        with engine_for_catalog.connect() as _conn:
            return (
                _conn.execute(
                    _sql_text(
                        """
                        SELECT table_name, display_name, description,
                               1 - (catalog_embedding <=> CAST(:emb AS vector)) AS score
                        FROM table_catalog
                        WHERE table_name LIKE 'raw.%'
                          AND catalog_embedding IS NOT NULL
                          AND 1 - (catalog_embedding <=> CAST(:emb AS vector)) >= :min_sim
                        ORDER BY catalog_embedding <=> CAST(:emb AS vector)
                        LIMIT :lim
                        """
                    ),
                    {
                        "emb": embedding_literal,
                        "min_sim": _MIN_SIMILARITY,
                        "lim": body.limit,
                    },
                )
            ).fetchall()

    try:
        import asyncio as _asyncio

        raw_rows = await _asyncio.to_thread(_query_raw_semantic_sync)
        for raw_row in raw_rows:
            qualified = str(raw_row.table_name)
            if qualified in seen_tables:
                continue
            seen_tables.add(qualified)
            out.append(
                DataSearchResult(
                    table_name=qualified,
                    name=qualified,
                    title=str(raw_row.display_name or qualified),
                    description=str(raw_row.description or ""),
                    relevance=round(float(raw_row.score), 3),
                )
            )
            if len(out) >= body.limit:
                break
    except Exception:
        logger.warning("Raw enrichment for /data/search failed", exc_info=True)

    for r in results:
        if len(out) >= body.limit:
            break
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

    # Sort merged results by relevance — marts may not always outrank
    # datasets if the query is more specific to a raw table.
    out.sort(key=lambda x: x.relevance, reverse=True)
    out = out[: body.limit]

    logger.info("Search completed: %d results for query: %s", len(out), body.query[:100])
    return DataSearchResponse(results=out)
