from __future__ import annotations

import hashlib
import json
import logging
import time
from uuid import uuid4

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.llm.llm_provider import IEmbeddingProvider, ILLMProvider, LLMMessage
from app.domain.ports.search.vector_search import IVectorSearch
from app.infrastructure.adapters.sandbox.table_validation import safe_table_query
from app.infrastructure.celery.tasks.analyst_tasks import analyze_query
from app.infrastructure.persistence_sqla.provider import MainAsyncSession
from app.setup.app_factory import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/query", tags=["query"])


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    user_id: str | None = None


class QueryResponse(BaseModel):
    query_id: str
    status: str
    message: str


class QueryStatusResponse(BaseModel):
    query_id: str
    status: str
    question: str
    analysis_result: str | None = None
    sources: list[dict] | None = None
    tokens_used: int = 0
    duration_ms: int = 0


@router.post("/", response_model=QueryResponse)
@inject
async def submit_query(
    body: QueryRequest,
    session: FromDishka[MainAsyncSession],
) -> QueryResponse:
    """Submit a new query for analysis. Returns immediately with query_id."""
    query_id = str(uuid4())

    await session.execute(
        text("""
            INSERT INTO user_queries (id, question, user_id, status)
            VALUES (CAST(:id AS uuid), :question, :user_id, 'pending')
        """),
        {"id": query_id, "question": body.question, "user_id": body.user_id},
    )
    await session.commit()

    # Dispatch to analyst worker
    analyze_query.delay(query_id, body.question)

    return QueryResponse(
        query_id=query_id,
        status="pending",
        message="Query submitted. Use GET /query/{query_id} to check status.",
    )


@router.get("/{query_id}", response_model=QueryStatusResponse)
@inject
async def get_query_status(
    query_id: str,
    session: FromDishka[MainAsyncSession],
) -> QueryStatusResponse:
    """Check the status and results of a query."""
    result = await session.execute(
        text("""
            SELECT question, status, analysis_result, sources_json, tokens_used, duration_ms
            FROM user_queries WHERE id = CAST(:id AS uuid)
        """),
        {"id": query_id},
    )
    row = result.fetchone()

    if not row:
        return QueryStatusResponse(
            query_id=query_id, status="not_found", question=""
        )

    sources = None
    if row.sources_json:
        try:
            sources = json.loads(row.sources_json)
        except json.JSONDecodeError:
            pass

    return QueryStatusResponse(
        query_id=query_id,
        status=row.status,
        question=row.question,
        analysis_result=row.analysis_result,
        sources=sources,
        tokens_used=row.tokens_used or 0,
        duration_ms=row.duration_ms or 0,
    )


def _cache_key(question: str) -> str:
    """Generate a deterministic cache key from a question."""
    normalized = question.strip().lower()
    h = hashlib.sha256(normalized.encode()).hexdigest()[:16]
    return f"openarg:query:{h}"


QUERY_CACHE_TTL = 3600  # 1 hour


async def _fetch_sample_data(
    session: MainAsyncSession,
    dataset_ids: list[str],
) -> dict[str, dict]:
    """Fetch sample rows from cached datasets for richer LLM context."""
    samples: dict[str, dict] = {}
    for did in dataset_ids[:3]:  # Max 3 datasets to keep context manageable
        try:
            row = await session.execute(
                text(
                    "SELECT table_name, columns_json, row_count FROM cached_datasets "
                    "WHERE dataset_id = CAST(:did AS uuid) AND status = 'ready'"
                ),
                {"did": did},
            )
            cached = row.fetchone()
            if not cached:
                continue

            # Fetch sample rows (first 10)
            safe_sql = safe_table_query(cached.table_name, 'SELECT * FROM "{}" LIMIT 10')
            if not safe_sql:
                continue
            sample_result = await session.execute(text(safe_sql))
            rows = sample_result.fetchall()
            columns = sample_result.keys()

            sample_rows = [dict(zip(columns, r, strict=False)) for r in rows]
            samples[did] = {
                "table_name": cached.table_name,
                "row_count": cached.row_count,
                "columns": list(columns),
                "sample_rows": sample_rows,
            }
        except Exception:
            logger.debug(f"Could not fetch sample data for dataset {did}", exc_info=True)
            continue
    return samples


@router.post("/quick")
@limiter.limit("15/minute")
@inject
async def quick_query(
    request: Request,
    body: QueryRequest,
    llm: FromDishka[ILLMProvider],
    embedding: FromDishka[IEmbeddingProvider],
    vector_search: FromDishka[IVectorSearch],
    cache: FromDishka[ICacheService],
    session: FromDishka[MainAsyncSession],
) -> dict:
    """
    Quick synchronous query — searches datasets and returns answer directly.
    When cached data is available, fetches real sample rows for richer analysis.
    Results are cached in Redis for 1 hour.
    Every query is also saved to user_queries for conversation history.
    """
    start_time = time.monotonic()
    cache_key = _cache_key(body.question)

    # Check cache first
    cached = await cache.get(cache_key)
    if cached:
        cached["cached"] = True
        return cached

    # Search relevant datasets
    query_embedding = await embedding.embed(body.question)
    all_results = await vector_search.search_datasets(query_embedding, limit=5)

    # Apply minimum score threshold
    results = [r for r in all_results if r.score >= 0.30]

    if not results:
        result = {"answer": "No encontré datasets relevantes para tu pregunta.", "sources": []}
        await cache.set(cache_key, result, ttl_seconds=300)

        # Save to conversation history even for empty results
        duration_ms = int((time.monotonic() - start_time) * 1000)
        try:
            query_id = str(uuid4())
            await session.execute(
                text("""
                    INSERT INTO user_queries (id, question, user_id, status, analysis_result, sources_json, tokens_used, duration_ms)
                    VALUES (CAST(:id AS uuid), :question, :user_id, 'completed', :analysis_result, :sources_json, 0, :duration_ms)
                """),
                {
                    "id": query_id,
                    "question": body.question,
                    "user_id": body.user_id,
                    "analysis_result": result["answer"],
                    "sources_json": json.dumps(result["sources"]),
                    "duration_ms": duration_ms,
                },
            )
            await session.commit()
        except Exception:
            logger.debug("Failed to save conversation history (no results)", exc_info=True)

        return result

    # Try to get real data from cached datasets
    dataset_ids = [r.dataset_id for r in results]
    sample_data = await _fetch_sample_data(session, dataset_ids)

    # Build context with real data when available
    context_parts = []
    for r in results:
        part = (
            f"Dataset: {r.title} (portal: {r.portal}, relevancia: {r.score:.2f})\n"
            f"Descripción: {r.description}\n"
            f"Columnas: {r.columns}"
        )

        if r.dataset_id in sample_data:
            sd = sample_data[r.dataset_id]
            part += f"\nDatos reales ({sd['row_count']} filas totales en tabla '{sd['table_name']}'):"
            part += f"\nColumnas: {', '.join(sd['columns'])}"
            # Add sample rows as a compact table
            for i, row in enumerate(sd["sample_rows"][:5]):
                row_str = " | ".join(f"{k}={v}" for k, v in row.items())
                part += f"\n  Fila {i+1}: {row_str}"

        context_parts.append(part)

    context = "\n---\n".join(context_parts)
    has_real_data = bool(sample_data)

    system_prompt = (
        "Sos OpenArg, un analista de datos públicos de Argentina entrenado por ColossusLab.tech. "
        "Respondé de forma breve y conversacional en español argentino. "
        "Máximo 4-5 oraciones. Destacá el dato más importante primero.\n"
    )
    if has_real_data:
        system_prompt += (
            "Tenés acceso a datos REALES de los datasets (filas de muestra). "
            "Usá esos datos para dar respuestas concretas con números y hechos. "
            "Terminá sugiriendo 2-3 preguntas de seguimiento concretas."
        )
    else:
        system_prompt += (
            "Solo tenés metadata de los datasets (no los datos reales). "
            "Describí qué información contienen y sugerí consultas más específicas."
        )

    response = await llm.chat(
        messages=[
            LLMMessage(role="system", content=system_prompt),
            LLMMessage(
                role="user",
                content=f"Pregunta: {body.question}\n\nDatasets disponibles:\n{context}",
            ),
        ],
        temperature=0.1,
    )

    sources = [
        {
            "title": r.title,
            "portal": r.portal,
            "score": round(r.score, 3),
            "has_data": r.dataset_id in sample_data,
        }
        for r in results
    ]

    result = {
        "answer": response.content,
        "sources": sources,
        "tokens_used": response.tokens_used,
        "has_real_data": has_real_data,
    }

    # Save to conversation history
    duration_ms = int((time.monotonic() - start_time) * 1000)
    try:
        query_id = str(uuid4())
        await session.execute(
            text("""
                INSERT INTO user_queries (id, question, user_id, status, analysis_result, sources_json, tokens_used, duration_ms)
                VALUES (CAST(:id AS uuid), :question, :user_id, 'completed', :result, :sources, :tokens, :duration_ms)
            """),
            {
                "id": query_id,
                "question": body.question,
                "user_id": body.user_id,
                "result": response.content,
                "sources": json.dumps(sources),
                "tokens": response.tokens_used or 0,
                "duration_ms": duration_ms,
            },
        )
        await session.commit()
    except Exception:
        logger.debug("Failed to save conversation history", exc_info=True)

    # Cache the result
    await cache.set(cache_key, result, ttl_seconds=QUERY_CACHE_TTL)

    return result


@router.delete("/cache/{question_hash}")
@inject
async def invalidate_cache(
    question_hash: str,
    cache: FromDishka[ICacheService],
) -> dict:
    """Invalidate a cached query result."""
    key = f"openarg:query:{question_hash}"
    await cache.delete(key)
    return {"status": "deleted", "key": key}


def _validate_ws_api_key(websocket: WebSocket) -> bool:
    """Check api_key query param against configured BACKEND_API_KEY."""
    import os
    import secrets as _secrets
    expected = os.getenv("BACKEND_API_KEY", "")
    if not expected:
        return True  # No key configured, allow all
    provided = websocket.query_params.get("api_key", "")
    return _secrets.compare_digest(provided, expected)


@router.websocket("/ws/stream")
@inject
async def stream_query(
    websocket: WebSocket,
    llm: FromDishka[ILLMProvider],
    embedding: FromDishka[IEmbeddingProvider],
    vector_search: FromDishka[IVectorSearch],
):
    """WebSocket endpoint for streaming query responses."""
    if not _validate_ws_api_key(websocket):
        await websocket.close(code=4401, reason="Invalid or missing API key")
        return
    await websocket.accept()

    try:
        while True:
            raw = await websocket.receive_text()
            if len(raw) > 10_000:
                await websocket.send_json({"type": "error", "content": "Message too large (max 10KB)"})
                continue
            data = json.loads(raw)
            question = data.get("question", "")

            if not question or len(question) > 2000:
                await websocket.send_json({"type": "error", "content": "Question is required"})
                continue

            # Search
            await websocket.send_json({"type": "status", "content": "Buscando datasets..."})
            query_embedding = await embedding.embed(question)
            results = await vector_search.search_datasets(query_embedding, limit=5)

            if not results:
                await websocket.send_json({
                    "type": "complete",
                    "content": "No encontré datasets relevantes.",
                    "sources": [],
                })
                continue

            # Stream response
            await websocket.send_json({"type": "status", "content": "Analizando..."})

            context_parts = []
            for r in results:
                context_parts.append(
                    f"Dataset: {r.title} ({r.portal})\n{r.description}\nColumnas: {r.columns}"
                )

            messages = [
                LLMMessage(
                    role="system",
                    content=(
                        "Sos un analista de datos públicos de Argentina. "
                        "Respondé en español basándote en los datos."
                    ),
                ),
                LLMMessage(
                    role="user",
                    content=f"Pregunta: {question}\n\nDatasets:\n{'---'.join(context_parts)}",
                ),
            ]

            async for chunk in llm.chat_stream(messages):
                await websocket.send_json({"type": "chunk", "content": chunk})

            sources = [
                {"title": r.title, "portal": r.portal, "score": round(r.score, 3)}
                for r in results
            ]
            await websocket.send_json({"type": "complete", "sources": sources})

    except WebSocketDisconnect:
        logger.debug("WS client disconnected")
