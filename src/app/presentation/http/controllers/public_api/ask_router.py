"""Public API endpoint for programmatic access via API keys.

Separate from the frontend endpoint (/query/smart) — uses Bearer token
auth instead of the shared BACKEND_API_KEY, has its own rate limiting
per plan, and does NOT save conversations.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

import app.application.pipeline.nodes as nodes_pkg
from app.application.api_key_service import check_rate_limit, verify_api_key
from app.application.pipeline.nodes import PipelineDeps
from app.application.pipeline.state import OpenArgState
from app.domain.entities.api_key.api_key import ApiUsage
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.presentation.http.controllers.query import smart_query_v2_router as smart_router
from app.presentation.http.controllers.query.smart_query_v2_router import _get_or_compile_graph

logger = logging.getLogger(__name__)

router = APIRouter(tags=["public-api"])


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=10000)


@router.post("/ask")
@inject
async def public_ask(
    request: Request,
    body: AskRequest,
    deps: FromDishka[PipelineDeps],
    cache: FromDishka[ICacheService],
    api_key_repo: FromDishka[IApiKeyRepository],
) -> dict[str, Any]:
    """Execute a query using a public API key.

    Auth: ``Authorization: Bearer oarg_sk_xxx``
    Rate limited per plan (free: 5/min, 10/day).
    Does NOT save conversations.
    """
    # 1. Authenticate Bearer token
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    token = auth_header[7:].strip()
    api_key = await verify_api_key(token, api_key_repo)

    # 2. Rate limit check (per-key + per-IP + global free cap)
    client_ip = request.client.host if request.client else ""
    rate_info = await check_rate_limit(api_key, cache, client_ip=client_ip)

    # 3. Reuse the same compiled graph as the frontend endpoint
    checkpointer = smart_router._checkpointer
    compiled_graph = await _get_or_compile_graph(deps, checkpointer)
    nodes_pkg.set_deps(deps)

    # 4. Run pipeline
    start_time = time.monotonic()
    initial_state: OpenArgState = {
        "question": body.question,
        "user_id": f"apikey:{api_key.id}",
        "conversation_id": "",
        "policy_mode": False,
        "replan_count": 0,
    }

    try:
        async with asyncio.timeout(30):
            result = await compiled_graph.ainvoke(initial_state)
    except TimeoutError:
        logger.error("Pipeline timeout for API key %s", api_key.key_prefix)
        await _log_usage(api_key_repo, api_key.id, body.question, 408, 0, 0)
        raise HTTPException(status_code=408, detail="Request timed out")
    except Exception:
        logger.exception("Pipeline failed for API key %s", api_key.key_prefix)
        await _log_usage(api_key_repo, api_key.id, body.question, 500, 0, 0)
        raise HTTPException(status_code=500, detail="Pipeline execution failed")

    duration_ms = int((time.monotonic() - start_time) * 1000)
    tokens_used = result.get("tokens_used", 0)

    # 5. Log usage (post-pipeline, fire-and-forget errors)
    await _log_usage(api_key_repo, api_key.id, body.question, 200, tokens_used, duration_ms)

    try:
        await api_key_repo.update_last_used(api_key.id)
    except Exception:
        logger.debug("Failed to update last_used_at", exc_info=True)

    # 6. Build response
    return {
        "answer": result.get("clean_answer", ""),
        "sources": result.get("sources", []),
        "chart_data": result.get("chart_data"),
        "map_data": result.get("map_data"),
        "confidence": result.get("confidence", 1.0),
        "citations": result.get("citations", []),
        "warnings": result.get("warnings", []),
        "usage": {
            "tokens": tokens_used,
            "duration_ms": duration_ms,
            "plan": api_key.plan,
            "requests_remaining_today": rate_info["remaining_day"],
            "requests_remaining_minute": rate_info["remaining_minute"],
        },
    }


async def _log_usage(
    repo: IApiKeyRepository,
    key_id: Any,
    question: str,
    status_code: int,
    tokens_used: int,
    duration_ms: int,
) -> None:
    try:
        await repo.record_usage(
            ApiUsage(
                api_key_id=key_id,
                endpoint="/api/v1/ask",
                question=question[:200],
                status_code=status_code,
                tokens_used=tokens_used,
                duration_ms=duration_ms,
            )
        )
    except Exception:
        logger.debug("Failed to log API usage", exc_info=True)
