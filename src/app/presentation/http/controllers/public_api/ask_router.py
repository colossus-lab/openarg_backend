"""Public API endpoint for programmatic access via API keys.

Separate from the frontend endpoint (/query/smart) — uses Bearer token
auth instead of the shared BACKEND_API_KEY, has its own rate limiting
per plan, and does NOT save conversations.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any
from uuid import uuid4

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

import app.application.pipeline.nodes as nodes_pkg
from app.application.api_key_service import check_rate_limit, verify_api_key
from app.application.pipeline.nodes import PipelineDeps
from app.application.pipeline.state import OpenArgState
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.presentation.http.controllers.public_api.usage_log import log_rejection, log_usage
from app.presentation.http.controllers.query.smart_query_v2_router import (
    _get_checkpointer,
    _get_or_compile_graph,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["public-api"])

_ENDPOINT = "/api/v1/ask"
_TOOL = "consultar_datos_publicos"

_DEFAULT_TIMEOUT_SECONDS = 30


def _pipeline_timeout() -> float:
    try:
        value = float(os.getenv("PUBLIC_API_TIMEOUT_SECONDS", ""))
    except ValueError:
        return _DEFAULT_TIMEOUT_SECONDS
    return value if value > 0 else _DEFAULT_TIMEOUT_SECONDS


async def authenticate_bearer(request: Request, repo: IApiKeyRepository) -> ApiKey:
    """Validate ``Authorization: Bearer oarg_sk_…`` and return the key."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return await verify_api_key(auth_header[7:].strip(), repo)


class AskRequest(BaseModel):
    # CONTRACT-02 (round v46): extra='forbid' makes API drift visible.
    # A future Bearer client that posts an unrecognised field gets 422
    # instead of having the field silently dropped.
    model_config = ConfigDict(extra="forbid")
    question: str = Field(..., min_length=1, max_length=10000)


@router.post("/ask")
@inject
async def public_ask(
    request: Request,
    body: AskRequest,
    deps: FromDishka[PipelineDeps],
    cache: FromDishka[ICacheService],
    api_key_repo: FromDishka[IApiKeyRepository],
    credits: FromDishka[ICreditRepository],
) -> dict[str, Any]:
    """Execute a query using a public API key.

    Auth: ``Authorization: Bearer oarg_sk_xxx``
    Rate limited: 2/min, a monthly allowance (10 questions free, 100 for
    Fundadores, then credits; see ``app.application.public_quota``), plus a
    per-IP daily limit and a shared daily cap for the free plan.
    Does NOT save conversations.
    """
    # 1. Authenticate Bearer token
    api_key = await authenticate_bearer(request, api_key_repo)

    # 2. Rate limit check (per-key + per-IP + global free cap)
    client_ip = request.client.host if request.client else ""
    try:
        rate_info = await check_rate_limit(api_key, cache, client_ip=client_ip, credits=credits)
    except HTTPException as exc:
        # Sin cupo: antes no quedaba rastro, y es la mejor señal de demanda.
        await log_rejection(
            api_key_repo,
            cache,
            api_key,
            request,
            endpoint=_ENDPOINT,
            mode="respuestas",
            tool=_TOOL,
            status_code=exc.status_code,
            question=body.question,
        )
        raise

    # 3. Reuse the same compiled graph as the frontend endpoint.
    # `_get_checkpointer()` (no el atributo del módulo) reintenta la init
    # después del TTL, igual que en /smart.
    checkpointer = await _get_checkpointer()
    compiled_graph = await _get_or_compile_graph(deps, checkpointer)
    nodes_pkg.set_deps(deps)

    # 4. Run pipeline
    start_time = time.monotonic()
    initial_state: OpenArgState = {
        "question": body.question,
        "user_id": f"apikey:{api_key.id}",
        "conversation_id": "",
        # La API pública no expone el modo profundo.
        "mode": "normal",
        "replan_count": 0,
    }

    # Un grafo compilado con checkpointer rechaza la invocación sin
    # `thread_id` (`ValueError: Checkpointer requires ... 'configurable'
    # keys`), y eso salía como un 500 en TODAS las consultas de la API
    # pública. Es el mismo arreglo que tiene /smart: un thread efímero por
    # request, que no se reutiliza porque la API no guarda conversaciones.
    invoke_config: dict[str, Any] = {}
    if checkpointer:
        invoke_config["configurable"] = {"thread_id": f"efimero-{uuid4()}"}

    try:
        async with asyncio.timeout(_pipeline_timeout()):
            result = await compiled_graph.ainvoke(initial_state, config=invoke_config)
    except TimeoutError:
        logger.error("Pipeline timeout for API key %s", api_key.key_prefix)
        await _usage(api_key_repo, api_key, request, body.question, 408, 0, 0)
        raise HTTPException(status_code=408, detail="Request timed out")
    except Exception:
        logger.exception("Pipeline failed for API key %s", api_key.key_prefix)
        await _usage(api_key_repo, api_key, request, body.question, 500, 0, 0)
        raise HTTPException(status_code=500, detail="Pipeline execution failed")

    duration_ms = int((time.monotonic() - start_time) * 1000)
    tokens_used = result.get("tokens_used", 0)

    # Injection blocked → 400, como en /smart. Sin esto la API devolvía un
    # 200 con la respuesta de rechazo como si fuera un dato.
    if result.get("plan_intent", "") == "injection_blocked":
        await _usage(api_key_repo, api_key, request, body.question, 400, 0, duration_ms)
        raise HTTPException(status_code=400, detail="Potential prompt injection detected")

    # 5. Log usage (post-pipeline, fire-and-forget errors)
    await _usage(api_key_repo, api_key, request, body.question, 200, tokens_used, duration_ms)

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
        "citations": result.get("citations", []),
        "warnings": result.get("warnings", []),
        "usage": {
            "tokens": tokens_used,
            "duration_ms": duration_ms,
            "plan": api_key.plan,
            # Del mes desde el 30-sep-2026; el nombre viejo queda por compatibilidad.
            "requests_remaining_today": rate_info["remaining_day"],
            "requests_remaining_month": rate_info["remaining_month"],
            "limit_month": rate_info["limit_month"],
            "quota_resets_at": rate_info["quota_resets_at"],
            "used_credit": rate_info["used_credit"],
            "tier": rate_info["tier"],
            "founder_until": rate_info["founder_until"],
            "requests_remaining_minute": rate_info["remaining_minute"],
        },
    }


async def _usage(
    repo: IApiKeyRepository,
    api_key: ApiKey,
    request: Request,
    question: str,
    status_code: int,
    tokens_used: int,
    duration_ms: int,
) -> None:
    await log_usage(
        repo,
        api_key,
        request,
        endpoint=_ENDPOINT,
        mode="respuestas",
        tool=_TOOL,
        status_code=status_code,
        question=question,
        tokens_used=tokens_used,
        duration_ms=duration_ms,
    )
