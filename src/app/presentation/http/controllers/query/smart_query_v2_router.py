"""Smart query router — LangGraph pipeline endpoint.

Canonical /smart and /ws/smart endpoints running the LangGraph pipeline.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import secrets as _secrets_mod
import threading
from typing import Any

from dishka import AsyncContainer
from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import ORJSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from app.application.pipeline.graph import build_pipeline_graph
from app.application.pipeline.nodes import PipelineDeps, set_deps
from app.application.pipeline.state import OpenArgState
from app.domain.ports.cache.cache_port import ICacheService
from app.infrastructure.audit.audit_logger import audit_rate_limited
from app.setup.app_factory import limiter

# Module-level cache for compiled graph (compile once, reuse)
_graph_lock = threading.Lock()
_checkpointer_lock = asyncio.Lock()
_compiled_graph = None
_checkpointer = None  # AsyncPostgresSaver instance (lazy)
_checkpointer_attempted = False  # Prevent repeated init attempts

logger = logging.getLogger(__name__)


async def _get_checkpointer():
    """Lazily create an ``AsyncPostgresSaver`` if DATABASE_URL is set.

    Returns the singleton checkpointer or *None* when checkpointing is
    unavailable (missing dependency or missing env var).
    Thread-safe via asyncio.Lock with double-check pattern.
    """
    global _checkpointer, _checkpointer_attempted  # noqa: PLW0603

    if _checkpointer is not None:
        return _checkpointer
    if _checkpointer_attempted:
        return None  # Already tried and failed — don't retry

    async with _checkpointer_lock:
        # Double-check after acquiring lock
        if _checkpointer is not None:
            return _checkpointer
        if _checkpointer_attempted:
            return None

        _checkpointer_attempted = True

        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            return None

        try:
            from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

            conn_str = db_url.replace("postgresql+psycopg://", "postgresql://")
            saver = AsyncPostgresSaver.from_conn_string(conn_str)
            await saver.setup()
            _checkpointer = saver
            logger.info("LangGraph checkpointer initialised (PostgreSQL)")
            return saver
        except Exception:
            logger.warning(
                "LangGraph checkpointer not available — running without persistence",
                exc_info=True,
            )
            return None


router = APIRouter(prefix="/query", tags=["smart-query"])

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def _verify_api_key(api_key: str | None = Depends(_api_key_header)) -> None:
    """Validate API key for POST endpoints. Skip if BACKEND_API_KEY is not set."""
    expected = os.getenv("BACKEND_API_KEY", "")
    if not expected:
        return
    if not api_key or not _secrets_mod.compare_digest(api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


class SmartQueryV2Request(BaseModel):
    question: str = Field(..., min_length=1, max_length=10000)
    user_email: str | None = None
    conversation_id: str | None = None
    policy_mode: bool = False


class SmartQueryV2Response(BaseModel):
    answer: str
    sources: list[dict[str, Any]]
    chart_data: list[dict[str, Any]] | None = None
    tokens_used: int = 0
    confidence: float = 1.0
    citations: list[dict[str, Any]] = []
    documents: list[dict[str, Any]] | None = None
    warnings: list[str] = []


# ── POST endpoint ──────────────────────────────────────────


@router.post("/smart", response_model=SmartQueryV2Response, dependencies=[Depends(_verify_api_key)])
@limiter.limit("10/minute;50/day")  # type: ignore[untyped-decorator]
@inject  # type: ignore[untyped-decorator]
async def smart_query_v2(
    request: Request,
    body: SmartQueryV2Request,
    deps: FromDishka[PipelineDeps],
) -> dict[str, Any] | ORJSONResponse:
    """Execute a query through the LangGraph pipeline."""
    global _compiled_graph  # noqa: PLW0603

    # Compile graph once (thread-safe), set deps per-request (ContextVar-safe)
    checkpointer = await _get_checkpointer()
    if _compiled_graph is None:
        with _graph_lock:
            if _compiled_graph is None:
                _compiled_graph = build_pipeline_graph(deps, checkpointer=checkpointer)
    set_deps(deps)

    user_id = body.user_email or "anonymous"
    conversation_id = body.conversation_id or ""

    initial_state: OpenArgState = {
        "question": body.question,
        "user_id": user_id,
        "conversation_id": conversation_id,
        "policy_mode": body.policy_mode,
        "replan_count": 0,
    }

    # When a checkpointer is active, pass thread_id so LangGraph
    # persists state per conversation (enables memory / resumable runs).
    invoke_config: dict[str, Any] = {}
    if checkpointer and conversation_id:
        invoke_config["configurable"] = {"thread_id": conversation_id}

    try:
        result = await _compiled_graph.ainvoke(initial_state, config=invoke_config)
    except Exception:
        logger.exception("LangGraph pipeline failed")
        return ORJSONResponse(
            status_code=500,
            content={"error": {"code": "PIPELINE_ERROR", "message": "Pipeline execution failed"}},
        )

    # Injection blocked → return 400
    plan_intent = result.get("plan_intent", "")
    if plan_intent == "injection_blocked":
        from app.infrastructure.adapters.search.prompt_injection_detector import is_suspicious

        _, score = is_suspicious(body.question)
        return ORJSONResponse(
            status_code=400,
            content={
                "error": {
                    "code": "SEC_001",
                    "message": "Potential prompt injection detected",
                    "details": {"score": round(score, 3)},
                }
            },
        )

    return {
        "answer": result.get("clean_answer", ""),
        "sources": result.get("sources", []),
        "chart_data": result.get("chart_data"),
        "tokens_used": result.get("tokens_used", 0),
        "confidence": result.get("confidence", 1.0),
        "citations": result.get("citations", []),
        **({"documents": result.get("documents")} if result.get("documents") else {}),
        **({"warnings": result.get("warnings")} if result.get("warnings") else {}),
    }


# ── WebSocket rate limit helper ────────────────────────────


async def _check_ws_rate_limit(cache: ICacheService, identifier: str) -> bool:
    """Return True if the identifier has exceeded the WS rate limit."""
    key = f"ws_rate:{identifier}"
    try:
        current = await cache.get(key)
        if current is not None:
            count = int(current) if not isinstance(current, int) else current
            if count >= 20:
                return True
            await cache.set(key, count + 1, ttl_seconds=60)
        else:
            await cache.set(key, 1, ttl_seconds=60)
        return False
    except Exception:
        return False


def _validate_api_key_value(provided: str) -> bool:
    """Validate an API key value against BACKEND_API_KEY."""
    import secrets as _secrets

    expected = os.getenv("BACKEND_API_KEY", "")
    if not expected:
        return True
    return _secrets.compare_digest(provided, expected) if provided else False


# ── WebSocket endpoint ─────────────────────────────────────


@router.websocket("/ws/smart")
async def ws_smart_query_v2(ws: WebSocket) -> None:
    """Stream the LangGraph pipeline via WebSocket."""
    # Try query-param auth first (backward compat)
    import secrets as _secrets

    expected = os.getenv("BACKEND_API_KEY", "")
    provided = ws.query_params.get("api_key", "")
    has_query_param_auth = not expected or (
        _secrets.compare_digest(provided, expected) if provided else False
    )

    await ws.accept()

    try:
        container: AsyncContainer = ws.app.state.dishka_container
        async with container() as session_scope:
            async with session_scope() as request_scope:
                cache = await request_scope.get(ICacheService)
                deps = await request_scope.get(PipelineDeps)

                # Compile graph once (thread-safe), set deps per-request
                global _compiled_graph  # noqa: PLW0603
                checkpointer = await _get_checkpointer()
                if _compiled_graph is None:
                    with _graph_lock:
                        if _compiled_graph is None:
                            _compiled_graph = build_pipeline_graph(deps, checkpointer=checkpointer)
                set_deps(deps)
                graph = _compiled_graph

                raw_text = await ws.receive_text()
                if len(raw_text) > 10_000:
                    await ws.send_json({"type": "error", "message": "Message too large (max 10KB)"})
                    await ws.close(code=4400)
                    return

                raw = json.loads(raw_text)

                # Validate API key
                if not has_query_param_auth:
                    msg_api_key = raw.get("api_key", "")
                    if not _validate_api_key_value(msg_api_key):
                        await ws.send_json(
                            {"type": "error", "message": "Invalid or missing API key"}
                        )
                        await ws.close(code=4401)
                        return

                # Rate limiting
                ws_identifier = raw.get("user_email") or (
                    ws.client.host if ws.client else "unknown"
                )
                if await _check_ws_rate_limit(cache, ws_identifier):
                    audit_rate_limited(user=ws_identifier, endpoint="ws/smart")
                    await ws.send_json({"type": "error", "message": "Rate limit exceeded"})
                    await ws.close(code=4429)
                    return

                question = raw.get("question", "")
                conversation_id = raw.get("conversation_id", "")
                policy_mode = raw.get("policy_mode", False)

                if not question or len(question) > 10000:
                    await ws.send_json({"type": "error", "message": "question is required"})
                    await ws.close()
                    return

                initial_state: OpenArgState = {
                    "question": question,
                    "user_id": ws_identifier,
                    "conversation_id": conversation_id,
                    "policy_mode": policy_mode,
                }

                # When a checkpointer is active, pass thread_id for persistence
                stream_config: dict[str, Any] = {}
                if checkpointer and conversation_id:
                    stream_config["configurable"] = {"thread_id": conversation_id}

                # Stream the graph execution
                async for mode, payload in graph.astream(
                    initial_state,
                    config=stream_config,
                    stream_mode=["updates", "custom"],
                ):
                    if mode == "custom":
                        # Custom events emitted by nodes via get_stream_writer()
                        # Filter to allowed fields only to prevent leaking internal
                        # data like prompts or tracebacks (SEC-07 audit fix)
                        _allowed = {
                            "type",
                            "step",
                            "detail",
                            "progress",
                            "message",
                            "status",
                            "content",
                            "question",
                            "options",
                        }
                        safe_payload = (
                            {k: v for k, v in payload.items() if k in _allowed}
                            if isinstance(payload, dict)
                            else payload
                        )
                        await ws.send_json(safe_payload)
                    elif mode == "updates":
                        # Node completed — check if it's a terminal node
                        for node_name, update in payload.items():
                            if node_name in ("fast_reply", "cache_reply", "clarify_reply"):
                                # Terminal node — send complete event
                                await ws.send_json(
                                    {
                                        "type": "complete",
                                        "answer": update.get("clean_answer", ""),
                                        "sources": update.get("sources", []),
                                        "chart_data": update.get("chart_data"),
                                        "confidence": update.get("confidence", 1.0),
                                        "citations": update.get("citations", []),
                                        "documents": update.get("documents"),
                                    }
                                )
                            elif node_name == "finalize":
                                # Pipeline complete — get full state
                                await ws.send_json(
                                    {
                                        "type": "complete",
                                        "answer": update.get("clean_answer", ""),
                                        "sources": update.get("sources", []),
                                        "chart_data": update.get("chart_data"),
                                        "confidence": update.get("confidence", 1.0),
                                        "citations": update.get("citations", []),
                                        "documents": update.get("documents"),
                                        "warnings": update.get("warnings", []),
                                    }
                                )

    except WebSocketDisconnect:
        logger.debug("WebSocket v2 client disconnected")
    except Exception:
        logger.exception("WebSocket v2 error")
        with contextlib.suppress(Exception):
            await ws.send_json({"type": "error", "message": "Internal error"})
    finally:
        with contextlib.suppress(Exception):
            await ws.close()
