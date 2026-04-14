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
from contextlib import AsyncExitStack
from typing import Any

from dishka import AsyncContainer
from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from app.application.common.privacy_gate import ensure_privacy_accepted
from app.application.pipeline.graph import build_pipeline_graph
from app.application.pipeline.nodes import PipelineDeps, set_deps
from app.application.pipeline.state import OpenArgState
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.user.user_repository import IUserRepository
from app.infrastructure.audit.audit_logger import audit_rate_limited
from app.infrastructure.serialization import safe_dumps, to_json_safe
from app.setup.app_factory import limiter

# Module-level cache for compiled graph (compile once, reuse)
_compiled_graphs_lock = asyncio.Lock()
_checkpointer_lock = asyncio.Lock()
_compiled_graphs: dict[bool, Any] = {}
_checkpointer = None  # AsyncPostgresSaver instance (lazy)
_checkpointer_stack: AsyncExitStack | None = None
_checkpointer_attempted = False  # Prevent repeated init attempts

logger = logging.getLogger(__name__)

# FR-038 / FR-038a: module-level allowlist of payload fields the streaming
# endpoint is willing to forward to the browser. Fail-closed — anything
# NOT in this set is dropped before the payload reaches the WebSocket.
# This is a security surface (SEC-07 audit fix): node-emitted dicts have
# historically included prompts, tracebacks, and internal state when
# developers forgot to filter. Keeping this as a single module-level
# constant makes membership discoverable from one place.
#
# When you add a new field to a streamable event, add it here too.
# FR-038b guarantees you will see a WARNING log on any dropped key, so
# you'll know immediately if you forgot.
_STREAM_ALLOWED_PAYLOAD_KEYS: frozenset[str] = frozenset(
    {
        "type",
        "step",
        "detail",
        "progress",
        "message",
        "status",
        "content",
        "question",
        "options",
        "map_data",
        "connector",
    }
)

_COMPLETE_EVENT_KEYS: tuple[str, ...] = (
    "answer",
    "sources",
    "chart_data",
    "map_data",
    "confidence",
    "citations",
    "documents",
    "warnings",
)

_TERMINAL_COMPLETE_NODES: frozenset[str] = frozenset(
    {
        "finalize",
        "cache_reply",
        "fast_reply",
    }
)


def _build_complete_event(node_name: str, update: Any) -> dict[str, Any] | None:
    """Return a browser ``complete`` event from a terminal-looking update."""
    if not isinstance(update, dict):
        return None
    if node_name not in _TERMINAL_COMPLETE_NODES:
        return None
    if "clean_answer" not in update:
        return None
    return {
        "type": "complete",
        "answer": update.get("clean_answer", ""),
        "sources": update.get("sources", []),
        "chart_data": update.get("chart_data"),
        "map_data": update.get("map_data"),
        "confidence": update.get("confidence", 1.0),
        "citations": update.get("citations", []),
        "documents": update.get("documents"),
        "warnings": update.get("warnings", []),
    }


async def _safe_send_json(ws: WebSocket, payload: Any) -> None:
    """Send ``payload`` as JSON text, absorbing non-primitive values.

    Starlette's ``WebSocket.send_json`` calls ``json.dumps`` internally
    without a ``default=`` hook, so any ``datetime`` / ``Decimal`` / ``UUID``
    / ``bytes`` that sneaks into the state aborts the ``complete`` event
    with ``TypeError`` and the browser sees *"respuesta no disponible"*.
    Normalize once with :func:`to_json_safe`, then serialize once with
    :func:`safe_dumps` — the goal is that the WebSocket send path never
    crashes on a common Python type and avoids retrying a failed dump in
    this hot path.

    See ``specs/FIX_BACKLOG.md#FIX-017``.
    """
    text = safe_dumps(to_json_safe(payload), ensure_ascii=False)
    await ws.send_text(text)


def _filter_stream_payload(payload: Any) -> Any:
    """FR-038 + FR-038b: drop non-allowlisted keys and log a WARNING per drop.

    Non-dict payloads pass through unchanged (defensive: LangGraph may
    emit sentinel values). Dict payloads are filtered to
    ``_STREAM_ALLOWED_PAYLOAD_KEYS`` so no internal state leaks to the
    browser. Keys that were dropped are logged once per call with the
    payload's ``type`` (if present) so developers can trace why a new
    node's field is not showing up in the frontend — the fix is to add
    the field to the allowlist above, not to disable the filter.

    DEBT-017 fix, 2026-04-11. See spec
    ``specs/001-query-pipeline/001e-finalization/spec.md`` FR-038/a/b.
    """
    if not isinstance(payload, dict):
        return payload
    dropped = [k for k in payload if k not in _STREAM_ALLOWED_PAYLOAD_KEYS]
    if dropped:
        logger.warning(
            "stream_payload dropped keys %s (type=%r) — add them to "
            "_STREAM_ALLOWED_PAYLOAD_KEYS if they should reach the browser",
            sorted(dropped),
            payload.get("type"),
        )
    return {k: v for k, v in payload.items() if k in _STREAM_ALLOWED_PAYLOAD_KEYS}


async def _get_or_compile_graph(deps: PipelineDeps, checkpointer=None):  # type: ignore[no-untyped-def]
    """Return the compiled graph, compiling it once (thread-safe)."""
    global _compiled_graphs  # noqa: PLW0603
    cache_key = bool(checkpointer)
    if cache_key not in _compiled_graphs:
        async with _compiled_graphs_lock:
            if cache_key not in _compiled_graphs:
                _compiled_graphs[cache_key] = build_pipeline_graph(
                    deps, checkpointer=checkpointer
                )
    return _compiled_graphs[cache_key]


async def _open_checkpointer(conn_str: str) -> tuple[AsyncExitStack, Any]:
    """Open an AsyncPostgresSaver inside an AsyncExitStack."""
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    stack = AsyncExitStack()
    saver = await stack.enter_async_context(AsyncPostgresSaver.from_conn_string(conn_str))
    return stack, saver


def _is_benign_checkpointer_setup_race(exc: Exception) -> bool:
    """Return True for the known concurrent setup race on checkpoint migrations."""
    message = str(exc)
    return (
        "checkpoint_migrations_pkey" in message
        and "duplicate key value violates unique constraint" in message
    )


async def _get_checkpointer():
    """Lazily create an ``AsyncPostgresSaver`` if DATABASE_URL is set.

    Returns the singleton checkpointer or *None* when checkpointing is
    unavailable (missing dependency or missing env var).
    Thread-safe via asyncio.Lock with double-check pattern.
    """
    global _checkpointer, _checkpointer_attempted, _checkpointer_stack  # noqa: PLW0603

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
            conn_str = db_url.replace("postgresql+psycopg://", "postgresql://")
            stack, saver = await _open_checkpointer(conn_str)
            try:
                await saver.setup()
                logger.info("LangGraph checkpointer initialised (PostgreSQL)")
            except Exception as exc:
                if not _is_benign_checkpointer_setup_race(exc):
                    raise
                with contextlib.suppress(Exception):
                    await stack.aclose()
                stack, saver = await _open_checkpointer(conn_str)
                logger.info(
                    "LangGraph checkpointer initialised after concurrent setup race"
                )
            _checkpointer = saver
            _checkpointer_stack = stack
            return saver
        except Exception:
            if _checkpointer_stack is not None:
                with contextlib.suppress(Exception):
                    await _checkpointer_stack.aclose()
                _checkpointer_stack = None
            logger.warning(
                "LangGraph checkpointer not available — running without persistence",
                exc_info=True,
            )
            return None


async def init_pipeline_persistence() -> None:
    """Warm up the optional LangGraph checkpointer during app startup."""
    await _get_checkpointer()


async def shutdown_pipeline_persistence() -> None:
    """Release app-scoped persistence resources on shutdown."""
    global _checkpointer, _checkpointer_stack, _checkpointer_attempted, _compiled_graphs  # noqa: PLW0603

    async with _checkpointer_lock:
        stack = _checkpointer_stack
        _checkpointer = None
        _checkpointer_stack = None
        _checkpointer_attempted = False
        _compiled_graphs = {}

    if stack is not None:
        with contextlib.suppress(Exception):
            await stack.aclose()


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
    map_data: dict[str, Any] | None = None
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
    user_repo: FromDishka[IUserRepository],
) -> dict[str, Any] | JSONResponse:
    """Execute a query through the LangGraph pipeline."""
    # Server-side privacy gate (defense in depth — the frontend also checks).
    await ensure_privacy_accepted(body.user_email, user_repo)

    # Compile graph once (thread-safe), set deps per-request (ContextVar-safe)
    checkpointer = _checkpointer
    compiled_graph = await _get_or_compile_graph(deps, checkpointer)
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
        result = await compiled_graph.ainvoke(initial_state, config=invoke_config)
    except Exception:
        logger.exception("LangGraph pipeline failed")
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "PIPELINE_ERROR", "message": "Pipeline execution failed"}},
        )

    # Injection blocked → return 400
    plan_intent = result.get("plan_intent", "")
    if plan_intent == "injection_blocked":
        from app.infrastructure.adapters.search.prompt_injection_detector import is_suspicious

        _, score = is_suspicious(body.question)
        return JSONResponse(
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
        "map_data": result.get("map_data"),
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
                checkpointer = _checkpointer
                set_deps(deps)
                graph = await _get_or_compile_graph(deps, checkpointer)

                raw_text = await ws.receive_text()
                if len(raw_text) > 10_000:
                    await _safe_send_json(
                        ws, {"type": "error", "message": "Message too large (max 10KB)"}
                    )
                    await ws.close(code=4400)
                    return

                raw = json.loads(raw_text)

                # Validate API key
                if not has_query_param_auth:
                    msg_api_key = raw.get("api_key", "")
                    if not _validate_api_key_value(msg_api_key):
                        await _safe_send_json(
                            ws, {"type": "error", "message": "Invalid or missing API key"}
                        )
                        await ws.close(code=4401)
                        return

                # Rate limiting
                ws_identifier = raw.get("user_email") or (
                    ws.client.host if ws.client else "unknown"
                )
                if await _check_ws_rate_limit(cache, ws_identifier):
                    audit_rate_limited(user=ws_identifier, endpoint="ws/smart")
                    await _safe_send_json(ws, {"type": "error", "message": "Rate limit exceeded"})
                    await ws.close(code=4429)
                    return

                # Server-side privacy gate (defense in depth).
                ws_user_email = raw.get("user_email") or ""
                if ws_user_email:
                    user_repo = await request_scope.get(IUserRepository)
                    try:
                        await ensure_privacy_accepted(ws_user_email, user_repo)
                    except HTTPException as exc:
                        detail = (
                            exc.detail
                            if isinstance(exc.detail, dict)
                            else {"message": str(exc.detail)}
                        )
                        await _safe_send_json(ws, {"type": "error", **detail})
                        await ws.close(code=4403)
                        return

                question = raw.get("question", "")
                conversation_id = raw.get("conversation_id", "")
                policy_mode = raw.get("policy_mode", False)

                if not question or len(question) > 10000:
                    await _safe_send_json(ws, {"type": "error", "message": "question is required"})
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
                        # Custom events emitted by nodes via get_stream_writer().
                        # _filter_stream_payload applies FR-038 (fail-closed
                        # allowlist, SEC-07) AND FR-038b (WARNING log on any
                        # dropped key — DEBT-017 fix 2026-04-11).
                        await _safe_send_json(ws, _filter_stream_payload(payload))
                    elif mode == "updates":
                        # Node completed — check if it's a terminal node
                        for node_name, update in payload.items():
                            complete_event = _build_complete_event(node_name, update)
                            if complete_event is None:
                                logger.debug(
                                    "Ignoring non-terminal stream update from node %s",
                                    node_name,
                                )
                                continue
                            await _safe_send_json(ws, complete_event)

    except WebSocketDisconnect:
        logger.debug("WebSocket v2 client disconnected")
    except Exception:
        logger.exception("WebSocket v2 error")
        with contextlib.suppress(Exception):
            await _safe_send_json(ws, {"type": "error", "message": "Internal error"})
    finally:
        with contextlib.suppress(Exception):
            await ws.close()
