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
import time
from contextlib import AsyncExitStack
from typing import Any

from dishka import AsyncContainer
from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field

from app.application.common.privacy_gate import ensure_privacy_accepted
from app.application.pipeline.graph import build_pipeline_graph
from app.application.pipeline.nodes import PipelineDeps, set_deps
from app.application.pipeline.state import OpenArgState
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.chat.chat_repository import IChatRepository
from app.domain.ports.user.user_repository import IUserRepository
from app.infrastructure.audit.audit_logger import audit_rate_limited
from app.infrastructure.auth import GoogleJwtValidator, InvalidGoogleToken
from app.infrastructure.serialization import safe_dumps, to_json_safe
from app.presentation.http.middleware.google_jwt_middleware import (
    get_request_user_email,
)
from app.setup.app_factory import limiter

# Module-level cache for compiled graph (compile once, reuse)
_compiled_graphs_lock = asyncio.Lock()
_checkpointer_lock = asyncio.Lock()
_compiled_graphs: dict[bool, Any] = {}
_checkpointer = None  # AsyncPostgresSaver instance (lazy)
_checkpointer_stack: AsyncExitStack | None = None
_checkpointer_attempted = False  # Most-recent init attempt happened
_checkpointer_last_attempt_ts: float = 0.0  # epoch seconds of last attempt
# Re-attempt the lazy init at most every N seconds. A transient DB blip at
# boot used to leave persistence permanently off until process restart;
# this TTL lets the next request retry on its own.
_CHECKPOINTER_RETRY_TTL_SECONDS = 30.0

# The checkpointer used to run on a single `psycopg.AsyncConnection`, opened
# once by `AsyncPostgresSaver.from_conn_string()` and kept for the lifetime of
# the process. That connection had no pre-ping, no recycle and no reconnect,
# and `_get_checkpointer()` handed the same object back forever, so the first
# time anything closed it authenticated chat stayed broken until the next
# deploy. Prod ran 68 days on one connection; an RDS OOM-kill on 2026-07-30
# 00:21 UTC dropped it and every logged-in WebSocket failed for five hours
# with `OperationalError('the connection is closed')`.
#
# A pool with `check` revalidates on every checkout, which is what
# `pool_pre_ping` does for the SQLAlchemy engine — that flag lives on a
# *different* engine (`persistence_sqla/provider.py`) and never covered this
# connection.
_CHECKPOINTER_POOL_MIN_SIZE = 1
_CHECKPOINTER_POOL_MAX_SIZE = 4
# Both ceilings stay below the pgbouncer timeouts in front of RDS
# (`server_idle_timeout = 300`, `server_lifetime` at its 3600s default) so the
# pool retires a connection before pgbouncer drops it unannounced.
_CHECKPOINTER_CONN_MAX_LIFETIME_S = 900.0
_CHECKPOINTER_CONN_MAX_IDLE_S = 120.0
# Bounds how long a checkout waits for a connection. Keeps `init_pipeline
# _persistence()` from stalling startup when the database is unreachable —
# the retry TTL above is what recovers from that, not a longer wait.
_CHECKPOINTER_POOL_TIMEOUT_S = 15.0

# BUG-022: how often the WebSocket emits a keepalive frame during a long
# pipeline step. Must be well below the tightest consumer idle timeout
# (the frontend bridge's per-message activity timer, proxies, and test
# clients' per-receive timeouts all sit at 30s+).
_WS_KEEPALIVE_INTERVAL_S = 15.0

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
    "citations",
    "documents",
    "warnings",
    # CONTRACT-03 (round v46): tokens_used was already in the HTTP
    # response of POST /smart but missing from the WS `complete` event,
    # so SPA telemetry that watches LLM cost went silent on the
    # streaming path. confidence was intentionally removed from the API
    # (commit acc884a) and is NOT restored here — the chip is gone from
    # the UI and the pipeline still computes it internally.
    "tokens_used",
)

_TERMINAL_COMPLETE_NODES: frozenset[str] = frozenset(
    {
        "finalize",
        "cache_reply",
        "fast_reply",
        # 2026-05-14: `clarify_reply` also produces `clean_answer` and is
        # a terminal node when the planner returns `intent="clarification"`.
        # Without it, ambiguous queries (e.g. "Cuáles son los proveedores
        # con más contratos en BAC?" — planner asks user to clarify what
        # 'BAC' means) emitted only the custom `clarification` event and
        # the WS closed without a `complete`. Frontends handle the
        # clarification chips event; API/QA consumers that don't subscribe
        # to it saw a graceful 1000 OK close with no payload and reported
        # it as a transport error.
        "clarify_reply",
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
        "citations": update.get("citations", []),
        "documents": update.get("documents"),
        "warnings": update.get("warnings", []),
        # CONTRACT-03 (round v46): paridad con la response HTTP POST /smart,
        # que ya emite tokens_used. Sin esto el frontend ve siempre 0 en
        # el path streaming. confidence NO se incluye: fue removida de la
        # API deliberadamente (commit acc884a) — el pipeline la sigue
        # calculando internamente pero no la expone al cliente.
        "tokens_used": update.get("tokens_used", 0),
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
    try:
        await ws.send_text(text)
    except RuntimeError as exc:
        # BUG-022 Capa 3: when a client closes the WS before the server
        # finishes (Dante's runner has a hard per-receive timeout that
        # fires around 45s), Starlette raises RuntimeError("Cannot call
        # 'send' once a close message has been sent."). The pipeline
        # keeps running and tries to send 'complete' or 'keepalive' — we
        # swallow it instead of crashing with a stack trace. The
        # production WebSocket logs still surface the abnormal close at
        # the WebSocketDisconnect catch in the handler.
        if "close message has been sent" in str(exc) or "WebSocket is not connected" in str(exc):
            logger.debug("WS send after close — client disconnected mid-stream")
            return
        raise


async def _ws_keepalive(ws: WebSocket, send_lock: asyncio.Lock) -> None:
    """Emit a lightweight keepalive frame while the pipeline runs.

    BUG-022: a single long pipeline step (a slow connector, the analyst
    LLM call) can leave the WebSocket with zero traffic for 30-45s — long
    enough for an intermediary, the browser bridge, or a client's
    per-receive timeout to drop the connection mid-stream. A periodic
    keepalive keeps bytes flowing so the connection survives any idle
    timeout. Consumers that don't recognise the ``keepalive`` type ignore
    it harmlessly (the frontend bridge's event switch has no such case
    and falls through; its 120s activity timer is reset by any frame).

    BUG-022 Capa 3: stop the loop on the first failed send. If the WS
    closed mid-stream there's no point pinging a dead socket every 15s
    until ``astream`` finishes; bail out quietly.
    """
    try:
        while True:
            await asyncio.sleep(_WS_KEEPALIVE_INTERVAL_S)
            async with send_lock:
                try:
                    await _safe_send_json(ws, {"type": "keepalive"})
                except Exception:
                    logger.debug("WS keepalive send failed — stopping task")
                    return
    except asyncio.CancelledError:
        pass


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
                _compiled_graphs[cache_key] = build_pipeline_graph(deps, checkpointer=checkpointer)
    return _compiled_graphs[cache_key]


async def _open_checkpointer(conn_str: str) -> tuple[AsyncExitStack, Any]:
    """Open an AsyncPostgresSaver over a self-healing pool, in an exit stack.

    Deliberately *not* `AsyncPostgresSaver.from_conn_string()`: that helper
    opens one bare `AsyncConnection` and nothing ever revalidates or replaces
    it. `_ainternal.Conn` also accepts an `AsyncConnectionPool`, and
    `get_connection()` then acquires per operation — so `check` runs on every
    checkout and a connection killed between requests is replaced instead of
    poisoning the saver.
    """
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    from psycopg.rows import dict_row
    from psycopg_pool import AsyncConnectionPool

    stack = AsyncExitStack()
    pool: AsyncConnectionPool[Any] = AsyncConnectionPool(
        conninfo=conn_str,
        min_size=_CHECKPOINTER_POOL_MIN_SIZE,
        max_size=_CHECKPOINTER_POOL_MAX_SIZE,
        # AsyncPostgresSaver assumes all three on whatever connection it is
        # handed. `from_conn_string` set them; a pool will not unless told.
        kwargs={
            "autocommit": True,
            "prepare_threshold": 0,
            "row_factory": dict_row,
        },
        check=AsyncConnectionPool.check_connection,
        max_lifetime=_CHECKPOINTER_CONN_MAX_LIFETIME_S,
        max_idle=_CHECKPOINTER_CONN_MAX_IDLE_S,
        timeout=_CHECKPOINTER_POOL_TIMEOUT_S,
        # psycopg warns when a pool opens from its own constructor; the
        # AsyncExitStack owns the lifecycle instead.
        open=False,
    )
    await stack.enter_async_context(pool)
    saver = AsyncPostgresSaver(conn=pool)
    return stack, saver


def _checkpointer_is_live() -> bool:
    """True when the cached saver still has a usable source of connections.

    The pool revalidates individual connections itself, so the only state it
    cannot come back from is the pool being closed — shutdown, or an init that
    was torn down halfway.
    """
    if _checkpointer is None:
        return False
    conn = getattr(_checkpointer, "conn", None)
    return not getattr(conn, "closed", False)


async def _teardown_checkpointer_locked() -> None:
    """Drop the cached saver, its pool and the compiled graphs.

    Caller must hold `_checkpointer_lock`. `_compiled_graphs` has to go too: a
    compiled graph captures the saver object, so leaving it cached would keep
    routing requests at the saver we just discarded.
    """
    global _checkpointer, _checkpointer_stack, _compiled_graphs  # noqa: PLW0603

    stack = _checkpointer_stack
    _checkpointer = None
    _checkpointer_stack = None
    _compiled_graphs = {}
    if stack is not None:
        with contextlib.suppress(Exception):
            await stack.aclose()


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

    Retry behaviour: a failed init flips `_checkpointer_attempted=True`.
    Subsequent calls re-attempt after `_CHECKPOINTER_RETRY_TTL_SECONDS`
    so a transient DB blip at boot doesn't leave persistence off forever.

    That TTL only ever covered a failed *init*. A saver whose connection died
    later was still handed back forever, which is what kept prod's chat broken
    for five hours on 2026-07-30 — so a cached-but-dead saver is now treated
    as a miss and rebuilt.
    """
    global _checkpointer, _checkpointer_attempted, _checkpointer_stack  # noqa: PLW0603
    global _checkpointer_last_attempt_ts  # noqa: PLW0603

    if _checkpointer is not None and _checkpointer_is_live():
        return _checkpointer

    import time as _time

    now = _time.monotonic()
    if (
        _checkpointer_attempted
        and (now - _checkpointer_last_attempt_ts) < _CHECKPOINTER_RETRY_TTL_SECONDS
    ):
        return None  # Recently failed — back off

    async with _checkpointer_lock:
        # Double-check after acquiring lock
        if _checkpointer is not None:
            if _checkpointer_is_live():
                return _checkpointer
            logger.warning("LangGraph checkpointer pool is closed — discarding it and rebuilding")
            await _teardown_checkpointer_locked()
        now = _time.monotonic()
        if (
            _checkpointer_attempted
            and (now - _checkpointer_last_attempt_ts) < _CHECKPOINTER_RETRY_TTL_SECONDS
        ):
            return None

        _checkpointer_attempted = True
        _checkpointer_last_attempt_ts = now

        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            return None

        stack: AsyncExitStack | None = None
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
                stack = None
                stack, saver = await _open_checkpointer(conn_str)
                logger.info("LangGraph checkpointer initialised after concurrent setup race")
            _checkpointer = saver
            _checkpointer_stack = stack
            return saver
        except Exception:
            # Close the stack we just opened, not the global one. On this path
            # the global is still unset, so the previous version closed nothing
            # and leaked whatever `_open_checkpointer` had already opened.
            if stack is not None:
                with contextlib.suppress(Exception):
                    await stack.aclose()
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
    # CONTRACT-02 (round v46): extra='forbid' so any drift between the
    # frontend BFF and this contract surfaces as 422 instead of a
    # silent drop. The pre-fix BFF posted `history` here and Pydantic
    # ignored it — context never reached the planner on the HTTP
    # fallback path. The field is now ACCEPTED at the wire boundary
    # (so legacy BFF deploys don't break) but the handler still loads
    # history from the DB via conversation_id (post-H3 ownership
    # check). The body-supplied history is treated as advisory only.
    model_config = ConfigDict(extra="forbid")
    question: str = Field(..., min_length=1, max_length=10000)
    user_email: str | None = None
    conversation_id: str | None = None
    policy_mode: bool = False
    history: list[dict[str, Any]] | None = None


class SmartQueryV2Response(BaseModel):
    answer: str
    sources: list[dict[str, Any]]
    chart_data: list[dict[str, Any]] | None = None
    map_data: dict[str, Any] | None = None
    tokens_used: int = 0
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
    chat_repo: FromDishka[IChatRepository],
) -> dict[str, Any] | JSONResponse:
    """Execute a query through the LangGraph pipeline."""
    # H3 fix: authenticated email comes from the Google JWT validated by
    # GoogleJwtAuthMiddleware (`request.state.user_email`), NEVER from the
    # body. A body.user_email that disagrees with the JWT is rejected to
    # prevent caller spoofing on a shared BACKEND_API_KEY. If the JWT email
    # is missing (only possible if the middleware exempted this path —
    # which it doesn't for /smart in prod), fall back to body.user_email
    # for compatibility but block conversation_id usage.
    authed_email = get_request_user_email(request)
    body_email = (body.user_email or "").strip()
    if authed_email and body_email and authed_email.lower() != body_email.lower():
        return JSONResponse(
            status_code=403,
            content={"error": {"code": "AUTH_SPOOF", "message": "user_email mismatch"}},
        )
    user_email = authed_email or body_email

    # Server-side privacy gate (defense in depth — the frontend also checks).
    await ensure_privacy_accepted(user_email, user_repo)

    # Compile graph once (thread-safe), set deps per-request (ContextVar-safe).
    # `_get_checkpointer()` re-attempts init after the TTL, so a DB blip at
    # boot doesn't permanently disable persistence.
    checkpointer = await _get_checkpointer()
    compiled_graph = await _get_or_compile_graph(deps, checkpointer)
    set_deps(deps)

    user_id = user_email or "anonymous"
    conversation_id = body.conversation_id or ""

    # H3 fix: verify conversation ownership before letting the pipeline
    # load history / use it as the checkpointer thread_id. Without this,
    # a holder of the shared BACKEND_API_KEY who guesses or steals a
    # conversation_id can read another user's history via the planner
    # context (load_chat_history feeds it directly to the LLM prompt).
    owner_user_id = None
    if conversation_id:
        if not authed_email:
            return JSONResponse(
                status_code=403,
                content={
                    "error": {
                        "code": "AUTH_REQUIRED",
                        "message": "conversation_id requires an authenticated user",
                    }
                },
            )
        from uuid import UUID

        try:
            conv_uuid = UUID(conversation_id)
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": {"code": "BAD_CONVERSATION_ID", "message": "invalid uuid"}},
            )
        user = await user_repo.get_by_email(authed_email)
        if user is None:
            # Authed but not synced — treat as no ownership.
            return JSONResponse(
                status_code=403,
                content={
                    "error": {"code": "NO_OWNERSHIP", "message": "conversation access denied"}
                },
            )
        conv = await chat_repo.get_conversation(conv_uuid, user_id=user.id)
        if conv is None:
            return JSONResponse(
                status_code=403,
                content={
                    "error": {"code": "NO_OWNERSHIP", "message": "conversation access denied"}
                },
            )
        owner_user_id = user.id

    initial_state: OpenArgState = {
        "question": body.question,
        "user_id": user_id,
        "conversation_id": conversation_id,
        "policy_mode": body.policy_mode,
        "replan_count": 0,
    }
    # Pass the owner_user_id down so load_chat_history can scope the
    # message fetch as defense-in-depth (the ownership check above already
    # gates entry, but the repo-level filter closes any future bypass).
    if owner_user_id is not None:
        initial_state["owner_user_id"] = str(owner_user_id)  # type: ignore[typeddict-unknown-key]

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
        "citations": result.get("citations", []),
        **({"documents": result.get("documents")} if result.get("documents") else {}),
        **({"warnings": result.get("warnings")} if result.get("warnings") else {}),
    }


# ── WebSocket rate limit helper ────────────────────────────


_WS_RATE_LIMIT_PER_MINUTE = 20


async def _check_ws_rate_limit(cache: ICacheService, identifier: str) -> bool:
    """Return True if the identifier has exceeded the WS rate limit.

    H8 (round v46): atomic INCR + EXPIRE NX via `increment_with_ttl`.
    The previous implementation did get → check → set, which (a) let
    two concurrent handshakes both observe count<cap and both bump it
    above the cap, and (b) refreshed the TTL on every hit so a steady
    stream of requests inside the window kept the counter alive
    indefinitely instead of resetting at the 60s boundary.

    Cache errors fail OPEN by design — degraded Redis must not block
    legitimate traffic. The Redis client logs the underlying exception
    so an operator can spot a sustained outage.
    """
    key = f"ws_rate:{identifier}"
    try:
        count = await cache.increment_with_ttl(key, ttl_seconds=60)
    except Exception:
        logger.warning("WS rate-limit cache failed; failing open", exc_info=True)
        return False
    return count > _WS_RATE_LIMIT_PER_MINUTE


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

                # Compile graph once (thread-safe), set deps per-request.
                # `_get_checkpointer()` re-attempts after TTL; a transient
                # DB blip at boot does not permanently disable persistence.
                checkpointer = await _get_checkpointer()
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

                # WS JWT-in-handshake (round v46, closes H3 residual gap).
                # The HTTP path validates the Google JWT in the
                # GoogleJwtAuthMiddleware; WebSockets bypass starlette
                # middleware entirely, so the body-supplied user_email is
                # the only identity hint we get. To stop a holder of
                # BACKEND_API_KEY from claiming any user_email + any
                # conversation_id, accept an optional `id_token` in the
                # handshake message and validate it server-side. When
                # present and valid, the JWT's verified email overrides
                # the body's claim and unlocks conversation_id access.
                # Absent the JWT we keep the legacy path working but
                # refuse conversation_id reads further down (H3 fix).
                verified_email = ""
                raw_id_token = (raw.get("id_token") or "").strip()
                if raw_id_token:
                    # `GoogleJwtValidator | None`, not the bare class: those are
                    # distinct container keys, and asking for the bare one is
                    # what made every logged-in socket die on NoFactoryError.
                    # None means the deployment has no GOOGLE_OAUTH_CLIENT_ID,
                    # which is legitimate outside prod — and means we cannot
                    # verify anything, so no elevation is granted.
                    validator = await request_scope.get(GoogleJwtValidator | None)
                    if validator is None:
                        logger.warning(
                            "WS received an id_token but no Google client id is "
                            "configured; continuing unverified"
                        )
                    try:
                        if validator is not None:
                            verified_email = await validator.validate(raw_id_token)
                    except InvalidGoogleToken as exc:
                        logger.warning("WS rejected invalid Google JWT: %s", exc)
                        await _safe_send_json(
                            ws,
                            {
                                "type": "error",
                                "message": "Invalid or expired token",
                            },
                        )
                        await ws.close(code=4401)
                        return

                # Anti-spoofing: if both are present and disagree, the
                # body lied — refuse.
                body_email = (raw.get("user_email") or "").strip()
                if verified_email and body_email and verified_email.lower() != body_email.lower():
                    await _safe_send_json(
                        ws,
                        {"type": "error", "message": "user_email mismatch"},
                    )
                    await ws.close(code=4403)
                    return

                # Rate limiting
                ws_identifier = (
                    verified_email
                    or raw.get("user_email")
                    or (ws.client.host if ws.client else "unknown")
                )
                if await _check_ws_rate_limit(cache, ws_identifier):
                    audit_rate_limited(user=ws_identifier, endpoint="ws/smart")
                    await _safe_send_json(ws, {"type": "error", "message": "Rate limit exceeded"})
                    await ws.close(code=4429)
                    return

                # Server-side privacy gate (defense in depth). Prefer the
                # JWT-verified email when present; body.user_email is now
                # advisory and only kicks in for legacy BFFs that haven't
                # been redeployed with the JWT-in-handshake change yet.
                ws_user_email = verified_email or raw.get("user_email") or ""
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

                # Round v46 WS JWT-in-handshake (closes H3 residual gap):
                # conversation_id reads now REQUIRE the JWT-verified email.
                # Pre-fix an attacker who knew BOTH the victim's email AND
                # a conversation_id of that user could pass both through
                # body fields and the planner would happily load the
                # history. Post-fix the request is refused unless the
                # caller proved possession of the Google ID token whose
                # `email` claim matches the conversation's owner.
                owner_user_id_ws = None
                if conversation_id:
                    if not verified_email:
                        await _safe_send_json(
                            ws,
                            {
                                "type": "error",
                                "message": "conversation_id requires an authenticated user",
                            },
                        )
                        await ws.close(code=4403)
                        return
                    from uuid import UUID as _UUID

                    try:
                        _conv_uuid = _UUID(conversation_id)
                    except ValueError:
                        await _safe_send_json(
                            ws, {"type": "error", "message": "Invalid conversation_id"}
                        )
                        await ws.close(code=4400)
                        return
                    chat_repo_ws = await request_scope.get(IChatRepository)
                    user_repo_ws = await request_scope.get(IUserRepository)
                    user_ws = await user_repo_ws.get_by_email(verified_email)
                    conv_ws = (
                        await chat_repo_ws.get_conversation(_conv_uuid, user_id=user_ws.id)
                        if user_ws
                        else None
                    )
                    if conv_ws is None:
                        await _safe_send_json(
                            ws,
                            {"type": "error", "message": "Conversation access denied"},
                        )
                        await ws.close(code=4403)
                        return
                    owner_user_id_ws = user_ws.id
                    owner_user_id_ws = user_ws.id

                initial_state: OpenArgState = {
                    "question": question,
                    "user_id": ws_identifier,
                    "conversation_id": conversation_id,
                    "policy_mode": policy_mode,
                }
                if owner_user_id_ws is not None:
                    initial_state["owner_user_id"] = str(owner_user_id_ws)  # type: ignore[typeddict-unknown-key]

                # When a checkpointer is active, pass thread_id for persistence
                stream_config: dict[str, Any] = {}
                if checkpointer and conversation_id:
                    stream_config["configurable"] = {"thread_id": conversation_id}

                # Stream the graph execution. BUG-022: a keepalive task
                # runs alongside so a long pipeline step never leaves the
                # socket idle long enough to be dropped mid-stream. A send
                # lock serializes the two producers (stream + keepalive).
                send_lock = asyncio.Lock()
                keepalive_task = asyncio.create_task(_ws_keepalive(ws, send_lock))
                # P1 last-resort logger: track whether a terminal `complete`
                # ever flew. If the pipeline starts but never emits one
                # (WS closed mid-stream, unhandled exception, etc.), the
                # finally block writes a synthetic query_analytics row so
                # the request stays visible in telemetry.
                complete_sent = False
                stream_started_at = time.monotonic()
                try:
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
                            async with send_lock:
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
                                async with send_lock:
                                    await _safe_send_json(ws, complete_event)
                                complete_sent = True
                finally:
                    keepalive_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await keepalive_task
                    # P1 last-resort logger: terminal-node analytics fires
                    # inline within each terminal node (FR-036t). If the
                    # request died before reaching any terminal, no row
                    # lands. Write a synthetic one tagged ws_closed_mid_stream
                    # so the call stays in coverage.
                    if not complete_sent and question:
                        with contextlib.suppress(Exception):
                            from app.application.pipeline.history import save_query_attempt

                            await save_query_attempt(
                                question=question,
                                served_table=None,
                                row_count=0,
                                success=False,
                                duration_ms=int((time.monotonic() - stream_started_at) * 1000),
                                error_message="ws_closed_mid_stream",
                                semantic_cache=deps.semantic_cache,
                            )

    except WebSocketDisconnect:
        logger.debug("WebSocket v2 client disconnected")
    except Exception:
        logger.exception("WebSocket v2 error")
        with contextlib.suppress(Exception):
            await _safe_send_json(ws, {"type": "error", "message": "Internal error"})
    finally:
        with contextlib.suppress(Exception):
            await ws.close()
