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
import weakref
from contextlib import AsyncExitStack
from typing import Any

from dishka import AsyncContainer
from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.application.answers.agent_engine import AgentEngine
from app.application.answers.engine import (
    CHANNEL_SMART,
    CHANNEL_WS,
    AnswerEngine,
    CompleteEvent,
    EngineRequest,
    selected_engine_name,
)
from app.application.answers.legacy_engine import LegacyGraphEngine
from app.application.answers.runner import EngineRunner
from app.application.common.privacy_gate import ensure_privacy_accepted
from app.application.pipeline.graph import build_pipeline_graph
from app.application.pipeline.nodes import PipelineDeps
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
#
# Los locks van por event loop y no como un `asyncio.Lock()` de módulo. Un
# lock se liga al primer loop que lo usa y a partir de ahí cualquier otro
# recibe `RuntimeError: <Lock> is bound to a different event loop`.
#
# En producción no se nota —uvicorn corre un loop por proceso y el módulo se
# importa una vez— pero basta un segundo loop para que todo request falle con
# un 500 genérico. Es lo que tenía la suite E2E, donde cada test crea el suyo:
# 131 de 135 caían así, y el mensaje no decía nada del loop.
_locks_por_loop: weakref.WeakKeyDictionary[Any, dict[str, asyncio.Lock]] = (
    weakref.WeakKeyDictionary()
)


def _lock(nombre: str) -> asyncio.Lock:
    """El lock de `nombre` para el event loop en curso.

    La clave es el loop mismo y no su `id()`: CPython reutiliza direcciones,
    así que un loop nuevo podría recibir el lock de uno muerto que cayó en la
    misma posición. Con referencias débiles, los locks de un loop se liberan
    con él y no queda nada que heredar.
    """
    loop = asyncio.get_running_loop()
    del_loop = _locks_por_loop.get(loop)
    if del_loop is None:
        del_loop = {}
        _locks_por_loop[loop] = del_loop
    lock = del_loop.get(nombre)
    if lock is None:
        lock = asyncio.Lock()
        del_loop[nombre] = lock
    return lock


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
        async with _lock("compiled_graphs"):
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
    try:
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
    except BaseException:
        # An opened pool must never outlive the failure that abandoned it: its
        # workers keep reconnecting in the background forever, which hangs the
        # process rather than erroring. Anything raised after `enter_async
        # _context` — a bad saver signature, cancellation — leaks it otherwise.
        with contextlib.suppress(Exception):
            await stack.aclose()
        raise
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

    Caller must hold `_lock("checkpointer")`. `_compiled_graphs` has to go too: a
    compiled graph captures the saver object, so leaving it cached would keep
    routing requests at the saver we just discarded.

    Clears `_checkpointer_attempted` as well, so the next call gets one
    immediate rebuild. The back-off exists to stop us hammering a database that
    fails at *init*; a connection that died after a healthy life is a different
    situation, and init is already known to work. If the rebuild then fails,
    the flag is set again and the back-off resumes its job.
    """
    global _checkpointer, _checkpointer_stack, _compiled_graphs  # noqa: PLW0603
    global _checkpointer_attempted  # noqa: PLW0603

    stack = _checkpointer_stack
    _checkpointer = None
    _checkpointer_stack = None
    _checkpointer_attempted = False
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

    # The retry back-off is consulted *inside* the lock, after any dead saver
    # has been discarded. Checking it up front — as an unlocked fast path —
    # meant a checkpointer that died within the TTL of its own successful init
    # was neither rebuilt nor torn down: the call just returned None and left
    # the corpse cached, with the graph still compiled around it.
    async with _lock("checkpointer"):
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


async def _answer_engine(deps: PipelineDeps) -> AnswerEngine:
    """El motor que contesta este turno, según ``ANSWERS_ENGINE``.

    Lo usan el chat, ``/smart`` y ``/ask``: los tres contestan con el mismo
    motor. Volver al grafo es poner ``legacy``, sin deploy.
    """
    if selected_engine_name() == "agent":
        if deps.agent_llm is not None:
            return AgentEngine(deps.agent_llm, deps)
        logger.error("ANSWERS_ENGINE=agent pero no hay modelo del agente; uso legacy")
    checkpointer = await _get_checkpointer()
    graph = await _get_or_compile_graph(deps, checkpointer)
    return LegacyGraphEngine(graph, deps, persistent=bool(checkpointer))


async def init_pipeline_persistence() -> None:
    """Warm up the optional LangGraph checkpointer during app startup."""
    await _get_checkpointer()


async def shutdown_pipeline_persistence() -> None:
    """Release app-scoped persistence resources on shutdown."""
    global _checkpointer, _checkpointer_stack, _checkpointer_attempted, _compiled_graphs  # noqa: PLW0603

    async with _lock("checkpointer"):
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
    # "normal" | "deep". `policy_mode` queda como alias de compatibilidad: el
    # frontend y el backend se despliegan por separado, así que durante un ciclo
    # de deploy conviven las dos formas del payload. `extra="forbid"` haría
    # fallar el request viejo si lo sacáramos de una.
    mode: str = "normal"
    policy_mode: bool = False
    history: list[dict[str, Any]] | None = None

    @model_validator(mode="after")
    def _resolve_mode(self) -> SmartQueryV2Request:
        if self.policy_mode and self.mode == "normal":
            self.mode = "deep"
        return self


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

    req = EngineRequest(
        question=body.question,
        user_id=user_id,
        conversation_id=conversation_id,
        # El dueño ya verificado baja hasta el repo como defensa en
        # profundidad: la lectura del historial se filtra por él.
        owner_user_id=str(owner_user_id) if owner_user_id is not None else None,
        mode=body.mode,
        channel=CHANNEL_SMART,
    )
    try:
        runner = EngineRunner(await _answer_engine(deps), deps)
        result = await runner.run(req)
    except Exception:
        logger.exception("Answer engine failed")
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "PIPELINE_ERROR", "message": "Pipeline execution failed"}},
        )

    # Injection blocked → return 400
    if result.injection_blocked:
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
        "answer": result.answer,
        "sources": result.sources,
        "chart_data": result.chart_data,
        "map_data": result.map_data,
        "tokens_used": result.tokens_used,
        "citations": result.citations,
        **({"documents": result.documents} if result.documents else {}),
        **({"warnings": result.warnings} if result.warnings else {}),
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
                engine = await _answer_engine(deps)

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
                # Mismo alias que en el POST: durante un ciclo de deploy puede
                # llegar el payload viejo por el WS.
                mode = raw.get("mode") or ("deep" if raw.get("policy_mode") else "normal")

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

                req = EngineRequest(
                    question=question,
                    user_id=ws_identifier,
                    conversation_id=conversation_id,
                    owner_user_id=(str(owner_user_id_ws) if owner_user_id_ws is not None else None),
                    mode=mode,
                    channel=CHANNEL_WS,
                )
                runner = EngineRunner(engine, deps)

                # BUG-022: a keepalive task runs alongside so a long engine
                # step never leaves the socket idle long enough to be dropped
                # mid-stream. A send lock serializes the two producers
                # (stream + keepalive). Un turno que no llega al `complete`
                # (el cliente cerró, el motor falló) lo registra el runner en
                # `query_analytics` como `ws_closed_mid_stream`.
                send_lock = asyncio.Lock()
                keepalive_task = asyncio.create_task(_ws_keepalive(ws, send_lock))
                try:
                    async with contextlib.aclosing(runner.stream(req)) as events:
                        async for event in events:
                            if isinstance(event, CompleteEvent):
                                payload = event.to_wire()
                            else:
                                # FR-038 (fail-closed allowlist, SEC-07) and
                                # FR-038b (WARNING log on any dropped key).
                                payload = _filter_stream_payload(event.to_wire())
                            async with send_lock:
                                await _safe_send_json(ws, payload)
                finally:
                    keepalive_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await keepalive_task

    except WebSocketDisconnect:
        logger.debug("WebSocket v2 client disconnected")
    except Exception:
        logger.exception("WebSocket v2 error")
        with contextlib.suppress(Exception):
            await _safe_send_json(ws, {"type": "error", "message": "Internal error"})
    finally:
        with contextlib.suppress(Exception):
            await ws.close()
