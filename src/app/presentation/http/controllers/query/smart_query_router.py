"""Smart query router — thin HTTP/WS layer delegating to SmartQueryService."""

import contextlib
import logging
import os
from typing import Any

from dishka import AsyncContainer
from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field

from app.application.smart_query_service import SmartQueryService
from app.domain.ports.cache.cache_port import ICacheService
from app.infrastructure.audit.audit_logger import audit_rate_limited
from app.infrastructure.persistence_sqla.provider import MainAsyncSession
from app.setup.app_factory import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/query", tags=["smart-query"])


class SmartQueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=10000)
    user_email: str | None = None
    conversation_id: str | None = None
    policy_mode: bool = False


class SmartQueryResponse(BaseModel):
    answer: str
    sources: list[dict[str, Any]]
    chart_data: list[dict[str, Any]] | None = None
    tokens_used: int = 0
    confidence: float = 1.0
    citations: list[dict[str, Any]] = []
    documents: list[dict[str, Any]] | None = None
    warnings: list[str] = []


# ── WebSocket rate limit helper ────────────────────────────


async def _check_ws_rate_limit(cache: ICacheService, identifier: str) -> bool:
    """Return True if the identifier has exceeded the WS rate limit (20/minute)."""
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


# ── POST endpoint ──────────────────────────────────────────


@router.post("/smart", response_model=SmartQueryResponse)
@limiter.limit("15/minute")  # type: ignore[untyped-decorator]
@inject  # type: ignore[untyped-decorator]
async def smart_query(
    request: Request,
    body: SmartQueryRequest,
    service: FromDishka[SmartQueryService],
    session: FromDishka[MainAsyncSession],
) -> dict[str, Any] | ORJSONResponse:
    user_id = body.user_email or "anonymous"

    result = await service.execute(
        question=body.question,
        user_id=user_id,
        conversation_id=body.conversation_id or "",
        session=session,
        policy_mode=body.policy_mode,
    )

    # Injection blocked → return 400
    if result.intent == "injection_blocked":
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
        "answer": result.answer,
        "sources": result.sources,
        "chart_data": result.chart_data,
        "tokens_used": result.tokens_used,
        "confidence": result.confidence,
        "citations": result.citations,
        **({"documents": result.documents} if result.documents else {}),
        **({"cached": True} if result.cached else {}),
        **({"casual": True} if result.casual else {}),
        **({"warnings": result.warnings} if result.warnings else {}),
    }


# ── WebSocket endpoint ─────────────────────────────────────


def _validate_ws_api_key_from_params(ws: WebSocket) -> bool:
    """Check api_key query param (backward compatibility)."""
    import secrets as _secrets

    expected = os.getenv("BACKEND_API_KEY", "")
    if not expected:
        return True
    provided = ws.query_params.get("api_key", "")
    return _secrets.compare_digest(provided, expected) if provided else False


def _validate_api_key_value(provided: str) -> bool:
    """Validate an API key value against BACKEND_API_KEY."""
    import secrets as _secrets

    expected = os.getenv("BACKEND_API_KEY", "")
    if not expected:
        return True
    return _secrets.compare_digest(provided, expected) if provided else False


@router.websocket("/ws/smart")
async def ws_smart_query(ws: WebSocket) -> None:
    # Try query-param auth first (backward compat)
    has_query_param_auth = _validate_ws_api_key_from_params(ws)

    await ws.accept()

    try:
        # Obtain the Dishka container and enter REQUEST scope so all
        # providers (including SmartQueryService) can be resolved.
        container: AsyncContainer = ws.app.state.dishka_container
        async with container() as session_scope:
            async with session_scope() as request_scope:
                cache = await request_scope.get(ICacheService)
                service = await request_scope.get(SmartQueryService)

                raw_text = await ws.receive_text()
                if len(raw_text) > 10_000:
                    await ws.send_json({"type": "error", "message": "Message too large (max 10KB)"})
                    await ws.close(code=4400)
                    return
                import json as _json

                raw = _json.loads(raw_text)

                # Validate API key from either query params or message payload
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

                async for event in service.execute_streaming(
                    question=question,
                    user_id=ws_identifier,
                    conversation_id=conversation_id,
                    policy_mode=policy_mode,
                ):
                    await ws.send_json(event)
                    if event.get("type") == "error":
                        code = 4400 if event.get("code") == "SEC_001" else 4500
                        await ws.close(code=code)
                        return

    except WebSocketDisconnect:
        logger.debug("WebSocket client disconnected")
    except Exception:
        logger.exception("WebSocket error")
        with contextlib.suppress(Exception):
            await ws.send_json({"type": "error", "message": "Internal error"})
    finally:
        with contextlib.suppress(Exception):
            await ws.close()
