"""Public API endpoint for programmatic access via API keys.

Separate from the frontend endpoint (/query/smart) — uses Bearer token
auth instead of the shared BACKEND_API_KEY, has its own rate limiting
per plan, and does NOT save conversations.

Cobro (desde el 05-oct-2026): la pregunta del mes se reserva al entrar y se
cobra sólo si el turno termina con una respuesta completa que usó el modelo
(``answer_is_billable``); si no, la reserva se devuelve. La misma pregunta de
la misma clave repetida dentro de 5 minutos devuelve la respuesta ya
calculada, o espera la que está en curso, sin cobrar ni correr el motor
(``app.application.ask_dedupe``).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from app.application.answers.engine import CHANNEL_ASK, EngineRequest, EngineResult
from app.application.answers.runner import EngineRunner
from app.application.api_key_service import (
    QuestionReservation,
    answer_is_billable,
    answer_used_model,
    check_question_rate,
    question_quota_snapshot,
    reserve_question,
    settle_question,
    verify_api_key,
)
from app.application.ask_dedupe import (
    LOCK_MARGIN_SECONDS,
    cached_answer,
    lead_or_wait,
    question_fingerprint,
    release,
    store_answer,
)
from app.application.pipeline.nodes import PipelineDeps
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.presentation.http.controllers.public_api.usage_log import log_rejection, log_usage
from app.presentation.http.controllers.query.smart_query_v2_router import _answer_engine

logger = logging.getLogger(__name__)

router = APIRouter(tags=["public-api"])

_ENDPOINT = "/api/v1/ask"
_TOOL = "consultar_datos_publicos"

_DEFAULT_TIMEOUT_SECONDS = 30
# Lo que espera un pedido repetido, además del tope del turno que espera.
_WAIT_MARGIN_SECONDS = 10

# Los campos de la respuesta que se guardan para los reintentos (el cupo no:
# se calcula en cada pedido).
_ANSWER_FIELDS = ("answer", "sources", "chart_data", "map_data", "citations", "warnings")


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
    per-IP daily limit and a shared daily cap for the free plan. A question
    is charged only when it ends in a complete answer that used the model.
    Does NOT save conversations.
    """
    # 1. Authenticate Bearer token
    api_key = await authenticate_bearer(request, api_key_repo)
    received = time.monotonic()
    fingerprint = question_fingerprint(api_key.id, body.question)

    async def replay(stored: dict[str, Any]) -> dict[str, Any]:
        return await _replay(
            stored, api_key, request, body.question, cache, api_key_repo, credits, received
        )

    async def rejected(exc: HTTPException) -> None:
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

    # 2. La misma pregunta, respondida hace menos de 5 minutos: se devuelve
    # esa, sin cobrar ni contar el límite por minuto.
    previous = await cached_answer(cache, fingerprint)
    if previous is not None:
        return await replay(previous)

    # 3. Límite por minuto: cuenta al entrar, también si después espera a otra.
    try:
        minute = await check_question_rate(api_key, cache)
    except HTTPException as exc:
        await rejected(exc)
        raise

    # 4. Si la misma pregunta está corriendo (un reintento), se espera esa.
    deadline_s = _pipeline_timeout()
    turn = await lead_or_wait(
        cache,
        fingerprint,
        lock_ttl=int(deadline_s) + LOCK_MARGIN_SECONDS,
        wait_s=deadline_s + _WAIT_MARGIN_SECONDS,
    )
    if turn.answer is not None:
        return await replay(turn.answer)
    if turn.gave_up:
        logger.warning("Gave up waiting for an identical question (key %s)", api_key.key_prefix)
        waited_ms = int((time.monotonic() - received) * 1000)
        await _usage(api_key_repo, api_key, request, body.question, 408, 0, waited_ms)
        raise HTTPException(status_code=408, detail="Request timed out")

    reservation: QuestionReservation | None = None
    try:
        # 5. Cupo del mes, créditos, IP y tope global: se verifican y se
        # reservan, sin cobrar.
        client_ip = request.client.host if request.client else ""
        try:
            reservation = await reserve_question(
                api_key, cache, client_ip=client_ip, credits=credits, minute=minute
            )
        except HTTPException as exc:
            await rejected(exc)
            raise

        # 6. Mismo motor que el chat y /smart. La API pública no expone el
        # modo profundo ni guarda conversaciones. Un timeout, un error o una
        # inyección bloqueada no se cobran.
        req = EngineRequest(
            question=body.question,
            user_id=f"apikey:{api_key.id}",
            mode="normal",
            deadline_s=deadline_s,
            channel=CHANNEL_ASK,
        )
        start_time = time.monotonic()
        try:
            runner = EngineRunner(await _answer_engine(deps), deps)
            result = await runner.run(req)
        except TimeoutError:
            logger.error("Pipeline timeout for API key %s", api_key.key_prefix)
            await settle_question(reservation, cache, credits, charge=False)
            await _usage(api_key_repo, api_key, request, body.question, 408, 0, 0)
            raise HTTPException(status_code=408, detail="Request timed out") from None
        except Exception:
            logger.exception("Pipeline failed for API key %s", api_key.key_prefix)
            await settle_question(reservation, cache, credits, charge=False)
            await _usage(api_key_repo, api_key, request, body.question, 500, 0, 0)
            raise HTTPException(status_code=500, detail="Pipeline execution failed") from None

        duration_ms = int((time.monotonic() - start_time) * 1000)

        # Injection blocked → 400, como en /smart. Sin esto la API devolvía un
        # 200 con la respuesta de rechazo como si fuera un dato. Lo bloquea el
        # clasificador, sin llamar al modelo.
        if result.injection_blocked:
            await settle_question(reservation, cache, credits, charge=False, used_model=False)
            await _usage(api_key_repo, api_key, request, body.question, 400, 0, duration_ms)
            raise HTTPException(status_code=400, detail="Potential prompt injection detected")

        # 7. Se cobra sólo una respuesta completa que usó el modelo.
        charged = answer_is_billable(result)
        rate_info = await settle_question(
            reservation, cache, credits, charge=charged, used_model=answer_used_model(result)
        )

        # 8. Log usage (post-pipeline, fire-and-forget errors)
        await _usage(
            api_key_repo,
            api_key,
            request,
            body.question,
            200,
            result.tokens_used,
            duration_ms,
            model=result.model,
            cost_usd=result.cost_usd,
        )
        await _touch(api_key_repo, api_key)

        answer = _answer_fields(result)
        await store_answer(cache, fingerprint, {**answer, "tokens": result.tokens_used})
        return _response(
            answer,
            tokens=result.tokens_used,
            duration_ms=duration_ms,
            plan=api_key.plan,
            rate_info=rate_info,
            charged=charged,
        )
    finally:
        # Lo que no llegó a cobrarse (un error inesperado, un pedido
        # cancelado) devuelve la reserva. El turno se suelta siempre, después
        # de guardar la respuesta.
        if reservation is not None and not reservation.settled:
            await settle_question(reservation, cache, credits, charge=False)
        await release(cache, fingerprint)


async def _replay(
    stored: dict[str, Any],
    api_key: ApiKey,
    request: Request,
    question: str,
    cache: ICacheService,
    repo: IApiKeyRepository,
    credits: ICreditRepository,
    received: float,
) -> dict[str, Any]:
    """Una respuesta ya calculada para la misma pregunta: no se cobra ni corre nada."""
    answer = {
        "answer": str(stored.get("answer") or ""),
        "sources": stored.get("sources") or [],
        "chart_data": stored.get("chart_data"),
        "map_data": stored.get("map_data"),
        "citations": stored.get("citations") or [],
        "warnings": stored.get("warnings") or [],
    }
    rate_info = await question_quota_snapshot(api_key, cache, credits)
    duration_ms = int((time.monotonic() - received) * 1000)
    # Costo medido en 0: no corrió el modelo (y el tablero no lo estima).
    await _usage(repo, api_key, request, question, 200, 0, duration_ms, cost_usd=0.0)
    await _touch(repo, api_key)
    return _response(
        answer,
        tokens=0,
        duration_ms=duration_ms,
        plan=api_key.plan,
        rate_info=rate_info,
        charged=False,
    )


def _answer_fields(result: EngineResult) -> dict[str, Any]:
    return {
        "answer": result.answer,
        "sources": result.sources,
        "chart_data": result.chart_data,
        "map_data": result.map_data,
        "citations": result.citations,
        "warnings": result.warnings,
    }


def _response(
    answer: dict[str, Any],
    *,
    tokens: int,
    duration_ms: int,
    plan: str,
    rate_info: dict[str, Any],
    charged: bool,
) -> dict[str, Any]:
    """La respuesta de ``/ask``. Los campos de siempre, con el mismo significado."""
    return {
        **{field: answer.get(field) for field in _ANSWER_FIELDS},
        "usage": {
            "tokens": tokens,
            "duration_ms": duration_ms,
            "plan": plan,
            # Del mes desde el 30-sep-2026; el nombre viejo queda por compatibilidad.
            # Es lo que queda DESPUÉS del cobro real de este pedido.
            "requests_remaining_today": rate_info["remaining_day"],
            "requests_remaining_month": rate_info["remaining_month"],
            "limit_month": rate_info["limit_month"],
            "quota_resets_at": rate_info["quota_resets_at"],
            "used_credit": rate_info["used_credit"],
            "tier": rate_info["tier"],
            "founder_until": rate_info["founder_until"],
            "requests_remaining_minute": rate_info["remaining_minute"],
            # Nuevo (05-oct-2026): si este pedido descontó una pregunta.
            "charged": charged,
        },
    }


async def _touch(repo: IApiKeyRepository, api_key: ApiKey) -> None:
    try:
        await repo.update_last_used(api_key.id)
    except Exception:
        logger.debug("Failed to update last_used_at", exc_info=True)


async def _usage(
    repo: IApiKeyRepository,
    api_key: ApiKey,
    request: Request,
    question: str,
    status_code: int,
    tokens_used: int,
    duration_ms: int,
    *,
    model: str | None = None,
    cost_usd: float | None = None,
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
        model=model,
        cost_usd=cost_usd,
    )
