"""Lo que rodea a un motor de respuestas, en un solo lugar.

Contestar la pregunta es trabajo del motor. Todo lo demás —descartar un
saludo o una inyección sin gastar el modelo, leer y escribir el caché, cargar
el historial, verificar las cifras citadas, avisar que un dato es viejo,
registrar el turno en ``query_analytics``, contar tokens, auditar— hoy está
repartido entre los nodos del grafo. El motor nuevo no tiene esos nodos, y no
tendría por qué reimplementarlos: lo hace el runner.

Para el grafo actual (``handles_cross_cutting = True``) el runner no repite
nada de eso; sólo agrega lo que antes vivía en los routers:

- el tope de tiempo del turno (``EngineRequest.deadline_s``), que tenía sólo
  ``/ask``;
- la fila de ``query_analytics`` de un turno que no terminó, que tenía sólo
  el WebSocket. Ahora también queda registrado un ``/ask`` que se pasó de
  tiempo o falló, que antes no dejaba rastro.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncGenerator
from dataclasses import replace
from typing import Any
from urllib.parse import urlparse

from app.application.answers.engine import (
    CHANNEL_WS,
    AnswerEngine,
    CompleteEvent,
    EngineEvent,
    EngineIncomplete,
    EngineRequest,
    EngineResult,
    EngineTimeout,
    StatusEvent,
)
from app.application.pipeline.cache_manager import check_cache, write_cache
from app.application.pipeline.citation_guard import ground_citations
from app.application.pipeline.classifiers import classify_request
from app.application.pipeline.history import (
    load_chat_history,
    load_previous_sources,
    record_terminal_analytics,
)
from app.infrastructure.audit.audit_logger import audit_query

logger = logging.getLogger(__name__)

# Clasificación → intent del resultado, como en `fast_reply_node`.
_FAST_REPLY_INTENTS = {
    "injection": "injection_blocked",
    "off_topic": "off_topic",
    "internal_table": "internal_table_blocked",
}

# Un turno que no terminó, en `query_analytics`. El del WS conserva el nombre
# de siempre para no partir la serie del tablero (`ws_closed` ~21 %).
_INCOMPLETE_WS = "ws_closed_mid_stream"

_EMPTY_CACHED_ANSWER = "No tengo una respuesta guardada para esa consulta. Probá reformulándola."


class EngineRunner:
    def __init__(self, engine: AnswerEngine, deps: Any) -> None:
        self._engine = engine
        self._deps = deps

    @property
    def engine(self) -> AnswerEngine:
        return self._engine

    # ── la entrada ─────────────────────────────────────────

    async def run(self, req: EngineRequest) -> EngineResult:
        """El turno entero, sin streaming: lo que usan ``/smart`` y ``/ask``."""
        result: EngineResult | None = None
        async with contextlib.aclosing(self.stream(req)) as events:
            async for event in events:
                # No se corta en el `complete`: el grafo todavía puede estar
                # guardando el checkpoint del turno.
                if isinstance(event, CompleteEvent) and result is None:
                    result = event.result
        if result is None:
            raise EngineIncomplete(f"{self._engine.name} terminó sin respuesta")
        return result

    async def stream(self, req: EngineRequest) -> AsyncGenerator[EngineEvent, None]:
        """Los eventos del turno. Usar con ``contextlib.aclosing``.

        Si el turno termina sin ``complete`` —el motor falla, se vence el
        tope o quien consume deja de hacerlo— queda una fila en
        ``query_analytics`` con el motivo, para que no desaparezca de las
        métricas.
        """
        started = time.monotonic()
        deadline = started + req.deadline_s if req.deadline_s else None
        completed = False
        reason = "closed"
        source = (
            self._engine.stream(req)
            if self._engine.handles_cross_cutting
            else self._managed(req, started)
        )
        try:
            async with contextlib.aclosing(source) as events:
                iterator = aiter(events)
                while True:
                    # El tope cubre sólo la espera del motor, no lo que tarda
                    # quien consume en mandar cada evento.
                    remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                    try:
                        async with asyncio.timeout(remaining):
                            event = await anext(iterator)
                    except StopAsyncIteration:
                        break
                    if isinstance(event, CompleteEvent):
                        completed = True
                    yield event
        except TimeoutError as exc:
            reason = "timeout"
            raise EngineTimeout(f"{self._engine.name}: más de {req.deadline_s}s") from exc
        except Exception:
            reason = "error"
            raise
        finally:
            if not completed:
                await self._record_incomplete(req, started, reason)

    async def _record_incomplete(self, req: EngineRequest, started: float, reason: str) -> None:
        label = _INCOMPLETE_WS if req.channel == CHANNEL_WS else f"engine_{reason}"
        await self._record(
            req,
            started,
            served_table=None,
            row_count=0,
            success=False,
            error_message=label,
        )

    # ── lo transversal, para motores que no lo hacen solos ──

    async def _managed(
        self, req: EngineRequest, started: float
    ) -> AsyncGenerator[EngineEvent, None]:
        yield StatusEvent("classifying", "Analizando consulta...")
        kind, text = classify_request(req.question, req.user_id)
        if kind:
            # Saludos, preguntas sobre OpenArg, inyecciones: sin gastar modelo.
            result = EngineResult(answer=text or "", intent=_FAST_REPLY_INTENTS.get(kind, ""))
            await self._record(
                req,
                started,
                served_table=None,
                row_count=0,
                success=kind not in _FAST_REPLY_INTENTS,
                error_message=None,
            )
            yield CompleteEvent(result)
            return

        history = ""
        previous_sources: tuple[str, ...] = ()
        if req.conversation_id:
            yield StatusEvent("loading_context", "Cargando contexto...")
            # Uno después del otro: el repo usa la sesión de la request, y una
            # sesión de SQLAlchemy no admite dos operaciones a la vez.
            history = await load_chat_history(
                req.conversation_id, self._deps.chat_repo, owner_user_id=req.owner_user_id
            )
            previous_sources = await load_previous_sources(
                req.conversation_id, self._deps.chat_repo, owner_user_id=req.owner_user_id
            )

        # El caché está indexado sólo por la pregunta. Con historial, "¿y en
        # 2023?" significa otra cosa en cada conversación, y servirla desde
        # el caché le daba a una persona la respuesta de otra charla. Sin
        # historial, la pregunta se entiende sola y se puede cachear.
        cacheable = req.mode != "deep" and not req.bypass_cache and not history
        embedding = None
        if cacheable:
            yield StatusEvent("cache_check", "Buscando en caché...")
            cached, embedding = await self._cache_lookup(req)
            if cached is not None:
                result = self._from_cache(cached)
                await self._record(
                    req,
                    started,
                    served_table="cache",
                    row_count=0,
                    success=bool(cached.get("answer")),
                    error_message=None if cached.get("answer") else "empty_cached_answer",
                )
                yield CompleteEvent(result)
                return

        engine_req = replace(req, history=history, previous_sources=previous_sources)
        async with contextlib.aclosing(self._engine.stream(engine_req)) as events:
            async for event in events:
                if isinstance(event, CompleteEvent):
                    result = await self._finish(
                        engine_req, event.result, started, cacheable, embedding
                    )
                    yield CompleteEvent(result)
                    return
                yield event

    async def _cache_lookup(self, req: EngineRequest) -> tuple[dict[str, Any] | None, Any]:
        try:
            return await check_cache(
                req.question,
                req.user_id,
                self._deps.cache,
                self._deps.embedding,
                self._deps.semantic_cache,
                self._deps.metrics,
            )
        except Exception:
            logger.warning("EngineRunner: cache lookup failed", exc_info=True)
            return None, None

    def _from_cache(self, cached: dict[str, Any]) -> EngineResult:
        answer = _scrub(str(cached.get("answer") or "")).strip() or _EMPTY_CACHED_ANSWER
        return EngineResult(
            answer=answer,
            sources=cached.get("sources") or [],
            chart_data=cached.get("chart_data"),
            map_data=cached.get("map_data"),
            citations=cached.get("citations") or [],
            documents=cached.get("documents"),
            warnings=cached.get("warnings") or [],
            tokens_used=int(cached.get("tokens_used") or 0),
            intent="cached",
            confidence=float(cached.get("confidence", 1.0)),
        )

    async def _finish(
        self,
        req: EngineRequest,
        result: EngineResult,
        started: float,
        cacheable: bool,
        embedding: Any,
    ) -> EngineResult:
        """Limpia y verifica la respuesta del motor, y deja registro del turno."""
        result.answer = _scrub(result.answer)
        result.sources = [_with_portal(s) for s in result.sources]
        warnings = list(result.warnings)

        if result.evidence:
            try:
                citations, extra, confidence = ground_citations(
                    result.answer, result.citations, result.evidence, result.confidence
                )
                result.citations, result.confidence = citations, confidence
                warnings += [w for w in extra if w not in warnings]
            except Exception:
                logger.warning("EngineRunner: citation grounding failed", exc_info=True)

        stale = await _staleness_line(result.served_table)
        if stale and stale not in warnings:
            warnings.append(stale)
        result.warnings = warnings

        answer_ok = bool(result.answer.strip())
        duration_ms = int((time.monotonic() - started) * 1000)
        if result.tokens_used:
            self._deps.metrics.record_tokens_used(result.tokens_used, mode=req.mode)
        audit_query(
            user=req.user_id,
            question=req.question,
            intent=result.intent or "unknown",
            duration_ms=duration_ms,
        )

        if cacheable and answer_ok and not result.no_data and result.intent != "clarification":
            try:
                await write_cache(
                    req.question,
                    _cache_dict(result),
                    result.intent,
                    self._deps.cache,
                    self._deps.embedding,
                    self._deps.semantic_cache,
                    last_embedding=embedding,
                )
            except Exception:
                logger.debug("EngineRunner: cache write failed", exc_info=True)

        success = result.success if result.success is not None else answer_ok
        error = result.error_message or (None if answer_ok else "empty_answer")
        if result.no_data:
            success, error = False, result.error_message or "no_data_deflection"
        await self._record(
            req,
            started,
            served_table=result.served_table,
            row_count=result.row_count,
            success=success,
            error_message=error,
        )
        return result

    async def _record(
        self,
        req: EngineRequest,
        started: float,
        *,
        served_table: str | None,
        row_count: int,
        success: bool,
        error_message: str | None,
    ) -> None:
        """Una fila de ``query_analytics``. Nunca le cuesta la respuesta a nadie."""
        try:
            await record_terminal_analytics(
                question=req.question,
                served_table=served_table,
                row_count=row_count,
                success=success,
                duration_ms=int((time.monotonic() - started) * 1000),
                error_message=error_message,
                semantic_cache=self._deps.semantic_cache,
            )
        except Exception:
            logger.debug("EngineRunner: analytics logging skipped", exc_info=True)


# ── ayudas ─────────────────────────────────────────────────


def _scrub(text: str) -> str:
    """Saca nombres internos de tablas (``cache_*``, ``raw.*``) del texto."""
    from app.application.pipeline.nodes.analyst import _scrub_internal_identifiers

    return _scrub_internal_identifiers(text)


def _with_portal(source: dict[str, Any]) -> dict[str, Any]:
    """Toda fuente con portal: si el motor no lo puso, el dominio de la URL."""
    if not isinstance(source, dict) or source.get("portal"):
        return source
    host = (urlparse(str(source.get("url") or "")).hostname or "").removeprefix("www.")
    return {**source, "portal": host} if host else source


async def _staleness_line(served_table: str | None) -> str | None:
    """El aviso de dato viejo de la tabla servida. Si falla, no hay aviso."""
    if not served_table:
        return None
    try:
        from app.application.quality.data_age import staleness_warning
        from app.infrastructure.celery.tasks._db import get_sync_engine

        return await asyncio.to_thread(staleness_warning, get_sync_engine(), served_table)
    except Exception:
        logger.debug("EngineRunner: freshness notice skipped", exc_info=True)
        return None


def _cache_dict(result: EngineResult) -> dict[str, Any]:
    """La misma forma que escribe ``finalize_node``: el caché lo comparten."""
    return {
        "answer": result.answer,
        "sources": result.sources,
        "chart_data": result.chart_data,
        "map_data": result.map_data,
        "tokens_used": result.tokens_used,
        "documents": result.documents,
        "confidence": result.confidence,
        "citations": result.citations,
        "warnings": result.warnings,
    }
