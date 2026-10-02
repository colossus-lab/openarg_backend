"""El agente: un modelo capaz en un ciclo de herramientas.

Es el patrón del modo datos del MCP, que funciona: buscar, mirar la tabla y
recién ahí consultar. Reemplaza al pipeline planificador → NL2SQL → analista,
que decidía todo de antemano sin mirar los datos.

Cada turno:

1. el modelo recibe la pregunta (con el historial y las fuentes de los turnos
   anteriores, si hay) y las herramientas;
2. pide herramientas; se ejecutan en paralelo y cada una avisa al usuario qué
   está haciendo (``status``);
3. con los resultados a la vista vuelve a pedir o contesta. El texto final
   sale en streaming (``chunk``), limpio de nombres internos;
4. si se agota el presupuesto (vueltas o tiempo), se le pide la respuesta con
   lo que tiene.

Las fuentes, los gráficos, el mapa y la verificación de cifras salen sólo de
lo que devolvieron las herramientas que leen datos. Lo transversal (caché,
historial, analytics, auditoría) lo hace ``EngineRunner``.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import AsyncGenerator
from typing import Any

from app.application.answers.engine import (
    ChunkEvent,
    ClarificationEvent,
    ClearAnswerEvent,
    CompleteEvent,
    EngineEvent,
    EngineRequest,
    EngineResult,
    StatusEvent,
)
from app.application.answers.pricing import cost_usd
from app.application.answers.prompt import FINAL_ROUND_NOTE, system_prompt, user_message
from app.application.answers.tools import build_tools
from app.application.answers.tools.base import (
    AgentToolImpl,
    ToolContext,
    ToolInputError,
    ToolOutcome,
    count,
    quoted,
)
from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.ports.llm.agent_llm import AgentTurn, AgentUsage, IAgentLLM, TextDelta, ToolCall

logger = logging.getLogger(__name__)

# Presupuesto por turno. El modo profundo puede mirar más fuentes.
MAX_TOOL_CALLS = {"normal": 10, "deep": 20}
# Tiempo de herramientas antes de pedir la respuesta con lo que hay. Deja
# margen para la redacción dentro del objetivo de p95 ≤ 45 s del chat.
SOFT_TIME_BUDGET_S = {"normal": 35.0, "deep": 90.0}
# Lo que se reserva para redactar cuando el canal tiene un tope duro (/ask).
_ANSWER_MARGIN_S = 10.0
TOOL_TIMEOUT_S = 25.0
MAX_ANSWER_TOKENS = 2048

_NO_ANSWER = (
    "No pude armar una respuesta para esta consulta. Probá reformulándola o "
    "acotándola a un lugar o período."
)
_REFUSAL = "No puedo ayudarte con esa consulta. Preguntame por datos públicos de Argentina."


# Los pasos que ve el usuario. Usan nombres de paso que el frontend ya conoce
# (`coordination` y `searching` muestran el texto tal cual; `generating`
# prende la fase de redacción), así el chat los muestra bien aunque no se haya
# actualizado todavía.
STEP_THINKING = "coordination"
STEP_TOOL = "searching"
STEP_WRITING = "generating"
THINKING_TEXT = "Pensando…"
WRITING_TEXT = "Escribiendo la respuesta…"


def _describe(tool: AgentToolImpl | None, call: ToolCall) -> str:
    """Qué está haciendo la herramienta, con lo que pidió el modelo."""
    if tool is None:
        return "Consultando…"
    describe = getattr(tool, "describe", None)
    if callable(describe):
        try:
            return str(describe(call.input))
        except Exception:
            logger.debug("agent: describe failed for %s", call.name, exc_info=True)
    return tool.status


def _summary(outcome: ToolOutcome) -> str | None:
    """Qué hizo la herramienta. Las que leen datos dicen qué y cuánto leyeron."""
    if outcome.is_error or outcome.clarification is not None:
        return None
    if outcome.summary:
        return outcome.summary
    if outcome.results:
        first = outcome.results[0]
        return (
            f"Leyó {quoted(first.dataset_title, 80)} "
            f"({count(len(first.records or []), 'fila', 'filas')})"
        )
    return None


class _ChunkCleaner:
    """Limpia el texto en streaming sin cortar un identificador a la mitad.

    Se manda hasta el último espacio: un nombre interno partido en dos
    fragmentos (`cache_` + `diputados`) no lo reconocería la limpieza.
    """

    def __init__(self) -> None:
        self._buf = ""

    def feed(self, text: str) -> str:
        from app.application.pipeline.nodes.analyst import _scrub_internal_identifiers

        self._buf += text
        cut = max(self._buf.rfind(" "), self._buf.rfind("\n"))
        if cut < 0:
            return ""
        ready, self._buf = self._buf[: cut + 1], self._buf[cut + 1 :]
        return _scrub_internal_identifiers(ready)

    def flush(self) -> str:
        from app.application.pipeline.nodes.analyst import _scrub_internal_identifiers

        ready, self._buf = self._buf, ""
        return _scrub_internal_identifiers(ready) if ready else ""


def _source(result: DataResult) -> dict[str, Any]:
    return {
        "name": result.dataset_title,
        "url": result.portal_url,
        "portal": result.portal_name,
        "accessed_at": (result.metadata or {}).get("fetched_at", ""),
    }


def _sources(results: list[DataResult]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for r in results:
        key = (r.dataset_title, r.portal_url)
        if key in seen:
            continue
        seen.add(key)
        out.append(_source(r))
    return out


def _tool_result_block(call: ToolCall, outcome: ToolOutcome) -> dict[str, Any]:
    block: dict[str, Any] = {
        "type": "tool_result",
        "tool_use_id": call.id,
        "content": outcome.content,
    }
    if outcome.is_error:
        block["is_error"] = True
    return block


def _clarification_answer(event: ClarificationEvent) -> str:
    options = "\n".join(f"- {o}" for o in event.options)
    return f"**{event.question}**\n\n{options}" if options else f"**{event.question}**"


class AgentEngine:
    """Un solo agente con herramientas, detrás de ``AnswerEngine``."""

    name = "agent"
    # Clasificación, caché, historial y analytics los hace el runner.
    handles_cross_cutting = False

    def __init__(self, llm: IAgentLLM, deps: Any, *, clock: Any = time.monotonic) -> None:
        self._llm = llm
        self._deps = deps
        self._clock = clock

    async def _run_tool(
        self, tool: AgentToolImpl | None, call: ToolCall, ctx: ToolContext
    ) -> ToolOutcome:
        if tool is None:
            return ToolOutcome(f"No existe la herramienta {call.name!r}.", is_error=True)
        try:
            async with asyncio.timeout(TOOL_TIMEOUT_S):
                return await tool.run(call.input, ctx)
        except ToolInputError as exc:
            return ToolOutcome(str(exc), is_error=True)
        except TimeoutError:
            logger.warning("agent tool %s timed out after %ss", call.name, TOOL_TIMEOUT_S)
            return ToolOutcome("La herramienta tardó demasiado. Probá con otra.", is_error=True)
        except ConnectorError as exc:
            logger.info("agent tool %s: connector error %s", call.name, exc)
            return ToolOutcome("La fuente no respondió. Probá con otra.", is_error=True)
        except Exception:
            logger.warning("agent tool %s failed", call.name, exc_info=True)
            return ToolOutcome("La herramienta falló. Probá con otra.", is_error=True)

    def _time_budget(self, req: EngineRequest) -> float:
        budget = SOFT_TIME_BUDGET_S.get(req.mode, SOFT_TIME_BUDGET_S["normal"])
        if req.deadline_s:
            budget = min(budget, max(1.0, req.deadline_s - _ANSWER_MARGIN_S))
        return budget

    async def stream(self, req: EngineRequest) -> AsyncGenerator[EngineEvent, None]:
        started = self._clock()
        tools = build_tools(self._deps)
        by_name = {t.spec.name: t for t in tools}
        specs = [t.spec for t in tools]
        ctx = ToolContext(deps=self._deps, req=req)
        system = system_prompt(deep=req.mode == "deep")
        messages: list[dict[str, Any]] = [
            {
                "role": "user",
                "content": user_message(
                    req.question, history=req.history, previous_sources=req.previous_sources
                ),
            }
        ]

        max_calls = MAX_TOOL_CALLS.get(req.mode, MAX_TOOL_CALLS["normal"])
        time_budget = self._time_budget(req)
        usage = AgentUsage()
        evidence: list[DataResult] = []
        calls_made = 0
        final: AgentTurn | None = None

        while True:
            out_of_budget = calls_made >= max_calls or (self._clock() - started) > time_budget
            if out_of_budget:
                messages.append({"role": "user", "content": FINAL_ROUND_NOTE})

            cleaner = _ChunkCleaner()
            streamed = False
            turn: AgentTurn | None = None
            yield StatusEvent(STEP_THINKING, THINKING_TEXT)
            async for item in self._llm.stream_turn(
                system=system,
                messages=messages,
                tools=specs,
                max_tokens=MAX_ANSWER_TOKENS,
                allow_tools=not out_of_budget,
            ):
                if isinstance(item, TextDelta):
                    text = cleaner.feed(item.text)
                    if text:
                        # Un espacio suelto antes de pedir herramientas no es
                        # el comienzo de la respuesta.
                        if text.strip() and not streamed:
                            yield StatusEvent(STEP_WRITING, WRITING_TEXT)
                            streamed = True
                        yield ChunkEvent(text)
                else:
                    turn = item
            if turn is None:
                raise RuntimeError("el modelo no devolvió un turno")
            usage.add(turn.usage)

            if not turn.tool_calls or out_of_budget or turn.stop_reason != "tool_use":
                tail = cleaner.flush()
                if tail:
                    yield ChunkEvent(tail)
                final = turn
                break

            # El modelo escribió algo antes de pedir herramientas: no es la
            # respuesta, se descarta de la pantalla.
            if streamed:
                yield ClearAnswerEvent()

            messages.append({"role": "assistant", "content": turn.content})
            for call in turn.tool_calls:
                tool = by_name.get(call.name)
                yield StatusEvent(STEP_TOOL, _describe(tool, call), connector=call.name)
            outcomes = await asyncio.gather(
                *(self._run_tool(by_name.get(c.name), c, ctx) for c in turn.tool_calls)
            )
            calls_made += len(turn.tool_calls)
            for call, outcome in zip(turn.tool_calls, outcomes, strict=True):
                done = _summary(outcome)
                if done:
                    yield StatusEvent(STEP_TOOL, done, connector=call.name)

            clarification = next((o.clarification for o in outcomes if o.clarification), None)
            if clarification is not None:
                yield clarification
                yield CompleteEvent(
                    EngineResult(
                        answer=_clarification_answer(clarification),
                        intent="clarification",
                        tokens_used=usage.total,
                        success=True,
                        model=self._llm.model,
                        cost_usd=cost_usd(self._llm.model, usage),
                    )
                )
                return

            for outcome in outcomes:
                evidence.extend(r for r in outcome.results if r.records)
            messages.append(
                {
                    "role": "user",
                    "content": [
                        _tool_result_block(c, o)
                        for c, o in zip(turn.tool_calls, outcomes, strict=True)
                    ],
                }
            )

        yield CompleteEvent(self._result(final, evidence, usage, req))

    def _result(
        self,
        turn: AgentTurn,
        evidence: list[DataResult],
        usage: AgentUsage,
        req: EngineRequest,
    ) -> EngineResult:
        from app.application.pipeline.chart_builder import build_deterministic_charts
        from app.application.pipeline.nodes.analyst import _build_map_data
        from app.application.pipeline.nodes.finalize import _extract_documents

        warnings: list[str] = []
        if turn.stop_reason == "refusal":
            answer = _REFUSAL
        else:
            answer = turn.text.strip() or _NO_ANSWER
            if turn.stop_reason == "max_tokens":
                warnings.append("La respuesta se cortó por largo; puede estar incompleta.")

        try:
            charts = build_deterministic_charts(evidence) or None
        except Exception:
            logger.debug("agent: chart building failed", exc_info=True)
            charts = None
        try:
            map_data = _build_map_data(evidence)
        except Exception:
            logger.debug("agent: map building failed", exc_info=True)
            map_data = None

        first = evidence[0] if evidence else None
        _record_tokens(self._llm.model, req.mode, usage)
        return EngineResult(
            answer=answer,
            sources=_sources(evidence),
            chart_data=charts,
            map_data=map_data,
            documents=_extract_documents(evidence),
            warnings=warnings,
            tokens_used=usage.total,
            intent="agent",
            served_table=(
                (first.metadata or {}).get("served_table") or first.source if first else None
            ),
            row_count=len(first.records) if first else 0,
            success=bool(evidence) and turn.stop_reason != "refusal",
            error_message=None if evidence else "sin_datos_leidos",
            # Sin datos leídos no se cachea: un "no lo encontré" cacheado se le
            # vuelve a servir a cada reformulación, aunque el dato aparezca.
            no_data=not evidence,
            evidence=evidence,
            model=self._llm.model,
            cost_usd=cost_usd(self._llm.model, usage),
        )


def _record_tokens(model: str, mode: str, usage: AgentUsage) -> None:
    """``openarg_llm_tokens_total``: hasta ahora nadie lo incrementaba."""
    try:
        from app.infrastructure.monitoring.prometheus_metrics import LLM_TOKENS

        short = re.sub(r"^(us|eu|global)\.anthropic\.", "", model or "")
        LLM_TOKENS.labels(model=short, mode=mode or "normal").inc(usage.total)
    except Exception:
        logger.debug("agent: token metric skipped", exc_info=True)
