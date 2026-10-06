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
   lo que tiene;
5. cada cifra de la respuesta se busca en lo que devolvieron las herramientas
   (``answers.verification``). Con ``ANSWERS_VERIFY_MODE=correct``, si hay
   cifras sin respaldo el modelo hace UNA vuelta más con esa lista (el texto
   que ya salió se borra con ``clear_answer``); si después siguen, la
   respuesta lleva un aviso arriba que las nombra. Con ``shadow`` (el modo
   por defecto) sólo se registran en el log ``answers.verify``. Con ``off``
   no se verifica.

Sólo con ``correct`` el verificador decide además qué se cita: las fuentes,
los gráficos, el mapa, `served_table`, las citas estructuradas y la evidencia
del aviso de atraso salen de las evidencias que la respuesta usó (las que
aportaron una cifra o se nombran en el texto). Con ``shadow`` y ``off`` se
cita todo lo leído y sin citas, como antes del verificador: una falsa alarma
(una cifra truncada) le sacaba la fuente y el aviso de atraso a un dato viejo,
y las citas salían ``verified`` sin mirar el período (revisión del 05-oct:
H019, H082, H083, H100). En ``shadow`` lo que habría elegido queda en el log,
y si todas las cifras quedaron respaldadas el aviso de atraso deja afuera lo
que no aportó cifras y se llama igual que algo que sí, y pone primero lo que
las aportó (revisión de #146); con alguna sin respaldo, todo lo leído. Con
``off``, todo lo leído. Lo
transversal (caché, historial, aviso de atraso, analytics, auditoría) lo hace
``EngineRunner``.
"""

from __future__ import annotations

import asyncio
import json
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
from app.application.answers.verification import (
    VERIFY_CORRECT,
    VERIFY_OFF,
    Verification,
    build_citations,
    claim_for,
    confidence_for,
    correction_note,
    dated_evidence,
    figure_evidence,
    seen_numbers,
    select_evidence,
    unverified_notice,
    verify_figures,
    verify_mode,
)
from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.ports.llm.agent_llm import AgentTurn, AgentUsage, IAgentLLM, TextDelta, ToolCall

logger = logging.getLogger(__name__)

# Presupuesto por turno. No hay modo profundo: medido el 02-oct con la
# batería, costaba ×1,19 y respondía igual que el normal, y se sacó.
MAX_TOOL_CALLS = 10
# Tiempo de herramientas antes de pedir la respuesta con lo que hay. Deja
# margen para la redacción dentro del objetivo de p95 ≤ 45 s del chat.
SOFT_TIME_BUDGET_S = 35.0
# Lo que se reserva para redactar cuando el canal tiene un tope duro (/ask).
_ANSWER_MARGIN_S = 10.0
TOOL_TIMEOUT_S = 25.0
MAX_ANSWER_TOKENS = 2048
# La vuelta correctiva cuesta ~US$ 0,01 y 2-4 s (medido con la última vuelta de
# la reproducción del 04-oct). Con un tope de canal (/ask) sólo arranca si
# quedan al menos estos segundos: pasarse del tope pierde la respuesta entera.
CORRECTION_RESERVE_S = 8.0
# Sin tope de canal (chat web), no arranca si el turno ya lleva esto.
CORRECTION_SOFT_LIMIT_S = 45.0

_NO_ANSWER = (
    "No pude armar una respuesta para esta consulta. Probá reformulándola o "
    "acotándola a un lugar o período."
)
_REFUSAL = "No puedo ayudarte con esa consulta. Preguntame por datos públicos de Argentina."


class _Unchecked:
    """``_result`` sin la verificación hecha: la hace él."""


_UNCHECKED = _Unchecked()


# Los pasos que ve el usuario. Usan nombres de paso que el frontend ya conoce
# (`coordination` y `searching` muestran el texto tal cual; `generating`
# prende la fase de redacción), así el chat los muestra bien aunque no se haya
# actualizado todavía.
STEP_THINKING = "coordination"
STEP_TOOL = "searching"
STEP_WRITING = "generating"
THINKING_TEXT = "Pensando…"
WRITING_TEXT = "Escribiendo la respuesta…"
CHECKING_TEXT = "Revisando las cifras…"


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
        budget = SOFT_TIME_BUDGET_S
        if req.deadline_s:
            budget = min(budget, max(1.0, req.deadline_s - _ANSWER_MARGIN_S))
        return budget

    def _can_correct(self, req: EngineRequest, started: float) -> bool:
        """¿Queda tiempo para la vuelta correctiva sin pasarse del tope del canal?"""
        elapsed = self._clock() - started
        if req.deadline_s:
            return req.deadline_s - elapsed > CORRECTION_RESERVE_S
        return elapsed < CORRECTION_SOFT_LIMIT_S

    async def stream(self, req: EngineRequest) -> AsyncGenerator[EngineEvent, None]:
        started = self._clock()
        tools = build_tools(self._deps)
        by_name = {t.spec.name: t for t in tools}
        specs = [t.spec for t in tools]
        ctx = ToolContext(deps=self._deps, req=req)
        system = system_prompt(tool_names=set(by_name))
        messages: list[dict[str, Any]] = [
            {
                "role": "user",
                "content": user_message(
                    req.question, history=req.history, previous_sources=req.previous_sources
                ),
            }
        ]

        max_calls = MAX_TOOL_CALLS
        time_budget = self._time_budget(req)
        usage = AgentUsage()
        evidence: list[DataResult] = []
        # Los números que el modelo tuvo a la vista de cada evidencia, y de
        # todo lo que leyó (también lo que no es evidencia: describir_tabla).
        evidence_seen: list[frozenset[float] | None] = []
        context_seen: set[float] = set()
        calls_made = 0
        final: AgentTurn | None = None
        mode = verify_mode()
        first_check: Verification | None = None
        # El texto que ya se verificó en el bucle (modo correct) y su resultado:
        # si es la respuesta final, no se verifica dos veces.
        checked: tuple[str, Verification | None, float] | None = None

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
                    streamed = streamed or bool(tail.strip())
                    yield ChunkEvent(tail)
                # Una sola vuelta correctiva, y sólo si el modelo terminó bien
                # (no cortado por largo ni negándose) y queda tiempo.
                if (
                    mode == VERIFY_CORRECT
                    and first_check is None
                    and evidence
                    and turn.stop_reason == "end_turn"
                    and self._can_correct(req, started)
                ):
                    text = _answer_of(turn)[0]
                    check, ms = await _verify_off_loop(text, evidence, evidence_seen, context_seen)
                    checked = (text, check, ms)
                    if check is not None and check.unsupported:
                        first_check = check
                        if streamed:
                            yield ClearAnswerEvent()
                        yield StatusEvent(STEP_THINKING, CHECKING_TEXT)
                        messages.append({"role": "assistant", "content": turn.content})
                        messages.append(
                            {"role": "user", "content": correction_note(check.unsupported)}
                        )
                        continue
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
                # Lo que el modelo leyó de verdad: las últimas filas de la
                # serie, no las 1.000 que trae la evidencia.
                numbers = seen_numbers(outcome.content)
                context_seen.update(numbers)
                for r in outcome.results:
                    if r.records:
                        evidence.append(r)
                        evidence_seen.append(numbers)
            messages.append(
                {
                    "role": "user",
                    "content": [
                        _tool_result_block(c, o)
                        for c, o in zip(turn.tool_calls, outcomes, strict=True)
                    ],
                }
            )

        # La verificación de la respuesta final, fuera del event loop. Con
        # `off` no se verifica: es el interruptor.
        final_check: Verification | None = None
        verify_ms: float | None = None
        if mode != VERIFY_OFF and evidence and final.stop_reason != "refusal":
            text = _answer_of(final)[0]
            if checked is not None and checked[0] == text:
                _, final_check, verify_ms = checked
            else:
                final_check, verify_ms = await _verify_off_loop(
                    text, evidence, evidence_seen, context_seen
                )

        yield CompleteEvent(
            self._result(
                final,
                evidence,
                usage,
                req,
                evidence_seen,
                mode,
                first_check,
                context_seen,
                check=final_check,
                verify_ms=verify_ms,
            )
        )

    def _result(
        self,
        turn: AgentTurn,
        evidence: list[DataResult],
        usage: AgentUsage,
        req: EngineRequest,
        evidence_seen: list[frozenset[float] | None] | None = None,
        mode: str = VERIFY_OFF,
        first_check: Verification | None = None,
        context_seen: set[float] | None = None,
        *,
        check: Verification | None | _Unchecked = _UNCHECKED,
        verify_ms: float | None = None,
    ) -> EngineResult:
        """El resultado del turno. ``check`` es la verificación de la respuesta
        ya hecha en ``stream`` (None si no hubo o falló); sin pasarla, se hace
        acá."""
        from app.application.pipeline.chart_builder import build_deterministic_charts
        from app.application.pipeline.citation_guard import quality_ceiling
        from app.application.pipeline.nodes.analyst import _build_map_data
        from app.application.pipeline.nodes.finalize import _extract_documents

        answer, warnings = _answer_of(turn)
        if isinstance(check, _Unchecked):
            check = (
                _safe_verify(answer, evidence, evidence_seen, context_seen)
                if mode != VERIFY_OFF and evidence and turn.stop_reason != "refusal"
                else None
            )
        # Las mismas evidencias para fuentes, gráficos, `served_table` y lo que
        # se guarda para el turno siguiente. El aviso de atraso mira sólo las
        # que aportaron cifras (`dated`), si hay.
        cited, consulted, citations, figures = _choose_sources(answer, evidence, check)
        dated = figures
        summary = _verification_log(mode, answer, check, first_check, cited, consulted, verify_ms)
        if mode != VERIFY_CORRECT:
            # Fuera de correct la selección queda sólo en el log: se cita todo
            # lo leído y sin citas estructuradas, como antes del verificador.
            cited, consulted, citations, figures = list(evidence), [], [], []
            dated = _dated_outside_correct(evidence, check)
        if mode == VERIFY_CORRECT and check is not None and check.unsupported:
            # Después de la vuelta correctiva (o sin tiempo para hacerla):
            # nunca se borra una cifra; se avisa arriba cuáles no se pudieron
            # verificar.
            answer = f"{unverified_notice(check.unsupported)}\n\n{answer}"

        try:
            charts = build_deterministic_charts(cited) or None
        except Exception:
            logger.debug("agent: chart building failed", exc_info=True)
            charts = None
        try:
            map_data = _build_map_data(cited)
        except Exception:
            logger.debug("agent: map building failed", exc_info=True)
            map_data = None

        # La tabla con la que se respondió: la primera que aportó cifras.
        first = (figures or cited or [None])[0]
        _record_tokens(self._llm.model, req.mode, usage)
        return EngineResult(
            answer=answer,
            sources=_sources(cited),
            chart_data=charts,
            map_data=map_data,
            citations=citations,
            documents=_extract_documents(cited),
            warnings=warnings,
            tokens_used=usage.total,
            intent="agent",
            confidence=confidence_for(check, quality_ceiling(evidence)),
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
            cited_evidence=cited,
            figure_evidence=dated,
            consulted=[r.dataset_title for r in consulted],
            verification=summary,
            model=self._llm.model,
            cost_usd=cost_usd(self._llm.model, usage),
        )


def _answer_of(turn: AgentTurn) -> tuple[str, list[str]]:
    """El texto de la respuesta (el que se verifica y se entrega) y sus avisos."""
    if turn.stop_reason == "refusal":
        return _REFUSAL, []
    warnings: list[str] = []
    if turn.stop_reason == "max_tokens":
        warnings.append("La respuesta se cortó por largo; puede estar incompleta.")
    return turn.text.strip() or _NO_ANSWER, warnings


def _safe_verify(
    answer: str,
    evidence: list[DataResult],
    seen: list[frozenset[float] | None] | None,
    context: set[float] | None = None,
) -> Verification | None:
    """La verificación nunca le cuesta la respuesta a nadie."""
    try:
        return verify_figures(answer, evidence, seen, frozenset(context or ()))
    except Exception:
        logger.warning("agent: figure verification failed", exc_info=True)
        return None


async def _verify_off_loop(
    answer: str,
    evidence: list[DataResult],
    seen: list[frozenset[float] | None],
    context: set[float],
) -> tuple[Verification | None, float]:
    """La verificación en un hilo, y cuánto tardó (ms).

    Es CPU puro: una tabla de 120 cifras sobre una serie de 24×10 tarda ~0,5 s
    (medido en la revisión del 05-oct), y en el event loop frenaba los demás
    WebSocket y /ask del worker. Se le pasan copias: el bucle no las toca
    mientras espera, pero así no depende de eso.
    """
    started = time.perf_counter()
    try:
        check = await asyncio.to_thread(
            _safe_verify, answer, list(evidence), list(seen), set(context)
        )
    except Exception:
        logger.warning("agent: figure verification thread failed", exc_info=True)
        check = None
    return check, (time.perf_counter() - started) * 1000


def _choose_sources(
    answer: str, evidence: list[DataResult], check: Verification | None
) -> tuple[list[DataResult], list[DataResult], list[dict[str, Any]], list[DataResult]]:
    """Lo citado, lo sólo consultado, las citas y lo que aportó cifras.

    Sin verificación (falló, o la respuesta es un rechazo) se cita toda la
    evidencia, como antes, sin volver a verificar: si la verificación tiró una
    excepción, repetirla tiraría la misma, fuera de la red de ``_safe_verify``.
    Y si algo de esto falla, lo mismo: la respuesta sale igual.
    """
    if not evidence:
        return [], [], [], []
    if check is None:
        return list(evidence), [], [], []
    try:
        cited, consulted = select_evidence(answer, evidence, check)
        citations = build_citations(answer, check, evidence)
        figures = figure_evidence(evidence, check)
    except Exception:
        logger.warning("agent: source selection failed", exc_info=True)
        return list(evidence), [], [], []
    return cited, consulted, citations, figures


def _dated_outside_correct(
    evidence: list[DataResult], check: Verification | None
) -> list[DataResult]:
    """Sobre qué se calcula el aviso de atraso fuera de correct (vacío = todo lo citado).

    Con alguna cifra sin respaldo (una falsa alarma, como un truncado) o sin
    verificación (off), todo lo leído: ante la duda, el lado seguro (H082).
    Con todas respaldadas, todo lo leído menos lo que no aportó cifras y se
    llama igual que algo que sí: mirar todo ponía "Dato atrasado" por 92.2,
    consultada y no usada, arriba de una respuesta al día hecha con 92.1, que
    se llama igual (revisión de #146). Mirar sólo lo que aportó cifras dejaba
    afuera una serie vieja usada de verdad cuando su cifra coincidía por azar
    con otra serie leída (``dated_evidence``). Lo que aportó cifras va
    primero, para que lo leído antes y no usado no le gane el tope de avisos
    ni la línea del catálogo. Si algo falla, todo lo leído.
    """
    if check is None or check.unsupported:
        return []
    try:
        return dated_evidence(evidence, check)
    except Exception:
        logger.warning("agent: dated evidence selection failed", exc_info=True)
        return []


def _verification_log(
    mode: str,
    answer: str,
    check: Verification | None,
    first_check: Verification | None,
    cited: list[DataResult],
    consulted: list[DataResult],
    verify_ms: float | None = None,
) -> dict[str, Any] | None:
    """El registro de la verificación (``answers.verify``), para medir el modo sombra.

    Una línea JSON por respuesta con cifras: cuántas, cuáles sin respaldo y en
    qué oración, si hubo vuelta correctiva, qué marcaba antes y cuánto tardó
    la verificación de la respuesta final (``ms``).
    """
    if check is None or not check.checks:
        return None
    try:
        summary: dict[str, Any] = {
            "modo": mode,
            **check.summary(),
            "contexto_sin_respaldo": [claim_for(answer, f) for f in check.unsupported[:8]],
            "vuelta_correctiva": first_check is not None,
            "sin_respaldo_antes": first_check.summary()["sin_respaldo"] if first_check else None,
            "fuentes_citadas": len(cited),
            "consultadas": len(consulted),
            "ms": round(verify_ms) if verify_ms is not None else None,
        }
    except Exception:
        logger.warning("agent: verification summary failed", exc_info=True)
        return None
    if mode != VERIFY_OFF:
        logger.info("answers.verify %s", json.dumps(summary, ensure_ascii=False))
    return summary


def _record_tokens(model: str, mode: str, usage: AgentUsage) -> None:
    """``openarg_llm_tokens_total``: hasta ahora nadie lo incrementaba."""
    try:
        from app.infrastructure.monitoring.prometheus_metrics import LLM_TOKENS

        short = re.sub(r"^(us|eu|global)\.anthropic\.", "", model or "")
        LLM_TOKENS.labels(model=short, mode=mode or "normal").inc(usage.total)
    except Exception:
        logger.debug("agent: token metric skipped", exc_info=True)
