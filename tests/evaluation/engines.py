"""Los motores que la batería sabe correr, detrás de una sola forma.

La batería no tiene que saber si del otro lado hay un grafo de LangGraph o un
agente con herramientas: le pasa la pregunta a ``EvalEngine.run`` y recibe un
``EngineOutput``. Así el mismo set de casos corre contra el sistema actual y
contra el agente, y la comparación es justa.

Hoy hay un solo motor, ``LegacyGraphEngine``, que arma el grafo igual que
``app/run.py``. Cuando exista la interfaz de producción (``AnswerEngine`` en
``app.application.answers.engine``, etapa 2 del plan), este módulo pasa a
envolver esa interfaz y el agente entra como un motor más.

**El costo se mide abajo, no se pide arriba.** El grafo sólo reporta los
tokens del analista (``tokens_used``); el planificador, el NL2SQL y los
reintentos no aparecen. Para saber cuánto cuesta de verdad una respuesta se
cuenta cada llamada a Bedrock en el adaptador, atribuida al caso que la
originó por una ``ContextVar``. La cifra que el motor dice de sí mismo queda
al lado (``tokens_reported``) para ver la diferencia.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import AsyncIterator, Iterator
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Protocol

# Precio por millón de tokens: (entrada, salida, escritura de caché, lectura de
# caché). Son los precios de lista de Anthropic; Bedrock factura aparte y
# puede diferir, así que el costo de la batería sirve para comparar motores
# entre sí, no para conciliar la factura de AWS. Se busca por substring del id
# del modelo, así cubre `us.anthropic.claude-haiku-4-5-20251001-v1:0` y
# `us.anthropic.claude-sonnet-4-6`.
PRICES_PER_MTOK: dict[str, tuple[float, float, float, float]] = {
    "claude-haiku-4-5": (1.00, 5.00, 1.25, 0.10),
    "claude-sonnet-4-6": (3.00, 15.00, 3.75, 0.30),
}


def price_for(model: str) -> tuple[float, float, float, float] | None:
    return next((p for key, p in PRICES_PER_MTOK.items() if key in (model or "")), None)


@dataclass
class ModelUsage:
    calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


@dataclass
class UsageMeter:
    """Lo que gastó un caso, separado por modelo."""

    by_model: dict[str, ModelUsage] = field(default_factory=dict)

    def add(
        self,
        model: str,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cache_read: int = 0,
        cache_write: int = 0,
    ) -> None:
        u = self.by_model.setdefault(model, ModelUsage())
        u.calls += 1
        u.input_tokens += int(input_tokens or 0)
        u.output_tokens += int(output_tokens or 0)
        u.cache_read_tokens += int(cache_read or 0)
        u.cache_write_tokens += int(cache_write or 0)

    @property
    def calls(self) -> int:
        return sum(u.calls for u in self.by_model.values())

    def totals(self) -> dict[str, int]:
        return {
            "input": sum(u.input_tokens for u in self.by_model.values()),
            "output": sum(u.output_tokens for u in self.by_model.values()),
            "cache_read": sum(u.cache_read_tokens for u in self.by_model.values()),
            "cache_write": sum(u.cache_write_tokens for u in self.by_model.values()),
        }

    def cost_usd(self) -> float | None:
        """None si algún modelo usado no tiene precio: mejor sin cifra que una baja."""
        total = 0.0
        for model, u in self.by_model.items():
            price = price_for(model)
            if price is None:
                return None
            p_in, p_out, p_cw, p_cr = price
            total += (
                u.input_tokens * p_in
                + u.output_tokens * p_out
                + u.cache_write_tokens * p_cw
                + u.cache_read_tokens * p_cr
            ) / 1_000_000
        return round(total, 6)

    def to_dict(self) -> dict[str, Any]:
        return {
            "llm_calls": self.calls,
            "tokens": self.totals(),
            "cost_usd": self.cost_usd(),
            "by_model": {m: vars(u) for m, u in self.by_model.items()},
        }


_meter: ContextVar[UsageMeter | None] = ContextVar("eval_usage_meter", default=None)


@contextlib.contextmanager
def metering() -> Iterator[UsageMeter]:
    """Cuenta las llamadas a Bedrock que ocurran adentro, en este contexto.

    Cada caso corre en su propia tarea de asyncio, y ``asyncio.to_thread``
    copia el contexto, así que dos casos en paralelo no se mezclan.
    """
    meter = UsageMeter()
    token = _meter.set(meter)
    try:
        yield meter
    finally:
        _meter.reset(token)


_installed = False


def install_usage_capture() -> None:
    """Envuelve el adaptador de Bedrock para que reporte cada llamada.

    Es un parche de la batería, no del código de producción: se instala una
    vez por proceso y sólo cuenta cuando hay un ``metering()`` activo.
    """
    global _installed
    if _installed:
        return
    from app.infrastructure.adapters.llm.bedrock_llm_adapter import BedrockLLMAdapter

    original_converse = BedrockLLMAdapter._converse_sync
    original_stream = BedrockLLMAdapter.chat_stream

    def _converse_sync(self: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        response = original_converse(self, *args, **kwargs)
        meter = _meter.get()
        if meter is not None:
            usage = response.get("usage", {}) or {}
            meter.add(
                self._model,
                usage.get("inputTokens", 0),
                usage.get("outputTokens", 0),
                usage.get("cacheReadInputTokens", 0),
                usage.get("cacheWriteInputTokens", 0),
            )
        return response

    async def chat_stream(
        self: Any,
        messages: Any,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        usage_out: dict[str, int] | None = None,
    ) -> AsyncIterator[str]:
        box: dict[str, int] = usage_out if usage_out is not None else {}
        async for chunk in original_stream(self, messages, temperature, max_tokens, usage_out=box):
            yield chunk
        meter = _meter.get()
        if meter is not None:
            meter.add(self._model, box.get("input_tokens", 0), box.get("output_tokens", 0))

    BedrockLLMAdapter._converse_sync = _converse_sync  # type: ignore[method-assign]
    BedrockLLMAdapter.chat_stream = chat_stream  # type: ignore[method-assign]
    _installed = True


# ── la forma común ─────────────────────────────────────────


@dataclass
class EngineOutput:
    answer: str
    sources: list[dict[str, Any]]
    latency_ms: int
    usage: dict[str, Any]
    # Lo que el motor dice que gastó. En el grafo actual es sólo el analista.
    tokens_reported: int = 0
    # Los datos que el motor tuvo a la vista, resumidos: es lo que el juez de
    # alucinación compara contra la respuesta.
    evidence: str = ""
    error: str | None = None
    # Diagnóstico propio de cada motor (plan, clasificación…). La batería no
    # lo puntúa salvo para la comparación con baselines viejos.
    diagnostics: dict[str, Any] = field(default_factory=dict)


class EvalEngine(Protocol):
    name: str

    async def start(self) -> None: ...

    async def run(
        self, question: str, *, case_id: str, mode: str, bypass_cache: bool
    ) -> EngineOutput: ...

    async def aclose(self) -> None: ...


# Tope del resumen de evidencia para el juez: alcanza para ver las cifras que
# se citaron sin mandarle al juez tablas enteras.
_EVIDENCE_MAX_CHARS = 12_000
# De una tabla larga se muestran el principio y, sobre todo, el final: en una
# serie el dato que se cita es el último. Con sólo las primeras 12 filas, el
# juez del 01-oct marcó como inventado el -6,28 % de julio de 2026, que era la
# última fila.
_EVIDENCE_HEAD_ROWS = 5
_EVIDENCE_TAIL_ROWS = 25


def summarize_evidence(results: list[Any]) -> str:
    parts: list[str] = []
    for r in results or []:
        title = getattr(r, "dataset_title", "") or ""
        portal = getattr(r, "portal_name", "") or ""
        meta = getattr(r, "metadata", {}) or {}
        records = getattr(r, "records", []) or []
        total = max(len(records), int(meta.get("total_records") or 0))
        head = f"## {title} — {portal} ({total} filas"
        head += f", unidades: {meta['units']})" if meta.get("units") else ")"
        if meta.get("generated_sql"):
            head += f"\nSQL: {str(meta['generated_sql'])[:400]}"
        if len(records) > _EVIDENCE_HEAD_ROWS + _EVIDENCE_TAIL_ROWS:
            skipped = len(records) - _EVIDENCE_HEAD_ROWS - _EVIDENCE_TAIL_ROWS
            shown = [str(rec) for rec in records[:_EVIDENCE_HEAD_ROWS]]
            shown.append(f"… {skipped} filas sin mostrar …")
            shown += [str(rec) for rec in records[-_EVIDENCE_TAIL_ROWS:]]
        else:
            shown = [str(rec) for rec in records]
        parts.append(head + "\n" + "\n".join(shown))
    return "\n\n".join(parts)[:_EVIDENCE_MAX_CHARS]


class LegacyGraphEngine:
    """El pipeline de LangGraph que hoy contesta el chat, /ask y el MCP."""

    name = "legacy"

    def __init__(self) -> None:
        self._stack = contextlib.AsyncExitStack()
        self._graph: Any = None
        self._container: Any = None

    async def start(self) -> None:
        from dishka import Scope

        from app.application.pipeline.graph import build_pipeline_graph
        from app.application.pipeline.nodes import PipelineDeps, set_deps
        from app.setup.config.settings import AppSettings
        from app.setup.ioc.provider_registry import create_async_ioc_container, get_providers

        install_usage_capture()
        settings = AppSettings()
        self._container = create_async_ioc_container(providers=get_providers(), settings=settings)
        request_scope = await self._stack.enter_async_context(self._container(scope=Scope.REQUEST))
        deps = await request_scope.get(PipelineDeps)
        # Antes de lanzar las tareas: cada una hereda la ContextVar de acá.
        set_deps(deps)
        # Sin checkpointer: cada caso es una conversación nueva de un turno.
        self._graph = build_pipeline_graph(deps)

    async def run(
        self, question: str, *, case_id: str, mode: str, bypass_cache: bool
    ) -> EngineOutput:
        state: dict[str, Any] = {
            "question": question,
            "user_id": f"eval:{case_id}",
            # Sin conversation_id a propósito: la batería nunca escribe en el
            # historial de nadie.
            "conversation_id": "",
            "mode": mode,
            "replan_count": 0,
            # El modo profundo gastaría un turno preguntando en vez de
            # contestar. La batería mide el turno de búsqueda.
            "scoping_done": True,
            "bypass_cache": bypass_cache,
        }
        started = time.monotonic()
        error: str | None = None
        out: dict[str, Any] = {}
        with metering() as meter:
            try:
                out = await self._graph.ainvoke(state)
            except Exception as exc:  # noqa: BLE001 — un caso que explota ES el hallazgo
                error = f"{type(exc).__name__}: {exc}"[:300]
        latency_ms = int((time.monotonic() - started) * 1000)

        plan = out.get("plan")
        steps = getattr(plan, "steps", None) or []
        return EngineOutput(
            answer=str(out.get("clean_answer") or ""),
            sources=[s for s in out.get("sources") or [] if isinstance(s, dict)],
            latency_ms=latency_ms,
            usage=meter.to_dict(),
            tokens_reported=int(out.get("tokens_used") or 0),
            evidence=summarize_evidence(out.get("data_results") or []),
            error=error,
            diagnostics={
                "classification": out.get("classification"),
                "plan_actions": [getattr(s, "action", "") for s in steps],
            },
        )

    async def aclose(self) -> None:
        await self._stack.aclose()
        if self._container is not None:
            await self._container.close()


ENGINES: dict[str, type] = {"legacy": LegacyGraphEngine}
