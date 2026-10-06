"""Una sola forma de contestar una pregunta, la haga quien la haga.

El chat por WebSocket, ``POST /query/smart`` y ``POST /ask`` (que es lo que usa
el MCP) invocaban el grafo de LangGraph cada uno por su cuenta: armaban el
estado inicial, el ``thread_id`` del checkpointer y la respuesta a mano, y en
cada uno faltaba algo distinto (``/ask`` no mandaba ``thread_id`` y daba 500;
el WS nunca reseteaba ``replan_count``; el ``complete`` del WS llevaba siempre
``tokens_used: 0``).

Ahora los tres hablan con un ``AnswerEngine`` a través de ``EngineRunner``:

- ``EngineRequest``: la pregunta y quién la hace. Nada específico del motor.
- Los eventos que un motor emite mientras trabaja (``StatusEvent``,
  ``ChunkEvent``, ``ClearAnswerEvent``, ``ClarificationEvent``) y el que cierra
  el turno (``CompleteEvent``). Cada uno sabe serializarse al mismo JSON que
  el frontend ya entiende.
- ``EngineResult``: la respuesta, con todo lo que cada canal necesita.

Hoy hay un motor, ``LegacyGraphEngine`` (el grafo de siempre). El agente con
herramientas de la etapa 3 entra como otro, y se elige con ``ANSWERS_ENGINE``.
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)

# ── el pedido ──────────────────────────────────────────────

# Por dónde llegó la pregunta. Lo usa el runner para nombrar en
# `query_analytics` un turno que no terminó, y nada más: ningún motor puede
# comportarse distinto según el canal.
CHANNEL_WS = "ws"
CHANNEL_SMART = "smart"
CHANNEL_ASK = "ask"


@dataclass(frozen=True)
class EngineRequest:
    question: str
    user_id: str
    conversation_id: str = ""
    # Dueño verificado de la conversación. El router ya controló la
    # pertenencia; esto llega abajo como defensa en profundidad para que la
    # lectura del historial se filtre por dueño también en el repositorio.
    owner_user_id: str | None = None
    mode: str = "normal"
    # Ni lee ni escribe el caché: corridas de evaluación.
    bypass_cache: bool = False
    # Tope de tiempo del turno entero, en segundos. None = sin tope.
    deadline_s: float | None = None
    channel: str = CHANNEL_SMART
    # El historial ya formateado. Lo llena el runner para los motores que no
    # lo cargan por su cuenta; el grafo actual lo carga en `load_memory`.
    history: str = ""
    # Las fuentes de los turnos anteriores ("título — portal"), leídas de los
    # mensajes guardados. Sin resumen de un modelo en el medio: lo que se usó,
    # tal cual. Las llena el runner.
    previous_sources: tuple[str, ...] = ()


# ── los eventos ────────────────────────────────────────────


@dataclass(frozen=True)
class StatusEvent:
    step: str
    detail: str
    connector: str | None = None

    def to_wire(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": "status", "step": self.step, "detail": self.detail}
        if self.connector is not None:
            payload["connector"] = self.connector
        return payload


@dataclass(frozen=True)
class ChunkEvent:
    content: str

    def to_wire(self) -> dict[str, Any]:
        return {"type": "chunk", "content": self.content}


@dataclass(frozen=True)
class ClearAnswerEvent:
    """Descartar lo que se streameó hasta acá: el motor vuelve a empezar."""

    def to_wire(self) -> dict[str, Any]:
        return {"type": "clear_answer"}


@dataclass(frozen=True)
class ClarificationEvent:
    """El motor necesita una precisión; el frontend pinta las opciones como chips."""

    question: str
    options: tuple[str, ...] = ()

    def to_wire(self) -> dict[str, Any]:
        return {"type": "clarification", "question": self.question, "options": list(self.options)}


@dataclass(frozen=True)
class PassthroughEvent:
    """Un evento del grafo actual que no tiene tipo propio.

    Existe para no perder nada al pasar al formato tipado: lo que el grafo
    emitía antes sigue llegando igual. El router le aplica la misma lista de
    campos permitidos que a todos los eventos.
    """

    payload: dict[str, Any]

    def to_wire(self) -> dict[str, Any]:
        return dict(self.payload)


@dataclass
class EngineResult:
    answer: str
    sources: list[dict[str, Any]] = field(default_factory=list)
    chart_data: list[dict[str, Any]] | None = None
    map_data: dict[str, Any] | None = None
    citations: list[dict[str, Any]] = field(default_factory=list)
    documents: list[dict[str, Any]] | None = None
    warnings: list[str] = field(default_factory=list)
    tokens_used: int = 0
    # "injection_blocked", "clarification", "cached", "off_topic"… o el que
    # declare el motor. Los routers usan "injection_blocked" para el 400.
    intent: str = ""
    # Se calcula pero no sale al cliente (se sacó de la API a propósito).
    confidence: float = 1.0

    # ── para el runner, nunca para el cliente ──
    # Con qué se respondió y si salió bien: es la fila de `query_analytics`.
    served_table: str | None = None
    row_count: int = 0
    # None = se deduce de que haya respuesta.
    success: bool | None = None
    error_message: str | None = None
    # No había datos para responder. No se cachea: una deflexión cacheada se
    # le vuelve a servir a cada reformulación, aunque el dato aparezca.
    no_data: bool = False
    # Todos los `DataResult` que leyó el motor: contra eso se verifican las
    # cifras de la respuesta.
    evidence: list[Any] = field(default_factory=list, repr=False)
    # Los que se citan: con ANSWERS_VERIFY_MODE=correct, los que aportaron una
    # cifra o se nombran en el texto (``answers.verification.select_evidence``);
    # si no, toda la evidencia. De acá salen las fuentes, los gráficos,
    # `served_table` y el aviso de atraso. Vacío = toda la evidencia.
    cited_evidence: list[Any] = field(default_factory=list, repr=False)
    # De las citadas, las que aportaron alguna cifra respaldada. El aviso de
    # atraso mira éstas: una citada sólo por el título no lo dispara. Vacío =
    # las citadas.
    figure_evidence: list[Any] = field(default_factory=list, repr=False)
    # Los títulos de lo que se leyó y no se citó ("consultadas").
    consulted: list[str] = field(default_factory=list)
    # El resumen de la verificación de cifras (cuántas, cuáles sin respaldo,
    # si hubo vuelta correctiva). Para el log, nunca para el cliente.
    verification: dict[str, Any] | None = None
    # Con qué modelo y a qué costo, sumando todas las vueltas del turno.
    # None = no se sabe (el grafo actual no lo mide entero).
    model: str = ""
    cost_usd: float | None = None

    @property
    def injection_blocked(self) -> bool:
        return self.intent == "injection_blocked"

    def complete_payload(self) -> dict[str, Any]:
        """El evento ``complete`` del WebSocket, con las mismas claves de siempre.

        ``confidence`` no va: se sacó de la API deliberadamente (commit
        acc884a). ``tokens_used`` sí, por paridad con la respuesta HTTP.
        """
        return {
            "type": "complete",
            "answer": self.answer,
            "sources": self.sources,
            "chart_data": self.chart_data,
            "map_data": self.map_data,
            "citations": self.citations,
            "documents": self.documents,
            "warnings": self.warnings,
            "tokens_used": self.tokens_used,
        }


@dataclass(frozen=True)
class CompleteEvent:
    result: EngineResult

    def to_wire(self) -> dict[str, Any]:
        return self.result.complete_payload()


EngineEvent = (
    StatusEvent
    | ChunkEvent
    | ClearAnswerEvent
    | ClarificationEvent
    | PassthroughEvent
    | CompleteEvent
)


# ── el motor ───────────────────────────────────────────────


class AnswerEngine(Protocol):
    """Un motor que contesta preguntas.

    ``stream`` emite eventos y termina con exactamente un ``CompleteEvent``.

    ``handles_cross_cutting`` dice si el motor resuelve por su cuenta lo que no
    es contestar: clasificar saludos e inyecciones, leer y escribir el caché,
    cargar el historial, registrar `query_analytics`, métricas y auditoría. El
    grafo actual lo hace adentro de sus nodos, así que el runner no lo repite;
    un motor nuevo lo deja en ``False`` y el runner se encarga.
    """

    name: str
    handles_cross_cutting: bool

    def stream(self, req: EngineRequest) -> AsyncGenerator[EngineEvent, None]: ...


class EngineIncomplete(RuntimeError):
    """El motor terminó sin emitir la respuesta."""


class EngineTimeout(TimeoutError):
    """Se venció ``EngineRequest.deadline_s``."""


# ── la elección ────────────────────────────────────────────

ENGINE_ENV = "ANSWERS_ENGINE"
DEFAULT_ENGINE = "legacy"
# Los que existen. Volver atrás es cambiar la variable a `legacy`, sin deploy.
KNOWN_ENGINES = frozenset({"legacy", "agent"})


def selected_engine_name() -> str:
    """El motor que pide ``ANSWERS_ENGINE``, o ``legacy``.

    Un valor desconocido no tira el servicio: contesta con el motor de
    siempre y lo deja en el log como ERROR. Es lo que pasaría si alguien
    pusiera un nombre mal escrito.
    """
    name = (os.getenv(ENGINE_ENV) or DEFAULT_ENGINE).strip().lower()
    if name not in KNOWN_ENGINES:
        logger.error("%s=%r no existe; uso %r", ENGINE_ENV, name, DEFAULT_ENGINE)
        return DEFAULT_ENGINE
    return name
