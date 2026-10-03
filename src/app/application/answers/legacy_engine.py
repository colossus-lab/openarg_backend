"""El grafo de LangGraph de siempre, detrás de ``AnswerEngine``.

No cambia cómo contesta el grafo: arma el mismo estado inicial y el mismo
``thread_id`` que armaban los routers, y traduce lo que el grafo emite a los
eventos tipados de ``engine.py``. Dos diferencias, las dos a propósito:

- **``replan_count`` vuelve a 0 en cada turno, en todos los canales.** El WS no
  lo mandaba, y con el checkpointer el contador del turno anterior seguía
  vivo: después de un turno que replanificó, el siguiente ya no podía.
- **La respuesta se arma con todo lo que el turno produjo**, no sólo con la
  última actualización del nodo terminal. ``finalize`` no devuelve
  ``tokens_used`` (lo pone el analista), así que el ``complete`` del WS salía
  siempre con 0.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any
from uuid import uuid4

import app.application.pipeline.nodes as nodes_pkg
from app.application.answers.engine import (
    ChunkEvent,
    ClarificationEvent,
    ClearAnswerEvent,
    CompleteEvent,
    EngineEvent,
    EngineRequest,
    EngineResult,
    PassthroughEvent,
    StatusEvent,
)

# Los nodos que cierran un turno: cuando uno de ellos actualiza
# `clean_answer`, esa es la respuesta.
TERMINAL_NODES: frozenset[str] = frozenset(
    {
        "finalize",
        "cache_reply",
        "fast_reply",
        # También cierra: el planner pidió una precisión. Sin esto, una
        # pregunta ambigua emitía sólo el evento `clarification` y el WS se
        # cerraba sin `complete` (2026-05-14).
        "clarify_reply",
    }
)


def event_from_custom(payload: Any) -> EngineEvent:
    """Un evento de ``get_stream_writer()`` como evento tipado.

    Sólo se tipa lo que tiene exactamente la forma conocida; cualquier otra
    cosa pasa intacta, para que el frontend reciba lo mismo que antes.
    """
    if not isinstance(payload, dict):
        return PassthroughEvent({"value": payload})
    kind = payload.get("type")
    keys = set(payload)
    if kind == "status" and keys in (
        {"type", "step", "detail"},
        {"type", "step", "detail", "connector"},
    ):
        return StatusEvent(payload["step"], payload["detail"], payload.get("connector"))
    if kind == "chunk" and keys == {"type", "content"}:
        return ChunkEvent(payload["content"])
    if kind == "clear_answer" and keys == {"type"}:
        return ClearAnswerEvent()
    if kind == "clarification" and keys == {"type", "question", "options"}:
        return ClarificationEvent(payload["question"], tuple(payload["options"] or ()))
    return PassthroughEvent(payload)


def result_from_state(state: dict[str, Any]) -> EngineResult:
    confidence = state.get("confidence")
    return EngineResult(
        answer=state.get("clean_answer") or "",
        sources=state.get("sources") or [],
        chart_data=state.get("chart_data"),
        map_data=state.get("map_data"),
        citations=state.get("citations") or [],
        documents=state.get("documents"),
        warnings=state.get("warnings") or [],
        tokens_used=int(state.get("tokens_used") or 0),
        intent=state.get("plan_intent") or "",
        confidence=float(confidence) if confidence is not None else 1.0,
    )


class LegacyGraphEngine:
    """El pipeline planificador → NL2SQL → analista."""

    name = "legacy"
    # Clasificación, caché, historial, analytics y auditoría viven en los
    # nodos del grafo: el runner no los repite.
    handles_cross_cutting = True

    def __init__(self, graph: Any, deps: Any, *, persistent: bool) -> None:
        """``persistent``: el grafo se compiló con checkpointer."""
        self._graph = graph
        self._deps = deps
        self._persistent = persistent

    def initial_state(self, req: EngineRequest) -> dict[str, Any]:
        state: dict[str, Any] = {
            "question": req.question,
            "user_id": req.user_id,
            "conversation_id": req.conversation_id,
            "mode": req.mode,
            "replan_count": 0,
        }
        if req.bypass_cache:
            state["bypass_cache"] = True
        # Para que `load_chat_history` filtre por dueño también en el repo.
        if req.owner_user_id is not None:
            state["owner_user_id"] = req.owner_user_id
        return state

    def config(self, req: EngineRequest) -> dict[str, Any]:
        """El ``thread_id`` del checkpointer.

        Un grafo compilado CON checkpointer rechaza cualquier invocación sin
        ``thread_id`` (``ValueError: Checkpointer requires one or more of the
        following 'configurable' keys``). Sin conversación va un hilo efímero
        por turno: no hay historial que continuar. Antes esto se armaba en
        dos routers y a `/ask` le faltaba: cada consulta de la API pública
        terminaba en 500.
        """
        if not self._persistent:
            return {}
        return {"configurable": {"thread_id": req.conversation_id or f"efimero-{uuid4()}"}}

    async def stream(self, req: EngineRequest) -> AsyncGenerator[EngineEvent, None]:
        # Los nodos leen las dependencias de una ContextVar.
        nodes_pkg.set_deps(self._deps)
        produced: dict[str, Any] = {}
        async for mode, payload in self._graph.astream(
            self.initial_state(req),
            config=self.config(req),
            stream_mode=["updates", "custom"],
        ):
            if mode == "custom":
                yield event_from_custom(payload)
                continue
            if mode != "updates" or not isinstance(payload, dict):
                continue
            for node_name, update in payload.items():
                if not isinstance(update, dict):
                    continue
                produced.update(update)
                if node_name in TERMINAL_NODES and "clean_answer" in update:
                    yield CompleteEvent(result_from_state(produced))
        # Se consume el stream hasta el final aunque el `complete` ya haya
        # salido: cortarlo antes podría dejar sin guardar el último checkpoint
        # de la conversación.
