"""El paso DNFCG como paso interno de la búsqueda profunda (decisión F4-b).

Antes se apendeaba a TODA respuesta con el toggle prendido. Ahora corre sólo
cuando el acotamiento reconoció una política pública concreta. Estos tests
fijan las dos mitades de esa decisión: que corra cuando corresponde, y —lo que
importa más— que la marca sobreviva al turno en que se toma.

`policy_relevant` se decide en el turno del acotamiento, que termina en
`clarify_reply` sin llegar nunca al análisis. El paso sólo puede correr en el
turno SIGUIENTE, así que la marca tiene que cruzar de uno a otro. Si no cruza,
la decisión de conservar el agente es letra muerta: nunca se ejecuta.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from app.application.pipeline.edges import route_after_coordinator
from app.application.pipeline.nodes import PipelineDeps, set_deps
from app.application.pipeline.nodes.scoping import scoping_node
from app.domain.ports.llm.llm_provider import LLMResponse


def _deps(**over: Any) -> PipelineDeps:
    kw: dict[str, Any] = {f.name: None for f in dataclasses.fields(PipelineDeps)}
    kw.update(over)
    return PipelineDeps(**kw)


class _LLM:
    def __init__(self, payload: str) -> None:
        self.payload = payload

    async def chat_json(self, **_: Any) -> LLMResponse:
        return LLMResponse(content=self.payload, tokens_used=0, model="fake")


_POLITICA = (
    '{"needs_scoping": true, "question": "¿Qué aspecto?",'
    ' "options": ["Cobertura", "Impacto", "Todo"], "policy_relevant": true}'
)


# ── cuándo corre ───────────────────────────────────────────


@pytest.mark.parametrize(
    ("estado", "espera"),
    [
        ({"mode": "deep", "policy_relevant": True}, "policy"),
        ({"mode": "deep", "policy_relevant": False}, "finalize"),
        ({"mode": "deep"}, "finalize"),
        ({"mode": "normal", "policy_relevant": True}, "finalize"),
        ({}, "finalize"),
    ],
)
def test_ruteo_al_paso_dnfcg(estado: dict, espera: str) -> None:
    assert route_after_coordinator(estado) == espera


@pytest.mark.asyncio
async def test_el_acotamiento_marca_una_pregunta_de_politica() -> None:
    set_deps(_deps(llm=_LLM(_POLITICA)))
    out = await scoping_node(
        {"mode": "deep", "question": "qué impacto tuvo la AUH en la pobreza infantil"}
    )
    assert out["policy_relevant"] is True
    # y el turno termina en repregunta: el análisis todavía no corrió
    assert out["plan"].intent == "clarification"


@pytest.mark.asyncio
async def test_una_pregunta_que_no_es_de_politica_no_lo_activa() -> None:
    set_deps(
        _deps(
            llm=_LLM(
                '{"needs_scoping": true, "question": "¿Qué año?",'
                ' "options": ["2023", "2024", "Todos"], "policy_relevant": false}'
            )
        )
    )
    out = await scoping_node({"mode": "deep", "question": "cuántos delitos hubo en CABA"})
    assert out["policy_relevant"] is False
    assert route_after_coordinator({"mode": "deep", **out}) == "finalize"


# ── la marca tiene que cruzar de turno ─────────────────────


@pytest.mark.asyncio
async def test_el_segundo_turno_no_vuelve_a_marcar() -> None:
    """El acotamiento sale temprano cuando ya corrió. Ése es el diseño —no
    queremos gastar una llamada por turno— pero significa que `policy_relevant`
    NO se recalcula: tiene que venir del estado anterior."""
    set_deps(_deps(llm=_LLM(_POLITICA)))
    out = await scoping_node(
        {"mode": "deep", "question": "Impacto", "scoping_done": True, "policy_relevant": True}
    )
    assert "policy_relevant" not in out, (
        "si el acotamiento reescribiera la marca, pisaría la del turno anterior"
    )


@pytest.mark.asyncio
async def test_con_historial_tampoco_vuelve_a_marcar() -> None:
    set_deps(_deps(llm=_LLM(_POLITICA)))
    out = await scoping_node({"mode": "deep", "question": "Impacto", "planner_ctx": "Usuario: ..."})
    assert out == {"scoping_done": True}


def test_la_marca_es_parte_del_estado_que_se_checkpointea() -> None:
    """Que cruce de turno no lo implementa este código: lo hace el checkpointer
    sobre `OpenArgState`. Lo que sí es nuestro es que la marca esté declarada
    ahí — si viviera fuera del estado, no se guardaría y el paso no correría
    nunca.

    (El cruce real se verificó end-to-end contra staging: turno 1 repregunta,
    turno 2 con un grafo NUEVO y el mismo `thread_id` no vuelve a preguntar.)
    """
    from app.application.pipeline.state import OpenArgState

    anotaciones = OpenArgState.__annotations__
    assert "policy_relevant" in anotaciones
    assert "scoping_done" in anotaciones
    assert "mode" in anotaciones


def test_sin_checkpointer_la_marca_se_pierde_y_el_paso_no_corre() -> None:
    """El grafo se compila también SIN checkpointer —el router cachea las dos
    variantes— y en ese camino el turno 2 arranca sin `policy_relevant`. El
    paso DNFCG simplemente no corre.

    Es una limitación conocida, no un accidente: sin estado entre turnos no hay
    de dónde sacar la marca, y recalcularla gastaría una llamada al modelo en
    cada turno. Queda fijado acá para que si alguien lo cambia, lo cambie a
    propósito.
    """
    turno2_sin_estado = {"mode": "deep", "question": "Impacto", "scoping_done": True}
    assert route_after_coordinator(turno2_sin_estado) == "finalize"
