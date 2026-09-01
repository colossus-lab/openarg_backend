"""El modo profundo: ruteo, elección de modelo y compatibilidad del payload.

Lo que se prueba acá es el contrato, no el prompt: que el modo normal siga el
mismo camino de código que antes, que el acotamiento pregunte una sola vez, y
que el payload viejo (`policy_mode`) siga entrando durante el ciclo de deploy
en que frontend y backend están desfasados.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from app.application.pipeline.edges import (
    route_after_coordinator,
    route_after_scoping,
    route_after_skill_resolver,
)
from app.application.pipeline.nodes import PipelineDeps, llm_for, set_deps
from app.application.pipeline.nodes.scoping import scoping_node
from app.domain.entities.connectors.data_result import ExecutionPlan
from app.domain.ports.llm.llm_provider import LLMResponse


def _deps(**over: Any) -> PipelineDeps:
    kw: dict[str, Any] = {f.name: None for f in dataclasses.fields(PipelineDeps)}
    kw.update(over)
    return PipelineDeps(**kw)


class _FakeLLM:
    def __init__(self, payload: str = "{}", *, boom: bool = False) -> None:
        self.payload = payload
        self.boom = boom
        self.calls = 0

    async def chat_json(self, **_: Any) -> LLMResponse:
        self.calls += 1
        if self.boom:
            raise RuntimeError("bedrock caído")
        return LLMResponse(content=self.payload, tokens_used=0, model="fake")


# ── ruteo ──────────────────────────────────────────────────


def test_normal_mode_no_pasa_por_scoping() -> None:
    """La garantía de no-regresión: sin modo profundo, el camino es el de siempre."""
    assert route_after_skill_resolver({"mode": "normal"}) == "planner"
    assert route_after_skill_resolver({}) == "planner"


def test_deep_mode_acota_una_sola_vez() -> None:
    assert route_after_skill_resolver({"mode": "deep"}) == "scoping"
    assert route_after_skill_resolver({"mode": "deep", "scoping_done": True}) == "planner"


def test_scoping_cierra_el_turno_solo_si_pregunto() -> None:
    plan = ExecutionPlan(query="q", intent="clarification", steps=[])
    assert route_after_scoping({"plan": plan}) == "clarify_reply"
    assert route_after_scoping({"plan": ExecutionPlan(query="q", intent="data")}) == "planner"
    assert route_after_scoping({}) == "planner"


def test_policy_solo_corre_en_deep_y_si_es_relevante() -> None:
    """El paso DNFCG dejó de apendearse a toda respuesta profunda."""
    assert route_after_coordinator({"mode": "deep", "policy_relevant": True}) == "policy"
    assert route_after_coordinator({"mode": "deep", "policy_relevant": False}) == "finalize"
    assert route_after_coordinator({"mode": "normal", "policy_relevant": True}) == "finalize"


# ── elección de modelo ─────────────────────────────────────


def test_llm_for_elige_el_profundo_solo_en_deep() -> None:
    fast, deep = object(), object()
    d = _deps(llm=fast, llm_deep=deep)
    assert llm_for(d, {"mode": "deep"}) is deep
    assert llm_for(d, {"mode": "normal"}) is fast
    assert llm_for(d, {}) is fast


def test_llm_for_sin_modelo_profundo_configurado() -> None:
    """Sin `BEDROCK_LLM_MODEL_DEEP` el comportamiento es idéntico al de hoy."""
    fast = object()
    assert llm_for(_deps(llm=fast, llm_deep=None), {"mode": "deep"}) is fast


# ── el nodo ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_scoping_emite_opciones_clickeables() -> None:
    llm = _FakeLLM(
        '{"needs_scoping": true, "question": "¿Qué período?",'
        ' "options": ["2024", "2023", "Todo"], "policy_relevant": true}'
    )
    set_deps(_deps(llm=llm))
    out = await scoping_node({"mode": "deep", "question": "gasto en salud"})

    assert out["scoping_done"] is True
    assert out["policy_relevant"] is True
    step = out["plan"].steps[0]
    # La forma exacta que `clarify_reply_node` lee para emitir los chips.
    assert step.action == "clarification"
    assert step.params["question"] == "¿Qué período?"
    assert step.params["options"] == ["2024", "2023", "Todo"]


@pytest.mark.asyncio
async def test_scoping_no_pregunta_si_la_pregunta_ya_es_especifica() -> None:
    """Preguntar de más es la fricción que hace que el modo se apague."""
    set_deps(_deps(llm=_FakeLLM('{"needs_scoping": false}')))
    out = await scoping_node({"mode": "deep", "question": "PBI de Argentina en 2024"})
    assert out["scoping_done"] is True
    assert "plan" not in out


@pytest.mark.asyncio
async def test_scoping_no_pregunta_con_una_sola_opcion() -> None:
    set_deps(_deps(llm=_FakeLLM('{"needs_scoping": true, "options": ["2024"]}')))
    out = await scoping_node({"mode": "deep", "question": "x"})
    assert "plan" not in out


@pytest.mark.asyncio
async def test_scoping_degrada_si_el_modelo_falla() -> None:
    """Acotar es una mejora, no un requisito: la búsqueda profunda corre igual."""
    set_deps(_deps(llm=_FakeLLM(boom=True)))
    out = await scoping_node({"mode": "deep", "question": "x"})
    assert out == {"scoping_done": True}


@pytest.mark.asyncio
async def test_scoping_no_gasta_una_llamada_en_modo_normal() -> None:
    llm = _FakeLLM()
    set_deps(_deps(llm=llm))
    assert await scoping_node({"mode": "normal", "question": "x"}) == {"scoping_done": True}
    assert llm.calls == 0


# ── compatibilidad del payload ─────────────────────────────


def test_payload_viejo_sigue_entrando() -> None:
    """Frontend y backend se despliegan por separado: durante ese hueco llega
    `policy_mode` contra un backend que ya habla de `mode`."""
    from app.presentation.http.controllers.query.smart_query_v2_router import (
        SmartQueryV2Request,
    )

    assert SmartQueryV2Request(question="q", policy_mode=True).mode == "deep"
    assert SmartQueryV2Request(question="q", policy_mode=False).mode == "normal"
    assert SmartQueryV2Request(question="q", mode="deep").mode == "deep"
    assert SmartQueryV2Request(question="q").mode == "normal"


def test_tokens_se_contabilizan_por_modo() -> None:
    """Sin esto no hay forma de saber cuánto cuesta el modo profundo."""
    from app.infrastructure.monitoring.metrics import MetricsCollector

    m = MetricsCollector()
    m.record_tokens_used(100, mode="deep")
    m.record_tokens_used(30)
    tokens = m.get_metrics()["tokens"]
    assert tokens["total_used"] == 130
    assert tokens["by_mode"] == {"deep": 100, "normal": 30}


# ── presupuestos ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_presupuesto_de_tiempo_se_amplia_en_deep(monkeypatch: Any) -> None:
    """El de tiempo es el que corta primero: con 20 s el techo de replans casi
    nunca se alcanza, así que subir sólo la profundidad no cambiaría nada."""
    import time as _time

    from app.application.pipeline.nodes import coordinator

    monkeypatch.setattr(coordinator, "get_stream_writer", lambda: lambda _: None)

    # 35 s de reloj: pasado el presupuesto normal, dentro del profundo.
    t0 = _time.monotonic() - 35.0
    base = {"_start_time": t0, "replan_count": 0, "data_results": []}

    normal = await coordinator.coordinator_node({**base, "mode": "normal"})
    assert normal["coordinator_decision"] == "escalate"

    deep = await coordinator.coordinator_node({**base, "mode": "deep"})
    assert deep["coordinator_decision"] != "escalate"


@pytest.mark.asyncio
async def test_profundidad_de_replan_se_amplia_en_deep(monkeypatch: Any) -> None:
    import time as _time

    from app.application.pipeline.nodes import coordinator

    monkeypatch.setattr(coordinator, "get_stream_writer", lambda: lambda _: None)
    base = {"_start_time": _time.monotonic(), "replan_count": 2, "data_results": []}

    assert (await coordinator.coordinator_node({**base, "mode": "normal"}))[
        "coordinator_decision"
    ] == "escalate"
    assert (await coordinator.coordinator_node({**base, "mode": "deep"}))[
        "coordinator_decision"
    ] != "escalate"


def test_el_presupuesto_profundo_entra_en_el_timeout_del_cliente() -> None:
    """El cliente corta a los 120 s de inactividad (`wsBridge.ts`). El
    presupuesto profundo es de reloj total, así que tiene que quedar abajo."""
    from app.application.pipeline.nodes.coordinator import _DEEP_TIME_BUDGET_SECONDS

    assert _DEEP_TIME_BUDGET_SECONDS < 120.0


# ── prompt profundo ────────────────────────────────────────


def test_el_addendum_profundo_no_duplica_el_prompt() -> None:
    """Un segundo `planner.txt` se separaría del primero a la primera edición.
    El addendum sólo puede agregar, no reemplazar."""
    from app.infrastructure.adapters.connectors.query_planner import _DEEP_ADDENDUM

    assert "MODO PROFUNDO" in _DEEP_ADDENDUM
    # Relaja explícitamente la regla de un-solo-step, que existe por wall-clock.
    assert "un solo step" in _DEEP_ADDENDUM


# ── configuración ──────────────────────────────────────────


def test_sin_env_el_modelo_profundo_es_el_de_siempre(monkeypatch: Any) -> None:
    """Rollback = sacar la variable. Sin ella, nada cambia."""
    from app.setup.config.settings import BedrockSettings

    monkeypatch.delenv("BEDROCK_LLM_MODEL_DEEP", raising=False)
    s = BedrockSettings()
    assert s.LLM_MODEL_DEEP == s.LLM_MODEL


def test_con_env_el_modelo_profundo_es_otro(monkeypatch: Any) -> None:
    from app.setup.config.settings import BedrockSettings

    monkeypatch.setenv("BEDROCK_LLM_MODEL_DEEP", "us.anthropic.claude-sonnet-4-6")
    s = BedrockSettings()
    assert s.LLM_MODEL_DEEP == "us.anthropic.claude-sonnet-4-6"
    assert s.LLM_MODEL != s.LLM_MODEL_DEEP


@pytest.mark.asyncio
async def test_scoping_no_repregunta_en_una_conversacion_ya_empezada() -> None:
    """Sin checkpointer `scoping_done` no sobrevive al turno; el historial es la
    señal que sí. Sin este corte, clickear un chip vuelve a caer en scoping y el
    usuario queda en un bucle de repreguntas."""
    llm = _FakeLLM()
    set_deps(_deps(llm=llm))
    out = await scoping_node(
        {"mode": "deep", "question": "2024", "planner_ctx": "Usuario: gasto en salud"}
    )
    assert out == {"scoping_done": True}
    assert llm.calls == 0


# ── plan cache ─────────────────────────────────────────────


def test_el_modo_profundo_no_lee_ni_escribe_el_plan_cache() -> None:
    """El plan cache está ON por defecto y es ciego al modo. Sin este corte:

    - leerlo hace el modo profundo un no-op — un hit saltea la llamada al
      planner, que es donde entran el modelo capaz y el addendum;
    - escribirlo envenena el modo normal con planes de varios steps pensados
      para 60 s, servidos después bajo un presupuesto de 20 s.

    Se verifica sobre el fuente porque la rama no es alcanzable sin una DB.
    """
    import inspect

    from app.application.pipeline.nodes import planner

    src = inspect.getsource(planner.planner_node)
    assert 'deep = state.get("mode") == "deep"' in src
    # lectura cortada
    assert "None if deep else await _try_plan_cache_hit" in src
    # escritura cortada
    assert "not has_history and not deep and" in src


def test_el_modo_profundo_mira_mas_ancho() -> None:
    from app.application.pipeline.nodes.planner import _DEEP_DISCOVER_LIMIT, _DISCOVER_LIMIT

    assert _DEEP_DISCOVER_LIMIT > _DISCOVER_LIMIT
