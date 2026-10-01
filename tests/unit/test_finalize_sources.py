"""Fuentes y registro honestos en ``finalize_node``.

Caso real (lanzamiento del MCP, 30-sep-2026): "¿Cuántas personas con
discapacidad hay en el partido de Pinamar?". El sandbox no trajo nada, georef
ubicó el partido y la respuesta quedó registrada como exitosa, "servida" desde
georef. Otra corrida de la misma pregunta listó como fuentes tablas de
cualquier tema, porque entraba todo resultado con filas.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.nodes.finalize import (
    ROLE_AUXILIAR,
    ROLE_PRINCIPAL,
    ROLE_RELLENO,
    _extract_sources,
    finalize_node,
    result_role,
)
from app.domain.entities.connectors.data_result import DataResult, PlanStep


def _georef() -> DataResult:
    return DataResult(
        source="georef",
        portal_name="API de Georef Argentina",
        portal_url="https://apis.datos.gob.ar/georef/api/departamentos?nombre=Pinamar",
        dataset_title="Entidades geográficas: departamentos",
        format="geo",
        records=[{"id": "06644", "nombre": "Pinamar"}],
    )


def _nl2sql(title: str, *, fallback: bool = False, served: str = "raw.tabla") -> DataResult:
    metadata: dict[str, Any] = {"served_table": served}
    if fallback:
        metadata["used_fallback"] = True
    return DataResult(
        source="sandbox:nl2sql",
        portal_name="datos.gob.ar",
        portal_url="https://datos.gob.ar/x",
        dataset_title=title,
        format="json",
        records=[{"a": 1}, {"a": 2}, {"a": 3}],
        metadata=metadata,
    )


def _plan(*actions: str) -> SimpleNamespace:
    return SimpleNamespace(
        intent="x",
        steps=[PlanStep(id=f"s{i}", action=a, description="") for i, a in enumerate(actions)],
    )


class TestRol:
    def test_georef_es_auxiliar_si_el_plan_buscaba_otro_dato(self) -> None:
        assert result_role(_georef(), _plan("query_georef", "query_sandbox")) == ROLE_AUXILIAR

    def test_georef_es_el_dato_si_la_pregunta_era_geografica(self) -> None:
        """ "¿Qué municipios tiene Córdoba?" se responde con georef."""
        assert result_role(_georef(), _plan("query_georef", "analyze")) == ROLE_PRINCIPAL

    def test_el_ultimo_recurso_del_nl2sql_es_relleno(self) -> None:
        assert result_role(_nl2sql("X", fallback=True), _plan("query_sandbox")) == ROLE_RELLENO
        assert result_role(_nl2sql("X"), _plan("query_sandbox")) == ROLE_PRINCIPAL


class TestFuentes:
    def test_solo_lo_que_respondio(self) -> None:
        plan = _plan("query_georef", "query_sandbox")
        results = [_georef(), _nl2sql("Relleno", fallback=True), _nl2sql("Estudio 2018")]
        assert [s["name"] for s in _extract_sources(results, plan)] == ["Estudio 2018"]

    def test_sin_principal_lista_lo_que_se_uso(self) -> None:
        """Si sólo hubo georef, es lo que se usó: se lista (y finalize lo
        registra como sin datos, ver abajo)."""
        plan = _plan("query_georef", "query_sandbox")
        sources = _extract_sources([_georef()], plan)
        assert [s["name"] for s in sources] == ["Entidades geográficas: departamentos"]

    def test_resultados_vacios_nunca_son_fuente(self) -> None:
        vacio = _nl2sql("Vacío")
        vacio.records = []
        assert _extract_sources([vacio], _plan("query_sandbox")) == []


class _Deps:
    metrics = SimpleNamespace(record_tokens_used=lambda *_a, **_k: None)
    cache = embedding = semantic_cache = llm = None


@pytest.fixture()
def capturado(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    async def _analytics(**kwargs: Any) -> None:
        captured["analytics"] = kwargs

    async def _write_cache(_q: str, result: dict, *_a: Any, **_k: Any) -> None:
        captured["cache"] = result

    monkeypatch.setattr(nodes_pkg, "get_deps", lambda: _Deps(), raising=False)
    monkeypatch.setattr("app.application.pipeline.nodes.finalize.write_cache", _write_cache)
    monkeypatch.setattr("app.application.pipeline.nodes.finalize.audit_query", lambda **_k: None)
    monkeypatch.setattr(
        "app.application.pipeline.nodes.finalize.spawn_background", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        "app.application.pipeline.nodes.finalize.record_terminal_analytics", _analytics
    )
    return captured


def _state(results: list, plan: Any, answer: str = "Respuesta.") -> dict[str, Any]:
    return {
        "question": "¿Cuántas personas con discapacidad hay en Pinamar?",
        "user_id": "u",
        "plan": plan,
        "plan_intent": "x",
        "clean_answer": answer,
        "data_results": results,
        "step_warnings": [],
        "conversation_id": "",
        "memory": None,
        "_start_time": 0.0,
    }


class TestRegistro:
    async def test_solo_georef_no_cuenta_como_exito(self, capturado: dict[str, Any]) -> None:
        plan = _plan("query_georef", "query_sandbox")
        await finalize_node(_state([_georef()], plan))  # type: ignore[arg-type]
        a = capturado["analytics"]
        assert a["success"] is False
        assert a["error_message"] == "sin_datos_principales"
        assert a["row_count"] == 0

    async def test_la_tabla_servida_es_la_principal(self, capturado: dict[str, Any]) -> None:
        plan = _plan("query_georef", "query_sandbox")
        results = [_georef(), _nl2sql("Estudio 2018", served="raw.estudio")]
        out = await finalize_node(_state(results, plan))  # type: ignore[arg-type]
        a = capturado["analytics"]
        assert a["success"] is True and a["error_message"] is None
        assert a["served_table"] == "raw.estudio"
        assert a["row_count"] == 3  # las de la tabla servida, no las de georef
        assert [s["name"] for s in out["sources"]] == ["Estudio 2018"]

    async def test_el_cache_guarda_las_fuentes_filtradas(self, capturado: dict[str, Any]) -> None:
        """Si el cache guardara la lista vieja, la re-serviría en cada acierto."""
        plan = _plan("query_georef", "query_sandbox")
        await finalize_node(_state([_georef(), _nl2sql("Estudio 2018")], plan))  # type: ignore[arg-type]
        assert [s["name"] for s in capturado["cache"]["sources"]] == ["Estudio 2018"]

    async def test_respuesta_conceptual_sin_datos_sigue_siendo_exito(
        self, capturado: dict[str, Any]
    ) -> None:
        await finalize_node(_state([], _plan("analyze")))  # type: ignore[arg-type]
        assert capturado["analytics"]["success"] is True
