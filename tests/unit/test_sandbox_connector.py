from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from app.application.pipeline.connectors.sandbox import execute_sandbox_step
from app.domain.entities.connectors.data_result import PlanStep


@dataclass
class _FakeTable:
    table_name: str
    row_count: int = 0
    columns: list[str] = field(default_factory=list)
    dataset_id: str | None = None


class _FakeSandbox:
    def __init__(self, tables: list[_FakeTable]):
        self._tables = tables

    async def list_cached_tables(self) -> list[_FakeTable]:
        return list(self._tables)


class _Unused:
    pass


@pytest.mark.asyncio
async def test_execute_sandbox_step_short_circuits_dataset_discovery(monkeypatch):
    tables = [
        _FakeTable("cache_datos_gob_ar_indicadores_educativos", 120, ["anio", "valor"]),
        _FakeTable("cache_datos_gob_ar_base_de_datos_por_escuela_2017", 450, ["cue", "nombre"]),
    ]
    sandbox = _FakeSandbox(tables)
    step = PlanStep(
        id="step_2",
        action="query_sandbox",
        description="Listar datasets de educación",
        params={"tables": [t.table_name for t in tables]},
    )

    async def _fake_catalog_entries(_table_names: list[str], _sandbox: Any):
        return {
            "cache_datos_gob_ar_indicadores_educativos": {
                "display_name": "Indicadores educativos",
                "description": "Indicadores agregados del sistema educativo.",
                "domain": "social",
                "subdomain": "educacion",
            },
            "cache_datos_gob_ar_base_de_datos_por_escuela_2017": {
                "display_name": "Base por escuela 2017",
                "description": "Información por establecimiento educativo.",
                "domain": "social",
                "subdomain": "educacion",
            },
        }

    async def _unexpected_subgraph():
        raise AssertionError("Dataset discovery should not invoke the NL2SQL subgraph")

    monkeypatch.setattr(
        "app.application.pipeline.connectors.sandbox.get_catalog_entries",
        _fake_catalog_entries,
    )
    monkeypatch.setattr(
        "app.application.pipeline.subgraphs.nl2sql.get_compiled_nl2sql_subgraph",
        _unexpected_subgraph,
    )

    results = await execute_sandbox_step(
        step,
        sandbox=sandbox,
        llm=_Unused(),
        embedding=_Unused(),
        vector_search=_Unused(),
        semantic_cache=_Unused(),
        user_query="¿Qué datasets de educación hay en datos.gob.ar?",
    )

    assert len(results) == 1
    result = results[0]
    assert result.source == "sandbox:dataset_discovery"
    assert result.metadata["dataset_discovery"] is True
    assert result.metadata["total_matches"] == 2
    assert [row["table_name"] for row in result.records] == [
        "cache_datos_gob_ar_indicadores_educativos",
        "cache_datos_gob_ar_base_de_datos_por_escuela_2017",
    ]

