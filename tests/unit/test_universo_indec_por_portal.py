"""Una pregunta que nombra al INDEC tiene que poder llegar a datos del INDEC
publicados en datos.gob.ar.

Caso real (lanzamiento del MCP, 30-sep-2026): "¿Cuántas personas con
discapacidad hay en el partido de Pinamar? Usá el Estudio Nacional sobre el
Perfil de las Personas con Discapacidad (2018) del INDEC". El estudio está en
`raw.datos_gob_ar__estudio_nacional_sobre_el_perfil_de__08ca74a9__v1`: su
nombre sale del portal, no del organismo. La palabra "INDEC" dispara el glob
`cache_indec_*`, que matchea decenas de tablas reales; con pistas que
matchean, el sandbox no consulta la búsqueda por catálogo (la que sí encuentra
el estudio) y el NL2SQL nunca lo ve. La respuesta dijo que el estudio "no está
disponible".

Mismo patrón que `test_raw_layer_name_parity.py`: universo falso, camino real
del sandbox, y se captura lo que le llega al NL2SQL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from app.application.pipeline.connectors.sandbox import execute_sandbox_step
from app.domain.entities.connectors.data_result import PlanStep

ESTUDIO = "raw.datos_gob_ar__estudio_nacional_sobre_el_perfil_de__08ca74a9__v1"
PREGUNTA = (
    "¿Cuántas personas con discapacidad hay en el partido de Pinamar, provincia de "
    "Buenos Aires? Usá el Estudio Nacional sobre el Perfil de las Personas con "
    "Discapacidad (2018) del INDEC si está disponible"
)


@dataclass
class _FakeTable:
    table_name: str
    row_count: int = 0
    columns: list[str] = field(default_factory=list)
    dataset_id: str | None = None


class _FakeSandbox:
    """Sin `_engine`: marts y tipos de columna se degradan a no-op."""

    def __init__(self, tables: list[_FakeTable]):
        self._tables = tables

    async def list_cached_tables(self) -> list[_FakeTable]:
        return list(self._tables)


class _Unused:
    pass


class _CapturingSubgraph:
    def __init__(self) -> None:
        self.state: dict[str, Any] | None = None

    async def ainvoke(self, state: dict[str, Any]) -> dict[str, Any]:
        self.state = state
        return {"data_results": []}


@pytest.mark.xfail(
    strict=True,
    reason="Con pistas cache_indec_* que matchean, el sandbox no consulta el catálogo "
    "(sandbox.py ~1209-1335). Se arregla en el PR de búsqueda (paso 3 del plan Pinamar).",
)
async def test_el_estudio_del_indec_en_datos_gob_ar_llega_al_nl2sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tables = [
        # Señuelos: el glob `cache_indec_*` los matchea y hoy le alcanza.
        *(_FakeTable(f"raw.cache_indec_serie_{i}", 100, ["periodo", "valor"]) for i in range(60)),
        _FakeTable(ESTUDIO, 82327, ["ID", "pondera", "dificultad_total", "tipo_dificultad"]),
    ]
    subgraph = _CapturingSubgraph()

    async def _catalogo(query: str, *_a: Any, **_k: Any) -> list[tuple[str, float]]:
        # Lo que devuelve la búsqueda por catálogo real para esta pregunta:
        # `buscar_datasets("discapacidad")` pone al estudio segundo.
        return [(ESTUDIO, 0.71)]

    async def _sin_entradas(_names: list[str], _sandbox: Any) -> dict:
        return {}

    async def _no_deberia_bajar_nada(_nl_query: str) -> list:
        raise AssertionError("el estudio está cacheado: no corresponde la descarga en vivo")

    async def _compiled() -> _CapturingSubgraph:
        return subgraph

    base = "app.application.pipeline.connectors.sandbox"
    monkeypatch.setattr(f"{base}.discover_tables_by_catalog_search", _catalogo)
    monkeypatch.setattr(f"{base}.get_catalog_entries", _sin_entradas)
    monkeypatch.setattr(f"{base}.indec_live_fallback", _no_deberia_bajar_nada)
    monkeypatch.setattr(
        "app.application.pipeline.subgraphs.nl2sql.get_compiled_nl2sql_subgraph", _compiled
    )

    step = PlanStep(
        id="step_2",
        action="query_sandbox",
        description="Estudio de discapacidad del INDEC",
        params={"tables": ["cache_indec_*"], "query": PREGUNTA},
    )

    await execute_sandbox_step(
        step,
        sandbox=_FakeSandbox(tables),
        llm=_Unused(),
        embedding=_Unused(),
        vector_search=_Unused(),
        semantic_cache=_Unused(),
        user_query=PREGUNTA,
    )

    assert subgraph.state is not None, "el paso no llegó al NL2SQL"
    universo = [t.table_name for t in subgraph.state["tables"]]
    # El NL2SQL ve las primeras 50: el estudio tiene que estar entre ellas.
    assert ESTUDIO in universo[:50]
