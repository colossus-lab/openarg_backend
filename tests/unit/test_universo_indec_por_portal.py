"""Una pregunta que nombra al INDEC tiene que poder llegar a datos del INDEC
publicados en datos.gob.ar.

Caso real (lanzamiento del MCP, 30-sep-2026): "¿Cuántas personas con
discapacidad hay en el partido de Pinamar? Usá el Estudio Nacional sobre el
Perfil de las Personas con Discapacidad (2018) del INDEC". El estudio está en
`raw.datos_gob_ar__estudio_nacional_sobre_el_perfil_de__08ca74a9__v1`: su
nombre sale del portal, no del organismo. La palabra "INDEC" dispara el glob
`cache_indec_*`, que matchea decenas de tablas reales; con pistas que
matcheaban, el sandbox no corría la búsqueda vectorial (la que sí encuentra
el estudio) y el NL2SQL nunca lo veía. La respuesta dijo que el estudio "no está
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


async def test_el_estudio_del_indec_en_datos_gob_ar_llega_al_nl2sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tables = [
        # Señuelos: el glob `cache_indec_*` los matchea y hoy le alcanza.
        *(_FakeTable(f"raw.cache_indec_serie_{i}", 100, ["periodo", "valor"]) for i in range(60)),
        _FakeTable(ESTUDIO, 82327, ["ID", "pondera", "dificultad_total", "tipo_dificultad"]),
    ]
    subgraph = _CapturingSubgraph()

    async def _semantica(_q: str, cached: list[Any], *_a: Any, **_k: Any) -> list[str]:
        # Lo que devuelve la búsqueda vectorial real para esta pregunta en
        # staging (2026-10-01): el estudio primero, similitud 0,660. Es la
        # misma búsqueda que usa `buscar_datasets`. La de `table_catalog`
        # no lo encuentra (lo mejor que devuelve es 0,44, de otros temas).
        assert any(t.table_name == ESTUDIO for t in cached)
        return [ESTUDIO]

    async def _sin_entradas(_names: list[str], _sandbox: Any) -> dict:
        return {}

    async def _no_deberia_bajar_nada(_nl_query: str) -> list:
        raise AssertionError("el estudio está cacheado: no corresponde la descarga en vivo")

    async def _compiled() -> _CapturingSubgraph:
        return subgraph

    base = "app.application.pipeline.connectors.sandbox"
    monkeypatch.setattr(f"{base}.discover_tables_by_vector_search", _semantica)
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
    # El NL2SQL ve las primeras 50: el estudio va adelante de los 60 señuelos
    # que matcheó el glob, y los señuelos siguen ahí.
    assert universo[0] == ESTUDIO
    assert universo.count(ESTUDIO) == 1
    assert "raw.cache_indec_serie_0" in universo


def test_el_analista_no_afirma_que_un_dataset_no_existe() -> None:
    """La segunda mitad del caso: aunque la búsqueda falle, la respuesta no
    puede decir que el estudio "no está disponible". El analista sólo ve lo
    que trajo esta búsqueda, no el catálogo entero."""
    from app.prompts import load_prompt

    prompt = load_prompt("analyst")
    prohibidas = prompt[prompt.index("FRASES ABSOLUTAMENTE PROHIBIDAS") :]
    assert '"no está disponible"' in prohibidas
    assert '"no está precargado"' in prohibidas
    # Y la salida correcta cuando la tabla está pero no llega al nivel pedido.
    assert "Sin el nivel geográfico pedido" in prompt
    assert "NO presentes una cifra nacional o regional como si fuera de ese lugar" in prompt


async def test_si_la_busqueda_semantica_no_trae_nada_sigue_el_glob(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sin resultados semánticos, el universo es el de siempre: lo del glob."""
    tables = [_FakeTable(f"raw.cache_indec_serie_{i}", 100, ["periodo", "valor"]) for i in range(3)]
    subgraph = _CapturingSubgraph()

    async def _nada(*_a: Any, **_k: Any) -> list[str]:
        return []

    async def _sin_entradas(_names: list[str], _sandbox: Any) -> dict:
        return {}

    async def _compiled() -> _CapturingSubgraph:
        return subgraph

    base = "app.application.pipeline.connectors.sandbox"
    monkeypatch.setattr(f"{base}.discover_tables_by_vector_search", _nada)
    monkeypatch.setattr(f"{base}.get_catalog_entries", _sin_entradas)
    monkeypatch.setattr(
        "app.application.pipeline.subgraphs.nl2sql.get_compiled_nl2sql_subgraph", _compiled
    )

    step = PlanStep(
        id="s",
        action="query_sandbox",
        description="IPC",
        params={"tables": ["cache_indec_*"], "query": "IPC del INDEC"},
    )
    await execute_sandbox_step(
        step,
        sandbox=_FakeSandbox(tables),
        llm=_Unused(),
        embedding=_Unused(),
        vector_search=_Unused(),
        semantic_cache=_Unused(),
        user_query="¿Cómo viene el IPC del INDEC?",
    )

    assert subgraph.state is not None
    assert [t.table_name for t in subgraph.state["tables"]] == [t.table_name for t in tables]
