"""El pipeline no puede depender de si la tabla se reporta calificada.

`list_cached_tables` devuelve `raw.cache_x` para la capa raw y `cache_x`
para las legacy de `public`. Las dos formas nombran la misma tabla al
ejecutar, porque el sandbox corre con `search_path = public,raw`. Todo lo
que compara nombres tiene que dar el mismo resultado con una u otra.

Ese invariante estuvo roto entre mayo y septiembre de 2026: el contrato
cambió y los ~9 sitios que comparaban siguieron hablando en nombres pelados.
El síntoma visible fue que los globs del planner dejaron de matchear, y una
pregunta sobre discapacidad terminó contestada con inflación en vivo.

La fixture `qualify` corre cada caso dos veces, sobre el mismo universo
dicho de las dos formas. Un décimo sitio que se agregue mañana sobre un
camino cubierto acá rompe la variante `raw` y sigue verde en la `public` —
que es exactamente la señal que no existía.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest

from app.application.pipeline.connectors.cache_table_selection import (
    build_table_compat_notes,
    hint_matches_table,
    rewrite_legacy_sql_tables,
)
from app.application.pipeline.connectors.sandbox import execute_sandbox_step
from app.domain.entities.connectors.data_result import PlanStep


@pytest.fixture(params=["public", "raw"])
def qualify(request: pytest.FixtureRequest) -> Callable[[str], str]:
    """El mismo nombre, dicho de las dos formas."""
    if request.param == "public":
        return lambda n: n
    return lambda n: f"raw.{n}"


# ── el matcher, aislado ────────────────────────────────────


@pytest.mark.parametrize(
    ("pattern", "table", "esperado"),
    [
        # El síntoma reportado.
        ("cache_indec_*", "raw.cache_indec_ipc", True),
        ("cache_indec_*", "cache_indec_ipc", True),
        # Un hint exacto, que es lo que emite discover_catalog_hints_for_planner.
        ("cache_indec_ipc", "raw.cache_indec_ipc", True),
        # Globs con `*` en el medio: existen de verdad en KEYWORD_ROUTES.
        ("cache_*export*", "raw.cache_aduana_export_2024", True),
        # No cruzar familias.
        ("cache_indec_*", "raw.cache_bcra_cotizaciones", False),
        # Un patrón que nombra schema se respeta tal cual.
        ("raw.cache_indec_*", "raw.cache_indec_ipc", True),
        ("raw.cache_indec_*", "cache_indec_ipc", False),
        # `mart` no está en el search_path: un glob `cache_*` no lo alcanza.
        # Los marts entran por re-inyección, que es la semántica de BUG-001.
        ("cache_*", "mart.presupuesto_consolidado", False),
        ("mart.*", "mart.presupuesto_consolidado", True),
    ],
)
def test_hint_matches_table(pattern: str, table: str, esperado: bool) -> None:
    assert hint_matches_table(pattern, table) is esperado


# ── las notas de compatibilidad ────────────────────────────


def test_las_notas_de_alias_aparecen_con_cualquiera_de_las_dos_formas(
    qualify: Callable[[str], str],
) -> None:
    notas = build_table_compat_notes(
        [
            qualify("cache_series_inflacion_ipc"),
            qualify("cache_bcra_cotizaciones"),
            qualify("cache_presupuesto_credito_2026"),
        ]
    )

    assert "cache_series_tiempo_ipc" in notas
    assert "cache_bcra_principales_variables" in notas
    assert "cache_presupuesto_nacional" in notas


def test_el_rewrite_de_sql_legacy_encuentra_la_tabla_real(
    qualify: Callable[[str], str],
) -> None:
    sql = rewrite_legacy_sql_tables(
        "SELECT * FROM cache_presupuesto_nacional",
        [qualify("cache_presupuesto_credito_2026")],
    )

    assert "cache_presupuesto_credito_2026" in sql
    assert "cache_presupuesto_nacional" not in sql


# ── el paso del sandbox, de punta a punta ──────────────────


@dataclass
class _FakeTable:
    table_name: str
    row_count: int = 0
    columns: list[str] = field(default_factory=list)
    dataset_id: str | None = None


class _FakeSandbox:
    """Sin `_engine` a propósito.

    Así el enriquecimiento de marts y `get_column_types` se degradan a no-op
    por sus propios `try/except`, y el test atraviesa el camino real sin
    necesitar una base.
    """

    def __init__(self, tables: list[_FakeTable]):
        self._tables = tables

    async def list_cached_tables(self) -> list[_FakeTable]:
        return list(self._tables)


class _Unused:
    pass


@pytest.mark.asyncio
async def test_un_glob_del_planner_encuentra_sus_tablas(
    qualify: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """El caso del incidente, en su forma más chica.

    Con la variante `raw` esto fallaba: el glob no matcheaba, la rama INDEC
    se disparaba y la pregunta se contestaba con una descarga en vivo.
    """
    tables = [
        _FakeTable(qualify("cache_indec_ipc_aperturas"), 120, ["periodo", "valor"]),
        _FakeTable(qualify("cache_indec_canasta_basica"), 80, ["periodo", "valor"]),
        _FakeTable(qualify("cache_bcra_cotizaciones"), 50, ["fecha", "valor"]),
    ]

    async def _fake_catalog_entries(table_names: list[str], _sandbox: Any) -> dict:
        return {}

    async def _no_deberia_bajar_nada(_nl_query: str) -> list:
        raise AssertionError("el glob matcheó tablas cacheadas: no corresponde la descarga en vivo")

    monkeypatch.setattr(
        "app.application.pipeline.connectors.sandbox.get_catalog_entries",
        _fake_catalog_entries,
    )
    monkeypatch.setattr(
        "app.application.pipeline.connectors.sandbox.indec_live_fallback",
        _no_deberia_bajar_nada,
    )

    step = PlanStep(
        id="step_1",
        action="query_sandbox",
        description="Datos del INDEC",
        params={"tables": ["cache_indec_*"], "query": "qué datasets del INDEC hay en el cache"},
    )

    results = await execute_sandbox_step(
        step,
        sandbox=_FakeSandbox(tables),
        llm=_Unused(),
        embedding=_Unused(),
        vector_search=_Unused(),
        semantic_cache=_Unused(),
        user_query="¿Qué datasets del INDEC hay en el cache?",
    )

    assert len(results) == 1
    servidas = {row["table_name"] for row in results[0].records}
    assert servidas == {
        qualify("cache_indec_ipc_aperturas"),
        qualify("cache_indec_canasta_basica"),
    }


@pytest.mark.asyncio
async def test_el_catalogo_encuentra_sus_metadatos(
    qualify: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`table_catalog` guarda las dos formas; los metadatos tienen que llegar igual.

    Sin esto el prompt de NL2SQL perdía display_name, description y domain,
    y el título caía al nombre crudo de la tabla.
    """
    tables = [_FakeTable(qualify("cache_indec_ipc_aperturas"), 120, ["periodo", "valor"])]

    async def _catalogo_pelado(table_names: list[str], _sandbox: Any) -> dict:
        # Simula el estado real: la fila existe indexada por el nombre que
        # pidió el llamador, sea cual sea la forma.
        return {
            name: {"display_name": "IPC — Aperturas", "description": "d"} for name in table_names
        }

    monkeypatch.setattr(
        "app.application.pipeline.connectors.sandbox.get_catalog_entries",
        _catalogo_pelado,
    )

    step = PlanStep(
        id="step_1",
        action="query_sandbox",
        description="Datos del INDEC",
        params={"tables": ["cache_indec_*"], "query": "qué datasets del INDEC hay en el cache"},
    )

    results = await execute_sandbox_step(
        step,
        sandbox=_FakeSandbox(tables),
        llm=_Unused(),
        embedding=_Unused(),
        vector_search=_Unused(),
        semantic_cache=_Unused(),
        user_query="¿Qué datasets del INDEC hay en el cache?",
    )

    assert results[0].records[0]["display_name"] == "IPC — Aperturas"
