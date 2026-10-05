"""``query_bcra`` del motor legacy ya no sustituye en silencio.

El planner legacy pide ``{"tipo": "variables", "id_variable": 1}`` para
"¿Cuántas reservas tiene el BCRA?" (``planner.txt``, ejemplo de reservas). El
conector caía a Cotizaciones USD para cualquier tipo distinto de
``cotizaciones``, así que la respuesta de reservas citaba "Cotizaciones
Cambiarias - USD (BCRA)" sin usar ninguna de sus cifras (auditoría del 04-oct,
ítem 1.5, corrida 7 de ``repro_2026_10_04.json``).

Ahora:
- una variable curada (las del agente que el BCRA no publica por adelantado)
  se lee de la API v4 con ``BCRAAdapter.get_variable``;
- cualquier otro pedido vuelve sin datos y con un aviso explícito en las
  advertencias del paso, sin reemplazarlo por otra fuente.

Con el adaptador real contra el doble de la API del BCRA.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from app.application.answers.tools.bcra import VARIABLES
from app.application.pipeline.connectors.bcra import (
    VARIABLES_V4,
    FuenteBCRANoDisponible,
    execute_bcra_step,
)
from app.application.pipeline.context_builder import build_data_context
from app.application.pipeline.step_executor import ConnectorDeps, execute_steps, is_retryable
from app.domain.entities.connectors.data_result import ExecutionPlan, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError
from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter
from app.infrastructure.resilience import retry as retry_module
from app.infrastructure.resilience.circuit_breaker import CircuitState, get_circuit_breaker
from tests.unit.bcra_fake_api import HOY, FakeBCRA

_COTIZACIONES = "/estadisticascambiarias/v1.0/Cotizaciones"


@pytest.fixture(autouse=True)
def _circuito_cerrado(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retry_module, "_backoff_delay", lambda *a: 0.0)
    for name in ("bcra_api", "bcra_api_catalogo"):
        breaker = get_circuit_breaker(name)
        breaker.state = CircuitState.CLOSED
        breaker.failure_count = 0


def _adapter(fake: FakeBCRA) -> BCRAAdapter:
    return BCRAAdapter(client=fake.client(), today=lambda: HOY)


def _step(**params: Any) -> PlanStep:
    return PlanStep(id="step_1", action="query_bcra", description="BCRA", params=params)


def _pidio_cotizaciones(fake: FakeBCRA) -> bool:
    return any(p.startswith(_COTIZACIONES) for p in fake.paths())


# ── variables curadas: API v4 ──────────────────────────────


@pytest.mark.asyncio
async def test_las_reservas_del_planner_se_leen_de_la_v4_y_no_de_cotizaciones() -> None:
    fake = FakeBCRA()

    results = await execute_bcra_step(_step(tipo="variables", id_variable=1), _adapter(fake))

    assert len(results) == 1
    reservas = results[0]
    assert reservas.dataset_title == "Reservas internacionales del BCRA (saldo diario)"
    assert "Cotizaciones" not in reservas.dataset_title
    assert reservas.records[-1] == {"fecha": "2026-09-30", "valor": 46092.0}
    assert reservas.metadata["ultima_observacion"] == "2026-09-30"
    assert "/estadisticas/v4.0/monetarias/1" in fake.paths()
    assert not _pidio_cotizaciones(fake)


@pytest.mark.asyncio
async def test_el_analista_ve_las_unidades_y_el_ultimo_dato() -> None:
    """``build_data_context`` imprime la descripción, no ``units``."""
    fake = FakeBCRA()

    results = await execute_bcra_step(_step(tipo="variables", id_variable=1), _adapter(fake))
    contexto = build_data_context(results)

    assert "Reservas internacionales del BCRA (saldo diario)" in contexto
    assert "millones de dólares" in contexto
    assert "2026-09-30" in contexto
    assert "46092" in contexto


@pytest.mark.asyncio
async def test_el_id_puede_venir_como_texto() -> None:
    fake = FakeBCRA()

    results = await execute_bcra_step(_step(tipo="variables", id_variable="4"), _adapter(fake))

    assert results[0].records[-1] == {"fecha": "2026-10-02", "valor": 1543.18}
    assert not _pidio_cotizaciones(fake)


@pytest.mark.asyncio
async def test_historica_trae_el_rango_pedido() -> None:
    fake = FakeBCRA()

    results = await execute_bcra_step(
        _step(tipo="historica", id_variable=1, fecha_desde="2026-09-01", fecha_hasta="2026-09-30"),
        _adapter(fake),
    )

    fechas = [r["fecha"] for r in results[0].records]
    assert fechas[0] >= "2026-09-01"
    assert fechas[-1] == "2026-09-30"
    assert len(fechas) == 22  # días hábiles de septiembre de 2026
    assert not _pidio_cotizaciones(fake)


@pytest.mark.asyncio
async def test_una_tasa_queda_marcada_como_porcentaje() -> None:
    fake = FakeBCRA(catalog_down=True)

    results = await execute_bcra_step(_step(tipo="variables", id_variable=7), _adapter(fake))

    assert results[0].metadata["unidad"] == "porcentaje"
    assert results[0].dataset_title == "Tasa BADLAR de bancos privados (BCRA)"


def test_las_rutas_son_las_curadas_del_agente_sin_las_adelantadas() -> None:
    """Mismos ids, títulos y unidades que ``variables_bcra``; afuera UVA, CER,
    ICL y bandas, que el BCRA publica por adelantado (medido contra la API
    real el 05-oct: la UVA llega al 15-oct, las bandas al 30-oct) y el legacy
    no separa lo que todavía no pasó."""
    adelantadas = {"uva", "cer", "icl", "banda_cambiaria_piso", "banda_cambiaria_techo"}
    esperadas = {
        v.id: (v.titulo, v.unidades, v.porcentaje)
        for key, v in VARIABLES.items()
        if key not in adelantadas
    }
    copiadas = {
        id_variable: (v.titulo, v.unidades, v.porcentaje) for id_variable, v in VARIABLES_V4.items()
    }
    assert copiadas == esperadas
    assert set(VARIABLES_V4).isdisjoint({30, 31, 40, 1187, 1188})


# ── todo lo demás: vacío con aviso, sin sustituir ──────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {"tipo": "variables"},  # rutas de dataset_index ("tasa de politica monetaria")
        {"tipo": "variables", "id_variable": 6},  # no curada
        {"tipo": "variables", "id_variable": 31},  # UVA: publica por adelantado
        {"tipo": "historica", "id_variable": 1187},  # bandas: publica por adelantado
        {"tipo": "variables", "id_variable": "reservas"},
        {"tipo": "variables", "id_variable": True},
        {"tipo": "reservas"},  # tipo desconocido
    ],
)
async def test_un_pedido_no_soportado_no_cae_en_cotizaciones(params: dict[str, Any]) -> None:
    fake = FakeBCRA()

    with pytest.raises(FuenteBCRANoDisponible) as info:
        await execute_bcra_step(_step(**params), _adapter(fake))

    assert fake.requests == []  # ni cotizaciones ni nada
    assert isinstance(info.value, ConnectorError)
    assert "no está disponible en este motor" in str(info.value)
    assert "no se la reemplazó por otra fuente" in str(info.value)


@pytest.mark.asyncio
async def test_el_aviso_llega_a_las_advertencias_y_sin_datos() -> None:
    """Por el ejecutor real de pasos: el paso no aporta filas ni fuente, y el
    aviso entra en ``step_warnings`` (que van al analista y al campo
    ``warnings`` de la respuesta)."""
    fake = FakeBCRA()
    deps = ConnectorDeps(
        series=MagicMock(),
        arg_datos=MagicMock(),
        georef=MagicMock(),
        ckan=MagicMock(),
        sesiones=MagicMock(),
        ddjj=MagicMock(),
        staff=None,
        bcra=_adapter(fake),
        sandbox=None,
        vector_search=MagicMock(),
        llm=MagicMock(),
        embedding=MagicMock(),
        semantic_cache=MagicMock(),
    )
    plan = ExecutionPlan(
        query="¿Cuál es la tasa de política monetaria?",
        intent="tasa",
        steps=[_step(tipo="variables")],
    )

    results, warnings = await execute_steps(plan, deps, MagicMock(), nl_query=plan.query)

    assert results == []
    assert len(warnings) == 1
    assert warnings[0].startswith("Conector 'query_bcra' no disponible: ")
    assert "no está disponible en este motor" in warnings[0]
    assert fake.requests == []


def test_el_aviso_no_se_reintenta() -> None:
    """``dispatch_step_with_retry`` reintenta si el texto dice "503",
    "connection", etc.: el aviso es un texto fijo, sin el id pedido."""
    for id_variable in (502, 503, 504):
        assert not is_retryable(FuenteBCRANoDisponible(f"variables id_variable={id_variable}"))


# ── cotizaciones: igual que antes ──────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("params", [{"tipo": "cotizaciones", "moneda": "USD"}, {}])
async def test_cotizaciones_sigue_igual(params: dict[str, Any]) -> None:
    fake = FakeBCRA()

    results = await execute_bcra_step(_step(**params), _adapter(fake))

    assert results[0].dataset_title == "Cotizaciones Cambiarias - USD"
    assert results[0].records[0]["tipoCotizacion"] == 1520.0
    assert fake.paths() == [_COTIZACIONES]


@pytest.mark.asyncio
async def test_sin_adaptador_sigue_vacio() -> None:
    assert await execute_bcra_step(_step(tipo="variables", id_variable=1), None) == []
