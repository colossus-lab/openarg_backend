"""El adaptador del BCRA contra un doble de la API (``bcra_fake_api``).

Lo que se prueba:

- cotizaciones (v1.0): cada registro trae la fecha del día publicado; los
  rangos usan los parámetros que la API acepta (antes daban 400) y la historia
  se pagina;
- variables monetarias (v4.0): la ruta y los ids, el orden cronológico, la
  paginación con ``offset``, el contrato de metadatos que leen el aviso de
  atraso y la verificación de cifras, y que un catálogo caído no tire abajo
  los datos.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from app.domain.exceptions.connector_errors import ConnectorError
from app.infrastructure.adapters.connectors import bcra_adapter as bcra_module
from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter
from app.infrastructure.resilience import retry as retry_module
from app.infrastructure.resilience.circuit_breaker import CircuitState, get_circuit_breaker
from tests.unit.bcra_fake_api import HOY, SERIES, FakeBCRA

_BREAKERS = ("bcra_api", "bcra_api_catalogo")


def _cerrar_circuitos() -> None:
    for name in _BREAKERS:
        breaker = get_circuit_breaker(name)
        breaker.state = CircuitState.CLOSED
        breaker.failure_count = 0
        breaker.success_count = 0


@pytest.fixture(autouse=True)
def _breaker_cerrado(monkeypatch: pytest.MonkeyPatch):
    """El circuito es global por proceso: que un test no abra el de otro."""
    _cerrar_circuitos()
    # Los reintentos sin espera: el backoff real (con jitter) no se prueba acá.
    monkeypatch.setattr(retry_module, "_backoff_delay", lambda *a: 0.0)
    yield
    _cerrar_circuitos()


def _adapter(fake: FakeBCRA) -> BCRAAdapter:
    return BCRAAdapter(client=fake.client(), today=lambda: HOY)


# ── cotizaciones (estadísticas cambiarias v1.0) ────────────


async def test_cada_cotizacion_trae_la_fecha_del_dia_publicado() -> None:
    """2.4: el endpoint trae la fecha en la cabecera y el adaptador la tiraba."""
    fake = FakeBCRA()
    result = await _adapter(fake).get_cotizaciones()

    assert fake.paths() == ["/estadisticascambiarias/v1.0/Cotizaciones"]
    assert {r["codigoMoneda"] for r in result.records} == {"EUR", "REF", "USD"}
    assert all(r["fecha"] == "2026-10-02" for r in result.records)
    assert result.metadata["last_updated"] == "2026-10-02"
    assert result.metadata["ultima_observacion"] == "2026-10-02"
    assert result.metadata["oficial"] is True


async def test_filtrar_por_moneda_conserva_la_fecha() -> None:
    result = await _adapter(FakeBCRA()).get_cotizaciones(moneda="USD")
    assert result.records == [
        {
            "fecha": "2026-10-02",
            "codigoMoneda": "USD",
            "descripcion": "DOLAR E.E.U.U.",
            "tipoPase": 0.0,
            "tipoCotizacion": 1520.0,
        }
    ]


async def test_un_rango_con_moneda_va_al_endpoint_historico() -> None:
    """Antes mandaba `fechaDesde`/`fechaHasta` y la API respondía 400."""
    fake = FakeBCRA()
    result = await _adapter(fake).get_cotizaciones(
        moneda="USD", fecha_desde="2026-09-28", fecha_hasta="2026-10-02"
    )

    request = fake.requests[-1]
    assert request.url.path == "/estadisticascambiarias/v1.0/Cotizaciones/USD"
    assert request.url.params["fechadesde"] == "2026-09-28"
    assert request.url.params["fechahasta"] == "2026-10-02"
    assert "fechaDesde" not in request.url.params
    fechas = [r["fecha"] for r in result.records]
    assert fechas == sorted(fechas)
    assert fechas[0] == "2026-09-28" and fechas[-1] == "2026-10-02"


async def test_una_fecha_sin_moneda_pide_ese_dia() -> None:
    fake = FakeBCRA()
    result = await _adapter(fake).get_cotizaciones(fecha_desde="2026-10-01")
    assert fake.requests[-1].url.params["fecha"] == "2026-10-01"
    assert [r["fecha"] for r in result.records] == ["2026-10-01"]


async def test_un_sabado_vuelve_vacio_sin_fecha() -> None:
    result = await _adapter(FakeBCRA()).get_cotizaciones(fecha_hasta="2026-10-03")
    assert result.records == []
    assert "ultima_observacion" not in result.metadata


async def test_la_historia_se_pagina_hasta_el_final(monkeypatch) -> None:
    monkeypatch.setattr(bcra_module, "CAMBIARIAS_PAGE_LIMIT", 200)
    fake = FakeBCRA()
    result = await _adapter(fake).get_cotizaciones_historicas("usd", "2025-01-01", "2026-10-02")

    offsets = [int(r.url.params["offset"]) for r in fake.requests]
    assert offsets == [0, 200, 400]  # ~460 días hábiles en páginas de 200
    assert len(result.records) == result.metadata["total_fuente"] > 400
    assert result.metadata["truncada"] is False
    assert result.records[-1]["fecha"] == "2026-10-02"
    assert result.dataset_title == "Cotizaciones Cambiarias - USD"


async def test_una_moneda_desde_una_fecha_trae_hasta_hoy() -> None:
    """El planner legacy pide query_bcra {moneda, fecha_desde}: era un solo día."""
    fake = FakeBCRA()
    result = await _adapter(fake).get_cotizaciones(moneda="USD", fecha_desde="2026-09-01")

    request = fake.requests[-1]
    assert request.url.path == "/estadisticascambiarias/v1.0/Cotizaciones/USD"
    assert request.url.params["fechadesde"] == "2026-09-01"
    assert request.url.params["fechahasta"] == HOY.isoformat()
    fechas = [r["fecha"] for r in result.records]
    assert fechas[0] == "2026-09-01" and fechas[-1] == "2026-10-02"
    assert len(fechas) > 20


async def test_un_rango_sin_moneda_no_devuelve_un_solo_dia_callado() -> None:
    fake = FakeBCRA()
    with pytest.raises(ConnectorError):
        await _adapter(fake).get_cotizaciones(fecha_desde="2026-09-01", fecha_hasta="2026-10-02")
    assert fake.requests == []


async def test_una_fecha_que_no_es_iso_no_llega_a_la_api() -> None:
    fake = FakeBCRA()
    with pytest.raises(ConnectorError):
        await _adapter(fake).get_cotizaciones(moneda="USD", fecha_desde="01/09/2026")
    assert fake.requests == []


# ── variables monetarias (estadísticas v4.0) ───────────────


async def test_reservas_trae_el_ultimo_dato_publicado_por_el_bcra() -> None:
    fake = FakeBCRA()
    result = await _adapter(fake).get_variable(1, title="Reservas internacionales del BCRA")

    data_requests = [r for r in fake.requests if r.url.path.endswith("/monetarias/1")]
    assert len(data_requests) == 1
    assert data_requests[0].url.params["limit"] == "60"
    assert result.source == "bcra"
    assert "Banco Central" in result.portal_name
    assert result.dataset_title == "Reservas internacionales del BCRA"
    assert len(result.records) == 60
    # Cronológico: el último es el más nuevo, como en Series de Tiempo.
    assert result.records[-1] == {"fecha": "2026-09-30", "valor": 46092.0}
    assert result.records[-2] == {"fecha": "2026-09-29", "valor": 47482.0}
    fechas = [r["fecha"] for r in result.records]
    assert fechas == sorted(fechas)


async def test_el_contrato_de_metadatos() -> None:
    result = await _adapter(FakeBCRA()).get_variable(1)
    meta = result.metadata
    assert meta["ultima_observacion"] == "2026-09-30"
    assert meta["fecha_fin_fuente"] == "2026-09-30"
    assert meta["frecuencia"] == "diaria"
    assert meta["oficial"] is True
    assert meta["actualizada_en_fuente"] is None
    assert meta["total_fuente"] == len(SERIES[1])
    # Sin `desde` se piden las últimas N, y vienen enteras: no está truncada.
    assert meta["truncada"] is False
    assert meta["units"] == "En millones de USD"
    assert meta["id_variable"] == 1
    assert "unidad" not in meta  # no es un porcentaje


async def test_una_tasa_se_marca_como_porcentaje() -> None:
    result = await _adapter(FakeBCRA()).get_variable(7)
    assert result.metadata["unidad"] == "porcentaje"
    assert result.records[-1] == {"fecha": "2026-10-01", "valor": 23.1875}


async def test_con_desde_trae_todo_el_rango_paginando(monkeypatch) -> None:
    monkeypatch.setattr(bcra_module, "V4_PAGE_LIMIT", 500)
    fake = FakeBCRA()
    result = await _adapter(fake).get_variable(1, desde="2022-01-01")

    offsets = [
        int(r.url.params["offset"]) for r in fake.requests if r.url.path.endswith("/monetarias/1")
    ]
    assert offsets[:3] == [0, 500, 1000]
    esperadas = [f for f, _ in SERIES[1] if f >= "2022-01-01"]
    assert [r["fecha"] for r in result.records] == esperadas
    assert result.metadata["total_fuente"] == len(esperadas)
    assert result.metadata["truncada"] is False


async def test_un_rango_mas_largo_que_el_tope_queda_marcado_truncado(monkeypatch) -> None:
    monkeypatch.setattr(bcra_module, "V4_PAGE_LIMIT", 500)
    monkeypatch.setattr(bcra_module, "V4_MAX_ROWS", 700)
    result = await _adapter(FakeBCRA()).get_variable(1, desde="2020-01-01")
    assert len(result.records) == 700
    assert result.metadata["truncada"] is True
    # Lo que se corta es lo más viejo: el último dato está.
    assert result.records[-1]["fecha"] == "2026-09-30"


async def test_con_hasta_la_fecha_fin_de_la_fuente_sale_del_catalogo() -> None:
    result = await _adapter(FakeBCRA()).get_variable(1, hasta="2026-06-30", limit=10)
    assert result.records[-1]["fecha"] <= "2026-06-30"
    assert result.metadata["ultima_observacion"] == result.records[-1]["fecha"]
    assert result.metadata["fecha_fin_fuente"] == "2026-09-30"


async def test_sin_catalogo_los_datos_igual_llegan() -> None:
    result = await _adapter(FakeBCRA(catalog_down=True)).get_variable(4, title="Minorista")
    assert result.records[-1] == {"fecha": "2026-10-02", "valor": 1543.18}
    assert result.dataset_title == "Minorista"
    assert result.metadata["frecuencia"] is None


async def test_un_catalogo_colgado_no_demora_los_datos(monkeypatch) -> None:
    """La herramienta se corta a los 25 s: antes los datos esperaban al catálogo
    (20 s de timeout por intento, con reintentos)."""
    monkeypatch.setattr(bcra_module, "CATALOG_WAIT_S", 0.05)
    adapter = _adapter(FakeBCRA(catalog_delay=30))
    started = time.monotonic()
    try:
        result = await asyncio.wait_for(adapter.get_variable(1), timeout=5)
    finally:
        if adapter._catalog_task is not None:
            adapter._catalog_task.cancel()
    assert time.monotonic() - started < 2
    assert result.records[-1] == {"fecha": "2026-09-30", "valor": 46092.0}
    assert result.metadata["frecuencia"] is None  # sin ficha, pero con datos


async def test_un_catalogo_lento_queda_en_cache_para_la_proxima(monkeypatch) -> None:
    monkeypatch.setattr(bcra_module, "CATALOG_WAIT_S", 0.01)
    fake = FakeBCRA(catalog_delay=0.2)
    adapter = _adapter(fake)
    first = await adapter.get_variable(7)
    assert "unidad" not in first.metadata  # el catálogo no llegó a tiempo
    await asyncio.sleep(0.4)  # siguió cargando en segundo plano
    second = await adapter.get_variable(7)
    assert second.metadata["unidad"] == "porcentaje"
    assert fake.catalog_requests() == 1


async def test_un_catalogo_caido_no_se_pide_en_cada_consulta() -> None:
    fake = FakeBCRA(catalog_down=True)
    adapter = _adapter(fake)
    await adapter.get_variable(4)
    pedidos = fake.catalog_requests()
    assert pedidos == 2  # un intento y un reintento
    await adapter.get_variable(5)
    await adapter.get_variable(1)
    assert fake.catalog_requests() == pedidos


async def test_el_catalogo_caido_no_abre_el_circuito_de_los_datos(monkeypatch) -> None:
    """Catálogo y datos compartían el circuito: con el catálogo caído, a la
    quinta falla se cortaban también los datos."""
    monkeypatch.setattr(bcra_module, "CATALOG_RETRY_S", 0.0)
    adapter = _adapter(FakeBCRA(catalog_down=True))
    for _ in range(7):
        result = await adapter.get_variable(4)
        assert result.records[-1]["valor"] == 1543.18
    assert get_circuit_breaker("bcra_api").state == CircuitState.CLOSED


async def test_un_pedido_mal_armado_no_abre_el_circuito() -> None:
    """Un 400 (variable inexistente, fecha futura) es del pedido, no de la API."""
    adapter = _adapter(FakeBCRA())
    for _ in range(6):
        with pytest.raises(ConnectorError):
            await adapter.get_variable(9999)
    result = await adapter.get_variable(1)
    assert result.records[-1]["valor"] == 46092.0


async def test_con_hasta_y_sin_catalogo_la_fuente_igual_dice_hasta_donde_llega() -> None:
    """Sin la ficha, un período pasado parecía un dato atrasado de meses."""
    fake = FakeBCRA(catalog_down=True)
    result = await _adapter(fake).get_variable(1, hasta="2026-06-30", limit=10)
    assert result.metadata["ultima_observacion"] <= "2026-06-30"
    assert result.metadata["fecha_fin_fuente"] == "2026-09-30"


async def test_el_catalogo_se_pagina_y_se_cachea(monkeypatch) -> None:
    monkeypatch.setattr(bcra_module, "V4_PAGE_LIMIT", 10)
    fake = FakeBCRA()
    adapter = _adapter(fake)
    first = await adapter.list_variables()
    again = await adapter.list_variables()
    catalog_calls = [r for r in fake.requests if r.url.path == "/estadisticas/v4.0/monetarias"]
    assert len(first) == 26 and again is first
    assert [r.url.params["offset"] for r in catalog_calls] == ["0", "10", "20"]


async def test_un_error_del_bcra_es_connector_error() -> None:
    with pytest.raises(ConnectorError):
        await _adapter(FakeBCRA()).get_variable(9999)


async def test_desde_posterior_a_hasta_no_llega_a_la_api() -> None:
    fake = FakeBCRA()
    with pytest.raises(ConnectorError):
        await _adapter(fake).get_variable(1, desde="2026-09-30", hasta="2026-09-01")
    assert not [r for r in fake.requests if r.url.path.endswith("/monetarias/1")]
