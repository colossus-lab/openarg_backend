from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from app.application.pipeline.connectors.argentina_datos import execute_argentina_datos_step
from app.domain.entities.connectors.data_result import PlanStep
from app.infrastructure.adapters.connectors.argentina_datos_adapter import ArgentinaDatosAdapter


@pytest.mark.asyncio
async def test_fetch_dolar_historico_uses_argentina_datos() -> None:
    captured: list[str] = []

    async def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(str(request.url))
        return httpx.Response(
            200,
            json=[
                {"fecha": "2026-04-11", "casa": "blue", "compra": 1100, "venta": 1120},
                {"fecha": "2026-04-12", "casa": "blue", "compra": 1110, "venta": 1130},
            ],
        )

    transport = httpx.MockTransport(_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = ArgentinaDatosAdapter(client)
        result = await adapter.fetch_dolar(casa="blue")

    assert result is not None
    assert captured == ["https://api.argentinadatos.com/v1/cotizaciones/dolares/blue"]
    assert result.source == "argentina_datos"
    assert result.records[-1]["fecha"] == "2026-04-12"
    assert result.metadata["realtime"] is False


@pytest.mark.asyncio
async def test_fetch_dolar_ultimo_uses_dolarapi() -> None:
    captured: list[str] = []

    async def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(str(request.url))
        return httpx.Response(
            200,
            json={
                "casa": "blue",
                "nombre": "Blue",
                "compra": 1190,
                "venta": 1210,
                "fechaActualizacion": "2026-04-13T10:15:00.000Z",
            },
        )

    transport = httpx.MockTransport(_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = ArgentinaDatosAdapter(client)
        result = await adapter.fetch_dolar(casa="blue", ultimo=True)

    assert result is not None
    assert captured == ["https://dolarapi.com/v1/dolares/blue"]
    assert result.source == "dolarapi"
    assert result.portal_name == "DolarApi (agregador no oficial)"
    assert result.dataset_title == "Cotización actual Dólar Blue vía DolarApi (no oficial)"
    assert result.metadata["oficial"] is False
    # El instante UTC de DolarApi, en hora argentina.
    assert result.records == [
        {
            "fecha": "2026-04-13T07:15-03:00",
            "casa": "blue",
            "compra": 1190,
            "venta": 1210,
            "nombre": "Blue",
        }
    ]
    assert result.metadata["realtime"] is True
    assert result.metadata["last_updated"] == "2026-04-13T07:15-03:00"
    assert result.metadata["ultima_observacion"] == "2026-04-13"


@pytest.mark.asyncio
async def test_execute_argentina_datos_step_passes_ultimo_for_dolar() -> None:
    adapter = AsyncMock()
    adapter.fetch_dolar.return_value = object()
    step = PlanStep(
        id="s1",
        action="query_argentina_datos",
        description="Cotización actual del dólar blue",
        params={"type": "dolar", "casa": "blue", "ultimo": True},
    )

    result = await execute_argentina_datos_step(step, adapter)

    assert result == [adapter.fetch_dolar.return_value]
    adapter.fetch_dolar.assert_awaited_once_with(casa="blue", ultimo=True)


@pytest.mark.asyncio
async def test_execute_argentina_datos_step_returns_current_and_historical_by_default() -> None:
    adapter = AsyncMock()
    current = object()
    historical = object()
    adapter.fetch_dolar.side_effect = [current, historical]
    step = PlanStep(
        id="s1",
        action="query_argentina_datos",
        description="Cotización del dólar blue",
        params={"type": "dolar", "casa": "blue"},
    )

    result = await execute_argentina_datos_step(step, adapter)

    assert result == [current, historical]
    assert adapter.fetch_dolar.await_args_list[0].kwargs == {"casa": "blue", "ultimo": True}
    assert adapter.fetch_dolar.await_args_list[1].kwargs == {"casa": "blue", "ultimo": False}


@pytest.mark.asyncio
async def test_execute_argentina_datos_step_passes_historico_for_dolar() -> None:
    adapter = AsyncMock()
    adapter.fetch_dolar.return_value = object()
    step = PlanStep(
        id="s1",
        action="query_argentina_datos",
        description="Serie histórica del dólar blue",
        params={"type": "dolar", "casa": "blue", "historico": True},
    )

    result = await execute_argentina_datos_step(step, adapter)

    assert result == [adapter.fetch_dolar.return_value]
    adapter.fetch_dolar.assert_awaited_once_with(casa="blue", ultimo=False)


# ── no oficial y sin fechas futuras (auditoría del 04-oct) ──


def _adapter_hoy(handler, hoy: date = date(2026, 10, 4)) -> tuple[ArgentinaDatosAdapter, Any]:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return ArgentinaDatosAdapter(client, today=lambda: hoy), client


@pytest.mark.asyncio
async def test_el_historico_no_trae_dias_que_no_llegaron() -> None:
    """El domingo 04-oct ArgentinaDatos terminaba en el lunes 05-oct."""

    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {"casa": "oficial", "compra": 1495, "venta": 1545, "fecha": "2026-10-02"},
                {"casa": "oficial", "compra": 1490, "venta": 1540, "fecha": "2026-10-03"},
                {"casa": "oficial", "compra": 1490, "venta": 1540, "fecha": "2026-10-04"},
                {"casa": "oficial", "compra": 1490, "venta": 1540, "fecha": "2026-10-05"},
            ],
        )

    adapter, client = _adapter_hoy(_handler)
    async with client:
        result = await adapter.fetch_dolar(casa="oficial")

    assert result is not None
    assert [r["fecha"] for r in result.records] == ["2026-10-02", "2026-10-03", "2026-10-04"]
    assert result.metadata["ultima_observacion"] == "2026-10-04"


@pytest.mark.asyncio
async def test_la_pizarra_del_banco_nacion_se_rotula_como_no_oficial() -> None:
    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "moneda": "USD",
                "casa": "oficial",
                "nombre": "Oficial",
                "compra": 1490,
                "venta": 1540,
                "fechaActualizacion": "2026-10-02T18:55:00.000Z",
            },
        )

    adapter, client = _adapter_hoy(_handler)
    async with client:
        result = await adapter.fetch_dolar(casa="oficial", ultimo=True)

    assert result is not None
    assert result.dataset_title == "Pizarra del Banco Nación vía DolarApi (no oficial)"
    assert result.metadata["oficial"] is False
    assert "Banco Nación" in result.metadata["description"]
    assert result.metadata["ultima_observacion"] == "2026-10-02"


@pytest.mark.asyncio
async def test_el_historico_del_oficial_tambien_dice_que_no_es_oficial() -> None:
    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json=[{"casa": "oficial", "compra": 1495, "venta": 1545, "fecha": "2026-10-02"}]
        )

    adapter, client = _adapter_hoy(_handler)
    async with client:
        result = await adapter.fetch_dolar(casa="oficial")

    assert result is not None
    assert "Banco Nación" in result.dataset_title and "no oficial" in result.dataset_title
    assert result.portal_name == "ArgentinaDatos (agregador no oficial)"
    assert result.metadata["oficial"] is False


@pytest.mark.asyncio
async def test_el_riesgo_pais_tampoco_trae_fechas_futuras() -> None:
    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {"valor": 636, "fecha": "2026-10-01"},
                {"valor": 655, "fecha": "2026-10-02"},
                {"valor": 655, "fecha": "2026-10-06"},
            ],
        )

    adapter, client = _adapter_hoy(_handler)
    async with client:
        result = await adapter.fetch_riesgo_pais()

    assert result is not None
    assert [r["fecha"] for r in result.records] == ["2026-10-01", "2026-10-02"]
    assert result.metadata["oficial"] is False


def test_hoy_es_la_fecha_argentina_no_la_del_servidor(monkeypatch) -> None:
    """A las 23:19 del domingo en Argentina, en UTC ya es lunes."""
    from app.infrastructure.adapters.connectors import argentina_datos_adapter as mod

    class _Reloj(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return datetime(2026, 10, 5, 2, 19, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr(mod, "datetime", _Reloj)
    assert mod._today_ar() == date(2026, 10, 4)


@pytest.mark.asyncio
async def test_un_valor_de_dolarapi_de_las_21_30_es_de_hoy_y_no_de_manana() -> None:
    """Domingo 04-oct, 21:30 en Argentina: DolarApi dice 2026-10-05T00:30Z.
    Comparar la fecha UTC con el hoy argentino lo descartaba y la herramienta
    contestaba "Sin datos" (cripto se actualiza todas las noches)."""

    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "moneda": "USD",
                "casa": "cripto",
                "nombre": "Cripto",
                "compra": 1530,
                "venta": 1560,
                "fechaActualizacion": "2026-10-05T00:30:00.000Z",
            },
        )

    adapter, client = _adapter_hoy(_handler)
    async with client:
        result = await adapter.fetch_dolar(casa="cripto", ultimo=True)

    assert result is not None
    assert result.records[0]["fecha"] == "2026-10-04T21:30-03:00"
    assert result.metadata["ultima_observacion"] == "2026-10-04"


@pytest.mark.asyncio
async def test_todas_las_casas_de_noche_no_pierden_blue_ni_cripto() -> None:
    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {
                    "casa": "oficial",
                    "compra": 1490,
                    "venta": 1540,
                    "fechaActualizacion": "2026-10-02T18:55:00.000Z",
                },
                {
                    "casa": "blue",
                    "compra": 1480,
                    "venta": 1500,
                    "fechaActualizacion": "2026-10-05T00:05:00.000Z",
                },
                {
                    "casa": "cripto",
                    "compra": 1530,
                    "venta": 1560,
                    "fechaActualizacion": "2026-10-05T02:59:00.000Z",
                },
                # 01:00 del lunes en Argentina: ése sí es de un día que no llegó.
                {
                    "casa": "bolsa",
                    "compra": 1,
                    "venta": 1,
                    "fechaActualizacion": "2026-10-05T04:00:00.000Z",
                },
            ],
        )

    adapter, client = _adapter_hoy(_handler)
    async with client:
        result = await adapter.fetch_dolar(ultimo=True)

    assert result is not None
    assert [r["casa"] for r in result.records] == ["oficial", "blue", "cripto"]
    assert result.metadata["ultima_observacion"] == "2026-10-04"
