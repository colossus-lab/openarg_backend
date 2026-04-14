from __future__ import annotations

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
    assert result.portal_name == "DolarApi"
    assert result.records == [
        {
            "fecha": "2026-04-13T10:15:00.000Z",
            "casa": "blue",
            "compra": 1190,
            "venta": 1210,
            "nombre": "Blue",
        }
    ]
    assert result.metadata["realtime"] is True
    assert result.metadata["last_updated"] == "2026-04-13T10:15:00.000Z"


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
