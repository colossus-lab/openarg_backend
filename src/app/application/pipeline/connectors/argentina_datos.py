"""Connector: ArgentinaDatos (dólar, riesgo país, inflación)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector


async def execute_argentina_datos_step(
    step: PlanStep,
    arg_datos: IArgentinaDatosConnector,
) -> list[DataResult]:
    params = step.params
    data_type = params.get("type", "dolar")

    if data_type == "dolar":
        casa = params.get("casa")
        ultimo = params.get("ultimo", False)
        historico = params.get("historico", False)

        if ultimo:
            result = await arg_datos.fetch_dolar(casa=casa, ultimo=True)
            return [result] if result else []
        if historico:
            result = await arg_datos.fetch_dolar(casa=casa, ultimo=False)
            return [result] if result else []

        current_result, historical_result = await asyncio.gather(
            arg_datos.fetch_dolar(casa=casa, ultimo=True),
            arg_datos.fetch_dolar(casa=casa, ultimo=False),
        )
        return [result for result in (current_result, historical_result) if result]
    elif data_type == "riesgo_pais":
        result = await arg_datos.fetch_riesgo_pais(ultimo=params.get("ultimo", False))
        return [result] if result else []
    elif data_type == "inflacion":
        result = await arg_datos.fetch_inflacion()
        return [result] if result else []
    return []
