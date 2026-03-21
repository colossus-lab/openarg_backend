"""Connector: ArgentinaDatos (dólar, riesgo país, inflación)."""

from __future__ import annotations

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
        result = await arg_datos.fetch_dolar(casa=params.get("casa"))
        return [result] if result else []
    elif data_type == "riesgo_pais":
        result = await arg_datos.fetch_riesgo_pais(ultimo=params.get("ultimo", False))
        return [result] if result else []
    elif data_type == "inflacion":
        result = await arg_datos.fetch_inflacion()
        return [result] if result else []
    return []
