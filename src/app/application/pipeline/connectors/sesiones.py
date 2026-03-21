"""Connector: Sesiones legislativas (transcripciones del Congreso)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.domain.ports.connectors.sesiones import ISesionesConnector


async def execute_sesiones_step(
    step: PlanStep,
    sesiones: ISesionesConnector,
) -> list[DataResult]:
    params = step.params
    result = await sesiones.search(
        query=params.get("query", step.description),
        periodo=params.get("periodo"),
        orador=params.get("orador"),
        limit=params.get("limit", 15),
    )
    return [result] if result else []
