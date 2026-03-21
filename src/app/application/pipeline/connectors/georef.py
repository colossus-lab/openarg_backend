"""Connector: Georef (geolocation normalization)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.domain.ports.connectors.georef import IGeorefConnector


async def execute_georef_step(
    step: PlanStep,
    georef: IGeorefConnector,
) -> list[DataResult]:
    query = step.params.get("query", step.description)
    result = await georef.normalize_location(query)
    return [result] if result else []
