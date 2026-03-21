"""Connector: Series de Tiempo API."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.infrastructure.adapters.connectors.series_tiempo_adapter import find_catalog_match

if TYPE_CHECKING:
    from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector

logger = logging.getLogger(__name__)


async def execute_series_step(
    step: PlanStep,
    series: ISeriesTiempoConnector,
) -> list[DataResult]:
    params = step.params
    series_ids = params.get("seriesIds", [])
    collapse = params.get("collapse")
    representation = params.get("representation")

    query_text = params.get("query", step.description)

    match = find_catalog_match(query_text)
    if match:
        if not series_ids:
            series_ids = match["ids"]
        if not collapse and "default_collapse" in match:
            collapse = match["default_collapse"]
        if not representation and "default_representation" in match:
            representation = match["default_representation"]
        logger.info("Series catalog match for '%s': %s", query_text[:60], series_ids)

    if not series_ids:
        search_results = await series.search(query_text)
        if search_results:
            series_ids = [r["id"] for r in search_results[:3]]
            logger.info("Series dynamic search for '%s': %s", query_text[:60], series_ids)

    if not series_ids:
        raise ConnectorError(
            error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
            details={"query": query_text[:100], "reason": "No se encontraron series matching"},
        )

    start_date = params.get("startDate")
    end_date = params.get("endDate")
    if not end_date:
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")

    result = await series.fetch(
        series_ids=series_ids,
        start_date=start_date,
        end_date=end_date,
        collapse=collapse,
        representation=representation,
    )
    if not result:
        raise ConnectorError(
            error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
            details={"series_ids": series_ids, "reason": "API respondió sin datos"},
        )
    return [result]
