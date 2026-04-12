"""Connector: Series de Tiempo API."""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.infrastructure.adapters.connectors.series_tiempo_adapter import find_catalog_match

if TYPE_CHECKING:
    from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector

logger = logging.getLogger(__name__)

# National-only desempleo series (used to detect when upgrade is needed)
_NATIONAL_DESEMPLEO = {"45.2_ECTDT_0_T_33"}

# Regional desempleo series: Total + 6 EPH regions
_REGIONAL_DESEMPLEO = [
    "45.2_ECTDT_0_T_33",  # Total nacional
    "45.2_ECTDTG_0_T_37",  # GBA
    "45.2_ECTDTNO_0_T_42",  # NOA
    "45.2_ECTDTNE_0_T_42",  # NEA
    "45.2_ECTDTCU_0_T_38",  # Cuyo
    "45.2_ECTDTRP_0_T_49",  # Pampeana
    "45.2_ECTDTP_0_T_43",  # Patagonia
]

# Regex detecting geographic/comparison context
_GEO_RE = re.compile(
    r"(?:provincia|region|comparar|comparado|comparacion|geografic"
    r"|gba|noa|nea|cuyo|pampeana|patagonia|otras provincias)",
    re.IGNORECASE,
)


def _strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def _needs_regional_upgrade(series_ids: list[str], query_text: str, description: str) -> bool:
    """Return True when national-only desempleo should be upgraded to regional series."""
    if set(series_ids) != _NATIONAL_DESEMPLEO:
        return False
    combined = _strip_accents(f"{query_text} {description}".lower())
    return bool(_GEO_RE.search(combined))


async def execute_series_step(
    step: PlanStep,
    series: ISeriesTiempoConnector,
    *,
    user_query: str = "",
) -> list[DataResult]:
    params = step.params
    # Accept both camelCase (schema convention) and snake_case (routing hints)
    series_ids = params.get("seriesIds") or params.get("series_ids") or []
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

    # Upgrade national-only desempleo to regional when the context is geographic.
    # Check both the step text and the original user query for geo keywords.
    if _needs_regional_upgrade(series_ids, query_text, f"{step.description} {user_query}"):
        logger.info(
            "Upgrading national desempleo to regional series for '%s'",
            (user_query or query_text)[:60],
        )
        series_ids = list(_REGIONAL_DESEMPLEO)

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
    # FR-009 / FIX-013: when a catalog-matched fetch returns empty AND the
    # request carried an explicit date range, retry once without the
    # range. This handles queries that asked for a month the upstream
    # API has not yet published (e.g. "IPC de febrero 2026" on a day
    # when only January data is out). The retry returns the latest
    # available series so the analyst can describe it honestly.
    if not result and (start_date or params.get("endDate")):
        logger.info(
            "Series fetch returned empty for %s with range [%s, %s]; retrying without date range",
            series_ids,
            start_date,
            end_date,
        )
        result = await series.fetch(
            series_ids=series_ids,
            start_date=None,
            end_date=None,
            collapse=collapse,
            representation=representation,
        )
    if not result:
        raise ConnectorError(
            error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
            details={"series_ids": series_ids, "reason": "API respondió sin datos"},
        )
    return [result]
