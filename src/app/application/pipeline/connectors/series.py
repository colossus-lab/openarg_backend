"""Connector: Series de Tiempo API."""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

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
    # De dónde vino el collapse importa para decidir si se puede degradar:
    # el del catálogo es curado por serie, el del planner es una conjetura.
    collapse_del_planner = bool(collapse or representation)

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

    # Intentos en orden decreciente de exigencia. Cada uno saca una capa de
    # parámetros que la API puede rechazar, y se para en el primero que trae
    # datos.
    intentos: list[tuple[str, dict[str, Any]]] = [
        (
            "lo pedido",
            {
                "start_date": start_date,
                "end_date": end_date,
                "collapse": collapse,
                "representation": representation,
            },
        )
    ]

    # FR-009 / FIX-013: cuando el pedido traía un rango de fechas explícito,
    # reintentar sin rango. Cubre preguntas por un mes que la API todavía no
    # publicó (p. ej. "IPC de febrero 2026" el día que sólo salió enero); la
    # serie más reciente le permite al analista describirla con honestidad.
    if start_date or params.get("endDate"):
        intentos.append(
            (
                "sin rango de fechas",
                {
                    "start_date": None,
                    "end_date": None,
                    "collapse": collapse,
                    "representation": representation,
                },
            )
        )

    # 2026-09: y sin `collapse`/`representation`. La API responde HTTP 400
    # ("Intervalo de collapse inválido … Pruebe con un intervalo mayor")
    # cuando el collapse pedido es más fino que la frecuencia de la serie —
    # p. ej. `month` sobre la serie trimestral de desempleo. El planner lo
    # pide de más porque el template del schema lo sugiere, así que la
    # degradación tiene que estar acá y no en el prompt.
    #
    # Sólo se degrada el collapse que puso el planner. El que sale de
    # `default_collapse` del catálogo es curado por serie: si ese no trae
    # datos, sacarlo es una llamada de más y no una chance de rescate.
    if collapse_del_planner:
        intentos.append(
            (
                "sin collapse ni representation",
                {
                    "start_date": None,
                    "end_date": None,
                    "collapse": None,
                    "representation": None,
                },
            )
        )

    result = None
    ultimo_error: ConnectorError | None = None
    for i, (etiqueta, kwargs) in enumerate(intentos):
        try:
            result = await series.fetch(series_ids=series_ids, **kwargs)
        except ConnectorError as exc:
            # Un 400 llega como excepción, no como resultado vacío. Sin este
            # `except`, la degradación no se alcanzaba nunca.
            ultimo_error = exc
            result = None
        if result:
            if i > 0:
                logger.info(
                    "Series fetch para %s resolvió con '%s' tras %d intento(s)",
                    series_ids,
                    etiqueta,
                    i,
                )
            return [result]
        if i + 1 < len(intentos):
            logger.info(
                "Series fetch para %s sin datos con '%s'; degradando a '%s'",
                series_ids,
                etiqueta,
                intentos[i + 1][0],
            )

    if ultimo_error is not None:
        raise ultimo_error
    raise ConnectorError(
        error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
        details={"series_ids": series_ids, "reason": "API respondió sin datos"},
    )
