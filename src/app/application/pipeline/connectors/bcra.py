"""Connector: BCRA (Banco Central de la República Argentina)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter

logger = logging.getLogger(__name__)


async def execute_bcra_step(
    step: PlanStep,
    bcra: BCRAAdapter | None,
) -> list[DataResult]:
    """Execute a BCRA plan step. Only ``tipo='cotizaciones'`` is supported.

    The ``variables`` and ``historica`` types were removed (FIX-002): the
    BCRA v2 ``PrincipalesVariables`` endpoint was deprecated and both old
    methods were silently calling ``/Cotizaciones`` anyway.

    Errors propagate as ``ConnectorError`` so the step_executor can add
    them to step_warnings and metrics (FIX-001: no silent ``return []``).
    """
    if not bcra:
        logger.warning("BCRAAdapter not configured, skipping step %s", step.id)
        return []

    params = step.params
    tipo = params.get("tipo", "cotizaciones")
    if tipo != "cotizaciones":
        logger.warning(
            "BCRA step %s requested unsupported tipo=%r; defaulting to 'cotizaciones'. "
            "Legacy types ('variables', 'historica') were removed because the v2 endpoint "
            "was deprecated by BCRA and both old methods were aliases for Cotizaciones.",
            step.id,
            tipo,
        )

    # Let ConnectorError propagate: step_executor catches it and adds to
    # step_warnings + increments error metric. No silent degradation.
    result = await bcra.get_cotizaciones(
        moneda=params.get("moneda", "USD"),
        fecha_desde=params.get("fecha_desde"),
        fecha_hasta=params.get("fecha_hasta"),
    )
    return [result] if result else []
