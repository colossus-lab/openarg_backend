"""Connector: BCRA (Banco Central de la República Argentina).

Lo que pide el planner del motor legacy en un paso ``query_bcra``:

- ``tipo='cotizaciones'`` (o sin tipo): Estadísticas Cambiarias v1.0.
- ``tipo='variables'`` o ``'historica'`` con un ``id_variable`` de
  ``VARIABLES_V4``: esa variable de la API de estadísticas v4.0
  (``BCRAAdapter.get_variable``, la misma que usa ``variables_bcra`` en el
  motor agente).
- Cualquier otra cosa: sin datos y con un aviso explícito.

Hasta el 05-oct todo tipo distinto de ``cotizaciones`` caía en silencio a
Cotizaciones USD. El planner pide ``{"tipo": "variables", "id_variable": 1}``
para las reservas (ejemplo de ``planner.txt``) y la respuesta citaba
"Cotizaciones Cambiarias - USD" sin usar ninguna de sus cifras (auditoría
del 04-oct, ítem 1.5).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode

if TYPE_CHECKING:
    from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VariableV4:
    titulo: str
    unidades: str
    # Los valores ya vienen en puntos porcentuales (23,19 = 23,19 %).
    porcentaje: bool = False


# Las variables que el legacy lee por id: las curadas de ``variables_bcra``
# (``answers/tools/bcra.py``, ids verificados contra el catálogo v4), con los
# mismos títulos y unidades, menos las que el BCRA publica por adelantado. La
# UVA (31), el CER (30) y el ICL (40) llegan hasta mediados del mes siguiente y
# las bandas cambiarias (1187, 1188) hasta fin de mes (medido el 05-oct): su
# última fila es un día que no llegó, y este motor no la separa como hace la
# herramienta del agente. Se copian en vez de importarse para que el rollback
# no dependa de los módulos del agente; un test las compara.
VARIABLES_V4: dict[int, VariableV4] = {
    1: VariableV4("Reservas internacionales del BCRA (saldo diario)", "millones de dólares"),
    4: VariableV4("Tipo de cambio minorista, promedio vendedor (BCRA)", "pesos por dólar"),
    5: VariableV4(
        "Tipo de cambio mayorista de referencia, Comunicación A 3500 (BCRA)", "pesos por dólar"
    ),
    7: VariableV4("Tasa BADLAR de bancos privados (BCRA)", "% nominal anual", porcentaje=True),
    12: VariableV4(
        "Tasa de depósitos a plazo fijo a 30 días, promedio de entidades (BCRA)",
        "% nominal anual",
        porcentaje=True,
    ),
    15: VariableV4("Base monetaria (BCRA)", "millones de pesos"),
    16: VariableV4("Circulación monetaria (BCRA)", "millones de pesos"),
    44: VariableV4("Tasa TAMAR de bancos privados (BCRA)", "% nominal anual", porcentaje=True),
}

# Observaciones que se piden sin ``fecha_desde``: las mismas que el agente.
_VENTANA_RECIENTE = 60

AVISO_NO_DISPONIBLE = (
    "la variable del BCRA que pidió el plan no está disponible en este motor; "
    "no se la reemplazó por otra fuente"
)


class FuenteBCRANoDisponible(ConnectorError):
    """Un pedido al BCRA que este motor no sabe responder.

    Viaja como ConnectorError para que ``execute_steps`` lo convierta en un
    paso sin datos más una advertencia (``step_warnings``), que llega al
    analista y al campo ``warnings`` de la respuesta. El texto es fijo, sin el
    tipo ni el id pedidos: ``dispatch_step_with_retry`` reintenta si el texto
    dice "503" o "connection", y un pedido no soportado no mejora reintentando.
    """

    def __init__(self, pedido: str) -> None:
        super().__init__(
            error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
            details={"reason": AVISO_NO_DISPONIBLE, "pedido": pedido},
        )

    def __str__(self) -> str:
        return AVISO_NO_DISPONIBLE


def _id_variable(raw: object) -> int | None:
    """El ``id_variable`` del plan como entero, o None si no es uno."""
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float) and raw.is_integer():
        return int(raw)
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw.strip())
    return None


async def execute_bcra_step(
    step: PlanStep,
    bcra: BCRAAdapter | None,
) -> list[DataResult]:
    """Execute a BCRA plan step.

    Errors propagate as ``ConnectorError`` so the step_executor can add
    them to step_warnings and metrics (FIX-001: no silent ``return []``).
    """
    if not bcra:
        logger.warning("BCRAAdapter not configured, skipping step %s", step.id)
        return []

    params = step.params
    tipo = params.get("tipo") or "cotizaciones"
    if tipo == "cotizaciones":
        result = await bcra.get_cotizaciones(
            moneda=params.get("moneda", "USD"),
            fecha_desde=params.get("fecha_desde"),
            fecha_hasta=params.get("fecha_hasta"),
        )
        return [result] if result else []

    id_variable = _id_variable(params.get("id_variable"))
    var = VARIABLES_V4.get(id_variable) if id_variable is not None else None
    if tipo not in ("variables", "historica") or id_variable is None or var is None:
        pedido = f"tipo={tipo!r}, id_variable={params.get('id_variable')!r}"
        logger.warning(
            "BCRA step %s: %s no está disponible en el motor legacy; no se sustituye",
            step.id,
            pedido,
        )
        raise FuenteBCRANoDisponible(pedido)

    result = await bcra.get_variable(
        id_variable,
        params.get("fecha_desde"),
        params.get("fecha_hasta"),
        limit=_VENTANA_RECIENTE,
        title=var.titulo,
    )
    meta = result.metadata
    meta["units"] = meta.get("units") or var.unidades
    if var.porcentaje:
        meta["unidad"] = "porcentaje"
    # El contexto del analista imprime la descripción, no ``units``.
    meta["description"] = f"{var.titulo}, en {var.unidades}"
    return [result]
