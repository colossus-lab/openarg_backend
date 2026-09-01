"""Sacar de circulación un mart que quedó vacío, y devolverlo cuando vuelve.

`row_count_drift` detecta desde hace semanas el caso exacto: la vista no tiene
ninguna fila y el registro dice que sí. Su propio mensaje lo explica —"sigue
siendo elegible para el routing y responde vacío"— y el hallazgo se emite como
CRITICAL. Pero era sólo un aviso.

Medido en producción el 2026-09-01, con el detector avisando por décima vez:

    sube_uso_transporte_publico   registro 2.853.291 · filas reales 0
    subsidios_transporte_publico  registro   125.425 · filas reales 0

Las diez tablas fuente de ambos existen y están vacías, y el último build es de
mayo — así que el conteo guardado quedó clavado de entonces. Un usuario que
pregunta por uso del transporte público puede recibir ese mart, elegido con
confianza por sus 2,8 millones de filas declaradas, y una respuesta vacía.

La maquinaria para evitarlo ya existía y estaba desconectada: `mart_definitions`
tiene `serving_blocked`, el adapter del sandbox rechaza consultas contra un mart
bloqueado, y la migración que lo creó dice que es "a gate for marts that are
*empty*". Sólo que se declaraba a mano en el YAML, así que nunca se encendía
solo. Hoy hay 1 bloqueado de 74.

**Dos escrituras, y las dos importan.** Bloquear sin corregir el conteo deja la
mentira en pie para todo lo demás que lo lea; corregir el conteo sin bloquear
deja el mart elegible hasta el próximo barrido.

**Reversible y sin pisar a una persona.** El bloqueo automático se marca con un
prefijo propio, y sólo se levanta lo que lleva esa marca: un mart bloqueado a
mano en su YAML conserva la decisión de quien la tomó.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

# La marca que distingue un bloqueo puesto por esta comprobación de uno que
# escribió una persona en el YAML. Sin ella, "desbloquear lo que se recuperó"
# borraría decisiones humanas.
AUTO_PREFIX = "auto:vacio"

# Tope por corrida. Si de golpe hay más marts vacíos que esto, es un problema
# sistémico —una migración a medias, un portal caído— y sacar la mitad del
# catálogo de circulación calladamente lo empeora en vez de contenerlo.
MAX_PER_RUN = 5

_BLOCK_SQL = text(
    """
    UPDATE public.mart_definitions
    SET last_row_count = 0,
        serving_blocked = TRUE,
        serving_blocked_reason = :reason,
        updated_at = NOW()
    WHERE mart_id = :mart_id
      AND NOT COALESCE(serving_blocked, FALSE)
    """
)

_UNBLOCK_SQL = text(
    """
    UPDATE public.mart_definitions
    SET serving_blocked = FALSE,
        serving_blocked_reason = NULL,
        updated_at = NOW()
    WHERE mart_id = :mart_id
      AND COALESCE(serving_blocked, FALSE)
      AND serving_blocked_reason LIKE :marca
    """
)


def block_empty(engine: Any, mart_id: str, *, stored: int) -> bool:
    """Retirar del servicio un mart vacío y corregir su conteo. Nunca levanta."""
    razon = f"{AUTO_PREFIX}: la vista no tiene filas y el registro decía {stored:,}"
    try:
        with engine.begin() as conn:
            cambiadas = conn.execute(_BLOCK_SQL, {"mart_id": mart_id, "reason": razon}).rowcount
    except Exception:
        logger.warning("serving gate: no se pudo bloquear %s", mart_id, exc_info=True)
        return False
    if cambiadas:
        logger.warning("serving gate: %s retirado del servicio (%s)", mart_id, razon)
    return bool(cambiadas)


def unblock_if_recovered(engine: Any, mart_id: str) -> bool:
    """Devolver al servicio un mart que volvió a tener filas.

    Sólo levanta bloqueos con la marca automática. Uno declarado en el YAML
    sigue bloqueado hasta que su autor decida otra cosa.
    """
    try:
        with engine.begin() as conn:
            cambiadas = conn.execute(
                _UNBLOCK_SQL, {"mart_id": mart_id, "marca": f"{AUTO_PREFIX}%"}
            ).rowcount
    except Exception:
        logger.warning("serving gate: no se pudo desbloquear %s", mart_id, exc_info=True)
        return False
    if cambiadas:
        logger.info("serving gate: %s vuelve al servicio", mart_id)
    return bool(cambiadas)
