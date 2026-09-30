"""Cupos mensuales de la API pública y del MCP: gratis, Fundadores y créditos.

Hasta el 30-sep-2026 los cupos eran diarios (10 preguntas y 200 consultas de
datos por día). Pasan a ser mensuales:

- Gratis: 10 preguntas y 200 consultas de datos por mes.
- Fundador (sostiene OpenArg con un aporte, o por cortesía): 100 y 2.000.
- Créditos: saldo extra que se usa sólo cuando se terminó el cupo del mes.

El mes es el calendario UTC: se renueva el 1° a las 00:00 UTC (21:00 del
último día en Argentina). Los contadores van por usuario, no por clave, así
que regenerar la clave no reinicia el cupo.

Los números se leen del entorno para poder ajustarlos sin tocar código.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID

    from app.domain.entities.credits.credits import CreditType
    from app.domain.ports.credits.credit_repository import ICreditRepository

logger = logging.getLogger(__name__)

# Los contadores mensuales viven un poco más que el mes más largo, y la clave
# lleva el mes: el período corta el 1° aunque haya tráfico constante.
MONTH_TTL = 40 * 24 * 3600

_DEFAULTS = {
    "PUBLIC_API_MONTHLY_PREGUNTAS": 10,
    "PUBLIC_API_MONTHLY_DATOS": 200,
    "PUBLIC_API_FOUNDER_PREGUNTAS": 100,
    "PUBLIC_API_FOUNDER_DATOS": 2000,
}


def _env_int(name: str) -> int:
    default = _DEFAULTS[name]
    try:
        value = int(os.getenv(name, ""))
    except ValueError:
        return default
    return value if value > 0 else default


@dataclass(frozen=True)
class Tier:
    """El cupo mensual que le toca a una persona."""

    nombre: str  # "gratis" | "fundador"
    preguntas: int
    datos: int
    fundador_hasta: datetime | None = None

    def limit(self, tipo: CreditType) -> int:
        return self.preguntas if tipo == "preguntas" else self.datos


def free_tier() -> Tier:
    return Tier(
        "gratis",
        _env_int("PUBLIC_API_MONTHLY_PREGUNTAS"),
        _env_int("PUBLIC_API_MONTHLY_DATOS"),
    )


def founder_tier(hasta: datetime | None) -> Tier:
    return Tier(
        "fundador",
        _env_int("PUBLIC_API_FOUNDER_PREGUNTAS"),
        _env_int("PUBLIC_API_FOUNDER_DATOS"),
        hasta,
    )


def month_key(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).strftime("%Y-%m")


def first_of_next_month_utc(now: datetime | None = None) -> datetime:
    now = now or datetime.now(UTC)
    if now.month == 12:
        return datetime(now.year + 1, 1, 1, tzinfo=UTC)
    return datetime(now.year, now.month + 1, 1, tzinfo=UTC)


def seconds_until_next_month(now: datetime | None = None) -> int:
    now = now or datetime.now(UTC)
    return max(int((first_of_next_month_utc(now) - now).total_seconds()), 1)


def monthly_counter_key(user_id: object, tipo: CreditType, now: datetime | None = None) -> str:
    """Clave del contador mensual. Preguntas y datos se cuentan aparte."""
    segment = "month" if tipo == "preguntas" else "catalog:month"
    return f"rl:user:{user_id}:{segment}:{month_key(now)}"


async def resolve_tier(user_id: UUID, credits: ICreditRepository | None) -> Tier:
    """Fundador vigente o gratis. Si la base falla, gratis: nunca bloquea."""
    if credits is None:
        return free_tier()
    try:
        supporter = await credits.get_active_supporter(user_id)
    except Exception:
        logger.warning("Supporter lookup failed for %s; using free tier", user_id, exc_info=True)
        return free_tier()
    return founder_tier(supporter.hasta) if supporter else free_tier()


async def try_debit(user_id: UUID, tipo: CreditType, credits: ICreditRepository | None) -> bool:
    """Gasta un crédito si hay. Si la base falla, no hay crédito (402, no 500)."""
    if credits is None:
        return False
    try:
        return await credits.debit(user_id, tipo)
    except Exception:
        logger.warning("Credit debit failed for %s (%s)", user_id, tipo, exc_info=True)
        return False


async def has_credit(user_id: UUID, tipo: CreditType, credits: ICreditRepository | None) -> bool:
    """Si queda al menos un crédito (lectura, sin gastar). Si la base falla, no."""
    if credits is None:
        return False
    try:
        return (await credits.balance(user_id)).get(tipo, 0) > 0
    except Exception:
        logger.warning("Credit balance lookup failed for %s (%s)", user_id, tipo, exc_info=True)
        return False
