"""Cupo mensual del chat web (02-oct-2026).

Hasta acá el chat de openarg.org no tenía tope mensual (sólo por minuto y por
día). Pasa a tener el mismo esquema que el MCP, con su propio número:

- Gratis: 30 preguntas por mes en la web (el MCP sigue con 10).
- Fundador: 100 por mes en la web.
- Créditos: la MISMA bolsa que el MCP (``api_credit_balances.preguntas``). Se
  usan sólo cuando se terminó el mes, en cualquiera de los dos canales.

Dos diferencias con el MCP, a propósito:

1. Se cuenta al terminar, no al empezar. Sólo descuenta una respuesta
   completa: no un saludo, un pedido de aclaración, un error o una respuesta
   que se cortó. Por eso el chequeo de entrada sólo LEE el contador y el
   descuento (``consume``) va después del ``complete``.
2. Si Redis no responde, se deja pasar (como los demás contadores por
   usuario): el chat es el producto principal y un Redis caído ya rompe
   bastante. El tope global diario de la web también falla abierto; el gasto
   lo sigue acotando el cupo por persona cuando Redis vuelve.

El costo de esto: dos preguntas simultáneas de alguien con 29/30 pueden
terminar las dos (una entra "gratis"). Es aceptable: el chat manda de a una.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from app.application.public_quota import (
    MONTH_TTL,
    first_of_next_month_utc,
    month_key,
    resolve_tier,
    try_debit,
)

if TYPE_CHECKING:
    from uuid import UUID

    from app.application.answers.engine import EngineResult
    from app.domain.ports.cache.cache_port import ICacheService
    from app.domain.ports.credits.credit_repository import ICreditRepository

logger = logging.getLogger(__name__)

_DEFAULTS = {
    "PUBLIC_WEB_MONTHLY_PREGUNTAS": 30,
    "PUBLIC_WEB_FOUNDER_PREGUNTAS": 100,
    # Techo de gasto del chat web por día, para todos juntos. Con Sonnet
    # (~US$ 0,04 por respuesta) son unos US$ 40 por día como máximo.
    "PUBLIC_WEB_GLOBAL_DAILY_CAP": 1000,
}

_DAY_TTL = 48 * 3600

# Turnos que no descuentan: respuestas sin modelo (saludos, preguntas sobre
# OpenArg, bloqueos) y pedidos de aclaración.
NON_BILLABLE_INTENTS = frozenset(
    {
        "casual",
        "meta",
        "clarification",
        "injection_blocked",
        "off_topic",
        "internal_table_blocked",
    }
)

QUOTA_EXHAUSTED = "QUOTA_EXHAUSTED"
DAILY_CAP_REACHED = "WEB_DAILY_CAP"


def _env_int(name: str) -> int:
    default = _DEFAULTS[name]
    try:
        value = int(os.getenv(name, ""))
    except ValueError:
        return default
    return value if value > 0 else default


def web_monthly_limit(founder: bool) -> int:
    return _env_int("PUBLIC_WEB_FOUNDER_PREGUNTAS" if founder else "PUBLIC_WEB_MONTHLY_PREGUNTAS")


def web_global_daily_cap() -> int:
    return _env_int("PUBLIC_WEB_GLOBAL_DAILY_CAP")


def web_counter_key(user_id: object, now: datetime | None = None) -> str:
    """Contador mensual del chat web, aparte del de la API (``…:month:…``)."""
    return f"rl:user:{user_id}:web:month:{month_key(now)}"


def web_global_key(now: datetime | None = None) -> str:
    return f"rl:global:web:day:{(now or datetime.now(UTC)).strftime('%Y-%m-%d')}"


def counts_against_quota(result: EngineResult) -> bool:
    """Si un turno terminado descuenta: hay respuesta y no es de las que no cuentan."""
    return bool((result.answer or "").strip()) and result.intent not in NON_BILLABLE_INTENTS


@dataclass(frozen=True)
class WebQuota:
    """Cómo está el cupo web de una persona, para el chequeo y para mostrarlo."""

    usadas: int
    limite: int
    creditos: int
    fundador: bool
    fundador_hasta: datetime | None = None

    @property
    def restantes(self) -> int:
        return max(self.limite - self.usadas, 0)

    @property
    def agotado(self) -> bool:
        return self.restantes == 0 and self.creditos <= 0

    def to_wire(self) -> dict[str, Any]:
        return {
            "usadas": min(self.usadas, self.limite),
            "limite": self.limite,
            "restantes": self.restantes,
            "creditos": self.creditos,
            "renueva": first_of_next_month_utc().isoformat(),
            "fundador": (
                {"hasta": self.fundador_hasta.isoformat() if self.fundador_hasta else None}
                if self.fundador
                else None
            ),
        }


async def _read_int(cache: ICacheService, key: str) -> int:
    try:
        return int(await cache.get(key) or 0)
    except Exception:
        logger.warning("Web quota counter unreadable (%s); treating as 0", key, exc_info=True)
        return 0


async def _credit_balance(user_id: UUID, credits: ICreditRepository | None) -> int:
    if credits is None:
        return 0
    try:
        return int((await credits.balance(user_id)).get("preguntas", 0))
    except Exception:
        logger.warning("Credit balance lookup failed for %s", user_id, exc_info=True)
        return 0


async def web_quota(
    user_id: UUID, cache: ICacheService, credits: ICreditRepository | None
) -> WebQuota:
    """El estado del cupo web, sin tocar nada."""
    tier = await resolve_tier(user_id, credits)
    founder = tier.nombre == "fundador"
    return WebQuota(
        usadas=await _read_int(cache, web_counter_key(user_id)),
        limite=web_monthly_limit(founder),
        creditos=await _credit_balance(user_id, credits),
        fundador=founder,
        fundador_hasta=tier.fundador_hasta,
    )


async def daily_cap_reached(cache: ICacheService) -> bool:
    """Si el chat web ya respondió el tope del día, sumando a todos."""
    return await _read_int(cache, web_global_key()) >= web_global_daily_cap()


async def consume_web_question(
    user_id: UUID, cache: ICacheService, credits: ICreditRepository | None
) -> WebQuota:
    """Descuenta una respuesta completa: del mes si queda, si no un crédito.

    Devuelve el estado después de descontar, para mandarlo con el ``complete``.
    """
    try:
        await cache.increment_with_ttl(web_global_key(), ttl_seconds=_DAY_TTL)
    except Exception:
        logger.warning("Web daily counter failed; not counted", exc_info=True)

    before = await web_quota(user_id, cache, credits)
    if before.restantes > 0:
        try:
            await cache.increment_with_ttl(web_counter_key(user_id), ttl_seconds=MONTH_TTL)
        except Exception:
            logger.warning("Web monthly counter failed for %s; not counted", user_id, exc_info=True)
            return before
        return WebQuota(
            usadas=before.usadas + 1,
            limite=before.limite,
            creditos=before.creditos,
            fundador=before.fundador,
            fundador_hasta=before.fundador_hasta,
        )

    if await try_debit(user_id, "preguntas", credits):
        return WebQuota(
            usadas=before.usadas,
            limite=before.limite,
            creditos=max(before.creditos - 1, 0),
            fundador=before.fundador,
            fundador_hasta=before.fundador_hasta,
        )
    # Entró con cupo y otra pregunta simultánea lo terminó: se responde igual.
    logger.info("Web question answered without quota or credit for %s (race)", user_id)
    return before


def exhausted_message(quota: WebQuota) -> str:
    renueva = first_of_next_month_utc()
    return (
        f"Usaste tus {quota.limite} preguntas de este mes. "
        f"Se renuevan el {renueva.day} de {_MESES[renueva.month - 1]}."
    )


DAILY_CAP_MESSAGE = (
    "OpenArg llegó al máximo de respuestas por hoy. Probá de nuevo mañana; "
    "tus preguntas del mes no se descontaron."
)

_MESES = (
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre",
)
