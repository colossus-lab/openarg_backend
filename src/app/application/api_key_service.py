"""API key generation, verification, and rate limiting."""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException

from app.application.public_quota import (
    MONTH_TTL,
    first_of_next_month_utc,
    has_credit,
    monthly_counter_key,
    resolve_tier,
    seconds_until_next_month,
    try_debit,
)
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService

if TYPE_CHECKING:
    from app.domain.entities.credits.credits import CreditType
    from app.domain.ports.credits.credit_repository import ICreditRepository

logger = logging.getLogger(__name__)

_KEY_PREFIX = "oarg_sk_"
_MAX_TOKEN_LENGTH = 100  # Reject absurdly long tokens before hashing

# Tope global: consultas del plan free sumando a TODOS los usuarios, por día.
# Es el techo de gasto de Bedrock de la API pública (y del MCP que la usa):
# con el presupuesto de USD 300/mes son USD 10/día, dividido por el costo
# medido de una consulta. Se lee del entorno para poder ajustarlo sin
# tocar código.
_DEFAULT_GLOBAL_FREE_DAILY_CAP = 300
_DEFAULT_IP_DAILY_LIMIT = 30

# Los contadores diarios llevan la fecha UTC en la clave y viven 48 h: el día
# corta a medianoche UTC aunque haya tráfico constante. Antes era un `set`
# con TTL de 24 h que se renovaba en cada consulta, así que con uso sostenido
# el contador nunca vencía y el tope global terminaba bloqueando a todos.
_DAY_TTL = 172800
_MIN_TTL = 60

# Sólo el límite por minuto: el cupo del período es mensual y depende de si la
# persona es Fundador (ver app.application.public_quota).
PLAN_LIMITS: dict[str, dict[str, int]] = {
    "free": {"per_min": 2},
    "basic": {"per_min": 15},
    "pro": {"per_min": 30},
}


def generate_api_key() -> tuple[str, str]:
    """Generate a new API key.

    Returns (raw_key, key_hash). The raw_key should be shown to the user
    exactly once and never stored. The key_hash is stored in the database.
    """
    random_part = secrets.token_urlsafe(32)
    raw_key = f"{_KEY_PREFIX}{random_part}"
    key_hash = hash_api_key(raw_key)
    return raw_key, key_hash


def hash_api_key(raw_key: str) -> str:
    """Hash an API key with SHA-256 for storage/lookup."""
    return hashlib.sha256(raw_key.encode()).hexdigest()


async def verify_api_key(
    token: str,
    repo: IApiKeyRepository,
) -> ApiKey:
    """Verify a Bearer token and return the ApiKey entity.

    Raises HTTPException(401) with a generic message for ALL failure reasons
    to prevent key enumeration attacks.
    """
    _AUTH_ERROR = "Invalid or unauthorized API key"

    if not token or len(token) > _MAX_TOKEN_LENGTH or not token.startswith(_KEY_PREFIX):
        raise HTTPException(status_code=401, detail=_AUTH_ERROR)

    key_hash = hash_api_key(token)
    api_key = await repo.get_by_key_hash(key_hash)

    if not api_key or not api_key.is_active:
        raise HTTPException(status_code=401, detail=_AUTH_ERROR)

    # api_keys.expires_at was dropped in Alembic 0030 (2026-04-11) —
    # it was dead code (read but never written). Keys now live until
    # explicitly revoked. See specs/008-developers-keys/[DEBT-003].

    return api_key


def _env_int(name: str, default: int) -> int:
    """Entero positivo del entorno; cualquier otra cosa cae al default."""
    try:
        value = int(os.getenv(name, ""))
    except ValueError:
        return default
    return value if value > 0 else default


def global_free_daily_cap() -> int:
    return _env_int("PUBLIC_API_GLOBAL_DAILY_CAP", _DEFAULT_GLOBAL_FREE_DAILY_CAP)


def ip_daily_limit() -> int:
    return _env_int("PUBLIC_API_IP_DAILY_LIMIT", _DEFAULT_IP_DAILY_LIMIT)


def _utc_day(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).strftime("%Y-%m-%d")


def seconds_until_utc_midnight(now: datetime | None = None) -> int:
    now = now or datetime.now(UTC)
    elapsed = now.hour * 3600 + now.minute * 60 + now.second
    return 86400 - elapsed


async def _incr_fail_open(cache: ICacheService, key: str, ttl: int) -> int:
    """Increment a counter atomically; on cache failure report 0 (fail-open)."""
    try:
        return await cache.increment_with_ttl(key, ttl_seconds=ttl)
    except Exception:
        logger.warning("Cache increment failed for %s, allowing request (fail-open)", key)
        return 0


def _too_many(detail: str, headers: dict[str, str]) -> HTTPException:
    return HTTPException(status_code=429, detail=detail, headers=headers)


async def _seconds_left(cache: ICacheService, key: str, window: int) -> int:
    """Segundos hasta que se abra la ventana de `key`: su TTL, entre 1 y `window`.

    La ventana arranca con el primer pedido (EXPIRE NX), así que el
    `Retry-After: 60` fijo de antes hacía esperar de más a quien chocaba el
    límite al final del minuto. Si el caché no sabe el TTL, la ventana entera.
    Un TTL 0 es la clave que vence en este segundo (Redis redondea): 1, no
    la ventana entera.
    """
    try:
        left = await cache.ttl(key)
    except Exception:
        logger.debug("Cache TTL lookup failed for %s", key, exc_info=True)
        left = None
    if not isinstance(left, int) or left < 0:
        return window
    return max(1, min(left, window))


async def check_rate_limit(
    api_key: ApiKey,
    cache: ICacheService,
    client_ip: str = "",
    credits: ICreditRepository | None = None,
) -> dict[str, Any]:
    """Check and enforce the answers-mode limits for one question.

    Orden: por minuto → cupo del mes → IP del día → tope global del día → y
    recién al final, si el cupo del mes ya estaba usado, se gasta un crédito.
    El crédito va último a propósito: si el pedido termina rechazado por la IP
    o por el tope global, no se quema un crédito.

    Cada contador es un INCR atómico (`increment_with_ttl`). Los contadores
    por usuario y por IP fallan abiertos si Redis no responde (un Redis
    degradado no le corta el servicio a nadie); el tope global falla CERRADO,
    porque es el techo de gasto de Bedrock.

    Raises HTTPException 429 (por minuto, IP), 402 (cupo del mes sin créditos)
    or 503 (tope global). Returns the remaining quota.
    """
    limits = PLAN_LIMITS.get(api_key.plan, PLAN_LIMITS["free"])
    user_id = str(api_key.user_id)
    day = _utc_day()

    min_key = f"rl:user:{user_id}:min"
    min_count = await _incr_fail_open(cache, min_key, _MIN_TTL)
    if min_count > limits["per_min"]:
        raise _too_many(
            f"Rate limit exceeded: {limits['per_min']} requests per minute",
            {
                "X-RateLimit-Limit-Minute": str(limits["per_min"]),
                "X-RateLimit-Remaining-Minute": "0",
                "Retry-After": str(await _seconds_left(cache, min_key, _MIN_TTL)),
            },
        )

    tier = await resolve_tier(api_key.user_id, credits)
    month_count = await _incr_fail_open(cache, monthly_counter_key(user_id, "preguntas"), MONTH_TTL)
    needs_credit = month_count > tier.preguntas
    # Sin saldo, se corta acá: si no, cada reintento de alguien que ya agotó su
    # mes sumaría al tope global del día y le comería lugar a los demás.
    if needs_credit and not await has_credit(api_key.user_id, "preguntas", credits):
        raise _quota_exhausted("preguntas", tier.preguntas)

    if client_ip:
        ip_count = await _incr_fail_open(cache, f"rl:ip:{client_ip}:day:{day}", _DAY_TTL)
        if ip_count > ip_daily_limit():
            logger.warning("IP %s exceeded daily limit (%d)", client_ip, ip_count)
            raise _too_many(
                "Too many requests from this IP. Try again tomorrow.",
                {"Retry-After": str(seconds_until_utc_midnight())},
            )

    if api_key.plan == "free":
        cap = global_free_daily_cap()
        try:
            global_count = await cache.increment_with_ttl(
                f"rl:global:free:day:{day}", ttl_seconds=_DAY_TTL
            )
        except Exception:
            logger.error("Global free cap cannot be checked (cache down); rejecting (fail-closed)")
            raise HTTPException(
                status_code=503,
                detail="Public API temporarily unavailable. Try again later.",
                headers={"Retry-After": "300"},
            ) from None
        if global_count > cap:
            logger.warning("Global free daily cap reached (%d/%d)", global_count, cap)
            raise HTTPException(
                status_code=503,
                detail="Free tier daily capacity reached. Try again tomorrow.",
                headers={"Retry-After": str(seconds_until_utc_midnight())},
            )

    used_credit = False
    if needs_credit:
        used_credit = await try_debit(api_key.user_id, "preguntas", credits)
        if not used_credit:
            raise _quota_exhausted("preguntas", tier.preguntas)

    remaining = max(tier.preguntas - month_count, 0)
    return {
        "remaining_minute": max(limits["per_min"] - min_count, 0),
        "limit_minute": limits["per_min"],
        "remaining_month": remaining,
        "limit_month": tier.preguntas,
        "quota_resets_at": first_of_next_month_utc().isoformat(),
        "used_credit": used_credit,
        "tier": tier.nombre,
        "founder_until": tier.fundador_hasta.isoformat() if tier.fundador_hasta else None,
        # Nombres viejos: hay integraciones que ya los leen. Ahora son del mes.
        "remaining_day": remaining,
        "limit_day": tier.preguntas,
    }


def _quota_exhausted(tipo: CreditType, limit: int) -> HTTPException:
    """402 como en Tomi: el cupo del mes se terminó y no quedan créditos."""
    what = "questions" if tipo == "preguntas" else "catalog requests"
    return HTTPException(
        status_code=402,
        detail=f"Monthly quota exceeded: {limit} {what} per month",
        headers={
            "X-Quota-Reset": first_of_next_month_utc().isoformat(),
            "Retry-After": str(seconds_until_next_month()),
        },
    )


# Modo datos (listar fuentes, buscar, describir, leer filas): no pasa por el
# LLM, así que no descuenta del cupo de preguntas. Igual lleva límite propio,
# porque cada pedido es una consulta a la base de producción: por mes para
# acotar el total y por minuto para que un script no la martille en ráfaga.
CATALOG_MINUTE_LIMIT = 30


async def check_catalog_rate_limit(
    api_key: ApiKey,
    cache: ICacheService,
    credits: ICreditRepository | None = None,
) -> None:
    """Enforce the per-person limits on data-mode (no-LLM) endpoints."""
    user_id = api_key.user_id
    min_key = f"rl:user:{user_id}:catalog:min"
    minute = await _incr_fail_open(cache, min_key, _MIN_TTL)
    if minute > CATALOG_MINUTE_LIMIT:
        raise _too_many(
            f"Rate limit exceeded: {CATALOG_MINUTE_LIMIT} catalog requests per minute",
            {"Retry-After": str(await _seconds_left(cache, min_key, _MIN_TTL))},
        )
    tier = await resolve_tier(user_id, credits)
    count = await _incr_fail_open(cache, monthly_counter_key(user_id, "datos"), MONTH_TTL)
    if count > tier.datos and not await try_debit(user_id, "datos", credits):
        raise _quota_exhausted("datos", tier.datos)
