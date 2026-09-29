"""API key generation, verification, and rate limiting."""

from __future__ import annotations

import hashlib
import logging
import math
import os
import secrets
from datetime import UTC, datetime

from fastapi import HTTPException

from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService

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

PLAN_LIMITS: dict[str, dict[str, int]] = {
    "free": {"per_min": 2, "per_day": 10},
    "basic": {"per_min": 15, "per_day": 200},
    "pro": {"per_min": 30, "per_day": 1000},
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


async def check_rate_limit(
    api_key: ApiKey,
    cache: ICacheService,
    client_ip: str = "",
) -> dict[str, int]:
    """Check and enforce rate limits: per-minute, per-day, per-IP, and global free cap.

    Cada contador es un INCR atómico (`increment_with_ttl`), así que dos
    pedidos simultáneos no pueden pasar los dos por debajo del límite. El
    orden importa: un pedido rechazado por minuto no consume el cupo del día,
    y uno rechazado por el día no llega a tocar el tope global.

    Los contadores por usuario y por IP fallan abiertos si Redis no responde:
    un Redis degradado no tiene que cortarle el servicio a nadie. El tope
    global falla CERRADO: es el techo de gasto, y sin Redis no hay forma de
    saber cuánto se gastó hoy.

    Returns a dict with remaining quotas. Raises HTTPException(429) when a
    per-caller limit is exceeded and HTTPException(503) when the shared free
    capacity for the day is used up or cannot be checked.
    """
    limits = PLAN_LIMITS.get(api_key.plan, PLAN_LIMITS["free"])
    user_id = str(api_key.user_id)
    day = _utc_day()

    min_count = await _incr_fail_open(cache, f"rl:user:{user_id}:min", _MIN_TTL)
    if min_count > limits["per_min"]:
        raise _too_many(
            f"Rate limit exceeded: {limits['per_min']} requests per minute",
            {
                "X-RateLimit-Limit-Minute": str(limits["per_min"]),
                "X-RateLimit-Remaining-Minute": "0",
                "Retry-After": "60",
            },
        )

    day_count = await _incr_fail_open(cache, f"rl:user:{user_id}:day:{day}", _DAY_TTL)
    if day_count > limits["per_day"]:
        raise _too_many(
            f"Rate limit exceeded: {limits['per_day']} requests per day",
            {
                "X-RateLimit-Limit-Day": str(limits["per_day"]),
                "X-RateLimit-Remaining-Day": "0",
                "Retry-After": str(seconds_until_utc_midnight()),
            },
        )

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

    # ── Abuse monitoring ─────────────────────────────────
    day_threshold = math.ceil(limits["per_day"] * 0.8)
    if day_count == day_threshold:
        logger.warning(
            "API key %s (%s plan) reached 80%% of daily limit (%d/%d)",
            api_key.key_prefix,
            api_key.plan,
            day_count,
            limits["per_day"],
        )

    return {
        "remaining_minute": max(limits["per_min"] - min_count, 0),
        "remaining_day": max(limits["per_day"] - day_count, 0),
        "limit_minute": limits["per_min"],
        "limit_day": limits["per_day"],
    }


# Consultas de catálogo (listar fuentes): no pasan por el LLM, así que no
# descuentan del cupo de preguntas. Igual llevan un límite propio para que
# una clave no pueda usarlas para martillar la base.
CATALOG_DAILY_LIMIT = 60


async def check_catalog_rate_limit(api_key: ApiKey, cache: ICacheService) -> None:
    """Enforce the per-key daily limit on catalog (no-LLM) endpoints."""
    key = f"rl:user:{api_key.user_id}:catalog:{_utc_day()}"
    count = await _incr_fail_open(cache, key, _DAY_TTL)
    if count > CATALOG_DAILY_LIMIT:
        raise _too_many(
            f"Rate limit exceeded: {CATALOG_DAILY_LIMIT} catalog requests per day",
            {"Retry-After": str(seconds_until_utc_midnight())},
        )
