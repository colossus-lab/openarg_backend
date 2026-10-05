"""API key generation, verification, and rate limiting."""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException

from app.application.public_quota import (
    MONTH_TTL,
    Tier,
    first_of_next_month_utc,
    has_credit,
    month_key,
    monthly_counter_key,
    resolve_tier,
    seconds_until_next_month,
    try_debit,
)
from app.application.web_quota import NON_BILLABLE_INTENTS, counts_against_quota
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService

if TYPE_CHECKING:
    from uuid import UUID

    from app.application.answers.engine import EngineResult
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

# El intent de una respuesta servida desde el caché semántico (lo ponen
# `EngineRunner._from_cache` y el nodo `cache_reply` del grafo).
CACHED_INTENT = "cached"


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


# El 503 del modo respuestas tiene dos causas que el usuario tiene que poder
# distinguir: el tope global del día (esperar a mañana) y Redis caído (es un
# problema nuestro, que se arregla en minutos). Antes el MCP mostraba las dos
# como "el cupo de hoy está agotado".
QUOTA_SERVICE_DOWN_DETAIL = (
    "Public API temporarily unavailable: the quota service is not responding. "
    "Try again in a few minutes."
)
DAILY_CAPACITY_DETAIL = "Free tier daily capacity reached. Try again tomorrow."


# ── Cobro de las preguntas (modo respuestas) ─────────────────
#
# Hasta el 05-oct-2026 `/ask` descontaba la pregunta del mes AL ENTRAR: un
# timeout, un error, un saludo o un acierto del caché costaban 1 de 10. Ahora
# es como el chat web (`web_quota.py`): se cobra sólo una respuesta completa
# que usó el modelo. El cupo del mes se RESERVA al entrar con el mismo INCR
# atómico de siempre y se DEVUELVE (DECR atómico) si el turno no se cobra.
#
# Por qué reservar y no sólo leer al entrar, como la web: con 1 pregunta
# restante, dos pedidos simultáneos no pueden pasar los dos. El INCR atómico
# decide cuál entra; el otro recibe el 402 en el momento. Si el que entró
# termina sin cobrarse (p. ej. un timeout), devuelve la reserva y la pregunta
# vuelve a estar disponible para el próximo pedido.
#
# Los créditos no tienen una operación de devolución, así que al entrar sólo
# se verifica que haya saldo y el débito (atómico en la base) va al final. Con
# un solo crédito, dos pedidos simultáneos que ya agotaron el mes pueden entrar
# los dos; el segundo débito falla y esa respuesta sale sin cobrarse. Es un
# sobregiro acotado por el límite por minuto (2 en el plan gratis) y se loguea.
#
# Si una respuesta gasta un crédito NO se decide al reservar sino al cobrar,
# con un contador aparte de respuestas cobradas del mes (`charged_counter_key`,
# sólo sube, nunca se devuelve): la respuesta cobrada número N gasta un crédito
# sólo si N supera el cupo. El contador de reservas no sirve para eso porque
# incluye pedidos en curso que después pueden devolverse: con 9 de 10 usadas,
# A reserva el 10 y B el 11; si A termina en timeout, B ocupa el lugar 10 y no
# tiene que pagar un crédito, termine antes o después que A.
#
# El límite por minuto cuenta al entrar y no se devuelve nunca: es lo que
# frena las ráfagas. Una pregunta repetida que ya tiene respuesta guardada no
# lo cuenta (el reintento de n8n caía en 429), pero tiene su propio límite por
# minuto, más laxo (`check_replay_rate`).
#
# La IP del día y el tope global del día no son el cupo de la persona sino
# techos de volumen y de gasto de Bedrock: se devuelven si el pedido termina
# rechazado por un control posterior o si el turno se resolvió sin el modelo
# (caché, saludo, bloqueo). Un timeout o un error sí quedan contados ahí,
# porque el modelo corrió y Bedrock cobró.


@dataclass(frozen=True)
class MinuteWindow:
    """El límite por minuto ya contado para este pedido."""

    limit: int
    count: int

    @property
    def remaining(self) -> int:
        return max(self.limit - self.count, 0)


@dataclass
class QuestionReservation:
    """Lo que `reserve_question` apartó para una pregunta, para cobrarlo o devolverlo.

    Las claves son las de los contadores que efectivamente subieron. ``None``
    si no hay nada que devolver (o Redis no respondió y el contador falló
    abierto).

    ``needs_credit`` es la decisión al entrar (el 402 sin saldo). Si la
    respuesta gasta un crédito lo decide `settle_question` con el contador de
    respuestas cobradas (``charged_key``), del mismo mes que ``month_counter``.
    """

    user_id: UUID
    tier: Tier
    minute: MinuteWindow
    month_key: str | None
    month_count: int
    needs_credit: bool
    month_counter: str
    charged_key: str
    ip_key: str | None = None
    global_key: str | None = None
    settled: bool = False


def _minute_key(user_id: object) -> str:
    return f"rl:user:{user_id}:min"


def _replay_minute_key(user_id: object) -> str:
    return f"rl:user:{user_id}:replay:min"


def charged_counter_key(user_id: object, now: datetime | None = None) -> str:
    """Respuestas de ``/ask`` cobradas en el mes (sólo sube; decide los créditos)."""
    return f"rl:user:{user_id}:charged:month:{month_key(now)}"


def _plan_per_min(api_key: ApiKey) -> int:
    return PLAN_LIMITS.get(api_key.plan, PLAN_LIMITS["free"])["per_min"]


# Repeticiones de una pregunta ya respondida, por minuto. No corren el motor ni
# cobran, pero cada una lee la base y deja rastro: sin tope, un bucle que manda
# la misma pregunta cada 2 s llenaba `api_usage` a la velocidad del cliente.
# Nunca por debajo del límite del plan: repetir no puede ser más caro que
# preguntar algo nuevo.
REPLAY_PER_MIN = 10


def replay_per_min(api_key: ApiKey) -> int:
    return max(REPLAY_PER_MIN, _plan_per_min(api_key))


async def check_replay_rate(api_key: ApiKey, cache: ICacheService) -> None:
    """El límite por minuto de las preguntas repetidas (falla abierto).

    Raises HTTPException 429. El detalle dice "minute" y el ``Retry-After`` es
    el TTL de este contador, así el MCP muestra cuántos segundos esperar.
    """
    limit = replay_per_min(api_key)
    replay_key = _replay_minute_key(api_key.user_id)
    count = await _incr_fail_open(cache, replay_key, _MIN_TTL)
    if count > limit:
        raise _too_many(
            f"Rate limit exceeded: {limit} repeated questions per minute",
            {
                "X-RateLimit-Limit-Minute": str(limit),
                "X-RateLimit-Remaining-Minute": "0",
                "Retry-After": str(await _seconds_left(cache, replay_key, _MIN_TTL)),
            },
        )


async def check_question_rate(api_key: ApiKey, cache: ICacheService) -> MinuteWindow:
    """El límite por minuto del modo respuestas. Cuenta al entrar (falla abierto).

    Raises HTTPException 429.
    """
    per_min = _plan_per_min(api_key)
    min_key = _minute_key(api_key.user_id)
    count = await _incr_fail_open(cache, min_key, _MIN_TTL)
    if count > per_min:
        raise _too_many(
            f"Rate limit exceeded: {per_min} requests per minute",
            {
                "X-RateLimit-Limit-Minute": str(per_min),
                "X-RateLimit-Remaining-Minute": "0",
                "Retry-After": str(await _seconds_left(cache, min_key, _MIN_TTL)),
            },
        )
    return MinuteWindow(per_min, count)


async def _refund(cache: ICacheService, key: str | None) -> int | None:
    """Devuelve una reserva. El valor que quedó, o None si no se pudo."""
    if key is None:
        return None
    try:
        return await cache.decrement(key)
    except Exception:
        logger.warning("Quota refund failed for %s; the reservation stays counted", key)
        return None


async def _refund_all(cache: ICacheService, *keys: str | None) -> None:
    for key in keys:
        await _refund(cache, key)


async def reserve_question(
    api_key: ApiKey,
    cache: ICacheService,
    client_ip: str = "",
    credits: ICreditRepository | None = None,
    *,
    minute: MinuteWindow | None = None,
) -> QuestionReservation:
    """Verifica y reserva el cupo de una pregunta, sin cobrarla todavía.

    Orden: por minuto → cupo del mes → (si hace falta un crédito y no hay
    saldo, 402 acá, antes de tocar la IP o el tope global) → IP del día →
    tope global del día. El crédito no se gasta acá: lo gasta
    `settle_question` si la respuesta se cobra.

    ``minute``: el límite por minuto ya contado (``/ask`` lo cuenta antes de
    esperar una pregunta igual que esté en curso). Sin él, se cuenta acá.

    Cada contador es un INCR atómico (`increment_with_ttl`). Los contadores
    por usuario y por IP fallan abiertos si Redis no responde (un Redis
    degradado no le corta el servicio a nadie); el tope global falla CERRADO,
    porque es el techo de gasto de Bedrock. Si un control rechaza el pedido,
    se devuelve lo que los anteriores ya habían reservado.

    Raises HTTPException 429 (por minuto, IP), 402 (cupo del mes sin créditos)
    or 503 (tope global o Redis caído).
    """
    if minute is None:
        minute = await check_question_rate(api_key, cache)
    user_id = api_key.user_id
    now = datetime.now(UTC)
    day = _utc_day(now)

    tier = await resolve_tier(user_id, credits)
    month_counter = monthly_counter_key(user_id, "preguntas", now)
    charged_key = charged_counter_key(user_id, now)
    month_count = await _incr_fail_open(cache, month_counter, MONTH_TTL)
    # 0 = el INCR falló y se dejó pasar: no quedó nada reservado.
    reserved_month = month_counter if month_count > 0 else None
    if month_count == 1:
        # Primera reserva del mes: no hay nada cobrado ni en curso, así que
        # las cobradas arrancan exactamente en 0.
        await _seed_charged(cache, charged_key, 0)
    needs_credit = month_count > tier.preguntas
    # Sin saldo, se corta acá: si no, cada reintento de alguien que ya agotó su
    # mes sumaría al tope global del día y le comería lugar a los demás.
    if needs_credit and not await has_credit(user_id, "preguntas", credits):
        await _refund(cache, reserved_month)
        raise _quota_exhausted("preguntas", tier.preguntas)

    reserved_ip: str | None = None
    if client_ip:
        ip_key = f"rl:ip:{client_ip}:day:{day}"
        ip_count = await _incr_fail_open(cache, ip_key, _DAY_TTL)
        reserved_ip = ip_key if ip_count > 0 else None
        if ip_count > ip_daily_limit():
            logger.warning("IP %s exceeded daily limit (%d)", client_ip, ip_count)
            await _refund_all(cache, reserved_month, reserved_ip)
            raise _too_many(
                "Too many requests from this IP. Try again tomorrow.",
                {"Retry-After": str(seconds_until_utc_midnight())},
            )

    reserved_global: str | None = None
    if api_key.plan == "free":
        cap = global_free_daily_cap()
        global_key = f"rl:global:free:day:{day}"
        try:
            global_count = await cache.increment_with_ttl(global_key, ttl_seconds=_DAY_TTL)
        except Exception:
            logger.error("Global free cap cannot be checked (cache down); rejecting (fail-closed)")
            await _refund_all(cache, reserved_month, reserved_ip)
            raise HTTPException(
                status_code=503,
                detail=QUOTA_SERVICE_DOWN_DETAIL,
                headers={"Retry-After": "300"},
            ) from None
        reserved_global = global_key
        if global_count > cap:
            logger.warning("Global free daily cap reached (%d/%d)", global_count, cap)
            await _refund_all(cache, reserved_month, reserved_ip, reserved_global)
            raise HTTPException(
                status_code=503,
                detail=DAILY_CAPACITY_DETAIL,
                headers={"Retry-After": str(seconds_until_utc_midnight())},
            )

    return QuestionReservation(
        user_id=user_id,
        tier=tier,
        minute=minute,
        month_key=reserved_month,
        month_count=month_count,
        needs_credit=needs_credit,
        month_counter=month_counter,
        charged_key=charged_key,
        ip_key=reserved_ip,
        global_key=reserved_global,
    )


async def _seed_charged(cache: ICacheService, key: str, value: int) -> None:
    try:
        await cache.set_if_absent(key, str(max(value, 0)), MONTH_TTL)
    except Exception:
        logger.warning("Charged counter seed failed for %s", key)


async def _count_charged(reservation: QuestionReservation, cache: ICacheService) -> int | None:
    """Suma esta respuesta a las cobradas del mes y devuelve cuántas van.

    Si el contador todavía no existe (el mes del despliegue: las reservas de
    antes ya estaban cobradas al entrar), arranca de las reservas del mes
    menos la de este pedido. None si Redis no responde.
    """
    try:
        if not await cache.exists(reservation.charged_key):
            reserved = int(await cache.get(reservation.month_counter) or 0)
            own = 1 if reservation.month_key else 0
            await _seed_charged(cache, reservation.charged_key, reserved - own)
        return await cache.increment_with_ttl(reservation.charged_key, MONTH_TTL)
    except Exception:
        logger.warning("Charged counter unavailable for %s", reservation.user_id)
        return None


async def settle_question(
    reservation: QuestionReservation,
    cache: ICacheService,
    credits: ICreditRepository | None = None,
    *,
    charge: bool,
    used_model: bool = True,
) -> dict[str, Any]:
    """Cierra una reserva: la cobra (``charge``) o la devuelve.

    Cobrar es dejar la reserva del mes como está y sumar la respuesta a las
    cobradas del mes; si es la número N y N supera el cupo, se gasta un
    crédito. No cobrar es devolver la reserva del mes y, si el turno no usó
    el modelo (``used_model=False``), también la de la IP y el tope global.
    Idempotente: una reserva ya cerrada no se toca.

    Devuelve el cupo para la respuesta (mismas claves de siempre).
    """
    month_count = reservation.month_count
    used_credit = False
    if reservation.settled:
        return quota_info(reservation.tier, reservation.minute, month_count, used_credit=False)
    reservation.settled = True
    if charge:
        charged = await _count_charged(reservation, cache)
        # Sin Redis, la decisión de la entrada (la única que hay).
        needs_credit = (
            reservation.needs_credit if charged is None else charged > reservation.tier.preguntas
        )
        if needs_credit:
            used_credit = await try_debit(reservation.user_id, "preguntas", credits)
            if not used_credit:
                logger.info(
                    "Question answered without quota or credit for %s (race)", reservation.user_id
                )
    else:
        refunded = await _refund(cache, reservation.month_key)
        if refunded is not None:
            month_count = refunded
        if not used_model:
            await _refund_all(cache, reservation.ip_key, reservation.global_key)
    return quota_info(reservation.tier, reservation.minute, month_count, used_credit=used_credit)


def quota_info(
    tier: Tier, minute: MinuteWindow, month_count: int, *, used_credit: bool
) -> dict[str, Any]:
    """El cupo de preguntas como lo devuelve ``/ask`` en ``usage``."""
    remaining = max(tier.preguntas - month_count, 0)
    return {
        "remaining_minute": minute.remaining,
        "limit_minute": minute.limit,
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


async def _read_counter(cache: ICacheService, key: str) -> int:
    try:
        return int(await cache.get(key) or 0)
    except Exception:
        logger.warning("Quota counter unreadable (%s); treating as 0", key)
        return 0


async def question_quota_snapshot(
    api_key: ApiKey, cache: ICacheService, credits: ICreditRepository | None = None
) -> dict[str, Any]:
    """El cupo de preguntas sin tocar nada: para una respuesta que no se cobró
    ni pasó por los controles (una pregunta repetida que ya estaba respondida)."""
    tier = await resolve_tier(api_key.user_id, credits)
    minute = MinuteWindow(
        _plan_per_min(api_key), await _read_counter(cache, _minute_key(api_key.user_id))
    )
    month_count = await _read_counter(cache, monthly_counter_key(api_key.user_id, "preguntas"))
    return quota_info(tier, minute, month_count, used_credit=False)


# Turnos que se resolvieron sin llamar al modelo: los fijos del clasificador
# (saludo, pregunta sobre OpenArg, explicación, bloqueos) y el caché. La
# aclaración NO está: para pedir una precisión el modelo ya corrió.
NO_MODEL_INTENTS = (NON_BILLABLE_INTENTS - {"clarification"}) | {CACHED_INTENT}


def answer_is_billable(result: EngineResult) -> bool:
    """Si una respuesta de ``/ask`` descuenta una pregunta.

    El mismo criterio que el chat web (``counts_against_quota``: no descuentan
    saludos, preguntas sobre OpenArg, explicaciones fijas, aclaraciones,
    bloqueos ni respuestas vacías) y, además, tampoco un acierto del caché: no
    usó el modelo, y una integración que repite la misma pregunta no tiene por
    qué pagarla dos veces.
    """
    return counts_against_quota(result) and result.intent != CACHED_INTENT


def answer_used_model(result: EngineResult) -> bool:
    """Si el turno llamó al modelo (y por lo tanto gastó Bedrock)."""
    return result.intent not in NO_MODEL_INTENTS


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
