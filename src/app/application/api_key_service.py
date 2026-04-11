"""API key generation, verification, and rate limiting."""

from __future__ import annotations

import hashlib
import logging
import math
import secrets

from fastapi import HTTPException

from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService

logger = logging.getLogger(__name__)

_KEY_PREFIX = "oarg_sk_"
_MAX_TOKEN_LENGTH = 100  # Reject absurdly long tokens before hashing

# Global cap: max free requests across ALL free users per day.
GLOBAL_FREE_DAILY_CAP = 5000

PLAN_LIMITS: dict[str, dict[str, int]] = {
    "free": {"per_min": 2, "per_day": 5},
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


async def _safe_cache_get(cache: ICacheService, key: str) -> int:
    """Read a counter from cache, returning 0 on any failure (fail-open)."""
    try:
        val = await cache.get(key)
        return int(val) if val is not None else 0
    except Exception:
        logger.warning("Cache read failed for %s, allowing request (fail-open)", key)
        return 0


async def _safe_cache_set(cache: ICacheService, key: str, value: int, ttl: int) -> None:
    """Write a counter to cache, logging on failure instead of crashing."""
    try:
        await cache.set(key, value, ttl_seconds=ttl)
    except Exception:
        logger.warning("Cache write failed for %s, rate limit may be inaccurate", key)


async def check_rate_limit(
    api_key: ApiKey,
    cache: ICacheService,
    client_ip: str = "",
) -> dict[str, int]:
    """Check and enforce rate limits: per-user, per-IP, and global free cap.

    All cache operations are wrapped in try-except (fail-open): if Redis is
    down, requests are allowed but logged. Counters are only incremented
    AFTER all checks pass (no counter leak on rejected requests).

    Returns a dict with remaining quotas. Raises HTTPException(429) if exceeded.
    """
    limits = PLAN_LIMITS.get(api_key.plan, PLAN_LIMITS["free"])
    user_id = str(api_key.user_id)

    # ── Phase 1: READ all counters ───────────────────────
    global_count = 0
    if api_key.plan == "free":
        global_count = await _safe_cache_get(cache, "rl:global:free:day")

    ip_count = 0
    if client_ip:
        ip_count = await _safe_cache_get(cache, f"rl:ip:{client_ip}:day")

    min_count = await _safe_cache_get(cache, f"rl:user:{user_id}:min")
    day_count = await _safe_cache_get(cache, f"rl:user:{user_id}:day")

    # ── Phase 2: CHECK all limits (no increments yet) ────
    if api_key.plan == "free" and global_count >= GLOBAL_FREE_DAILY_CAP:
        logger.warning("Global free daily cap reached (%d)", global_count)
        raise HTTPException(
            status_code=429,
            detail="Free tier daily limit reached. Try again tomorrow or upgrade.",
        )

    if client_ip and ip_count >= 20:
        logger.warning("IP %s exceeded daily limit (%d)", client_ip, ip_count)
        raise HTTPException(
            status_code=429,
            detail="Too many requests from this IP. Try again tomorrow.",
        )

    if min_count >= limits["per_min"]:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded: {limits['per_min']} requests per minute",
            headers={
                "X-RateLimit-Limit-Minute": str(limits["per_min"]),
                "X-RateLimit-Remaining-Minute": "0",
                "Retry-After": "60",
            },
        )

    if day_count >= limits["per_day"]:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded: {limits['per_day']} requests per day",
            headers={
                "X-RateLimit-Limit-Day": str(limits["per_day"]),
                "X-RateLimit-Remaining-Day": "0",
                "Retry-After": "3600",
            },
        )

    # ── Phase 3: INCREMENT all counters (only after all checks pass) ──
    if api_key.plan == "free":
        await _safe_cache_set(cache, "rl:global:free:day", global_count + 1, 86400)

    if client_ip:
        await _safe_cache_set(cache, f"rl:ip:{client_ip}:day", ip_count + 1, 86400)

    await _safe_cache_set(cache, f"rl:user:{user_id}:min", min_count + 1, 60)
    await _safe_cache_set(cache, f"rl:user:{user_id}:day", day_count + 1, 86400)

    # ── Abuse monitoring ─────────────────────────────────
    day_threshold = math.ceil(limits["per_day"] * 0.8)
    if day_count + 1 == day_threshold:
        logger.warning(
            "API key %s (%s plan) reached 80%% of daily limit (%d/%d)",
            api_key.key_prefix,
            api_key.plan,
            day_count + 1,
            limits["per_day"],
        )

    return {
        "remaining_minute": limits["per_min"] - min_count - 1,
        "remaining_day": limits["per_day"] - day_count - 1,
        "limit_minute": limits["per_min"],
        "limit_day": limits["per_day"],
    }
