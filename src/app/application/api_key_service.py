"""API key generation, verification, and rate limiting."""

from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import UTC, datetime

from fastapi import HTTPException

from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService

logger = logging.getLogger(__name__)

_KEY_PREFIX = "oarg_sk_"

PLAN_LIMITS: dict[str, dict[str, int]] = {
    "free": {"per_min": 5, "per_day": 10},
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

    Raises HTTPException(401) if invalid, inactive, or expired.
    """
    if not token.startswith(_KEY_PREFIX):
        raise HTTPException(status_code=401, detail="Invalid API key format")

    key_hash = hash_api_key(token)
    api_key = await repo.get_by_key_hash(key_hash)

    if not api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")

    if not api_key.is_active:
        raise HTTPException(status_code=401, detail="API key has been revoked")

    if api_key.expires_at and api_key.expires_at < datetime.now(UTC):
        raise HTTPException(status_code=401, detail="API key has expired")

    return api_key


async def check_rate_limit(
    api_key: ApiKey,
    cache: ICacheService,
) -> dict[str, int]:
    """Check and enforce rate limits for the given API key.

    Returns a dict with remaining quotas. Raises HTTPException(429) if exceeded.
    """
    limits = PLAN_LIMITS.get(api_key.plan, PLAN_LIMITS["free"])
    key_id = str(api_key.id)

    # Per-minute check
    min_key = f"rl:{key_id}:min"
    min_count = await cache.get(min_key)
    min_count = int(min_count) if min_count is not None else 0

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

    # Per-day check
    day_key = f"rl:{key_id}:day"
    day_count = await cache.get(day_key)
    day_count = int(day_count) if day_count is not None else 0

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

    # Increment counters
    await cache.set(min_key, min_count + 1, ttl_seconds=60)
    await cache.set(day_key, day_count + 1, ttl_seconds=86400)

    return {
        "remaining_minute": limits["per_min"] - min_count - 1,
        "remaining_day": limits["per_day"] - day_count - 1,
        "limit_minute": limits["per_min"],
        "limit_day": limits["per_day"],
    }
