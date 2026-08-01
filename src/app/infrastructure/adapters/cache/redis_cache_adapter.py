from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis

from app.domain.ports.cache.cache_port import ICacheService
from app.infrastructure.serialization import safe_dumps


class RedisCacheAdapter(ICacheService):
    def __init__(self, redis_url: str = "redis://localhost:6379/2") -> None:
        self._redis = aioredis.from_url(redis_url, decode_responses=True)

    async def get(self, key: str) -> Any | None:
        value = await self._redis.get(key)
        if value is None:
            return None
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    async def set(self, key: str, value: Any, ttl_seconds: int = 3600) -> None:
        serialized = safe_dumps(value) if not isinstance(value, str) else value
        await self._redis.set(key, serialized, ex=ttl_seconds)

    async def delete(self, key: str) -> None:
        await self._redis.delete(key)

    async def exists(self, key: str) -> bool:
        return bool(await self._redis.exists(key))

    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        # H8 (round v46): atomic INCR + EXPIRE NX in a pipeline. The two
        # commands fan out to Redis as a single round-trip; INCR is
        # itself atomic on the Redis side; EXPIRE NX only sets the TTL
        # when the key has none yet — so a steady stream of hits inside
        # the window does NOT extend the expiry, and concurrent callers
        # see strictly monotonic counts (no two-callers-bump-to-N race).
        pipe = self._redis.pipeline()
        pipe.incr(key, 1)
        pipe.expire(key, ttl_seconds, nx=True)
        results = await pipe.execute()
        return int(results[0])
