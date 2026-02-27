from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis

from app.domain.ports.cache.cache_port import ICacheService


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
        serialized = json.dumps(value) if not isinstance(value, str) else value
        await self._redis.set(key, serialized, ex=ttl_seconds)

    async def delete(self, key: str) -> None:
        await self._redis.delete(key)

    async def exists(self, key: str) -> bool:
        return bool(await self._redis.exists(key))
