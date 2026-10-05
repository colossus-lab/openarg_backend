from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis

from app.domain.ports.cache.cache_port import ICacheService
from app.infrastructure.serialization import safe_dumps

# DECR with a floor and without creating the key, in one atomic step. A plain
# DECR on a key that expired between the reservation and the refund would
# create it at -1 with no TTL, and that counter would never expire.
_DECREMENT_FLOOR_ZERO = """
local v = tonumber(redis.call('GET', KEYS[1]))
if v == nil then return 0 end
if v <= 0 then return v end
return redis.call('DECR', KEYS[1])
"""

# Compare-and-delete: release a lock only while it still holds our token.
_DELETE_IF_EQUALS = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


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

    async def decrement(self, key: str) -> int:
        # EVAL runs atomically on the Redis side: no other command can land
        # between the GET and the DECR. DECR keeps the key's TTL.
        return int(await self._redis.eval(_DECREMENT_FLOOR_ZERO, 1, key))

    async def set_if_absent(self, key: str, value: str, ttl_seconds: int) -> bool:
        # SET NX EX is a single atomic command: the key and its TTL land together.
        return bool(await self._redis.set(key, value, ex=ttl_seconds, nx=True))

    async def delete_if_equals(self, key: str, value: str) -> bool:
        return bool(await self._redis.eval(_DELETE_IF_EQUALS, 1, key, value))

    async def ttl(self, key: str) -> int | None:
        # Redis: -2 if the key does not exist, -1 if it has no expiry.
        seconds = int(await self._redis.ttl(key))
        return seconds if seconds >= 0 else None
