from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ICacheService(ABC):
    @abstractmethod
    async def get(self, key: str) -> Any | None: ...

    @abstractmethod
    async def set(self, key: str, value: Any, ttl_seconds: int = 3600) -> None: ...

    @abstractmethod
    async def delete(self, key: str) -> None: ...

    @abstractmethod
    async def exists(self, key: str) -> bool: ...

    @abstractmethod
    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        """Atomically increment `key` and set TTL only when the key is new.

        Implementations MUST use a Redis-side atomic primitive (INCR +
        EXPIRE NX in a pipeline, or a Lua script) — a get/set sequence
        in Python is racy: two concurrent callers can both read `count`,
        both bump it to `count+1`, and the cap is exceeded. Also, each
        SET refreshes the TTL, so a steady stream of requests inside
        the window can hold the counter open indefinitely. EXPIRE NX
        only stamps the TTL the first time the key is created, which
        means the window starts the moment the first hit lands and
        closes 60s later regardless of subsequent traffic.

        Returns the post-increment integer counter.

        See round v46 H8 for the previous race.
        """
        ...
