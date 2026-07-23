from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.infrastructure.adapters.cache.redis_cache_adapter import RedisCacheAdapter


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    return redis


@pytest.fixture
def cache(mock_redis):
    adapter = RedisCacheAdapter.__new__(RedisCacheAdapter)
    adapter._redis = mock_redis
    return adapter


class TestRedisCacheAdapter:
    async def test_get_returns_none_when_key_missing(self, cache, mock_redis):
        mock_redis.get.return_value = None
        result = await cache.get("missing_key")
        assert result is None

    async def test_get_returns_parsed_json(self, cache, mock_redis):
        mock_redis.get.return_value = json.dumps({"answer": "test", "sources": []})
        result = await cache.get("valid_key")
        assert result == {"answer": "test", "sources": []}

    async def test_get_returns_raw_string_on_invalid_json(self, cache, mock_redis):
        mock_redis.get.return_value = "plain text"
        result = await cache.get("string_key")
        assert result == "plain text"

    async def test_set_serializes_dict(self, cache, mock_redis):
        await cache.set("key", {"data": 123}, ttl_seconds=60)
        mock_redis.set.assert_awaited_once_with("key", json.dumps({"data": 123}), ex=60)

    async def test_set_stores_string_directly(self, cache, mock_redis):
        await cache.set("key", "raw", ttl_seconds=30)
        mock_redis.set.assert_awaited_once_with("key", "raw", ex=30)

    async def test_delete(self, cache, mock_redis):
        await cache.delete("key")
        mock_redis.delete.assert_awaited_once_with("key")

    async def test_exists_true(self, cache, mock_redis):
        mock_redis.exists.return_value = 1
        assert await cache.exists("key") is True

    async def test_exists_false(self, cache, mock_redis):
        mock_redis.exists.return_value = 0
        assert await cache.exists("key") is False

    # H8 (round v46) — atomic increment + EXPIRE NX
    async def test_increment_with_ttl_pipelines_incr_and_expire_nx(self, cache, mock_redis):
        """The implementation MUST use a pipeline: INCR with bump=1,
        EXPIRE with nx=True. The previous get/set sequence let
        concurrent callers both observe count<cap and both write
        count+1, exceeding the cap.

        `pipe.incr` and `pipe.expire` are synchronous queue ops on the
        redis-py pipeline (they return the pipe so calls chain), only
        `pipe.execute()` is awaited. We mock them as MagicMock to mirror
        that protocol.
        """
        pipe = MagicMock()
        # First call returns the post-INCR counter, second returns the
        # boolean result of EXPIRE NX (1 the first time, 0 after).
        pipe.execute = AsyncMock(return_value=[3, 0])
        mock_redis.pipeline = lambda: pipe

        result = await cache.increment_with_ttl("ws_rate:user@example.com", 60)

        assert result == 3
        pipe.incr.assert_called_once_with("ws_rate:user@example.com", 1)
        # EXPIRE must carry nx=True so a stream of hits inside the
        # window doesn't refresh the TTL and hold the counter open.
        pipe.expire.assert_called_once_with("ws_rate:user@example.com", 60, nx=True)
        pipe.execute.assert_awaited_once()

    async def test_increment_with_ttl_returns_first_call_count(self, cache, mock_redis):
        """First INCR on a fresh key returns 1 — used to detect the
        edge from "not in window" to "in window" in callers."""
        pipe = MagicMock()
        pipe.execute = AsyncMock(return_value=[1, 1])
        mock_redis.pipeline = lambda: pipe

        assert await cache.increment_with_ttl("fresh", 60) == 1
