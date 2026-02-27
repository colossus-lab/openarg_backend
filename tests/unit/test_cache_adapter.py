from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

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
        mock_redis.set.assert_awaited_once_with(
            "key", json.dumps({"data": 123}), ex=60
        )

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
