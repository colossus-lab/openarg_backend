"""Tests for the cached embedding service."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from app.infrastructure.adapters.cache.cached_embedding_service import CachedEmbeddingService


@pytest.fixture
def mock_base() -> AsyncMock:
    base = AsyncMock()
    base.embed.return_value = [0.1, 0.2, 0.3]
    base.embed_batch.return_value = [[0.1], [0.2]]
    return base


@pytest.fixture
def mock_cache() -> AsyncMock:
    cache = AsyncMock()
    cache.get.return_value = None
    cache.set.return_value = None
    return cache


@pytest.fixture
def service(mock_base: AsyncMock, mock_cache: AsyncMock) -> CachedEmbeddingService:
    return CachedEmbeddingService(base=mock_base, cache=mock_cache)


@pytest.mark.asyncio
async def test_embed_cache_miss(
    service: CachedEmbeddingService,
    mock_base: AsyncMock,
    mock_cache: AsyncMock,
) -> None:
    result = await service.embed("hello")
    assert result == [0.1, 0.2, 0.3]
    mock_base.embed.assert_called_once_with("hello")
    mock_cache.set.assert_called_once()


@pytest.mark.asyncio
async def test_embed_cache_hit_json_string(
    service: CachedEmbeddingService,
    mock_base: AsyncMock,
    mock_cache: AsyncMock,
) -> None:
    mock_cache.get.return_value = json.dumps([0.4, 0.5, 0.6])
    result = await service.embed("hello")
    assert result == [0.4, 0.5, 0.6]
    mock_base.embed.assert_not_called()


@pytest.mark.asyncio
async def test_embed_cache_hit_list(
    service: CachedEmbeddingService,
    mock_base: AsyncMock,
    mock_cache: AsyncMock,
) -> None:
    mock_cache.get.return_value = [0.7, 0.8, 0.9]
    result = await service.embed("hello")
    assert result == [0.7, 0.8, 0.9]
    mock_base.embed.assert_not_called()


@pytest.mark.asyncio
async def test_embed_cache_read_error_falls_through(
    service: CachedEmbeddingService,
    mock_base: AsyncMock,
    mock_cache: AsyncMock,
) -> None:
    mock_cache.get.side_effect = ConnectionError("Redis down")
    result = await service.embed("hello")
    assert result == [0.1, 0.2, 0.3]
    mock_base.embed.assert_called_once()


@pytest.mark.asyncio
async def test_embed_cache_write_error_does_not_raise(
    service: CachedEmbeddingService,
    mock_base: AsyncMock,
    mock_cache: AsyncMock,
) -> None:
    mock_cache.set.side_effect = ConnectionError("Redis down")
    result = await service.embed("hello")
    assert result == [0.1, 0.2, 0.3]


@pytest.mark.asyncio
async def test_embed_batch_delegates_directly(
    service: CachedEmbeddingService,
    mock_base: AsyncMock,
) -> None:
    result = await service.embed_batch(["a", "b"])
    assert result == [[0.1], [0.2]]
    mock_base.embed_batch.assert_called_once_with(["a", "b"])
