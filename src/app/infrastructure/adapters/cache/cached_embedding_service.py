"""Cached embedding service — decorator over IEmbeddingProvider that caches embed() results in Redis."""

from __future__ import annotations

import hashlib
import json
import logging

from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.llm.llm_provider import IEmbeddingProvider

logger = logging.getLogger(__name__)

_CACHE_TTL = 3600  # 1 hour


class CachedEmbeddingService(IEmbeddingProvider):
    """Decorator that caches single embed() calls in Redis.

    Only caches embed() — embed_batch() delegates directly to the base provider.
    Falls through gracefully on any Redis error.
    """

    def __init__(self, base: IEmbeddingProvider, cache: ICacheService) -> None:
        self._base = base
        self._cache = cache

    @staticmethod
    def _cache_key(text: str) -> str:
        h = hashlib.sha256(text.encode()).hexdigest()
        return f"emb_cache:{h}"

    async def embed(self, text: str) -> list[float]:
        key = self._cache_key(text)

        # Try cache read
        try:
            cached = await self._cache.get(key)
            if cached is not None:
                if isinstance(cached, str):
                    return json.loads(cached)
                if isinstance(cached, list):
                    return cached
        except Exception:
            logger.debug("Embedding cache read failed for key %s", key)

        # Generate embedding
        result = await self._base.embed(text)

        # Try cache write
        try:
            await self._cache.set(key, json.dumps(result), ttl_seconds=_CACHE_TTL)
        except Exception:
            logger.debug("Embedding cache write failed for key %s", key)

        return result

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return await self._base.embed_batch(texts)
