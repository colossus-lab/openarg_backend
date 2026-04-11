"""Cache check/write logic for the smart query pipeline."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import TYPE_CHECKING, Any

from app.infrastructure.adapters.cache.semantic_cache import ttl_for_intent
from app.infrastructure.audit.audit_logger import audit_cache_hit
from app.infrastructure.monitoring.metrics import MetricsCollector

if TYPE_CHECKING:
    from app.domain.ports.cache.cache_port import ICacheService
    from app.domain.ports.llm.llm_provider import IEmbeddingProvider
    from app.infrastructure.adapters.cache.semantic_cache import SemanticCache

logger = logging.getLogger(__name__)


def cache_key(question: str) -> str:
    """Build a deterministic Redis cache key from a question."""
    normalized = question.strip().lower()
    h = hashlib.sha256(normalized.encode()).hexdigest()[:16]
    return f"openarg:smart:{h}"


async def get_embedding(
    embedding: IEmbeddingProvider,
    question: str,
) -> list[float] | None:
    """Generate an embedding for *question*, returning None on failure."""
    try:
        return await embedding.embed(question)
    except Exception:
        logger.debug("Embedding generation failed for cache", exc_info=True)
        return None


async def check_cache(
    question: str,
    user_id: str,
    cache: ICacheService,
    embedding_provider: IEmbeddingProvider,
    semantic_cache: SemanticCache,
    metrics: MetricsCollector,
) -> tuple[dict[str, Any] | None, list[float] | None]:
    """Check Redis and semantic cache for a hit.

    Returns (result_dict_or_None, embedding_or_None).
    The caller can use the embedding for later cache writes.
    """

    async def _redis_lookup() -> dict[str, Any] | None:
        try:
            key = cache_key(question)
            cached = await cache.get(key)
            if cached and isinstance(cached, dict):
                return cached
        except Exception:
            logger.debug("Redis cache read failed", exc_info=True)
        return None

    redis_result, q_embedding = await asyncio.gather(
        _redis_lookup(),
        get_embedding(embedding_provider, question),
    )

    # 1. Redis hit
    if redis_result is not None:
        metrics.record_cache_hit()
        audit_cache_hit(user=user_id, question=question)
        return redis_result, q_embedding

    # 2. Semantic cache
    if q_embedding:
        try:
            sem_cached = await semantic_cache.get(question, embedding=q_embedding)
            if sem_cached:
                metrics.record_cache_hit()
                audit_cache_hit(user=user_id, question=question)
                return sem_cached, q_embedding
        except Exception:
            logger.debug("Semantic cache read failed", exc_info=True)

    metrics.record_cache_miss()
    return None, q_embedding


async def get_cached_dict(
    question: str,
    cache: ICacheService,
    embedding_provider: IEmbeddingProvider,
    semantic_cache: SemanticCache,
) -> tuple[dict[str, Any] | None, list[float] | None]:
    """Return raw cached dict for the streaming endpoint.

    Returns (result_dict_or_None, embedding_or_None).
    """

    async def _redis_lookup() -> dict[str, Any] | None:
        try:
            cached = await cache.get(cache_key(question))
            if cached and isinstance(cached, dict):
                return cached
        except Exception:
            logger.debug("WS Redis cache read failed", exc_info=True)
        return None

    redis_result, q_embedding = await asyncio.gather(
        _redis_lookup(),
        get_embedding(embedding_provider, question),
    )

    if redis_result is not None:
        return redis_result, None

    if q_embedding:
        try:
            sem = await semantic_cache.get(question, embedding=q_embedding)
            if sem and isinstance(sem, dict):
                return sem, q_embedding
        except Exception:
            logger.debug("WS semantic cache read failed", exc_info=True)
    return None, q_embedding


_ERROR_MARKERS = ("ocurrió un error", "error al analizar", "pipeline_error")

_CACHE_WRITE_MAX_RETRIES = 2
_CACHE_WRITE_BACKOFF_BASE = 0.5


async def _retry_async(op_name: str, op: Any) -> bool:
    """Retry an async cache write with exponential backoff. Returns True if it eventually succeeded."""
    last_exc: Exception | None = None
    for attempt in range(1 + _CACHE_WRITE_MAX_RETRIES):
        try:
            await op()
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_exc = exc
            if attempt < _CACHE_WRITE_MAX_RETRIES:
                delay = _CACHE_WRITE_BACKOFF_BASE * (2**attempt)
                logger.debug(
                    "%s attempt %d/%d failed, retrying in %.1fs",
                    op_name,
                    attempt + 1,
                    1 + _CACHE_WRITE_MAX_RETRIES,
                    delay,
                    exc_info=True,
                )
                await asyncio.sleep(delay)
    logger.warning(
        "%s failed after %d attempts: %s",
        op_name,
        1 + _CACHE_WRITE_MAX_RETRIES,
        last_exc,
    )
    return False


async def write_cache(
    question: str,
    result: dict[str, Any],
    intent: str,
    cache: ICacheService,
    embedding_provider: IEmbeddingProvider,
    semantic_cache: SemanticCache,
    last_embedding: list[float] | None = None,
) -> None:
    """Write to both Redis and semantic cache. Skip error responses.

    Each write is retried independently with exponential backoff so a
    transient failure in Redis does not block the semantic cache write
    (and vice versa). Matches the retry behavior of the memory update
    path — see finalize.py `_update_memory_bg`.
    """
    answer = (result.get("answer") or "").lower()
    if any(marker in answer for marker in _ERROR_MARKERS):
        logger.debug("Skipping cache write for error response")
        return
    ttl = ttl_for_intent(intent)

    await _retry_async(
        "Redis cache write",
        lambda: cache.set(cache_key(question), result, ttl_seconds=ttl),
    )

    q_embedding = last_embedding
    if not q_embedding:
        q_embedding = await get_embedding(embedding_provider, question)
    if q_embedding:
        await _retry_async(
            "Semantic cache write",
            lambda: semantic_cache.set(question, q_embedding, result, ttl=ttl),
        )
