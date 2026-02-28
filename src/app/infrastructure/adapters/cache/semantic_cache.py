from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

# TTL by data freshness (seconds)
TTL_REALTIME = 300       # 5 min — dolar, riesgo pais
TTL_DAILY = 1800         # 30 min — inflacion, series
TTL_STATIC = 7200        # 2 hours — ddjj, sesiones, georef


class SemanticCache:
    """Cache that supports exact hash match and vector similarity lookup."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        similarity_threshold: float = 0.95,
    ) -> None:
        self._session_factory = session_factory
        self._similarity_threshold = similarity_threshold

    @staticmethod
    def _hash(question: str) -> str:
        return hashlib.sha256(question.strip().lower().encode()).hexdigest()

    async def get(
        self, question: str, embedding: list[float] | None = None
    ) -> dict | None:
        """Try exact hash first, then vector similarity."""
        q_hash = self._hash(question)
        now = datetime.now(UTC)

        try:
            async with self._session_factory() as session:
                # 1. Exact hash match
                result = await session.execute(
                    text("""
                        SELECT response FROM query_cache
                        WHERE question_hash = :hash AND expires_at > :now
                        LIMIT 1
                    """),
                    {"hash": q_hash, "now": now},
                )
                row = result.fetchone()
                if row:
                    return row[0] if isinstance(row[0], dict) else json.loads(row[0])

                # 2. Vector similarity (only if embedding provided)
                if embedding:
                    emb_str = "[" + ",".join(str(v) for v in embedding) + "]"
                    result = await session.execute(
                        text("""
                            SELECT response, 1 - (embedding <=> :emb::vector) AS similarity
                            FROM query_cache
                            WHERE expires_at > :now AND embedding IS NOT NULL
                            ORDER BY embedding <=> :emb::vector
                            LIMIT 1
                        """),
                        {"emb": emb_str, "now": now},
                    )
                    row = result.fetchone()
                    if row and row[1] >= self._similarity_threshold:
                        logger.debug("Semantic cache hit (similarity=%.3f)", row[1])
                        return row[0] if isinstance(row[0], dict) else json.loads(row[0])

        except Exception:
            logger.debug("Semantic cache lookup failed", exc_info=True)

        return None

    async def set(
        self,
        question: str,
        embedding: list[float] | None,
        response: dict[str, Any],
        ttl: int = TTL_DAILY,
    ) -> None:
        q_hash = self._hash(question)
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=ttl)

        try:
            async with self._session_factory() as session:
                emb_str = None
                if embedding:
                    emb_str = "[" + ",".join(str(v) for v in embedding) + "]"

                await session.execute(
                    text("""
                        INSERT INTO query_cache (question_hash, question, embedding, response, ttl_seconds, expires_at)
                        VALUES (:hash, :question, :emb::vector, :response::jsonb, :ttl, :expires_at)
                        ON CONFLICT (question_hash) DO UPDATE SET
                            response = EXCLUDED.response,
                            embedding = EXCLUDED.embedding,
                            ttl_seconds = EXCLUDED.ttl_seconds,
                            expires_at = EXCLUDED.expires_at,
                            created_at = NOW()
                    """),
                    {
                        "hash": q_hash,
                        "question": question,
                        "emb": emb_str,
                        "response": json.dumps(response, ensure_ascii=False),
                        "ttl": ttl,
                        "expires_at": expires_at,
                    },
                )
                await session.commit()
        except Exception:
            logger.warning("Semantic cache write failed", exc_info=True)

    async def cleanup(self) -> int:
        """Delete expired entries. Returns count deleted."""
        try:
            async with self._session_factory() as session:
                result = await session.execute(
                    text("DELETE FROM query_cache WHERE expires_at < :now"),
                    {"now": datetime.now(UTC)},
                )
                await session.commit()
                return result.rowcount or 0
        except Exception:
            logger.warning("Semantic cache cleanup failed", exc_info=True)
            return 0
