from __future__ import annotations

import hashlib
import json
import logging
import math
import unicodedata
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

# TTL by data freshness (seconds)
TTL_REALTIME = 300       # 5 min — dolar, riesgo pais
TTL_DAILY = 1800         # 30 min — inflacion, series
TTL_STATIC = 7200        # 2 hours — ddjj, sesiones, georef

# Intent-based TTL mapping
INTENT_TTL_MAP: dict[str, int] = {
    "dolar": TTL_REALTIME,
    "riesgo_pais": TTL_REALTIME,
    "cotizacion": TTL_REALTIME,
    "inflacion": TTL_DAILY,
    "series": TTL_DAILY,
    "emae": TTL_DAILY,
    "tipo_cambio": TTL_DAILY,
    "reservas": TTL_DAILY,
    "base_monetaria": TTL_DAILY,
    "desempleo": TTL_DAILY,
    "exportaciones": TTL_DAILY,
    "importaciones": TTL_DAILY,
    "balanza_comercial": TTL_DAILY,
    "ddjj": TTL_STATIC,
    "sesiones": TTL_STATIC,
    "ckan": TTL_STATIC,
    "busqueda": TTL_STATIC,
    "catalogo": TTL_STATIC,
    "legisladores": TTL_STATIC,
    "georef": TTL_STATIC,
}


def ttl_for_intent(intent: str) -> int:
    """Return the appropriate TTL for a given intent string.

    Uses word-boundary aware matching: splits intent into tokens
    and checks for exact token matches against INTENT_TTL_MAP keys.
    Falls back to substring matching for compound intents like 'consulta_dolar'.
    """
    intent_lower = intent.lower()
    # Split on common separators (spaces, underscores, hyphens)
    import re
    tokens = set(re.split(r"[\s_\-]+", intent_lower))

    # Exact token match first (most precise)
    for key, ttl in INTENT_TTL_MAP.items():
        if key in tokens:
            return ttl

    # Fallback: substring match for compound words (e.g. "cotizaciones" contains "cotizacion")
    for key, ttl in INTENT_TTL_MAP.items():
        if key in intent_lower:
            return ttl

    return TTL_DAILY


class SemanticCache:
    """Cache that supports exact hash match and vector similarity lookup."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        similarity_threshold: float = 0.92,
    ) -> None:
        self._session_factory = session_factory
        self._similarity_threshold = similarity_threshold

    @staticmethod
    def _normalize(question: str) -> str:
        """Normalize question for consistent hashing: lowercase, strip, NFC Unicode."""
        return unicodedata.normalize("NFC", question.strip().lower())

    @staticmethod
    def _hash(question: str) -> str:
        normalized = SemanticCache._normalize(question)
        return hashlib.sha256(normalized.encode()).hexdigest()

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
                    # Pre-filter by distance so pgvector can use the HNSW index
                    max_distance = 1 - self._similarity_threshold
                    result = await session.execute(
                        text("""
                            SELECT response, 1 - (embedding <=> CAST(:emb AS vector)) AS similarity
                            FROM query_cache
                            WHERE expires_at > :now
                              AND embedding IS NOT NULL
                              AND embedding <=> CAST(:emb AS vector) < :max_dist
                            ORDER BY embedding <=> CAST(:emb AS vector)
                            LIMIT 1
                        """),
                        {"emb": emb_str, "now": now, "max_dist": max_distance},
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

        if embedding:
            if any(math.isnan(v) or math.isinf(v) for v in embedding):
                logger.warning("Semantic cache write skipped: embedding contains NaN or Inf values")
                return

        try:
            async with self._session_factory() as session:
                emb_str = None
                if embedding:
                    emb_str = "[" + ",".join(str(v) for v in embedding) + "]"

                await session.execute(
                    text("""
                        INSERT INTO query_cache (question_hash, question, embedding, response, ttl_seconds, expires_at)
                        VALUES (:hash, :question, CAST(:emb AS vector), CAST(:response AS jsonb), :ttl, :expires_at)
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
