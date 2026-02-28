"""Tests for semantic cache hash utility and TTL constants."""
from __future__ import annotations

import pytest

from app.infrastructure.adapters.cache.semantic_cache import (
    SemanticCache,
    TTL_DAILY,
    TTL_REALTIME,
    TTL_STATIC,
)


class TestSemanticCacheHash:
    def test_hash_deterministic(self):
        h1 = SemanticCache._hash("¿cuánto vale el dólar?")
        h2 = SemanticCache._hash("¿cuánto vale el dólar?")
        assert h1 == h2

    def test_hash_normalized(self):
        h1 = SemanticCache._hash("   Test Question   ")
        h2 = SemanticCache._hash("test question")
        assert h1 == h2

    def test_hash_different_questions(self):
        h1 = SemanticCache._hash("inflación")
        h2 = SemanticCache._hash("desempleo")
        assert h1 != h2

    def test_hash_length(self):
        h = SemanticCache._hash("test")
        assert len(h) == 64  # SHA-256 hex


class TestTTLConstants:
    def test_realtime_is_shortest(self):
        assert TTL_REALTIME < TTL_DAILY < TTL_STATIC

    def test_values(self):
        assert TTL_REALTIME == 300
        assert TTL_DAILY == 1800
        assert TTL_STATIC == 7200
