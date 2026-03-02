"""Tests for semantic cache hash utility, TTL constants, and intent-based TTL."""
from __future__ import annotations

from app.infrastructure.adapters.cache.semantic_cache import (
    INTENT_TTL_MAP,
    TTL_DAILY,
    TTL_REALTIME,
    TTL_STATIC,
    SemanticCache,
    ttl_for_intent,
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


class TestDefaultThreshold:
    def test_default_threshold_is_088(self):
        from unittest.mock import AsyncMock
        cache = SemanticCache(session_factory=AsyncMock())
        assert cache._similarity_threshold == 0.88


class TestTTLForIntent:
    def test_dolar_is_realtime(self):
        assert ttl_for_intent("dolar") == TTL_REALTIME

    def test_riesgo_pais_is_realtime(self):
        assert ttl_for_intent("riesgo_pais") == TTL_REALTIME

    def test_inflacion_is_daily(self):
        assert ttl_for_intent("inflacion") == TTL_DAILY

    def test_emae_is_daily(self):
        assert ttl_for_intent("emae") == TTL_DAILY

    def test_series_is_daily(self):
        assert ttl_for_intent("series") == TTL_DAILY

    def test_ddjj_is_static(self):
        assert ttl_for_intent("ddjj") == TTL_STATIC

    def test_sesiones_is_static(self):
        assert ttl_for_intent("sesiones") == TTL_STATIC

    def test_ckan_is_static(self):
        assert ttl_for_intent("ckan") == TTL_STATIC

    def test_unknown_defaults_to_daily(self):
        assert ttl_for_intent("unknown_intent") == TTL_DAILY

    def test_case_insensitive(self):
        assert ttl_for_intent("DOLAR") == TTL_REALTIME

    def test_partial_match_in_compound_intent(self):
        assert ttl_for_intent("consulta_dolar_blue") == TTL_REALTIME

    def test_intent_ttl_map_not_empty(self):
        assert len(INTENT_TTL_MAP) >= 10
