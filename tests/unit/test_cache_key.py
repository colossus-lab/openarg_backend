"""Tests for `app.application.pipeline.cache_manager.cache_key`.

Migrated 2026-05-09 from skip-marked legacy tests (spec 020). Originally
imported `_cache_key` from `presentation.http.controllers.query.query_router`
when SmartQueryService was a monolith; the canonical home is now
`pipeline.cache_manager` and the prefix is `openarg:smart:` (not
`openarg:query:`).
"""

from __future__ import annotations

from app.application.pipeline.cache_manager import cache_key


class TestCacheKey:
    def test_deterministic(self):
        assert cache_key("hola") == cache_key("hola")

    def test_case_insensitive(self):
        assert cache_key("Inflacion") == cache_key("inflacion")

    def test_trims_whitespace(self):
        assert cache_key("  hola  ") == cache_key("hola")

    def test_different_questions_different_keys(self):
        assert cache_key("inflacion") != cache_key("presupuesto")

    def test_prefix(self):
        key = cache_key("test")
        assert key.startswith("openarg:smart:")

    def test_returns_hex_suffix(self):
        # 16-char sha256 truncation
        key = cache_key("test")
        suffix = key.split(":")[-1]
        assert len(suffix) == 16
        assert all(c in "0123456789abcdef" for c in suffix)
