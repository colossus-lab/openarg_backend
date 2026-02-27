from __future__ import annotations

from app.presentation.http.controllers.query.query_router import _cache_key


class TestCacheKey:
    def test_deterministic(self):
        assert _cache_key("hola") == _cache_key("hola")

    def test_case_insensitive(self):
        assert _cache_key("Inflacion") == _cache_key("inflacion")

    def test_trims_whitespace(self):
        assert _cache_key("  hola  ") == _cache_key("hola")

    def test_different_questions_different_keys(self):
        assert _cache_key("inflacion") != _cache_key("presupuesto")

    def test_prefix(self):
        key = _cache_key("test")
        assert key.startswith("openarg:query:")
