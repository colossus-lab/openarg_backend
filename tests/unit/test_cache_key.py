from __future__ import annotations

import pytest

pytestmark = pytest.mark.skip(
    reason="Legacy SmartQueryService removed; tests TODO — see specs/020-legacy-pipeline-tests-migration/spec.md"
)

# Import preserved as comment for the future migration:
# from app.presentation.http.controllers.query.query_router import _cache_key
_cache_key = lambda *a, **kw: None  # placeholder — tests below are skipped



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
