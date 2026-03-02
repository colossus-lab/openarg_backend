"""Tests for query planner fallback logic."""
from __future__ import annotations

from app.infrastructure.adapters.connectors.query_planner import _fallback_plan


class TestFallbackPlan:
    def test_national_catalog_query(self):
        plan = _fallback_plan("qué datasets hay a nivel nacional")
        assert "nacional" in plan.intent.lower() or "catálogo" in plan.intent.lower()
        assert len(plan.steps) >= 1
        assert plan.steps[0].action == "search_ckan"
        assert plan.steps[0].params.get("portalId") == "nacional"

    def test_list_all_query(self):
        plan = _fallback_plan("listado de todos los datasets")
        assert plan.steps[0].params.get("query") == "*"
        assert plan.steps[0].params.get("rows") == 20

    def test_generic_search(self):
        plan = _fallback_plan("datos de vacunación COVID")
        assert plan.steps[0].action == "search_ckan"
        assert "vacunación COVID" in plan.steps[0].params.get("query", "")

    def test_plan_has_analyze_step(self):
        plan = _fallback_plan("presupuesto 2024")
        actions = [s.action for s in plan.steps]
        assert "analyze" in actions

    def test_plan_preserves_query(self):
        q = "¿cuánto gasta Argentina en educación?"
        plan = _fallback_plan(q)
        assert plan.query == q
