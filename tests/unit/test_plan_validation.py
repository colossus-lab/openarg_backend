"""Tests for _validate_plan in query_planner."""

from __future__ import annotations

from app.infrastructure.adapters.connectors.query_planner import _validate_plan


class TestValidatePlan:
    """Unit tests for plan schema validation."""

    def test_valid_plan_passes_through(self):
        plan = {
            "steps": [
                {"id": "s1", "action": "search_ckan", "params": {"query": "test"}},
                {"id": "s2", "action": "analyze", "params": {}, "dependsOn": ["s1"]},
            ]
        }
        result = _validate_plan(plan)
        assert len(result["steps"]) == 2
        assert result["steps"][0]["id"] == "s1"
        assert result["steps"][1]["dependsOn"] == ["s1"]

    def test_invalid_action_is_removed(self):
        plan = {
            "steps": [
                {"id": "s1", "action": "hack_the_planet", "params": {}},
                {"id": "s2", "action": "query_series", "params": {"query": "ipc"}},
            ]
        }
        result = _validate_plan(plan)
        assert len(result["steps"]) == 1
        assert result["steps"][0]["action"] == "query_series"

    def test_missing_id_gets_generated(self):
        plan = {
            "steps": [
                {"action": "search_ckan", "params": {"query": "x"}},
            ]
        }
        result = _validate_plan(plan)
        assert len(result["steps"]) == 1
        assert result["steps"][0]["id"] == "step_1"

    def test_non_dict_params_reset_to_empty(self):
        plan = {
            "steps": [
                {"id": "s1", "action": "query_bcra", "params": "bad"},
            ]
        }
        result = _validate_plan(plan)
        assert result["steps"][0]["params"] == {}

    def test_missing_params_reset_to_empty(self):
        plan = {
            "steps": [
                {"id": "s1", "action": "query_bcra"},
            ]
        }
        result = _validate_plan(plan)
        assert result["steps"][0]["params"] == {}

    def test_invalid_depends_on_refs_removed(self):
        plan = {
            "steps": [
                {"id": "s1", "action": "search_ckan", "params": {}},
                {"id": "s2", "action": "analyze", "params": {}, "dependsOn": ["s1", "nonexistent"]},
            ]
        }
        result = _validate_plan(plan)
        assert result["steps"][1]["dependsOn"] == ["s1"]

    def test_forward_reference_in_depends_on_removed(self):
        """depends_on should only reference *earlier* steps."""
        plan = {
            "steps": [
                {"id": "s1", "action": "search_ckan", "params": {}, "dependsOn": ["s2"]},
                {"id": "s2", "action": "analyze", "params": {}},
            ]
        }
        result = _validate_plan(plan)
        assert result["steps"][0]["dependsOn"] == []

    def test_non_list_steps_resets(self):
        plan = {"steps": "not a list"}
        result = _validate_plan(plan)
        assert result["steps"] == []

    def test_non_dict_step_skipped(self):
        plan = {
            "steps": [
                "just a string",
                {"id": "s1", "action": "search_ckan", "params": {}},
            ]
        }
        result = _validate_plan(plan)
        assert len(result["steps"]) == 1

    def test_all_steps_invalid_returns_empty(self):
        plan = {
            "steps": [
                {"id": "s1", "action": "invalid_1", "params": {}},
                {"id": "s2", "action": "invalid_2", "params": {}},
            ]
        }
        result = _validate_plan(plan)
        assert result["steps"] == []

    def test_empty_plan_passes(self):
        plan = {"steps": []}
        result = _validate_plan(plan)
        assert result["steps"] == []

    def test_missing_steps_key(self):
        plan = {"query": "test"}
        result = _validate_plan(plan)
        assert result["steps"] == []

    def test_all_valid_actions_accepted(self):
        from app.infrastructure.adapters.connectors.query_planner import _VALID_ACTIONS

        for action in _VALID_ACTIONS:
            plan = {"steps": [{"id": "s1", "action": action, "params": {}}]}
            result = _validate_plan(plan)
            assert len(result["steps"]) == 1, f"Action '{action}' was unexpectedly rejected"

    def test_depends_on_key_variant(self):
        """Some plans might use depends_on instead of dependsOn."""
        plan = {
            "steps": [
                {"id": "s1", "action": "search_ckan", "params": {}},
                {"id": "s2", "action": "analyze", "params": {}, "depends_on": ["s1"]},
            ]
        }
        result = _validate_plan(plan)
        assert result["steps"][1]["depends_on"] == ["s1"]

    def test_non_string_depends_on_entries_removed(self):
        plan = {
            "steps": [
                {"id": "s1", "action": "search_ckan", "params": {}},
                {"id": "s2", "action": "analyze", "params": {}, "dependsOn": ["s1", 42, None]},
            ]
        }
        result = _validate_plan(plan)
        assert result["steps"][1]["dependsOn"] == ["s1"]

    def test_numeric_id_gets_replaced(self):
        plan = {
            "steps": [
                {"id": 123, "action": "search_ckan", "params": {}},
            ]
        }
        result = _validate_plan(plan)
        assert isinstance(result["steps"][0]["id"], str)

    def test_non_list_depends_on_reset(self):
        plan = {
            "steps": [
                {"id": "s1", "action": "search_ckan", "params": {}, "dependsOn": "s0"},
            ]
        }
        result = _validate_plan(plan)
        assert result["steps"][0]["dependsOn"] == []
