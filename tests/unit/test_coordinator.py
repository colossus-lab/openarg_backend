"""Unit tests for the coordinator node — heuristic decision-making."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from unittest.mock import patch

import pytest

from app.application.pipeline.nodes.coordinator import (
    _MAX_REPLAN_DEPTH,
    _has_useful_data,
    _select_strategy,
    coordinator_node,
)

# ── Helpers ──────────────────────────────────────────────────


@dataclass
class FakeDataResult:
    source: str = "series_tiempo"
    records: list = field(default_factory=list)


def _state(
    data_results=None,
    step_warnings=None,
    replan_count=0,
    start_time=None,
    policy_mode=False,
):
    """Build a minimal OpenArgState dict for testing."""
    return {
        "data_results": data_results or [],
        "step_warnings": step_warnings or [],
        "replan_count": replan_count,
        "_start_time": start_time or time.monotonic(),
        "policy_mode": policy_mode,
        "question": "test question",
    }


# ── _has_useful_data ─────────────────────────────────────────


class TestHasUsefulData:
    def test_empty_results(self) -> None:
        assert _has_useful_data(_state()) is False

    def test_results_with_records(self) -> None:
        results = [FakeDataResult(records=[{"a": 1}])]
        assert _has_useful_data(_state(data_results=results)) is True

    def test_results_without_records(self) -> None:
        results = [FakeDataResult(records=[])]
        assert _has_useful_data(_state(data_results=results)) is False

    def test_pgvector_source_counts_as_data(self) -> None:
        results = [FakeDataResult(source="pgvector:portal", records=[])]
        assert _has_useful_data(_state(data_results=results)) is True

    def test_ckan_source_counts_as_data(self) -> None:
        results = [FakeDataResult(source="ckan:datos_gob_ar", records=[])]
        assert _has_useful_data(_state(data_results=results)) is True


# ── _select_strategy ─────────────────────────────────────────


class TestSelectStrategy:
    def test_first_replan_broadens(self) -> None:
        state = _state(replan_count=0)
        assert _select_strategy(state) == "broaden"

    def test_second_replan_with_failures_switches_source(self) -> None:
        state = _state(
            replan_count=1,
            step_warnings=["Conector 'query_series' falló"],
            data_results=[FakeDataResult(records=[])],
        )
        assert _select_strategy(state) == "switch_source"

    def test_second_replan_with_partial_data_narrows(self) -> None:
        state = _state(
            replan_count=1,
            step_warnings=[],
            data_results=[FakeDataResult(records=[{"a": 1}])],
        )
        assert _select_strategy(state) == "narrow"

    def test_second_replan_no_data_no_warnings_broadens_not(self) -> None:
        """No failures + no data on second attempt → narrow (has_some_data is False but no warnings)."""
        state = _state(replan_count=1, step_warnings=[], data_results=[])
        # No warnings and no data → narrow (the else branch)
        assert _select_strategy(state) == "narrow"


# ── coordinator_node ─────────────────────────────────────────


class TestCoordinatorNode:
    @pytest.fixture(autouse=True)
    def _mock_stream_writer(self):
        with patch("app.application.pipeline.nodes.coordinator.get_stream_writer") as m:
            m.return_value = lambda x: None
            yield

    @pytest.mark.asyncio
    async def test_continue_when_has_data(self) -> None:
        state = _state(data_results=[FakeDataResult(records=[{"val": 42}])])
        result = await coordinator_node(state)
        assert result["coordinator_decision"] == "continue"
        assert result["replan_strategy"] is None

    @pytest.mark.asyncio
    async def test_replan_broaden_first_attempt(self) -> None:
        state = _state(data_results=[], replan_count=0)
        result = await coordinator_node(state)
        assert result["coordinator_decision"] == "replan"
        assert result["replan_strategy"] == "broaden"

    @pytest.mark.asyncio
    async def test_replan_switch_source_second_attempt(self) -> None:
        state = _state(
            data_results=[FakeDataResult(records=[])],
            step_warnings=["Conector 'query_series' falló"],
            replan_count=1,
        )
        result = await coordinator_node(state)
        assert result["coordinator_decision"] == "replan"
        assert result["replan_strategy"] == "switch_source"

    @pytest.mark.asyncio
    async def test_replan_narrow_partial_data(self) -> None:
        state = _state(
            data_results=[FakeDataResult(records=[{"val": 1}])],
            replan_count=1,
        )
        # Has data → continue (not replan)
        result = await coordinator_node(state)
        assert result["coordinator_decision"] == "continue"

    @pytest.mark.asyncio
    async def test_escalate_max_replans(self) -> None:
        state = _state(data_results=[], replan_count=_MAX_REPLAN_DEPTH)
        result = await coordinator_node(state)
        assert result["coordinator_decision"] == "escalate"

    @pytest.mark.asyncio
    async def test_escalate_time_budget(self) -> None:
        state = _state(
            data_results=[],
            replan_count=0,
            start_time=time.monotonic() - 25,  # 25 seconds ago
        )
        result = await coordinator_node(state)
        assert result["coordinator_decision"] == "escalate"

    @pytest.mark.asyncio
    async def test_escalate_many_failures(self) -> None:
        state = _state(
            data_results=[],
            step_warnings=[
                "Conector 'query_series' falló",
                "Conector 'query_bcra' falló",
                "Conector 'query_sandbox' falló",
            ],
            replan_count=0,
        )
        result = await coordinator_node(state)
        assert result["coordinator_decision"] == "escalate"

    @pytest.mark.asyncio
    async def test_escalate_maps_to_finalize(self) -> None:
        """Escalate decision should route to finalize (same as continue)."""
        state = _state(data_results=[], replan_count=_MAX_REPLAN_DEPTH)
        result = await coordinator_node(state)
        # escalate means "finalize with whatever we have"
        assert result["coordinator_decision"] == "escalate"
        # In edges.py, escalate routes to finalize (same as continue)


# ── Strategy hint in replan ──────────────────────────────────


class TestStrategyHint:
    def test_broaden_hint_exists(self) -> None:
        from app.application.pipeline.nodes.replan import _strategy_hint

        hint = _strategy_hint("broaden")
        assert "AMPLIAR" in hint
        assert "search_datasets" in hint

    def test_switch_source_hint_exists(self) -> None:
        from app.application.pipeline.nodes.replan import _strategy_hint

        hint = _strategy_hint("switch_source")
        assert "CAMBIAR FUENTE" in hint
        assert "NUNCA repitas" in hint

    def test_narrow_hint_exists(self) -> None:
        from app.application.pipeline.nodes.replan import _strategy_hint

        hint = _strategy_hint("narrow")
        assert "ENFOCAR" in hint

    def test_unknown_strategy_returns_fallback(self) -> None:
        from app.application.pipeline.nodes.replan import _strategy_hint

        hint = _strategy_hint("unknown")
        assert len(hint) > 0


# ── Resettable reducer ───────────────────────────────────────


class TestResettableReducer:
    def test_normal_add(self) -> None:
        from app.application.pipeline.state import _resettable_add

        result = _resettable_add([1, 2], [3, 4])
        assert result == [1, 2, 3, 4]

    def test_add_empty_does_not_reset(self) -> None:
        from app.application.pipeline.state import _resettable_add

        result = _resettable_add([1, 2], [])
        assert result == [1, 2]

    def test_reset_sentinel_clears_list(self) -> None:
        from app.application.pipeline.state import RESET_LIST, _resettable_add

        result = _resettable_add([1, 2, 3], RESET_LIST)
        assert result == []

    def test_reset_sentinel_is_identity(self) -> None:
        from app.application.pipeline.state import RESET_LIST

        assert RESET_LIST is RESET_LIST
        other_empty = []
        assert other_empty is not RESET_LIST
