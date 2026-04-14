"""Regression tests for ``finalize_node`` return shape.

Pins the FIX-008 fix: the LangGraph ``updates`` stream forwards only what
the node returns, so any field the node reads from state and forgets to
re-emit is silently dropped from the final ``complete`` event the browser
sees. Before this fix, ``chart_data`` lived in state but was missing
from ``finalize_node.return`` — so fresh (non-cached) answers reached
the browser with ``chart_data: null`` and charts never rendered. The
test asserts all user-facing fields are present in the return dict so
the regression cannot happen again silently.

Spec: ``specs/001-query-pipeline/001e-finalization/spec.md`` FR-036a.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.nodes.finalize import finalize_node


class _NoopMetrics:
    def record_tokens_used(self, _tokens: int) -> None:  # pragma: no cover - trivial
        return None


class _StubDeps:
    """Minimal dependency bundle to let finalize_node run without I/O."""

    def __init__(self) -> None:
        self.metrics = _NoopMetrics()
        self.cache = None
        self.embedding = None
        self.semantic_cache = None
        self.llm = None


@pytest.fixture()
def _deps(monkeypatch: pytest.MonkeyPatch) -> _StubDeps:
    deps = _StubDeps()
    monkeypatch.setattr(nodes_pkg, "get_deps", lambda: deps, raising=False)

    # Stub out all side-effects so finalize_node only exercises its
    # return-dict assembly logic (the thing we care about for FIX-008).
    async def _noop_write_cache(*_args: Any, **_kwargs: Any) -> None:
        return None

    def _noop_audit(**_kwargs: Any) -> None:
        return None

    def _noop_spawn(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(
        "app.application.pipeline.nodes.finalize.write_cache",
        _noop_write_cache,
    )
    monkeypatch.setattr(
        "app.application.pipeline.nodes.finalize.audit_query",
        _noop_audit,
    )
    monkeypatch.setattr(
        "app.application.pipeline.nodes.finalize.spawn_background",
        _noop_spawn,
    )
    return deps


def _make_state(**overrides: Any) -> dict[str, Any]:
    """Build an OpenArgState-ish dict with the fields finalize_node reads."""
    base: dict[str, Any] = {
        "question": "test question",
        "user_id": "user@example.com",
        "plan": SimpleNamespace(intent="test_intent"),
        "clean_answer": "The answer.",
        "chart_data": [
            {
                "type": "line_chart",
                "title": "Inflación mensual",
                "data": [
                    {"fecha": "2025-01", "valor": 2.5},
                    {"fecha": "2025-02", "valor": 3.1},
                ],
                "xKey": "fecha",
                "yKeys": ["valor"],
            }
        ],
        "map_data": None,
        "tokens_used": 42,
        "last_embedding": None,
        "step_warnings": [],
        "data_results": [],
        "plan_intent": "test_intent",
        "conversation_id": "",
        "memory": None,
        "_start_time": 0.0,
        "confidence": 0.87,
        "citations": [{"source": "BCRA", "note": "monthly series"}],
    }
    base.update(overrides)
    return base


@pytest.mark.asyncio
async def test_finalize_returns_chart_data(_deps: _StubDeps) -> None:
    """FIX-008: chart_data populated in state MUST be in the return dict.

    Before the fix this key was missing from the return, so the
    ``updates`` stream forwarded ``None`` to the browser even when the
    analyst had built a valid chart.
    """
    state = _make_state()

    result = await finalize_node(state)  # type: ignore[arg-type]

    assert "chart_data" in result, (
        "chart_data must be in finalize_node's return dict — otherwise "
        "LangGraph's updates stream silently drops it and the complete "
        "event reaches the browser without charts. See FIX-008."
    )
    assert result["chart_data"] == state["chart_data"]


@pytest.mark.asyncio
async def test_finalize_returns_all_user_facing_fields(_deps: _StubDeps) -> None:
    """FR-036a: the return dict must carry every field the `complete`
    event exposes to the frontend.

    This pins the full set so any future change to finalize_node has to
    consciously add/remove keys rather than silently drop one.
    """
    state = _make_state()

    result = await finalize_node(state)  # type: ignore[arg-type]

    required_keys = {
        "clean_answer",
        "sources",
        "documents",
        "chart_data",
        "map_data",
        "confidence",
        "citations",
        "warnings",
        "duration_ms",
    }
    missing = required_keys - set(result.keys())
    assert not missing, f"finalize_node dropped keys from its return: {missing}"

    assert result["clean_answer"] == "The answer."
    assert result["confidence"] == 0.87
    assert result["citations"] == [{"source": "BCRA", "note": "monthly series"}]


@pytest.mark.asyncio
async def test_finalize_propagates_none_chart_data(_deps: _StubDeps) -> None:
    """When the analyst produced no chart, chart_data must still appear
    in the return dict (as None) so the frontend can distinguish "no
    chart" from "key missing" — the latter was the old silent-drop bug.
    """
    state = _make_state(chart_data=None)

    result = await finalize_node(state)  # type: ignore[arg-type]

    assert "chart_data" in result
    assert result["chart_data"] is None


@pytest.mark.asyncio
async def test_finalize_uses_default_confidence_and_citations(
    _deps: _StubDeps,
) -> None:
    """When upstream nodes did not populate confidence/citations, finalize
    still emits sensible defaults (1.0 and []) in the return dict."""
    state = _make_state()
    del state["confidence"]
    del state["citations"]

    result = await finalize_node(state)  # type: ignore[arg-type]

    assert result["confidence"] == 1.0
    assert result["citations"] == []


@pytest.mark.asyncio
async def test_finalize_writes_confidence_citations_and_warnings_to_cache(
    _deps: _StubDeps,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def _capture_write_cache(
        question: str,
        result: dict[str, Any],
        intent: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        captured["question"] = question
        captured["intent"] = intent
        captured["result"] = result

    monkeypatch.setattr(
        "app.application.pipeline.nodes.finalize.write_cache",
        _capture_write_cache,
    )

    state = _make_state(step_warnings=["warning-a"])

    await finalize_node(state)  # type: ignore[arg-type]

    assert captured["question"] == "test question"
    assert captured["intent"] == "test_intent"
    assert captured["result"]["confidence"] == 0.87
    assert captured["result"]["citations"] == [{"source": "BCRA", "note": "monthly series"}]
    assert captured["result"]["warnings"] == ["warning-a"]
