"""Tests for the analyst no-data branch.

Covers the 2026-07-23 deflection-loop fixes: the ``no_data_deflection``
flag must reach the pipeline state (so finalize can skip the cache write
and mark analytics), and the no-data prompt must carry the live-marts
block so the LLM only suggests answerable questions.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.nodes.analyst import (
    _build_analysis_prompt,
    _is_no_data_fallback,
    analyst_node,
)


def _result(records: list | None = None, source: str = "sandbox:test") -> SimpleNamespace:
    return SimpleNamespace(
        records=records or [],
        source=source,
        metadata={},
        dataset_title="t",
        portal_name="p",
        portal_url="u",
        format="json",
    )


class TestIsNoDataFallback:
    def test_empty_results(self) -> None:
        assert _is_no_data_fallback([])

    def test_results_without_records(self) -> None:
        assert _is_no_data_fallback([_result()])

    def test_records_present(self) -> None:
        assert not _is_no_data_fallback([_result(records=[{"a": 1}])])

    def test_vector_source_counts_as_data(self) -> None:
        assert not _is_no_data_fallback([_result(source="pgvector:datasets")])


class TestNoDataPrompt:
    def test_prompt_includes_live_marts_block(self) -> None:
        prompt = _build_analysis_prompt(
            "pregunta",
            SimpleNamespace(intent="i"),
            [],
            "",
            [],
            live_marts_block="DATASETS VIVOS (los únicos temas con datos consultables ahora mismo):\n• presupuesto nacional ejecutado",
        )
        assert "DATASETS VIVOS" in prompt
        assert "presupuesto nacional ejecutado" in prompt

    def test_prompt_marks_missing_block(self) -> None:
        prompt = _build_analysis_prompt(
            "pregunta",
            SimpleNamespace(intent="i"),
            [],
            "",
            [],
        )
        assert "(no disponible)" in prompt


class _FakeLLM:
    async def chat_stream(self, **_kwargs: Any):
        yield "Respuesta de prueba."

    async def chat(self, **_kwargs: Any) -> Any:  # pragma: no cover - fallback
        return SimpleNamespace(content="Respuesta de prueba.")


class _Deps(SimpleNamespace):
    pass


@pytest.fixture()
def _deps(monkeypatch: pytest.MonkeyPatch) -> _Deps:
    deps = _Deps(llm=_FakeLLM(), sandbox=None)
    monkeypatch.setattr(nodes_pkg, "get_deps", lambda: deps, raising=False)

    # analyst_node emits stream events; outside a LangGraph runnable
    # context get_stream_writer() raises, so stub it with a no-op writer.
    monkeypatch.setattr(
        "app.application.pipeline.nodes.analyst.get_stream_writer",
        lambda: lambda _event: None,
    )

    async def _no_marts(_sandbox: Any) -> str:
        return ""

    monkeypatch.setattr(
        "app.application.pipeline.nodes.analyst.build_live_marts_block",
        _no_marts,
    )
    return deps


def _state(results: list) -> dict[str, Any]:
    return {
        "question": "pregunta de prueba",
        "plan": SimpleNamespace(intent="i"),
        "data_results": results,
        "memory_ctx_analyst": "",
        "step_warnings": [],
        "replan_count": 0,
    }


@pytest.mark.asyncio
async def test_analyst_node_flags_no_data_deflection(_deps: _Deps) -> None:
    result = await analyst_node(_state([]))  # type: ignore[arg-type]
    assert result["no_data_deflection"] is True


@pytest.mark.asyncio
async def test_analyst_node_clears_flag_when_data_present(_deps: _Deps) -> None:
    """A post-replan pass that finds data must overwrite a stale True."""
    result = await analyst_node(_state([_result(records=[{"anio": 2026, "monto": 1}])]))  # type: ignore[arg-type]
    assert result["no_data_deflection"] is False
