"""Unit tests for the NL2SQL subgraph (FIX-004).

Covers the 10 cases documented in
``specs/010-sandbox-sql/010b-nl2sql/plan.md`` §9. Each test constructs a
minimal initial state with mocked ``llm`` / ``sandbox`` / ``embedding``
and invokes the compiled subgraph directly via ``ainvoke``. No real
database, no real LLM.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from app.application.pipeline.subgraphs.nl2sql import (
    build_nl2sql_subgraph,
    get_compiled_nl2sql_subgraph,
)

# ── Fakes / fixtures ─────────────────────────────────────────


@dataclass
class FakeSandboxResult:
    rows: list[dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    columns: list[str] = field(default_factory=list)
    truncated: bool = False
    error: str | None = None


@dataclass
class FakeTable:
    table_name: str
    row_count: int = 100
    columns: list[str] = field(default_factory=lambda: ["col_a", "col_b"])


@dataclass
class FakeLLMResponse:
    content: str


class FakeLLM:
    """Minimal ILLMProvider stub.

    Accepts a list of pre-scripted responses to return in order. Each
    call pops the next response. Raises if exhausted to surface
    over-calling bugs.
    """

    def __init__(self, responses: list[str]):
        self._responses = list(responses)
        self.calls: list[Any] = []

    async def chat(self, *, messages: Any, temperature: float, max_tokens: int) -> FakeLLMResponse:
        self.calls.append(messages)
        if not self._responses:
            raise AssertionError("FakeLLM ran out of scripted responses")
        return FakeLLMResponse(content=self._responses.pop(0))


class FakeSandbox:
    """Minimal ISQLSandbox stub.

    Accepts a list of pre-scripted results. Each ``execute_readonly``
    call pops the next one.
    """

    def __init__(self, results: list[FakeSandboxResult]):
        self._results = list(results)
        self.calls: list[str] = []

    async def execute_readonly(
        self, sql: str, timeout_seconds: int | None = None
    ) -> FakeSandboxResult:
        self.calls.append(sql)
        if not self._results:
            raise AssertionError("FakeSandbox ran out of scripted results")
        return self._results.pop(0)


def _base_state(llm: FakeLLM, sandbox: FakeSandbox, **overrides: Any) -> dict[str, Any]:
    state: dict[str, Any] = {
        "nl_query": "how many rows are in the data",
        "tables": [FakeTable(table_name="cache_test")],
        "tables_context": "Table: cache_test\n  Columns: col_a, col_b",
        "table_notes": "",
        "catalog_entries": {},
        "table_descriptions": ["cache_test: Test dataset"],
        "few_shot_block": "",
        "llm": llm,
        "sandbox": sandbox,
        "embedding": None,
        "semantic_cache": None,
        "max_attempts": 2,
    }
    state.update(overrides)
    return state


# Stub load_prompt so we don't require real prompt files in unit tests.
@pytest.fixture(autouse=True)
def _stub_prompts(monkeypatch):
    monkeypatch.setattr(
        "app.application.pipeline.subgraphs.nl2sql.load_prompt",
        lambda *args, **kwargs: "stub prompt",
    )
    # Make INDEC_PATTERN.search a no-op unless a test explicitly matches it.
    monkeypatch.setattr(
        "app.application.pipeline.subgraphs.nl2sql._compute_indec_match",
        lambda q: False,
    )


# ── Tests ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_happy_path_single_generation():
    """Generated SQL executes clean on the first try."""
    llm = FakeLLM(["SELECT 1"])
    sandbox = FakeSandbox([FakeSandboxResult(rows=[{"col_a": 1}], row_count=1, columns=["col_a"])])

    subgraph = build_nl2sql_subgraph()
    final = await subgraph.ainvoke(_base_state(llm, sandbox))

    data_results = final["data_results"]
    assert len(data_results) == 1
    dr = data_results[0]
    assert dr.source == "sandbox:nl2sql"
    assert len(dr.records) == 1
    assert dr.metadata.get("used_fallback") is not True
    assert len(llm.calls) == 1  # exactly one generate, zero fixes


@pytest.mark.asyncio
async def test_self_correction_one_retry():
    """First execute errors, second succeeds after fix_sql."""
    llm = FakeLLM(["SELECT x", "SELECT 1"])
    sandbox = FakeSandbox(
        [
            FakeSandboxResult(error="column x does not exist"),
            FakeSandboxResult(rows=[{"col_a": 1}], row_count=1, columns=["col_a"]),
        ]
    )

    subgraph = build_nl2sql_subgraph()
    final = await subgraph.ainvoke(_base_state(llm, sandbox))

    assert len(final["data_results"]) == 1
    assert final["data_results"][0].records
    assert final["data_results"][0].metadata.get("used_fallback") is not True
    assert len(llm.calls) == 2  # generate + 1 fix
    assert len(sandbox.calls) == 2  # first execute + retry execute


@pytest.mark.asyncio
async def test_retries_exhausted_triggers_last_resort_success(monkeypatch):
    """Both retries error, last_resort SELECT * LIMIT 10 succeeds."""

    # Stub safe_table_query to return a deterministic fallback SQL.
    monkeypatch.setattr(
        "app.infrastructure.adapters.sandbox.table_validation.safe_table_query",
        lambda name, tmpl: f'SELECT * FROM "{name}" LIMIT 10',
    )

    # 3 LLM calls: generate + 2 fixes. Sandbox gets 3 failing executes then
    # 1 successful last-resort execute.
    llm = FakeLLM(["SELECT x", "SELECT y", "SELECT z"])
    sandbox = FakeSandbox(
        [
            FakeSandboxResult(error="err 1"),
            FakeSandboxResult(error="err 2"),
            FakeSandboxResult(error="err 3"),
            FakeSandboxResult(rows=[{"col_a": 1}], row_count=1, columns=["col_a"]),
        ]
    )

    subgraph = build_nl2sql_subgraph()
    final = await subgraph.ainvoke(_base_state(llm, sandbox))

    dr = final["data_results"][0]
    assert dr.records
    assert dr.metadata.get("used_fallback") is True


@pytest.mark.asyncio
async def test_last_resort_also_fails_emits_error_data_result(monkeypatch):
    """Every execute errors including last_resort — emit a clean error DataResult."""
    monkeypatch.setattr(
        "app.infrastructure.adapters.sandbox.table_validation.safe_table_query",
        lambda name, tmpl: f'SELECT * FROM "{name}" LIMIT 10',
    )

    llm = FakeLLM(["SELECT x", "SELECT y", "SELECT z"])
    sandbox = FakeSandbox(
        [
            FakeSandboxResult(error="err 1"),
            FakeSandboxResult(error="err 2"),
            FakeSandboxResult(error="err 3"),
            FakeSandboxResult(error="even fallback failed"),
        ]
    )

    subgraph = build_nl2sql_subgraph()
    final = await subgraph.ainvoke(_base_state(llm, sandbox))

    dr = final["data_results"][0]
    assert dr.records == []
    assert dr.metadata.get("error") == "even fallback failed"


@pytest.mark.asyncio
async def test_empty_indec_triggers_fallback(monkeypatch):
    """Sandbox returns zero rows + INDEC pattern match → live INDEC fallback used."""

    monkeypatch.setattr(
        "app.application.pipeline.subgraphs.nl2sql._compute_indec_match",
        lambda q: True,
    )

    from app.domain.entities.connectors.data_result import DataResult

    live_dr = DataResult(
        source="indec:live",
        portal_name="INDEC",
        portal_url="https://www.indec.gob.ar",
        dataset_title="IPC live",
        format="json",
        records=[{"month": "2026-03", "ipc": 3.2}],
        metadata={},
    )

    async def _fake_indec_fallback(nl_query: str) -> list[DataResult]:
        return [live_dr]

    monkeypatch.setattr(
        "app.application.pipeline.connectors.sandbox.indec_live_fallback",
        _fake_indec_fallback,
    )

    llm = FakeLLM(["SELECT * FROM nothing"])
    sandbox = FakeSandbox([FakeSandboxResult(rows=[], row_count=0, columns=["col_a"])])

    subgraph = build_nl2sql_subgraph()
    final = await subgraph.ainvoke(_base_state(llm, sandbox))

    data_results = final["data_results"]
    assert len(data_results) == 1
    assert data_results[0].source == "indec:live"


@pytest.mark.asyncio
async def test_empty_non_indec_skips_fallback():
    """Empty rows + no INDEC match → normal empty formatting, no live call."""
    llm = FakeLLM(["SELECT * FROM nothing"])
    sandbox = FakeSandbox([FakeSandboxResult(rows=[], row_count=0, columns=["col_a"])])

    subgraph = build_nl2sql_subgraph()
    final = await subgraph.ainvoke(_base_state(llm, sandbox))

    dr = final["data_results"][0]
    assert dr.source == "sandbox:nl2sql"
    assert dr.records == []
    # No INDEC live call should have happened — we can't assert a negative
    # directly, but reaching this branch without patching indec_live_fallback
    # means _route_after_format never picked the indec_fallback branch.


@pytest.mark.asyncio
async def test_prod_env_redacts_generated_sql(monkeypatch):
    """APP_ENV=prod must drop generated_sql from metadata (SEC-03)."""
    monkeypatch.setenv("APP_ENV", "prod")

    llm = FakeLLM(["SELECT secret_thing FROM users"])
    sandbox = FakeSandbox([FakeSandboxResult(rows=[{"x": 1}], row_count=1, columns=["x"])])

    subgraph = build_nl2sql_subgraph()
    final = await subgraph.ainvoke(_base_state(llm, sandbox))

    meta = final["data_results"][0].metadata
    assert "generated_sql" not in meta


@pytest.mark.asyncio
async def test_local_env_includes_generated_sql(monkeypatch):
    """APP_ENV=local must keep generated_sql in metadata for debugging."""
    monkeypatch.setenv("APP_ENV", "local")

    llm = FakeLLM(["SELECT 1"])
    sandbox = FakeSandbox([FakeSandboxResult(rows=[{"x": 1}], row_count=1, columns=["x"])])

    subgraph = build_nl2sql_subgraph()
    final = await subgraph.ainvoke(_base_state(llm, sandbox))

    meta = final["data_results"][0].metadata
    assert meta.get("generated_sql") == "SELECT 1"


@pytest.mark.asyncio
async def test_missing_embedding_reaches_save_success_without_crash(caplog):
    """embedding=None still runs save_success; any error is surfaced via logs, not raised."""
    llm = FakeLLM(["SELECT 1"])
    sandbox = FakeSandbox([FakeSandboxResult(rows=[{"x": 1}], row_count=1, columns=["x"])])

    subgraph = build_nl2sql_subgraph()
    # Base state already has embedding=None, semantic_cache=None.
    final = await subgraph.ainvoke(_base_state(llm, sandbox))

    # The subgraph must still produce a DataResult — it does not crash on
    # missing deps. The save_success side effect may log a warning asynchronously,
    # but the subgraph return path is unaffected.
    assert len(final["data_results"]) == 1
    assert final["data_results"][0].records


@pytest.mark.asyncio
async def test_compile_once_is_idempotent():
    """get_compiled_nl2sql_subgraph returns the same compiled instance."""
    # Reset module-level cache so this test is self-contained.
    import app.application.pipeline.subgraphs.nl2sql as nl2sql_mod

    nl2sql_mod._compiled_subgraph = None

    first = await get_compiled_nl2sql_subgraph()
    second = await get_compiled_nl2sql_subgraph()
    assert first is second
