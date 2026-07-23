"""Tests for LLM-based re-ranker."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from app.application.pipeline.connectors.planner_candidates import PlannerCandidate
from app.domain.ports.search.vector_search import SearchResult
from app.infrastructure.adapters.search.reranker import (
    LLMReranker,
    PlannerCandidateReranker,
)


@dataclass
class FakeLLMResponse:
    content: str
    tokens_used: int = 20
    model: str = "test"


@pytest.fixture
def llm_mock():
    return AsyncMock()


@pytest.fixture
def reranker(llm_mock):
    return LLMReranker(llm=llm_mock)


@pytest.fixture
def planner_reranker(llm_mock):
    return PlannerCandidateReranker(llm=llm_mock)


@pytest.fixture
def sample_results():
    return [
        SearchResult(
            dataset_id="1",
            title="Inflación IPC",
            description="IPC mensual",
            portal="nacional",
            download_url="",
            columns="",
            score=0.8,
        ),
        SearchResult(
            dataset_id="2",
            title="PBI Argentina",
            description="PBI trimestral",
            portal="nacional",
            download_url="",
            columns="",
            score=0.7,
        ),
        SearchResult(
            dataset_id="3",
            title="Dólar Blue",
            description="Cotización diaria",
            portal="nacional",
            download_url="",
            columns="",
            score=0.6,
        ),
    ]


@pytest.fixture
def sample_candidates():
    return [
        PlannerCandidate(
            candidate_id="table:cache_a",
            kind="legacy_table",
            layer="cache_legacy",
            title="IPC",
            description="IPC mensual",
            portal="nacional",
            resource_id=None,
            table_name="cache_a",
            queryability="direct_sql",
            base_score=0.8,
            source="table_catalog",
        ),
        PlannerCandidate(
            candidate_id="table:cache_b",
            kind="legacy_table",
            layer="cache_legacy",
            title="PBI",
            description="PBI trimestral",
            portal="nacional",
            resource_id=None,
            table_name="cache_b",
            queryability="direct_sql",
            base_score=0.7,
            source="table_catalog",
        ),
        PlannerCandidate(
            candidate_id="table:cache_c",
            kind="legacy_table",
            layer="cache_legacy",
            title="Dólar Blue",
            description="Cotización diaria",
            portal="nacional",
            resource_id=None,
            table_name="cache_c",
            queryability="direct_sql",
            base_score=0.6,
            source="table_catalog",
        ),
    ]


class TestRerank:
    async def test_rerank_reorders(self, reranker, llm_mock, sample_results):
        llm_mock.chat.return_value = FakeLLMResponse(content="[2, 0, 1]")
        result = await reranker.rerank("dólar blue", sample_results)
        assert result[0].dataset_id == "3"  # dólar blue first
        assert result[1].dataset_id == "1"
        assert result[2].dataset_id == "2"

    async def test_rerank_with_top_k(self, reranker, llm_mock, sample_results):
        llm_mock.chat.return_value = FakeLLMResponse(content="[2, 0, 1]")
        result = await reranker.rerank("dólar", sample_results, top_k=2)
        assert len(result) == 2

    async def test_rerank_single_result(self, reranker):
        results = [
            SearchResult(
                dataset_id="1",
                title="Test",
                description="",
                portal="",
                download_url="",
                columns="",
                score=0.5,
            )
        ]
        result = await reranker.rerank("test", results)
        assert len(result) == 1
        assert result[0].dataset_id == "1"

    async def test_rerank_empty_results(self, reranker):
        result = await reranker.rerank("test", [])
        assert result == []


class TestRerankFallback:
    async def test_llm_failure_returns_original(self, reranker, llm_mock, sample_results):
        llm_mock.chat.side_effect = Exception("LLM error")
        result = await reranker.rerank("test", sample_results)
        assert len(result) == 3
        assert result[0].dataset_id == "1"  # original order preserved

    async def test_invalid_json_returns_original(self, reranker, llm_mock, sample_results):
        llm_mock.chat.return_value = FakeLLMResponse(content="not json")
        result = await reranker.rerank("test", sample_results)
        assert len(result) == 3

    async def test_out_of_range_indices_handled(self, reranker, llm_mock, sample_results):
        llm_mock.chat.return_value = FakeLLMResponse(content="[99, 0, 1]")
        result = await reranker.rerank("test", sample_results)
        # 99 is skipped, 0 and 1 are valid, 2 is appended
        assert len(result) == 3


class TestPlannerCandidateRerank:
    async def test_rerank_reorders_candidates(self, planner_reranker, llm_mock, sample_candidates):
        llm_mock.chat.return_value = FakeLLMResponse(content="[2, 0, 1]")
        result = await planner_reranker.rerank("dólar blue", sample_candidates)
        assert result[0].table_name == "cache_c"
        assert result[1].table_name == "cache_a"
        assert result[2].table_name == "cache_b"

    async def test_llm_failure_returns_original_candidates(
        self, planner_reranker, llm_mock, sample_candidates
    ):
        llm_mock.chat.side_effect = Exception("LLM error")
        result = await planner_reranker.rerank("test", sample_candidates)
        assert [c.table_name for c in result] == ["cache_a", "cache_b", "cache_c"]

    async def test_rerank_prompt_includes_operational_fields(
        self, planner_reranker, llm_mock, sample_candidates
    ):
        llm_mock.chat.return_value = FakeLLMResponse(content="[0, 1, 2]")

        await planner_reranker.rerank("inflación", sample_candidates)

        call = llm_mock.chat.await_args
        assert call is not None
        prompt = call.kwargs["messages"][0].content
        assert "kind=legacy_table" in prompt
        assert "layer=cache_legacy" in prompt
        assert "queryability=direct_sql" in prompt
        assert "Candidatos:" in prompt
