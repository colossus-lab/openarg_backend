"""Tests for LLM-based re-ranker."""
from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from app.domain.ports.search.vector_search import SearchResult
from app.infrastructure.adapters.search.reranker import LLMReranker


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
def sample_results():
    return [
        SearchResult(
            dataset_id="1", title="Inflación IPC",
            description="IPC mensual", portal="nacional",
            download_url="", columns="", score=0.8,
        ),
        SearchResult(
            dataset_id="2", title="PBI Argentina",
            description="PBI trimestral", portal="nacional",
            download_url="", columns="", score=0.7,
        ),
        SearchResult(
            dataset_id="3", title="Dólar Blue",
            description="Cotización diaria", portal="nacional",
            download_url="", columns="", score=0.6,
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
        results = [SearchResult(
            dataset_id="1", title="Test", description="",
            portal="", download_url="", columns="", score=0.5,
        )]
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
