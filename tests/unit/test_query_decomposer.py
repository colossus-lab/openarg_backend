"""Tests for query decomposer."""
from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from app.infrastructure.adapters.search.query_decomposer import QueryDecomposer


@dataclass
class FakeLLMResponse:
    content: str
    tokens_used: int = 50
    model: str = "test"


@pytest.fixture
def llm_mock():
    return AsyncMock()


@pytest.fixture
def decomposer(llm_mock):
    return QueryDecomposer(llm=llm_mock)


class TestDecomposerSimple:
    async def test_short_query_skipped(self, decomposer):
        result = await decomposer.decompose("dólar blue")
        assert result == ["dólar blue"]

    async def test_five_words_skipped(self, decomposer):
        result = await decomposer.decompose("a b c d e")
        assert result == ["a b c d e"]


class TestDecomposerComplex:
    async def test_single_topic_returns_one(self, decomposer, llm_mock):
        llm_mock.chat.return_value = FakeLLMResponse(
            content='["inflación del último mes en Argentina"]'
        )
        result = await decomposer.decompose("cómo viene la inflación del último mes en Argentina")
        assert len(result) == 1

    async def test_two_topics_returns_two(self, decomposer, llm_mock):
        llm_mock.chat.return_value = FakeLLMResponse(
            content='["inflación en 2025", "dólar blue en 2025"]'
        )
        result = await decomposer.decompose("comparar la inflación con el dólar blue en 2025")
        assert len(result) == 2
        assert "inflación" in result[0]
        assert "dólar" in result[1]

    async def test_three_topics_returns_three(self, decomposer, llm_mock):
        llm_mock.chat.return_value = FakeLLMResponse(
            content=(
                '["PBI del último año",'
                ' "exportaciones del último año",'
                ' "importaciones del último año"]'
            )
        )
        result = await decomposer.decompose("PBI, exportaciones e importaciones del último año")
        assert len(result) == 3

    async def test_capped_at_three(self, decomposer, llm_mock):
        llm_mock.chat.return_value = FakeLLMResponse(
            content='["a", "b", "c", "d", "e"]'
        )
        result = await decomposer.decompose("a very complex multi-part question with many topics")
        assert len(result) == 3


class TestDecomposerFallback:
    async def test_llm_failure_returns_original(self, decomposer, llm_mock):
        llm_mock.chat.side_effect = Exception("LLM error")
        result = await decomposer.decompose("comparar inflación con dólar blue en 2025")
        assert result == ["comparar inflación con dólar blue en 2025"]

    async def test_invalid_json_returns_original(self, decomposer, llm_mock):
        llm_mock.chat.return_value = FakeLLMResponse(content="not valid json")
        result = await decomposer.decompose("comparar inflación con dólar blue en 2025")
        assert result == ["comparar inflación con dólar blue en 2025"]

    async def test_empty_array_returns_original(self, decomposer, llm_mock):
        llm_mock.chat.return_value = FakeLLMResponse(content="[]")
        result = await decomposer.decompose("comparar inflación con dólar blue en 2025")
        assert result == ["comparar inflación con dólar blue en 2025"]

    async def test_json_in_code_block(self, decomposer, llm_mock):
        llm_mock.chat.return_value = FakeLLMResponse(
            content='```json\n["inflación 2025", "dólar 2025"]\n```'
        )
        result = await decomposer.decompose("comparar inflación con dólar blue en el año 2025")
        assert len(result) == 2
