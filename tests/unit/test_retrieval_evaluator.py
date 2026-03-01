"""Tests for CRAG retrieval evaluator."""
from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.connectors.data_result import (
    DataResult,
    ExecutionPlan,
    PlanStep,
)
from app.domain.ports.search.retrieval_evaluator import RetrievalQuality
from app.infrastructure.adapters.search.retrieval_evaluator import (
    RetrievalEvaluator,
)


@dataclass
class FakeLLMResponse:
    content: str
    tokens_used: int = 50
    model: str = "test"


@pytest.fixture
def llm_mock():
    return AsyncMock()


@pytest.fixture
def evaluator(llm_mock):
    return RetrievalEvaluator(llm=llm_mock)


@pytest.fixture
def sample_plan():
    return ExecutionPlan(
        query="test query",
        intent="consulta_datos",
        steps=[PlanStep(
            id="s1", action="query_series", description="test",
        )],
    )


class TestEvaluateCorrect:
    async def test_correct_verdict(self, evaluator, llm_mock, sample_plan):
        llm_mock.chat.return_value = FakeLLMResponse(
            content=(
                '{"quality": "correct", "confidence": 0.9,'
                ' "reasoning": "Data is relevant",'
                ' "suggested_actions": ["none"]}'
            )
        )
        results = [
            DataResult(
                source="series", portal_name="Series", portal_url="",
                dataset_title="Inflación", format="time_series",
                records=[{"fecha": "2025-01", "valor": 2.5}],
                metadata={},
            )
        ]
        verdict = await evaluator.evaluate(
            "inflación", results, sample_plan,
        )
        assert verdict.quality == RetrievalQuality.CORRECT
        assert verdict.confidence == 0.9


class TestEvaluateIncorrect:
    async def test_incorrect_verdict(self, evaluator, llm_mock, sample_plan):
        llm_mock.chat.return_value = FakeLLMResponse(
            content=(
                '{"quality": "incorrect", "confidence": 0.85,'
                ' "reasoning": "No relevant data",'
                ' "suggested_actions": ["retry_with_broader_query"]}'
            )
        )
        verdict = await evaluator.evaluate("inflación", [], sample_plan)
        assert verdict.quality == RetrievalQuality.INCORRECT
        assert verdict.confidence == 0.85
        assert "retry_with_broader_query" in verdict.suggested_actions


class TestEvaluateAmbiguous:
    async def test_ambiguous_verdict(self, evaluator, llm_mock, sample_plan):
        llm_mock.chat.return_value = FakeLLMResponse(
            content=(
                '{"quality": "ambiguous", "confidence": 0.6,'
                ' "reasoning": "Partial match",'
                ' "suggested_actions": ["try_alternative_connector"]}'
            )
        )
        verdict = await evaluator.evaluate("test", [], sample_plan)
        assert verdict.quality == RetrievalQuality.AMBIGUOUS
        assert verdict.confidence == 0.6


class TestEvaluateFallback:
    async def test_llm_failure_returns_correct(
        self, evaluator, llm_mock, sample_plan,
    ):
        llm_mock.chat.side_effect = Exception("LLM error")
        verdict = await evaluator.evaluate("test", [], sample_plan)
        assert verdict.quality == RetrievalQuality.CORRECT
        assert verdict.confidence == 0.5


class TestSummarizeResults:
    def test_empty_results(self):
        summary = RetrievalEvaluator._summarize_results([])
        assert "vacía" in summary.lower()

    def test_with_data_results(self):
        results = [
            DataResult(
                source="series", portal_name="Series de Tiempo",
                portal_url="", dataset_title="IPC",
                format="time_series",
                records=[{"fecha": "2025-01", "valor": 2.5}],
                metadata={},
            )
        ]
        summary = RetrievalEvaluator._summarize_results(results)
        assert "IPC" in summary
        assert "series" in summary.lower()

    def test_with_empty_records(self):
        results = [
            DataResult(
                source="ckan", portal_name="Portal", portal_url="",
                dataset_title="Empty", format="json",
                records=[], metadata={},
            )
        ]
        summary = RetrievalEvaluator._summarize_results(results)
        assert "Empty" in summary
        assert "registros: 0" in summary


class TestParseVerdict:
    def test_valid_json(self):
        verdict = RetrievalEvaluator._parse_verdict(
            '{"quality": "correct", "confidence": 0.95,'
            ' "reasoning": "good", "suggested_actions": ["none"]}'
        )
        assert verdict.quality == RetrievalQuality.CORRECT
        assert verdict.confidence == 0.95

    def test_json_in_code_block(self):
        verdict = RetrievalEvaluator._parse_verdict(
            '```json\n{"quality": "incorrect",'
            ' "confidence": 0.8, "reasoning": "bad"}\n```'
        )
        assert verdict.quality == RetrievalQuality.INCORRECT

    def test_invalid_json_returns_correct(self):
        verdict = RetrievalEvaluator._parse_verdict("not json at all")
        assert verdict.quality == RetrievalQuality.CORRECT
        assert verdict.confidence == 0.5

    def test_confidence_clamped(self):
        verdict = RetrievalEvaluator._parse_verdict(
            '{"quality": "correct", "confidence": 1.5}'
        )
        assert verdict.confidence == 1.0

    def test_suggested_actions_as_string(self):
        verdict = RetrievalEvaluator._parse_verdict(
            '{"quality": "correct", "confidence": 0.5,'
            ' "suggested_actions": "retry_with_broader_query"}'
        )
        assert verdict.suggested_actions == ["retry_with_broader_query"]
