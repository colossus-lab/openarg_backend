"""Unit tests for evaluation framework — no LLM calls required."""

from __future__ import annotations

from tests.evaluation.evaluator import (
    EvalResult,
    aggregate_results,
    check_answer_contains,
    compute_retrieval_precision,
)


class TestComputeRetrievalPrecision:
    def test_perfect_match(self):
        assert compute_retrieval_precision(["Series de Tiempo"], ["Series de Tiempo API"]) == 1.0

    def test_partial_match(self):
        result = compute_retrieval_precision(
            ["Series de Tiempo", "ArgentinaDatos"],
            ["Series de Tiempo API"],
        )
        assert result == 0.5

    def test_no_match(self):
        assert compute_retrieval_precision(["DDJJ"], ["Series de Tiempo"]) == 0.0

    def test_empty_expected(self):
        assert compute_retrieval_precision([], ["anything"]) == 1.0

    def test_empty_actual(self):
        assert compute_retrieval_precision(["DDJJ"], []) == 0.0

    def test_case_insensitive(self):
        assert compute_retrieval_precision(["series de tiempo"], ["SERIES DE TIEMPO"]) == 1.0


class TestCheckAnswerContains:
    def test_all_keywords_present(self):
        assert (
            check_answer_contains(
                "La inflación fue del 4.2% según el IPC",
                ["inflación", "%"],
            )
            == 1.0
        )

    def test_partial_keywords(self):
        assert (
            check_answer_contains(
                "El dólar subió hoy",
                ["dólar", "blue"],
            )
            == 0.5
        )

    def test_no_keywords_present(self):
        assert (
            check_answer_contains(
                "Buenos días",
                ["inflación", "PBI"],
            )
            == 0.0
        )

    def test_empty_keywords(self):
        assert check_answer_contains("cualquier respuesta", []) == 1.0

    def test_case_insensitive(self):
        assert check_answer_contains("El PBI creció", ["pbi"]) == 1.0


class TestAggregateResults:
    def test_empty_results(self):
        summary = aggregate_results([])
        assert summary.total == 0
        assert summary.avg_retrieval_precision == 0.0

    def test_single_result(self):
        results = [
            EvalResult(
                question_id="test_001",
                category="series_tiempo",
                retrieval_precision=0.8,
                answer_relevance=0.9,
                hallucination_score=0.1,
                intent_match=True,
                connector_match=True,
                latency_ms=150,
            ),
        ]
        summary = aggregate_results(results)
        assert summary.total == 1
        assert summary.avg_retrieval_precision == 0.8
        assert summary.avg_answer_relevance == 0.9
        assert summary.intent_accuracy == 1.0
        assert summary.connector_accuracy == 1.0
        assert summary.avg_latency_ms == 150

    def test_multiple_categories(self):
        results = [
            EvalResult(
                question_id="s1",
                category="series_tiempo",
                retrieval_precision=1.0,
                intent_match=True,
                connector_match=True,
            ),
            EvalResult(
                question_id="s2",
                category="series_tiempo",
                retrieval_precision=0.5,
                intent_match=True,
                connector_match=False,
            ),
            EvalResult(
                question_id="d1",
                category="ddjj",
                retrieval_precision=1.0,
                intent_match=False,
                connector_match=True,
            ),
        ]
        summary = aggregate_results(results)
        assert summary.total == 3
        assert len(summary.by_category) == 2

        series_cat = summary.by_category["series_tiempo"]
        assert series_cat["count"] == 2
        assert series_cat["avg_retrieval_precision"] == 0.75
        assert series_cat["intent_accuracy"] == 1.0

        ddjj_cat = summary.by_category["ddjj"]
        assert ddjj_cat["count"] == 1
        assert ddjj_cat["intent_accuracy"] == 0.0

    def test_latency_aggregation(self):
        results = [
            EvalResult(question_id="a", category="test", latency_ms=100),
            EvalResult(question_id="b", category="test", latency_ms=200),
            EvalResult(question_id="c", category="test", latency_ms=300),
        ]
        summary = aggregate_results(results)
        assert summary.avg_latency_ms == 200.0
