"""Unit tests for retrieval strategy evaluation helpers."""

from __future__ import annotations

from .retrieval_strategy_evaluator import (
    RetrievalCandidate,
    RetrievalEvalCase,
    build_case_result,
    candidate_matches_expected,
    compare_strategy_results,
    compute_hit_at_k,
    compute_reciprocal_rank,
    summarize_strategy_results,
    validate_retrieval_cases,
)


class TestValidateRetrievalCases:
    def test_valid_cases_pass(self) -> None:
        cases = [
            RetrievalEvalCase(
                id="case_1",
                category="siglas",
                query="ipc",
                expected_title_keywords=["IPC"],
                difficulty="hard",
            )
        ]
        assert validate_retrieval_cases(cases) == []

    def test_duplicate_ids_fail(self) -> None:
        cases = [
            RetrievalEvalCase(
                id="dup",
                category="siglas",
                query="ipc",
                expected_title_keywords=["IPC"],
                difficulty="hard",
            ),
            RetrievalEvalCase(
                id="dup",
                category="siglas",
                query="sube",
                expected_title_keywords=["SUBE"],
                difficulty="medium",
            ),
        ]
        assert validate_retrieval_cases(cases) == ["Duplicate case id: dup"]


class TestRankingMetrics:
    def test_candidate_matches_expected(self) -> None:
        candidate = RetrievalCandidate(
            title="Sistema Integrado de Transporte Automotor (SISTAU)",
            portal="transporte",
            score=0.9,
        )
        assert candidate_matches_expected(candidate, ["sistau"]) is True

    def test_compute_hit_at_k(self) -> None:
        candidates = [
            RetrievalCandidate(title="Otro dataset", portal="x", score=0.8),
            RetrievalCandidate(title="Índice de Precios al Consumidor", portal="y", score=0.7),
        ]
        assert compute_hit_at_k(candidates, ["IPC", "Índice de Precios"], 1) is False
        assert compute_hit_at_k(candidates, ["IPC", "Índice de Precios"], 2) is True

    def test_compute_reciprocal_rank(self) -> None:
        candidates = [
            RetrievalCandidate(title="No relevante", portal="x", score=0.9),
            RetrievalCandidate(title="Producto Bruto Geográfico (PBG)", portal="y", score=0.8),
        ]
        assert compute_reciprocal_rank(candidates, ["PBG"]) == 0.5


class TestSummaryAndComparison:
    def test_build_case_result_and_summary(self) -> None:
        case = RetrievalEvalCase(
            id="case_1",
            category="siglas",
            query="ipc",
            expected_title_keywords=["IPC"],
            difficulty="hard",
        )
        result = build_case_result(
            case=case,
            strategy="vector_only",
            candidates=[
                RetrievalCandidate(title="Series de Tiempo — IPC", portal="series_tiempo", score=0.88)
            ],
            latency_ms=120.0,
        )
        summary = summarize_strategy_results("vector_only", [result])

        assert result.hit_at_1 is True
        assert result.reciprocal_rank == 1.0
        assert summary.hit_at_5 == 1.0
        assert summary.avg_latency_ms == 120.0

    def test_compare_strategy_results(self) -> None:
        baseline = [
            RetrievalCandidate(title="No relevante", portal="x", score=0.8),
            RetrievalCandidate(title="No relevante 2", portal="y", score=0.7),
        ]
        candidate = [
            RetrievalCandidate(title="Producto Bruto Geográfico (PBG)", portal="z", score=0.6),
        ]
        case = RetrievalEvalCase(
            id="case_pbg",
            category="siglas",
            query="pbg",
            expected_title_keywords=["PBG", "Producto Bruto Geográfico"],
            difficulty="hard",
        )

        baseline_result = build_case_result(case, "vector_only", baseline, latency_ms=100.0)
        candidate_result = build_case_result(case, "hybrid_strategy", candidate, latency_ms=140.0)

        comparison = compare_strategy_results([baseline_result], [candidate_result])

        assert comparison.hit_at_5_delta == 1.0
        assert comparison.mean_reciprocal_rank_delta == 1.0
        assert comparison.avg_latency_ms_delta == 40.0
        assert comparison.candidate_better_cases == ["case_pbg"]
