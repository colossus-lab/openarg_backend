"""Utilities for comparing retrieval strategies on a versioned case set."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RetrievalEvalCase:
    """A retrieval-only evaluation case."""

    id: str
    category: str
    query: str
    expected_title_keywords: list[str]
    difficulty: str
    notes: str = ""


@dataclass(frozen=True)
class RetrievalCandidate:
    """A retrieved dataset candidate."""

    title: str
    portal: str
    score: float


@dataclass
class RetrievalCaseResult:
    """Per-case metrics for a single strategy."""

    case_id: str
    strategy: str
    hit_at_1: bool
    hit_at_3: bool
    hit_at_5: bool
    reciprocal_rank: float
    latency_ms: float
    top_titles: list[str] = field(default_factory=list)


@dataclass
class RetrievalStrategySummary:
    """Aggregated metrics for one retrieval strategy."""

    strategy: str
    total_cases: int
    hit_at_1: float
    hit_at_3: float
    hit_at_5: float
    mean_reciprocal_rank: float
    avg_latency_ms: float


@dataclass
class RetrievalComparisonSummary:
    """Head-to-head comparison between two strategies."""

    baseline: RetrievalStrategySummary
    candidate: RetrievalStrategySummary
    hit_at_1_delta: float
    hit_at_3_delta: float
    hit_at_5_delta: float
    mean_reciprocal_rank_delta: float
    avg_latency_ms_delta: float
    candidate_better_cases: list[str] = field(default_factory=list)
    baseline_better_cases: list[str] = field(default_factory=list)


def load_retrieval_cases(path: Path) -> list[RetrievalEvalCase]:
    """Load retrieval cases from JSON."""
    with path.open(encoding="utf-8") as f:
        payload = json.load(f)

    return [
        RetrievalEvalCase(
            id=entry["id"],
            category=entry["category"],
            query=entry["query"],
            expected_title_keywords=list(entry["expected_title_keywords"]),
            difficulty=entry["difficulty"],
            notes=entry.get("notes", ""),
        )
        for entry in payload.get("entries", [])
    ]


def validate_retrieval_cases(cases: list[RetrievalEvalCase]) -> list[str]:
    """Validate retrieval case definitions."""
    errors: list[str] = []
    seen_ids: set[str] = set()

    for index, case in enumerate(cases):
        if case.id in seen_ids:
            errors.append(f"Duplicate case id: {case.id}")
        seen_ids.add(case.id)

        if not case.query.strip():
            errors.append(f"Case {case.id or index}: empty query")
        if not case.expected_title_keywords:
            errors.append(f"Case {case.id}: expected_title_keywords is empty")
        if not case.category.strip():
            errors.append(f"Case {case.id}: empty category")
        if not case.difficulty.strip():
            errors.append(f"Case {case.id}: empty difficulty")

    return errors


def candidate_matches_expected(candidate: RetrievalCandidate, expected_keywords: list[str]) -> bool:
    """Return whether a candidate title matches any expected keyword fragment."""
    title_lower = candidate.title.lower()
    return any(keyword.lower() in title_lower for keyword in expected_keywords)


def compute_hit_at_k(
    candidates: list[RetrievalCandidate],
    expected_keywords: list[str],
    k: int,
) -> bool:
    """Check whether at least one relevant item appears in the first k candidates."""
    return any(
        candidate_matches_expected(candidate, expected_keywords) for candidate in candidates[:k]
    )


def compute_reciprocal_rank(
    candidates: list[RetrievalCandidate],
    expected_keywords: list[str],
) -> float:
    """Return reciprocal rank of the first relevant candidate, else 0.0."""
    for index, candidate in enumerate(candidates, start=1):
        if candidate_matches_expected(candidate, expected_keywords):
            return 1.0 / index
    return 0.0


def build_case_result(
    case: RetrievalEvalCase,
    strategy: str,
    candidates: list[RetrievalCandidate],
    latency_ms: float,
) -> RetrievalCaseResult:
    """Build a retrieval evaluation result from raw candidates."""
    return RetrievalCaseResult(
        case_id=case.id,
        strategy=strategy,
        hit_at_1=compute_hit_at_k(candidates, case.expected_title_keywords, 1),
        hit_at_3=compute_hit_at_k(candidates, case.expected_title_keywords, 3),
        hit_at_5=compute_hit_at_k(candidates, case.expected_title_keywords, 5),
        reciprocal_rank=compute_reciprocal_rank(candidates, case.expected_title_keywords),
        latency_ms=latency_ms,
        top_titles=[candidate.title for candidate in candidates[:5]],
    )


def summarize_strategy_results(
    strategy: str,
    results: list[RetrievalCaseResult],
) -> RetrievalStrategySummary:
    """Aggregate per-case results into strategy-level metrics."""
    if not results:
        return RetrievalStrategySummary(
            strategy=strategy,
            total_cases=0,
            hit_at_1=0.0,
            hit_at_3=0.0,
            hit_at_5=0.0,
            mean_reciprocal_rank=0.0,
            avg_latency_ms=0.0,
        )

    total = len(results)
    return RetrievalStrategySummary(
        strategy=strategy,
        total_cases=total,
        hit_at_1=sum(1 for result in results if result.hit_at_1) / total,
        hit_at_3=sum(1 for result in results if result.hit_at_3) / total,
        hit_at_5=sum(1 for result in results if result.hit_at_5) / total,
        mean_reciprocal_rank=sum(result.reciprocal_rank for result in results) / total,
        avg_latency_ms=sum(result.latency_ms for result in results) / total,
    )


def compare_strategy_results(
    baseline_results: list[RetrievalCaseResult],
    candidate_results: list[RetrievalCaseResult],
) -> RetrievalComparisonSummary:
    """Compare two strategies case-by-case and in aggregate."""
    baseline_summary = summarize_strategy_results(
        baseline_results[0].strategy if baseline_results else "baseline",
        baseline_results,
    )
    candidate_summary = summarize_strategy_results(
        candidate_results[0].strategy if candidate_results else "candidate",
        candidate_results,
    )

    baseline_map = {result.case_id: result for result in baseline_results}
    candidate_map = {result.case_id: result for result in candidate_results}

    candidate_better_cases: list[str] = []
    baseline_better_cases: list[str] = []
    for case_id in sorted(set(baseline_map) & set(candidate_map)):
        baseline = baseline_map[case_id]
        candidate = candidate_map[case_id]
        if candidate.reciprocal_rank > baseline.reciprocal_rank:
            candidate_better_cases.append(case_id)
        elif baseline.reciprocal_rank > candidate.reciprocal_rank:
            baseline_better_cases.append(case_id)

    return RetrievalComparisonSummary(
        baseline=baseline_summary,
        candidate=candidate_summary,
        hit_at_1_delta=candidate_summary.hit_at_1 - baseline_summary.hit_at_1,
        hit_at_3_delta=candidate_summary.hit_at_3 - baseline_summary.hit_at_3,
        hit_at_5_delta=candidate_summary.hit_at_5 - baseline_summary.hit_at_5,
        mean_reciprocal_rank_delta=(
            candidate_summary.mean_reciprocal_rank - baseline_summary.mean_reciprocal_rank
        ),
        avg_latency_ms_delta=candidate_summary.avg_latency_ms - baseline_summary.avg_latency_ms,
        candidate_better_cases=candidate_better_cases,
        baseline_better_cases=baseline_better_cases,
    )


def comparison_to_dict(summary: RetrievalComparisonSummary) -> dict[str, Any]:
    """Convert comparison summary into a JSON-friendly structure."""
    return {
        "baseline": summary.baseline.__dict__,
        "candidate": summary.candidate.__dict__,
        "hit_at_1_delta": summary.hit_at_1_delta,
        "hit_at_3_delta": summary.hit_at_3_delta,
        "hit_at_5_delta": summary.hit_at_5_delta,
        "mean_reciprocal_rank_delta": summary.mean_reciprocal_rank_delta,
        "avg_latency_ms_delta": summary.avg_latency_ms_delta,
        "candidate_better_cases": summary.candidate_better_cases,
        "baseline_better_cases": summary.baseline_better_cases,
    }
