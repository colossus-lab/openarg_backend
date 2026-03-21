"""Evaluation framework for the OpenArg RAG pipeline.

Provides metrics computation for retrieval precision, answer relevance,
hallucination detection, and intent/connector matching.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage

logger = logging.getLogger(__name__)


@dataclass
class EvalResult:
    """Evaluation metrics for a single question-answer pair."""

    question_id: str
    category: str
    retrieval_precision: float = 0.0
    answer_relevance: float = 0.0
    answer_faithfulness: float = 0.0
    hallucination_score: float = 0.0
    intent_match: bool = False
    connector_match: bool = False
    latency_ms: int = 0


@dataclass
class EvalSummary:
    """Aggregated evaluation results."""

    total: int = 0
    avg_retrieval_precision: float = 0.0
    avg_answer_relevance: float = 0.0
    avg_hallucination_score: float = 0.0
    intent_accuracy: float = 0.0
    connector_accuracy: float = 0.0
    avg_latency_ms: float = 0.0
    by_category: dict[str, dict[str, Any]] = field(default_factory=dict)


def compute_retrieval_precision(
    expected_sources: list[str],
    actual_sources: list[str],
) -> float:
    """Compute precision of retrieved sources against expected sources.

    Returns the fraction of expected sources that appear in actual sources.
    Case-insensitive partial matching.
    """
    if not expected_sources:
        return 1.0

    hits = 0
    for expected in expected_sources:
        expected_lower = expected.lower()
        for actual in actual_sources:
            if expected_lower in actual.lower():
                hits += 1
                break

    return hits / len(expected_sources)


def check_answer_contains(answer: str, expected_keywords: list[str]) -> float:
    """Check what fraction of expected keywords appear in the answer.

    Case-insensitive matching.
    """
    if not expected_keywords:
        return 1.0

    answer_lower = answer.lower()
    hits = sum(1 for kw in expected_keywords if kw.lower() in answer_lower)
    return hits / len(expected_keywords)


async def judge_answer_relevance(
    llm: ILLMProvider,
    question: str,
    answer: str,
) -> float:
    """Use an LLM as judge to evaluate answer relevance (0.0-1.0)."""
    prompt = (
        "Rate the relevance of this answer to the question on a scale of 0.0 to 1.0.\n"
        "Only respond with a single number.\n\n"
        f"Question: {question}\n"
        f"Answer: {answer}\n\n"
        "Relevance score:"
    )
    try:
        response = await llm.chat(
            messages=[LLMMessage(role="user", content=prompt)],
            temperature=0.0,
            max_tokens=16,
        )
        score = float(response.content.strip())
        return max(0.0, min(1.0, score))
    except Exception:
        logger.debug("LLM judge failed for relevance", exc_info=True)
        return 0.5


async def judge_hallucination(
    llm: ILLMProvider,
    question: str,
    answer: str,
    sources_summary: str,
) -> float:
    """Use LLM judge to detect hallucination (0.0=grounded, 1.0=fabricated)."""
    prompt = (
        "Rate the hallucination level of this answer on a scale of 0.0 to 1.0.\n"
        "0.0 means fully grounded in the provided sources, 1.0 means completely fabricated.\n"
        "Only respond with a single number.\n\n"
        f"Question: {question}\n"
        f"Sources: {sources_summary}\n"
        f"Answer: {answer}\n\n"
        "Hallucination score:"
    )
    try:
        response = await llm.chat(
            messages=[LLMMessage(role="user", content=prompt)],
            temperature=0.0,
            max_tokens=16,
        )
        score = float(response.content.strip())
        return max(0.0, min(1.0, score))
    except Exception:
        logger.debug("LLM judge failed for hallucination", exc_info=True)
        return 0.5


def aggregate_results(results: list[EvalResult]) -> EvalSummary:
    """Aggregate individual evaluation results into a summary."""
    if not results:
        return EvalSummary()

    total = len(results)

    by_category: dict[str, list[EvalResult]] = {}
    for r in results:
        by_category.setdefault(r.category, []).append(r)

    category_summaries: dict[str, dict[str, Any]] = {}
    for cat, cat_results in by_category.items():
        n = len(cat_results)
        category_summaries[cat] = {
            "count": n,
            "avg_retrieval_precision": sum(r.retrieval_precision for r in cat_results) / n,
            "avg_answer_relevance": sum(r.answer_relevance for r in cat_results) / n,
            "avg_hallucination_score": sum(r.hallucination_score for r in cat_results) / n,
            "intent_accuracy": sum(1 for r in cat_results if r.intent_match) / n,
            "connector_accuracy": sum(1 for r in cat_results if r.connector_match) / n,
            "avg_latency_ms": sum(r.latency_ms for r in cat_results) / n,
        }

    return EvalSummary(
        total=total,
        avg_retrieval_precision=sum(r.retrieval_precision for r in results) / total,
        avg_answer_relevance=sum(r.answer_relevance for r in results) / total,
        avg_hallucination_score=sum(r.hallucination_score for r in results) / total,
        intent_accuracy=sum(1 for r in results if r.intent_match) / total,
        connector_accuracy=sum(1 for r in results if r.connector_match) / total,
        avg_latency_ms=sum(r.latency_ms for r in results) / total,
        by_category=category_summaries,
    )
