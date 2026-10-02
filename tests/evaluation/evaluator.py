"""Evaluation framework for the OpenArg RAG pipeline.

Provides metrics computation for retrieval precision, answer relevance,
hallucination detection, and intent/connector matching.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
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


# Lo que el juez tiene que mirar. Sin esto, "¿la respuesta es relevante?"
# aprueba una respuesta sobre el EMAE a una pregunta sobre el PBI: habla de
# economía, tiene números, suena bien.
_RELEVANCE_RUBRIC = """Evaluás si una respuesta contesta la pregunta que se hizo.

Bajá el puntaje si la respuesta:
- usa otro indicador que el pedido (EMAE cuando se pidió PBI, la línea de pobreza cuando se pidió la tasa);
- usa otra unidad (pesos cuando el dato es en dólares) u otro nivel geográfico (un total nacional presentado como local);
- usa otro período sin decirlo;
- esquiva la pregunta cuando el dato existe, o rellena con información que no se pidió.

Una respuesta que dice con claridad que el dato no existe y explica por qué es relevante.
Puntaje: 0.0 = no contesta lo pedido, 1.0 = contesta exactamente lo pedido."""

_HALLUCINATION_RUBRIC = """Evaluás si las cifras y afirmaciones de una respuesta salen de los datos que tuvo a la vista.

Cada cifra tiene que estar en los datos o derivarse de ellos con una cuenta simple (suma, variación, promedio).
Una cifra que no está, una unidad cambiada o un período distinto cuentan como inventados.
Las frases generales sin cifras no cuentan.
Puntaje: 0.0 = todo sale de los datos, 1.0 = todo inventado."""

# Medido el 01-oct: con "respondé sólo con un número" y 16 tokens, Sonnet 4.6
# arrancaba a razonar ("I need to evaluate…") y se cortaba antes del número en
# 11 de 12 corridas del juez de alucinación. Ahora razona en dos o tres
# oraciones y cierra con una línea fija, que es lo único que se lee.
_FORMAT = (
    "\n\nExplicá tu evaluación en dos o tres oraciones, en castellano, y terminá con una "
    "última línea exactamente así: PUNTAJE: <número entre 0.0 y 1.0>"
)

_FINAL_RE = re.compile(r"PUNTAJE:\s*\**\s*([01](?:[.,]\d+)?)", re.IGNORECASE)
_SCORE_RE = re.compile(r"(?<![\d.])(?:0(?:\.\d+)?|1(?:\.0+)?)(?![\d.])")


def parse_judge_score(text: str) -> float | None:
    """El puntaje de la línea ``PUNTAJE:``; si no está, un número solo; si no, None.

    Nunca un número cualquiera del medio del razonamiento: "las 2 cifras" o
    "el 1 de marzo" no son el puntaje.
    """
    finals = _FINAL_RE.findall(text or "")
    if finals:
        value = float(finals[-1].replace(",", "."))
        return value if 0.0 <= value <= 1.0 else None
    bare = (text or "").strip().replace(",", ".")
    m = _SCORE_RE.fullmatch(bare)
    return float(m.group(0)) if m else None


@dataclass
class JudgeVerdict:
    """El puntaje y por qué. ``score`` es None si el juez falló o no puntuó."""

    score: float | None
    reason: str = ""


async def _judge(llm: ILLMProvider, system: str, user: str) -> JudgeVerdict:
    try:
        response = await llm.chat(
            messages=[
                LLMMessage(role="system", content=system + _FORMAT),
                LLMMessage(role="user", content=user),
            ],
            temperature=0.0,
            max_tokens=600,
        )
    except Exception as exc:
        logger.warning("LLM judge call failed", exc_info=True)
        return JudgeVerdict(None, f"{type(exc).__name__}: {exc}"[:200])
    text = response.content or ""
    score = parse_judge_score(text)
    if score is None:
        logger.warning("LLM judge returned no score: %r", text[-120:])
    reason = _FINAL_RE.split(text)[0].strip()
    return JudgeVerdict(score, reason[:800])


def _today() -> str:
    """Sin la fecha, el juez tomó "abril de 2026" por un dato del futuro y bajó
    la relevancia de una respuesta correcta sobre reservas (01-oct)."""
    return f"Hoy es {date.today().isoformat()}.\n\n"


async def judge_answer_relevance(
    llm: ILLMProvider,
    question: str,
    answer: str,
) -> JudgeVerdict:
    """0.0 = no contesta lo pedido, 1.0 = contesta exactamente lo pedido.

    Si el juez falla, el puntaje es None. Antes devolvía 0.5, que en un
    promedio no se distingue de una respuesta regular.
    """
    return await _judge(
        llm, _RELEVANCE_RUBRIC, f"{_today()}Pregunta:\n{question}\n\nRespuesta:\n{answer}"
    )


async def judge_hallucination(
    llm: ILLMProvider,
    question: str,
    answer: str,
    sources_summary: str,
) -> JudgeVerdict:
    """0.0 = todo sale de los datos, 1.0 = todo inventado. Puntaje None si falló."""
    return await _judge(
        llm,
        _HALLUCINATION_RUBRIC,
        f"{_today()}Pregunta:\n{question}\n\nDatos que tuvo a la vista:\n{sources_summary}"
        f"\n\nRespuesta:\n{answer}",
    )


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
