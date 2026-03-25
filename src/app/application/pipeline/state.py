"""LangGraph state definition for the OpenArg pipeline."""

from __future__ import annotations

import operator
from typing import Annotated, Any

from typing_extensions import TypedDict

from app.domain.entities.connectors.data_result import DataResult, ExecutionPlan


class OpenArgState(TypedDict, total=False):
    """Shared state flowing through the LangGraph pipeline.

    Fields with ``Annotated[list, operator.add]`` use a *reducer* so that
    multiple parallel nodes can append to the same list without overwriting
    each other (critical for the fan-out connector pattern).
    """

    # ── Input (set once at entry) ───────────────────────────
    question: str
    user_id: str
    conversation_id: str
    policy_mode: bool

    # ── Classification ──────────────────────────────────────
    classification: str | None  # casual, meta, injection, off_topic, educational, or None
    classification_response: str | None

    # ── Cache ───────────────────────────────────────────────
    cached_result: dict[str, Any] | None
    last_embedding: list[float] | None  # reused for cache write

    # ── Preprocessing ───────────────────────────────────────
    preprocessed_query: str

    # ── Memory / History ────────────────────────────────────
    memory: Any  # MemoryContext object
    memory_ctx: str  # planner-facing context
    memory_ctx_analyst: str  # analyst-facing context (lightweight)
    planner_ctx: str  # chat_history or memory_ctx

    # ── Planning ────────────────────────────────────────────
    catalog_hints: str
    plan: ExecutionPlan | None
    plan_intent: str

    # ── Execution ───────────────────────────────────────────
    data_results: Annotated[list[DataResult], operator.add]
    step_warnings: Annotated[list[str], operator.add]

    # ── Analysis ────────────────────────────────────────────
    analysis_prompt: str
    analysis_response: str
    clean_answer: str
    confidence: float
    citations: list[dict[str, Any]]

    # ── Charts ──────────────────────────────────────────────
    chart_data: list[dict[str, Any]] | None

    # ── Maps ──────────────────────────────────────────────
    map_data: dict[str, Any] | None  # GeoJSON FeatureCollection for frontend

    # ── Policy ──────────────────────────────────────────────
    policy_text: str | None

    # ── Output ──────────────────────────────────────────────
    sources: list[dict[str, Any]]
    documents: list[dict[str, Any]] | None
    tokens_used: int
    duration_ms: int
    warnings: list[str]

    # ── Timing ──────────────────────────────────────────────
    _start_time: float  # time.monotonic() at pipeline start

    # ── Re-planning ────────────────────────────────────────
    replan_count: int  # 0 initially, max 1

    # ── Error ───────────────────────────────────────────────
    error: str | None
