"""LangGraph state definition for the OpenArg pipeline."""

from __future__ import annotations

from typing import Annotated, Any

from typing_extensions import TypedDict

from app.domain.entities.connectors.data_result import DataResult, ExecutionPlan

# Sentinel value: return this from a node to reset a reducer-list to empty.
# Using operator.add, returning [] does nothing (old + [] = old).
# This sentinel tells the reducer to discard prior values.
RESET_LIST: list = []  # identity object, compared by `is`


def _resettable_add(left: list, right: list) -> list:
    """Reducer that supports resetting: if *right* is the RESET_LIST sentinel,
    discard *left* and return empty.  Otherwise behave like operator.add."""
    if right is RESET_LIST:
        return []
    return left + right


class OpenArgState(TypedDict, total=False):
    """Shared state flowing through the LangGraph pipeline.

    Fields with ``Annotated[list, _resettable_add]`` use a custom reducer
    that supports both accumulation (for parallel fan-out) and reset
    (for replan passes that need a clean slate).
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

    # ── Skills ──────────────────────────────────────────────
    active_skill: str | None
    skill_context: dict[str, str] | None  # {planner: str, analyst: str}

    # ── Planning ────────────────────────────────────────────
    catalog_hints: str
    plan: ExecutionPlan | None
    plan_intent: str

    # ── Execution ───────────────────────────────────────────
    data_results: Annotated[list[DataResult], _resettable_add]
    step_warnings: Annotated[list[str], _resettable_add]

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

    # ── Coordination / Re-planning ─────────────────────────
    replan_count: int  # 0 initially, max 2
    replan_strategy: str | None  # "broaden" | "narrow" | "switch_source"
    coordinator_decision: str | None  # "continue" | "replan" | "escalate"

    # ── Error ───────────────────────────────────────────────
    error: str | None
