"""LangGraph node: coordinator — heuristic decision-maker for re-planning.

Analyzes execution results after the analyst and decides whether to:
- **continue** → finalize (sufficient data found)
- **replan** → try again with a different strategy
- **escalate** → finalize with whatever data we have (max retries or time budget)

All logic is heuristic (no LLM calls) — zero additional token cost.
"""

from __future__ import annotations

import logging
import time

from langgraph.config import get_stream_writer

from app.application.pipeline.state import OpenArgState

logger = logging.getLogger(__name__)

_MAX_REPLAN_DEPTH = 2
_TIME_BUDGET_SECONDS = 20.0

_STRATEGY_LABELS = {
    "broaden": "ampliando búsqueda",
    "narrow": "enfocando búsqueda",
    "switch_source": "probando fuentes alternativas",
}


def _has_useful_data(state: OpenArgState) -> bool:
    """Return True if data_results contain actual records (not just metadata)."""
    data_results = state.get("data_results", [])
    return bool(data_results) and any(
        r.records or r.source.startswith("pgvector:") or r.source.startswith("ckan:")
        for r in data_results
    )


def _select_strategy(state: OpenArgState) -> str:
    """Choose a replan strategy based on failure pattern."""
    replan_count = state.get("replan_count", 0)
    step_warnings = state.get("step_warnings", [])
    data_results = state.get("data_results", [])

    failure_count = len(step_warnings)
    has_some_data = any(r.records for r in data_results) if data_results else False

    if replan_count == 0:
        # First replan: broaden the search
        return "broaden"
    elif failure_count > 0 and not has_some_data:
        # Second replan: connectors failed, try different ones
        return "switch_source"
    else:
        # Second replan: got some data but not enough, focus
        return "narrow"


async def coordinator_node(state: OpenArgState) -> dict:
    """Analyze execution results and decide next action.

    Heuristic-based (no LLM cost):
    - continue: sufficient data found → finalize
    - replan: insufficient data, retries remain → try different strategy
    - escalate: max retries reached or time budget exceeded → finalize as-is
    """
    writer = get_stream_writer()
    replan_count = state.get("replan_count", 0)
    step_warnings = state.get("step_warnings", [])

    # ── Rule 1: Time budget exceeded → escalate ──────────
    start_time = state.get("_start_time")
    if start_time and (time.monotonic() - start_time) > _TIME_BUDGET_SECONDS:
        logger.info("Coordinator: time budget exceeded (%.1fs), escalating", time.monotonic() - start_time)
        return {"coordinator_decision": "escalate", "replan_strategy": None}

    # ── Rule 2: Has useful data → continue ───────────────
    if _has_useful_data(state):
        return {"coordinator_decision": "continue", "replan_strategy": None}

    # ── Rule 3: Max replan depth reached → escalate ──────
    if replan_count >= _MAX_REPLAN_DEPTH:
        logger.info("Coordinator: max replan depth (%d) reached, escalating", _MAX_REPLAN_DEPTH)
        return {"coordinator_decision": "escalate", "replan_strategy": None}

    # ── Rule 4: Too many connector failures → escalate ───
    if len(step_warnings) >= 3:
        logger.info("Coordinator: %d connector failures, escalating", len(step_warnings))
        return {"coordinator_decision": "escalate", "replan_strategy": None}

    # ── Rule 5: Replan with strategy ─────────────────────
    strategy = _select_strategy(state)
    label = _STRATEGY_LABELS.get(strategy, strategy)

    logger.info(
        "Coordinator: replan #%d with strategy '%s'",
        replan_count + 1,
        strategy,
    )
    writer({
        "type": "status",
        "step": "coordination",
        "detail": f"Replanificando: {label}...",
    })

    return {
        "coordinator_decision": "replan",
        "replan_strategy": strategy,
    }
