"""LangGraph node: re-plan when analyst detects insufficient data."""

from __future__ import annotations

import logging

from langgraph.config import get_stream_writer

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.connectors.sandbox import discover_catalog_hints_for_planner
from app.application.pipeline.state import OpenArgState
from app.infrastructure.adapters.connectors.query_planner import generate_plan

logger = logging.getLogger(__name__)


async def replan_node(state: OpenArgState) -> dict:
    """Re-plan with modified query when analyst detects insufficient data.

    Increments *replan_count*, adjusts context based on what was found
    (or not found), and generates a new plan for a second execution pass.
    """
    writer = get_stream_writer()
    writer({"type": "status", "step": "replanning", "detail": "Replanificando búsqueda..."})
    deps = nodes_pkg.get_deps()

    replan_count = state.get("replan_count", 0) + 1

    try:
        preprocessed_q = state.get("preprocessed_query", state["question"])
        planner_ctx = state.get("planner_ctx", "")

        # Enrich planner context with info about what was already tried
        step_warnings = list(state.get("step_warnings", []))
        data_results = state.get("data_results", [])

        replan_context = planner_ctx
        if step_warnings:
            replan_context += "\n\nINTENTO ANTERIOR - ADVERTENCIAS:\n" + "\n".join(
                f"- {w}" for w in step_warnings
            )
        if not data_results or not any(r.records for r in data_results):
            replan_context += (
                "\n\nEl intento anterior no devolvió datos. "
                "Probá con fuentes de datos alternativas o una consulta más amplia."
            )

        # Discover catalog hints again (same logic as planner_node)
        catalog_hints = await discover_catalog_hints_for_planner(
            preprocessed_q, deps.sandbox, deps.embedding
        )

        # Generate a new plan with the enriched context
        plan = await generate_plan(
            deps.llm,
            preprocessed_q,
            memory_context=replan_context,
            catalog_hints=catalog_hints,
        )

        return {
            "replan_count": replan_count,
            "catalog_hints": catalog_hints,
            "plan": plan,
            "plan_intent": plan.intent,
            # Reset data_results and step_warnings for the new execution pass
            "data_results": [],
            "step_warnings": [],
        }
    except Exception:
        logger.exception("replan_node failed")
        return {
            "replan_count": replan_count,
            "error": "Replan failed",
        }
