"""LangGraph nodes: planner (plan generation) and clarification reply."""

from __future__ import annotations

import logging

from langgraph.config import get_stream_writer

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.connectors.sandbox import discover_catalog_hints_for_planner
from app.application.pipeline.state import OpenArgState
from app.infrastructure.adapters.connectors.query_planner import generate_plan

logger = logging.getLogger(__name__)


async def planner_node(state: OpenArgState) -> dict:
    """Discover relevant cached tables and generate an execution plan via LLM.

    Writes *catalog_hints*, *plan*, and *plan_intent* to state.
    """
    writer = get_stream_writer()
    writer({"type": "status", "step": "planning", "detail": "Planificando estrategia..."})
    deps = nodes_pkg.get_deps()

    try:
        preprocessed_q = state.get("preprocessed_query", state["question"])
        planner_ctx = state.get("planner_ctx", "")

        # Inject skill context if a skill was detected
        skill_ctx = state.get("skill_context")
        if skill_ctx and skill_ctx.get("planner"):
            planner_ctx = (planner_ctx or "") + "\n\n--- SKILL ACTIVA ---\n" + skill_ctx["planner"]

        # Discover relevant cached tables for the planner
        catalog_hints = await discover_catalog_hints_for_planner(
            preprocessed_q, deps.sandbox, deps.embedding
        )

        # Generate the plan (1 LLM call)
        plan = await generate_plan(
            deps.llm,
            preprocessed_q,
            memory_context=planner_ctx,
            catalog_hints=catalog_hints,
        )

        return {
            "catalog_hints": catalog_hints,
            "plan": plan,
            "plan_intent": plan.intent,
        }
    except Exception:
        logger.exception("planner_node failed")
        return {
            "catalog_hints": "",
            "plan": None,
            "plan_intent": "error",
            "error": "Planner failed",
        }


async def clarify_reply_node(state: OpenArgState) -> dict:
    """Build a clarification answer from the plan (terminal node).

    When the planner determines the query is ambiguous, it returns a
    plan with ``intent="clarification"`` and a clarification step
    containing a question and clickable options.
    """
    plan = state.get("plan")
    if not plan:
        return {
            "clean_answer": "No pude procesar tu consulta. Probá reformulándola.",
            "sources": [],
            "chart_data": None,
            "confidence": 1.0,
            "citations": [],
            "documents": None,
            "tokens_used": 0,
            "warnings": [],
        }

    from langgraph.config import get_stream_writer

    writer = get_stream_writer()

    clar_step = next((s for s in plan.steps if s.action == "clarification"), None)
    if clar_step:
        clar_q = clar_step.params.get("question", "¿Podés ser más específico?")
        clar_opts = clar_step.params.get("options", [])
    else:
        clar_q = "¿Podés ser más específico?"
        clar_opts = []

    # Emit clarification event so the frontend renders clickable chips
    writer(
        {
            "type": "clarification",
            "question": clar_q,
            "options": clar_opts,
        }
    )

    opts_text = "\n".join(f"- {o}" for o in clar_opts) if clar_opts else ""
    answer = f"**{clar_q}**\n\n{opts_text}" if opts_text else f"**{clar_q}**"

    return {
        "clean_answer": answer,
        "sources": [],
        "plan_intent": "clarification",
        "chart_data": None,
        "confidence": 1.0,
        "citations": [],
        "documents": None,
        "tokens_used": 0,
        "warnings": [],
    }
