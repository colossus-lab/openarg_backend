"""LangGraph node: optional public policy analysis."""

from __future__ import annotations

import logging

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.state import OpenArgState
from app.infrastructure.adapters.connectors.policy_agent import analyze_policy

logger = logging.getLogger(__name__)


async def policy_node(state: OpenArgState) -> dict:
    """Run the policy analysis agent when *policy_mode* is True.

    Appends a policy evaluation section to the existing *clean_answer*.
    If the policy agent fails, the original answer is preserved.
    """
    deps = nodes_pkg._deps
    assert deps is not None, "PipelineDeps not initialised"

    if not state.get("policy_mode", False):
        return {"policy_text": None}

    try:
        plan = state.get("plan")
        results = state.get("data_results", [])
        clean_answer = state.get("clean_answer", "")
        memory_ctx = state.get("memory_ctx", "")

        policy_text = await analyze_policy(deps.llm, plan, results, clean_answer, memory_ctx)

        # Append policy text to the answer with a separator
        updated_answer = clean_answer + "\n\n---\n\n" + policy_text

        return {
            "policy_text": policy_text,
            "clean_answer": updated_answer,
        }
    except Exception:
        logger.exception("policy_node failed")
        return {"policy_text": None}
