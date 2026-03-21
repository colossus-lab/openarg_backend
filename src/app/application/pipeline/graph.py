"""Build and compile the LangGraph pipeline for the OpenArg smart query flow.

The graph mirrors the exact flow of ``SmartQueryService.execute()``
in ``smart_query_service.py``:

    START
      -> classify
      -> (fast_reply | cache_check)
      -> (cache_reply | load_memory)
      -> preprocess
      -> planner
      -> (clarify_reply | inject_fallbacks)
      -> execute_steps
      -> analyst
      -> (policy | finalize)
      -> END
"""

from __future__ import annotations

from langgraph.graph import END, START, StateGraph

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.edges import (
    route_after_analysis,
    route_after_cache,
    route_after_classify,
    route_after_plan,
    route_cache_reply,
    route_clarify_reply,
    route_fast_reply,
)
from app.application.pipeline.nodes import PipelineDeps
from app.application.pipeline.nodes.analyst import analyst_node
from app.application.pipeline.nodes.cache import cache_check_node, cache_reply_node
from app.application.pipeline.nodes.classify import classify_node
from app.application.pipeline.nodes.executor import (
    execute_steps_node,
    inject_fallbacks_node,
)
from app.application.pipeline.nodes.fast_reply import fast_reply_node
from app.application.pipeline.nodes.finalize import finalize_node
from app.application.pipeline.nodes.memory import load_memory_node
from app.application.pipeline.nodes.planner import clarify_reply_node, planner_node
from app.application.pipeline.nodes.policy import policy_node
from app.application.pipeline.nodes.preprocess import preprocess_node
from app.application.pipeline.state import OpenArgState


def build_pipeline_graph(deps: PipelineDeps):  # -> CompiledStateGraph
    """Build and compile the LangGraph pipeline.

    Sets the module-level ``_deps`` so that all node functions can
    access shared dependencies (LLM, embedding, connectors, etc.)
    without requiring constructor injection.

    Returns a compiled ``StateGraph`` ready for invocation.
    """
    # Wire up dependency injection for all nodes (ContextVar — request-safe)
    nodes_pkg.set_deps(deps)

    builder = StateGraph(OpenArgState)

    # ── Register nodes ──────────────────────────────────────
    builder.add_node("classify", classify_node)
    builder.add_node("fast_reply", fast_reply_node)
    builder.add_node("cache_check", cache_check_node)
    builder.add_node("cache_reply", cache_reply_node)
    builder.add_node("load_memory", load_memory_node)
    builder.add_node("preprocess", preprocess_node)
    builder.add_node("planner", planner_node)
    builder.add_node("clarify_reply", clarify_reply_node)
    builder.add_node("inject_fallbacks", inject_fallbacks_node)
    builder.add_node("execute_steps", execute_steps_node)
    builder.add_node("analyst", analyst_node)
    builder.add_node("policy", policy_node)
    builder.add_node("finalize", finalize_node)

    # ── Edges ───────────────────────────────────────────────

    # Entry point
    builder.add_edge(START, "classify")

    # After classify: fast_reply (terminal) or cache_check
    builder.add_conditional_edges(
        "classify",
        route_after_classify,
        {"fast_reply": "fast_reply", "cache_check": "cache_check"},
    )

    # fast_reply -> END
    builder.add_conditional_edges(
        "fast_reply",
        route_fast_reply,
        {END: END},
    )

    # After cache_check: cache_reply (terminal) or load_memory
    builder.add_conditional_edges(
        "cache_check",
        route_after_cache,
        {"cache_reply": "cache_reply", "load_memory": "load_memory"},
    )

    # cache_reply -> END
    builder.add_conditional_edges(
        "cache_reply",
        route_cache_reply,
        {END: END},
    )

    # load_memory -> preprocess -> planner (sequential)
    builder.add_edge("load_memory", "preprocess")
    builder.add_edge("preprocess", "planner")

    # After planner: clarify_reply (terminal) or inject_fallbacks
    builder.add_conditional_edges(
        "planner",
        route_after_plan,
        {"clarify_reply": "clarify_reply", "inject_fallbacks": "inject_fallbacks"},
    )

    # clarify_reply -> END
    builder.add_conditional_edges(
        "clarify_reply",
        route_clarify_reply,
        {END: END},
    )

    # inject_fallbacks -> execute_steps -> analyst (sequential)
    builder.add_edge("inject_fallbacks", "execute_steps")
    builder.add_edge("execute_steps", "analyst")

    # After analyst: policy (if policy_mode) or finalize
    builder.add_conditional_edges(
        "analyst",
        route_after_analysis,
        {"policy": "policy", "finalize": "finalize"},
    )

    # policy -> finalize -> END
    builder.add_edge("policy", "finalize")
    builder.add_edge("finalize", END)

    return builder.compile()
