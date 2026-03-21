"""Conditional edge routing functions for the LangGraph pipeline."""

from __future__ import annotations

from langgraph.graph import END

from app.application.pipeline.state import OpenArgState


def route_after_classify(state: OpenArgState) -> str:
    """After classification: fast-reply or continue to cache check."""
    if state.get("classification") is not None:
        return "fast_reply"
    return "cache_check"


def route_after_cache(state: OpenArgState) -> str:
    """After cache check: return cached result or continue to memory."""
    if state.get("cached_result") is not None:
        return "cache_reply"
    return "load_memory"


def route_after_plan(state: OpenArgState) -> str:
    """After planner: clarification, or continue to execution."""
    plan = state.get("plan")
    if plan and plan.intent == "clarification":
        return "clarify_reply"
    return "inject_fallbacks"


def route_after_analysis(state: OpenArgState) -> str:
    """After analysis: policy analysis or finalize."""
    if state.get("policy_mode"):
        return "policy"
    return "finalize"


def route_fast_reply(_state: OpenArgState) -> str:
    """Fast reply always terminates."""
    return END


def route_cache_reply(_state: OpenArgState) -> str:
    """Cache reply always terminates."""
    return END


def route_clarify_reply(_state: OpenArgState) -> str:
    """Clarification reply always terminates."""
    return END
