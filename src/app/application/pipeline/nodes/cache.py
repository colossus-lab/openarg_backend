"""LangGraph nodes: cache check and cache reply."""

from __future__ import annotations

import logging
from typing import Any

from langgraph.config import get_stream_writer

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.cache_manager import check_cache
from app.application.pipeline.state import OpenArgState

logger = logging.getLogger(__name__)


async def cache_check_node(state: OpenArgState) -> dict:
    """Check Redis and semantic cache for a prior answer.

    Skipped when *policy_mode* is True (always fetch fresh data).
    Sets *cached_result* and *last_embedding* when a hit is found.
    """
    writer = get_stream_writer()
    writer({"type": "status", "step": "cache_check", "detail": "Buscando en caché..."})
    deps = nodes_pkg.get_deps()

    # Skip cache in policy mode — always fetch fresh data
    if state.get("policy_mode", False):
        return {
            "cached_result": None,
            "last_embedding": None,
        }

    try:
        question = state["question"]
        user_id = state["user_id"]
        cached_dict, last_embedding = await check_cache(
            question,
            user_id,
            deps.cache,
            deps.embedding,
            deps.semantic_cache,
            deps.metrics,
        )
        return {
            "cached_result": cached_dict,
            "last_embedding": last_embedding,
        }
    except Exception:
        logger.exception("cache_check_node failed")
        return {
            "cached_result": None,
            "last_embedding": None,
        }


async def cache_reply_node(state: OpenArgState) -> dict:
    """Build the final answer from a cache hit (terminal node).

    FIX-011 / FIX-012 defense-in-depth: the analyst scrubs now run on
    every fresh generation, but the semantic cache may still contain
    pre-fix answers (apologetic prefaces, ``cache_*`` leaks, etc).
    Re-applying the scrubs here means even stale cached answers are
    cleaned before reaching the browser, so we never have to
    forcibly flush caches on every deploy.
    """
    # Local import to avoid a circular dependency between nodes.
    from app.application.pipeline.nodes.analyst import (
        _drop_apologetic_preface,
        _scrub_internal_identifiers,
    )

    cached: dict[str, Any] = state.get("cached_result") or {}
    raw_answer = cached.get("answer", "")
    clean_answer = _scrub_internal_identifiers(raw_answer)
    clean_answer = _drop_apologetic_preface(clean_answer)

    return {
        "clean_answer": clean_answer,
        "sources": cached.get("sources", []),
        "chart_data": cached.get("chart_data"),
        "map_data": cached.get("map_data"),
        "tokens_used": cached.get("tokens_used", 0),
        "documents": cached.get("documents"),
        "plan_intent": "cached",
        "confidence": cached.get("confidence", 1.0),
        "citations": cached.get("citations", []),
        "warnings": cached.get("warnings", []),
    }
