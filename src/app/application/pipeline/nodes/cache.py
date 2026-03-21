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
    """Build the final answer from a cache hit (terminal node)."""
    cached: dict[str, Any] = state.get("cached_result") or {}

    return {
        "clean_answer": cached.get("answer", ""),
        "sources": cached.get("sources", []),
        "chart_data": cached.get("chart_data"),
        "tokens_used": cached.get("tokens_used", 0),
        "documents": cached.get("documents"),
        "plan_intent": "cached",
        "confidence": 1.0,
        "citations": [],
        "warnings": [],
    }
