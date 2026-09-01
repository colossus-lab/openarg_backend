"""LangGraph nodes: cache check and cache reply."""

from __future__ import annotations

import logging
import time
from typing import Any

from langgraph.config import get_stream_writer

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.cache_manager import check_cache
from app.application.pipeline.history import record_terminal_analytics
from app.application.pipeline.state import OpenArgState

logger = logging.getLogger(__name__)


async def cache_check_node(state: OpenArgState) -> dict:
    """Check Redis and semantic cache for a prior answer.

    Skipped in deep mode (una búsqueda profunda siempre va a datos frescos).
    Sets *cached_result* and *last_embedding* when a hit is found.
    """
    writer = get_stream_writer()
    writer({"type": "status", "step": "cache_check", "detail": "Buscando en caché..."})
    deps = nodes_pkg.get_deps()

    # Una búsqueda profunda siempre va a datos frescos. `bypass_cache` es la
    # misma puerta abierta explícitamente: una corrida de evaluación tiene que
    # ejercitar el pipeline, no el caché. Sin esto la batería sólo sirve una
    # vez — la segunda corrida mide el caché tibio que dejó la primera y da
    # p50 de 17 ms, que no es una medición de nada.
    if state.get("mode") == "deep" or state.get("bypass_cache"):
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

    # BUG-016: a cache hit whose stored answer is empty (or scrubbed to
    # nothing) used to ship "" silently. Substitute a usable message.
    answer_ok = bool(clean_answer and clean_answer.strip())
    if not answer_ok:
        logger.error(
            "cache_reply_node: empty cached answer for question=%r",
            state.get("question", "")[:120],
        )
        clean_answer = "No tengo una respuesta guardada para esa consulta. Probá reformulándola."

    # BUG-016/017: log every terminal exit to query_analytics (best-effort —
    # telemetry must never break the response path).
    try:
        deps = nodes_pkg.get_deps()
        duration_ms = int((time.monotonic() - state.get("_start_time", time.monotonic())) * 1000)
        await record_terminal_analytics(
            question=state.get("question", ""),
            served_table="cache",
            row_count=0,
            success=answer_ok,
            duration_ms=duration_ms,
            error_message=None if answer_ok else "empty_cached_answer",
            semantic_cache=deps.semantic_cache,
        )
    except Exception:
        logger.debug("cache_reply_node: analytics logging skipped", exc_info=True)

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
