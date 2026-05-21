"""LangGraph node: terminal node for fast (non-data) replies."""

from __future__ import annotations

import logging
import time

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.history import record_terminal_analytics
from app.application.pipeline.state import OpenArgState

logger = logging.getLogger(__name__)


async def fast_reply_node(state: OpenArgState) -> dict:
    """Build the final answer from the classification response.

    This is a terminal node — the graph ends here when the request was
    classified as casual, meta, injection, educational, or off-topic.
    """
    classification = state.get("classification", "")
    answer = state.get("classification_response", "")

    intent = ""
    if classification == "injection":
        intent = "injection_blocked"
    elif classification == "off_topic":
        intent = "off_topic"
    elif classification == "internal_table":
        intent = "internal_table_blocked"

    # BUG-016: the classifier always returns text, but guard anyway so an
    # empty answer never ships silently.
    if not answer or not answer.strip():
        logger.error("fast_reply_node: empty answer for classification=%r", classification)
        answer = "¿Sobre qué dato público argentino querés que te ayude?"

    # BUG-016/017: log every terminal exit to query_analytics (best-effort —
    # telemetry must never break the response path).
    try:
        deps = nodes_pkg.get_deps()
        duration_ms = int((time.monotonic() - state.get("_start_time", time.monotonic())) * 1000)
        await record_terminal_analytics(
            question=state.get("question", ""),
            served_table=None,
            row_count=0,
            success=classification not in ("injection", "off_topic", "internal_table"),
            duration_ms=duration_ms,
            error_message=None,
            semantic_cache=deps.semantic_cache,
        )
    except Exception:
        logger.debug("fast_reply_node: analytics logging skipped", exc_info=True)

    return {
        "clean_answer": answer,
        "sources": [],
        "plan_intent": intent,
        "chart_data": None,
        "confidence": 1.0,
        "citations": [],
        "documents": None,
        "tokens_used": 0,
        "warnings": [],
    }
