"""LangGraph node: terminal node for fast (non-data) replies."""

from __future__ import annotations

import logging

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
