"""LangGraph node: classify the incoming request."""

from __future__ import annotations

import logging
import time

from app.application.pipeline.classifiers import classify_request
from app.application.pipeline.state import OpenArgState

logger = logging.getLogger(__name__)


async def classify_node(state: OpenArgState) -> dict:
    """Classify the request as casual/meta/injection/educational/off_topic or data query.

    Sets *classification* and *classification_response* when the request
    does NOT need the full pipeline (greeting, meta, injection, etc.).
    Both remain ``None`` when the request should proceed to data retrieval.
    """
    try:
        question = state["question"]
        user_id = state["user_id"]
        cls_type, cls_text = classify_request(question, user_id)
        return {
            "classification": cls_type,
            "classification_response": cls_text,
            "_start_time": time.monotonic(),
        }
    except Exception:
        logger.exception("classify_node failed")
        return {
            "classification": None,
            "classification_response": None,
        }
