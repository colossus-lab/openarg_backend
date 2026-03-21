"""LangGraph node: query preprocessing (acronym/temporal/province/synonym expansion)."""

from __future__ import annotations

import logging

from app.application.pipeline.state import OpenArgState
from app.infrastructure.adapters.search.query_preprocessor import (
    expand_acronyms,
    expand_synonyms,
    normalize_provinces,
    normalize_temporal,
)

logger = logging.getLogger(__name__)


async def preprocess_node(state: OpenArgState) -> dict:
    """Expand acronyms, normalise temporal references, provinces, and synonyms.

    Writes the preprocessed query into *preprocessed_query* for
    downstream consumption by the planner and executor.
    """
    try:
        question = state["question"]
        q = expand_acronyms(question)
        q, _ = normalize_temporal(q)
        q = normalize_provinces(q)
        q = expand_synonyms(q)
        return {"preprocessed_query": q}
    except Exception:
        logger.exception("preprocess_node failed")
        # Fall back to the raw question
        return {"preprocessed_query": state["question"]}
