"""Connector: vector search over cached dataset chunks in pgvector."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.domain.ports.llm.llm_provider import IEmbeddingProvider
    from app.domain.ports.search.vector_search import IVectorSearch

logger = logging.getLogger(__name__)


async def execute_search_datasets_step(
    step: PlanStep,
    embedding: IEmbeddingProvider,
    vector_search: IVectorSearch,
) -> list[DataResult]:
    """Hybrid retrieval over cached dataset chunks in pgvector."""
    params = step.params
    query = params.get("query", step.description)
    try:
        q_embedding = await embedding.embed(query)
        vector_results = await vector_search.search_datasets_hybrid(
            q_embedding,
            query,
            limit=params.get("limit", 10),
        )
        if not vector_results:
            return []
        return [
            DataResult(
                source=f"pgvector:{vr.portal}",
                portal_name=vr.portal,
                portal_url=vr.download_url or "",
                dataset_title=vr.title,
                format="json",
                records=[],
                metadata={
                    "total_records": 0,
                    "description": vr.description,
                    "columns": vr.columns,
                    "score": round(vr.score, 3),
                },
            )
            for vr in vector_results
        ]
    except Exception:
        logger.warning("search_datasets step failed", exc_info=True)
        return []
