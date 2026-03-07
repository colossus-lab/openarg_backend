from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.infrastructure.adapters.connectors.dataset_index import (
    TAXONOMY,
    resolve_hints,
    resolve_taxonomy_context,
)

router = APIRouter(prefix="/taxonomy", tags=["taxonomy"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class RoutingHintSchema(BaseModel):
    action: str
    params: dict
    confidence: float
    description: str


class TaxonomyHintsResponse(BaseModel):
    query: str
    hints: list[RoutingHintSchema]
    taxonomy_context: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("")
async def get_taxonomy() -> dict:
    """Return the full taxonomy: 6 domains x 34 categories with labels, actions, and cache_patterns."""
    return TAXONOMY


@router.get("/hints", response_model=TaxonomyHintsResponse)
async def get_taxonomy_hints(
    q: str = Query(..., min_length=1, description="User query in natural language"),
) -> TaxonomyHintsResponse:
    """Resolve routing hints and taxonomy context for a given query."""
    hints = resolve_hints(q)
    taxonomy_context = resolve_taxonomy_context(q)

    return TaxonomyHintsResponse(
        query=q,
        hints=[RoutingHintSchema(**asdict(h)) for h in hints],
        taxonomy_context=taxonomy_context,
    )
