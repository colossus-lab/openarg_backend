"""Serving-layer DTOs (stable contract between LangGraph pipeline and data layers).

Per MASTERPLAN.md, the pipeline only ever sees these DTOs — never raw `cache_*`,
`staging_*`, or `mart_*` table names. The Serving Port adapter chooses which
underlying physical layer to read from.
"""

from app.domain.entities.serving.serving_dtos import (
    CatalogEntry,
    Resource,
    Rows,
    Schema,
    ServingLayer,
)

__all__ = ["CatalogEntry", "Resource", "Rows", "Schema", "ServingLayer"]
