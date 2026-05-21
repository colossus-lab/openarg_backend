"""DTOs returned by the Serving Port.

These types are the *only* shape the LangGraph pipeline sees. They are stable
across the medallion migration: when the adapter switches from `cache_*` to
`staging_*` to `mart_*`, the DTOs stay the same.

Adding a field here is a breaking change for the pipeline; treat with care.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class ServingLayer(StrEnum):
    """Physical layer that backed a particular Serving Port response.

    Surfaced to callers (and metrics) so we can measure mart coverage and
    detect regressions where the pipeline keeps falling back to raw.
    """

    RAW = "raw"
    STAGING = "staging"
    MART = "mart"
    CONNECTOR_LIVE_API = "connector_live_api"
    CACHE_LEGACY = "cache_legacy"


@dataclass(frozen=True)
class Schema:
    """Schema of a queryable resource.

    `columns` is ordered. `column_types` keys must be a subset of `columns`.
    `semantics` is a free-form map from column name to a human description
    (sourced from `COMMENT ON COLUMN` for marts, contract metadata for staging).
    """

    columns: list[str]
    column_types: dict[str, str]
    semantics: dict[str, str] = field(default_factory=dict)
    primary_key: list[str] = field(default_factory=list)
    row_count_estimate: int | None = None
    layer: ServingLayer = ServingLayer.CACHE_LEGACY


@dataclass(frozen=True)
class Rows:
    """Result of a query.

    `columns` matches the SELECT shape, not the underlying table schema. `data`
    is a list of rows where each row is a list (not a dict) for memory.
    """

    columns: list[str]
    data: list[list[Any]]
    truncated: bool = False
    layer: ServingLayer = ServingLayer.CACHE_LEGACY


@dataclass(frozen=True)
class Resource:
    """A discoverable consultable resource.

    Returned by `IServingPort.discover()`. `resource_id` is the stable identity
    used in subsequent `get_schema()` / `query()` / `explain()` calls.
    """

    resource_id: str
    title: str
    domain: str | None = None
    subdomain: str | None = None
    portal: str | None = None
    description: str | None = None
    score: float | None = None
    layer: ServingLayer = ServingLayer.CACHE_LEGACY


@dataclass(frozen=True)
class CatalogEntry:
    """Detailed metadata for a resource.

    Returned by `IServingPort.explain()`. Carries all the semantic hints the
    planner needs to decide whether this resource answers the user's question.
    """

    resource: Resource
    schema: Schema
    sample_queries: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    parser_version: str | None = None
    last_refreshed_at: str | None = None
