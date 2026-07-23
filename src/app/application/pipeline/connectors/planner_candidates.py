"""Planner candidate normalization for multi-source discovery.

This module does NOT run reranking. It defines the common contract that
future planner discovery stages can use before applying any LLM-based
reordering.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.application.discovery import DiscoveredResource
from app.domain.entities.serving import Resource, ServingLayer


@dataclass(frozen=True)
class PlannerCandidate:
    candidate_id: str
    kind: str
    layer: str
    title: str
    description: str
    portal: str | None
    resource_id: str | None
    table_name: str | None
    queryability: str
    base_score: float
    source: str


@dataclass(frozen=True)
class TableCatalogMatch:
    table_name: str
    display_name: str
    description: str
    row_count: int
    score: float


_QUERYABILITY_PRIORITY = {
    "direct_sql": 0,
    "requires_connector": 1,
    "not_queryable_yet": 2,
}

_KIND_PRIORITY = {
    "mart": 0,
    "staging": 1,
    "raw": 2,
    "legacy_table": 3,
    "logical_resource": 4,
    "live_api": 5,
}


def _infer_kind_and_layer_from_table_name(table_name: str) -> tuple[str, str]:
    if table_name.startswith("mart."):
        return "mart", "mart"
    if table_name.startswith("staging."):
        return "staging", "staging"
    if table_name.startswith("raw."):
        return "raw", "raw"
    return "legacy_table", "cache_legacy"


def _infer_catalog_queryability(
    resource: DiscoveredResource,
) -> tuple[str, str, str, str | None]:
    materialized_name = resource.materialized_table_name or None
    if resource.materialization_status == "ready" and materialized_name:
        kind, layer = _infer_kind_and_layer_from_table_name(materialized_name)
        return kind, layer, "direct_sql", materialized_name
    if resource.materialization_status == "live_api":
        return "live_api", "connector_live_api", "requires_connector", None
    return "logical_resource", "logical", "not_queryable_yet", materialized_name


def planner_candidate_from_serving_resource(resource: Resource) -> PlannerCandidate:
    kind_by_layer = {
        ServingLayer.MART: "mart",
        ServingLayer.STAGING: "staging",
        ServingLayer.RAW: "raw",
        ServingLayer.CACHE_LEGACY: "legacy_table",
        ServingLayer.CONNECTOR_LIVE_API: "live_api",
    }
    kind = kind_by_layer.get(resource.layer, "logical_resource")
    queryability = (
        "requires_connector" if resource.layer == ServingLayer.CONNECTOR_LIVE_API else "direct_sql"
    )
    return PlannerCandidate(
        candidate_id=f"resource:{resource.resource_id}",
        kind=kind,
        layer=resource.layer.value,
        title=resource.title or resource.resource_id,
        description=resource.description or "",
        portal=resource.portal,
        resource_id=resource.resource_id,
        table_name=None,
        queryability=queryability,
        base_score=float(resource.score or 0.0),
        source="serving",
    )


def planner_candidate_from_table_catalog_match(
    match: TableCatalogMatch,
) -> PlannerCandidate:
    kind, layer = _infer_kind_and_layer_from_table_name(match.table_name)
    title = match.display_name or match.table_name
    return PlannerCandidate(
        candidate_id=f"table:{match.table_name}",
        kind=kind,
        layer=layer,
        title=title,
        description=match.description or "",
        portal=None,
        resource_id=None,
        table_name=match.table_name,
        queryability="direct_sql",
        base_score=float(match.score),
        source="table_catalog",
    )


def planner_candidate_from_catalog_resource(
    resource: DiscoveredResource,
) -> PlannerCandidate:
    kind, layer, queryability, materialized_name = _infer_catalog_queryability(resource)
    title = resource.display_name or resource.canonical_title or resource.resource_identity
    description = ""
    if resource.display_name and resource.canonical_title:
        if resource.display_name != resource.canonical_title:
            description = resource.canonical_title
    return PlannerCandidate(
        candidate_id=f"resource:{resource.resource_identity}",
        kind=kind,
        layer=layer,
        title=title,
        description=description,
        portal=resource.portal,
        resource_id=resource.resource_identity,
        table_name=materialized_name,
        queryability=queryability,
        base_score=float(resource.score or 0.0),
        source="catalog_resources",
    )


def canonical_candidate_key(candidate: PlannerCandidate) -> str:
    if candidate.resource_id:
        return f"resource:{candidate.resource_id.lower()}"
    if candidate.table_name:
        return f"table:{candidate.table_name.lower()}"
    return f"{candidate.source}:{candidate.candidate_id.lower()}"


def candidate_sort_key(candidate: PlannerCandidate) -> tuple[int, int, float, str]:
    return (
        _QUERYABILITY_PRIORITY.get(candidate.queryability, 99),
        _KIND_PRIORITY.get(candidate.kind, 99),
        -float(candidate.base_score),
        candidate.title.lower(),
    )


def sort_planner_candidates(
    candidates: list[PlannerCandidate],
) -> list[PlannerCandidate]:
    return sorted(candidates, key=candidate_sort_key)


def dedupe_candidates(candidates: list[PlannerCandidate]) -> list[PlannerCandidate]:
    ordered = sort_planner_candidates(candidates)
    seen: set[str] = set()
    out: list[PlannerCandidate] = []
    for candidate in ordered:
        key = canonical_candidate_key(candidate)
        if key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out


def limit_candidates(candidates: list[PlannerCandidate], *, limit: int) -> list[PlannerCandidate]:
    if limit < 1:
        return []
    return candidates[:limit]


def collect_planner_candidates(
    *,
    serving_resources: list[Resource] | None = None,
    table_catalog_matches: list[TableCatalogMatch] | None = None,
    catalog_resources: list[DiscoveredResource] | None = None,
    limit: int | None = None,
) -> list[PlannerCandidate]:
    combined: list[PlannerCandidate] = []
    for resource in serving_resources or []:
        combined.append(planner_candidate_from_serving_resource(resource))
    for match in table_catalog_matches or []:
        combined.append(planner_candidate_from_table_catalog_match(match))
    for resource in catalog_resources or []:
        combined.append(planner_candidate_from_catalog_resource(resource))
    deduped = dedupe_candidates(combined)
    if limit is None:
        return deduped
    return limit_candidates(deduped, limit=limit)
