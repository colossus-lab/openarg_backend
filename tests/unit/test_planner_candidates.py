from __future__ import annotations

from app.application.discovery import DiscoveredResource
from app.application.pipeline.connectors.planner_candidates import (
    PlannerCandidate,
    TableCatalogMatch,
    canonical_candidate_key,
    collect_planner_candidates,
    dedupe_candidates,
    planner_candidate_from_catalog_resource,
    planner_candidate_from_serving_resource,
    planner_candidate_from_table_catalog_match,
)
from app.domain.entities.serving import Resource, ServingLayer


def test_serving_resource_normalizes_to_mart_candidate() -> None:
    resource = Resource(
        resource_id="mart::series_economicas",
        title="series_economicas",
        portal=None,
        description="Curated economic series",
        score=0.91,
        layer=ServingLayer.MART,
    )

    candidate = planner_candidate_from_serving_resource(resource)

    assert candidate.kind == "mart"
    assert candidate.layer == "mart"
    assert candidate.queryability == "direct_sql"
    assert candidate.resource_id == "mart::series_economicas"
    assert candidate.source == "serving"
    assert candidate.base_score == 0.91


def test_table_catalog_match_normalizes_legacy_table() -> None:
    match = TableCatalogMatch(
        table_name="cache_budget_sample",
        display_name="Budget Sample",
        description="National budget sample",
        row_count=100,
        score=0.77,
    )

    candidate = planner_candidate_from_table_catalog_match(match)

    assert candidate.kind == "legacy_table"
    assert candidate.layer == "cache_legacy"
    assert candidate.queryability == "direct_sql"
    assert candidate.table_name == "cache_budget_sample"
    assert candidate.source == "table_catalog"


def test_catalog_resource_ready_raw_normalizes_to_direct_sql() -> None:
    resource = DiscoveredResource(
        resource_identity="energia::foo",
        portal="energia",
        source_id="foo",
        canonical_title="Energia Foo",
        display_name="Energia Foo",
        materialization_status="ready",
        materialized_table_name="raw.energia_foo_v2",
        resource_kind="dataset",
        score=0.63,
    )

    candidate = planner_candidate_from_catalog_resource(resource)

    assert candidate.kind == "raw"
    assert candidate.layer == "raw"
    assert candidate.queryability == "direct_sql"
    assert candidate.table_name == "raw.energia_foo_v2"


def test_catalog_resource_pending_normalizes_to_logical_only() -> None:
    resource = DiscoveredResource(
        resource_identity="indec::ipc",
        portal="indec",
        source_id="ipc",
        canonical_title="IPC",
        display_name="Indice de precios",
        materialization_status="pending",
        materialized_table_name=None,
        resource_kind="dataset",
        score=0.52,
    )

    candidate = planner_candidate_from_catalog_resource(resource)

    assert candidate.kind == "logical_resource"
    assert candidate.layer == "logical"
    assert candidate.queryability == "not_queryable_yet"
    assert candidate.table_name is None


def test_dedupe_prefers_more_queryable_and_more_curated_candidate() -> None:
    low_value = PlannerCandidate(
        candidate_id="resource:presupuesto::credito",
        kind="logical_resource",
        layer="logical",
        title="Credito",
        description="Logical only",
        portal="presupuesto",
        resource_id="presupuesto::credito",
        table_name=None,
        queryability="not_queryable_yet",
        base_score=0.95,
        source="catalog_resources",
    )
    high_value = PlannerCandidate(
        candidate_id="resource:presupuesto::credito",
        kind="mart",
        layer="mart",
        title="presupuesto_consolidado",
        description="Curated mart",
        portal="presupuesto",
        resource_id="presupuesto::credito",
        table_name="mart.presupuesto_consolidado",
        queryability="direct_sql",
        base_score=0.50,
        source="serving",
    )

    deduped = dedupe_candidates([low_value, high_value])

    assert len(deduped) == 1
    assert deduped[0] == high_value


def test_collect_candidates_orders_queryable_mart_before_raw_and_logical() -> None:
    serving = [
        Resource(
            resource_id="mart::series_economicas",
            title="series_economicas",
            score=0.40,
            layer=ServingLayer.MART,
        )
    ]
    table_catalog = [
        TableCatalogMatch(
            table_name="raw.energia_foo_v2",
            display_name="Energia Foo",
            description="Raw energy table",
            row_count=50,
            score=0.95,
        )
    ]
    logical = [
        DiscoveredResource(
            resource_identity="indec::ipc",
            portal="indec",
            source_id="ipc",
            canonical_title="IPC",
            display_name="IPC",
            materialization_status="pending",
            materialized_table_name=None,
            resource_kind="dataset",
            score=0.99,
        )
    ]

    candidates = collect_planner_candidates(
        serving_resources=serving,
        table_catalog_matches=table_catalog,
        catalog_resources=logical,
    )

    assert [c.kind for c in candidates] == ["mart", "raw", "logical_resource"]


def test_canonical_candidate_key_prefers_resource_identity() -> None:
    candidate = PlannerCandidate(
        candidate_id="x",
        kind="raw",
        layer="raw",
        title="foo",
        description="",
        portal="energia",
        resource_id="energia::foo",
        table_name="raw.energia_foo_v2",
        queryability="direct_sql",
        base_score=0.1,
        source="serving",
    )

    assert canonical_candidate_key(candidate) == "resource:energia::foo"
