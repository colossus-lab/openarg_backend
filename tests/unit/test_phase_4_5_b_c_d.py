"""Unit tests for MASTERPLAN Fase 4.5 b/c/d.

Covers:
  - 4.5b: `_mart_semantics_block` enriches the NL2SQL context with
          per-column semantics for mart tables.
  - 4.5c: `detect_serving_layer_in_sql` reports the medallion layer a
          query touches (used for layer-coverage metrics).
  - 4.5d: `_resolve_resource_identity_for_table` looks up the canonical
          key from `cached_datasets`, and
          `_project_enrichment_to_catalog_resources` UPSERTs the LLM-
          generated metadata into `catalog_resources`.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.application.pipeline.connectors.sandbox import (
    _mart_semantics_block,
    detect_serving_layer_in_sql,
)
from app.domain.entities.serving import Schema, ServingLayer


# ── 4.5b: mart semantics block ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_mart_semantics_block_returns_descriptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.get_schema.return_value = Schema(
        columns=["fecha", "valor"],
        column_types={"fecha": "date", "valor": "float"},
        semantics={
            "fecha": "Fecha de la observación",
            "valor": "Valor numérico de la serie",
        },
        layer=ServingLayer.MART,
    )
    block = await _mart_semantics_block(port, ["series_economicas"])
    assert "SEMÁNTICA DE COLUMNAS — series_economicas:" in block
    assert "fecha: Fecha de la observación" in block
    assert "valor: Valor numérico de la serie" in block


@pytest.mark.asyncio
async def test_mart_semantics_block_skips_non_mart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.get_schema.return_value = Schema(
        columns=["x"],
        column_types={"x": "text"},
        semantics={"x": "ignored"},
        layer=ServingLayer.CACHE_LEGACY,
    )
    block = await _mart_semantics_block(port, ["cache_legacy_table"])
    assert block == ""


@pytest.mark.asyncio
async def test_mart_semantics_block_handles_resolver_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.get_schema.side_effect = RuntimeError("not found")
    block = await _mart_semantics_block(port, ["nonexistent"])
    assert block == ""


@pytest.mark.asyncio
async def test_mart_semantics_block_empty_when_flag_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "0")
    port = AsyncMock()
    block = await _mart_semantics_block(port, ["any"])
    assert block == ""


# ── 4.5c: layer detection in SQL ─────────────────────────────────────────


def test_detect_serving_layer_mart() -> None:
    sql = "SELECT * FROM mart.presupuesto_consolidado WHERE anio = 2024"
    assert detect_serving_layer_in_sql(sql) == "mart"


def test_detect_serving_layer_staging() -> None:
    sql = "SELECT * FROM staging.presupuesto__abc"
    assert detect_serving_layer_in_sql(sql) == "staging"


def test_detect_serving_layer_raw() -> None:
    sql = 'SELECT * FROM raw."foo__v1"'
    assert detect_serving_layer_in_sql(sql) == "raw"


def test_detect_serving_layer_legacy() -> None:
    sql = 'SELECT * FROM cache_caba_subte_viajes'
    assert detect_serving_layer_in_sql(sql) == "cache_legacy"


def test_detect_serving_layer_mart_priority() -> None:
    """When SQL touches multiple layers, mart wins (it's the curated one)."""
    sql = """
    SELECT m.fecha, c.value
    FROM mart.series_economicas m
    JOIN cache_legacy_thing c ON c.fecha = m.fecha
    """
    assert detect_serving_layer_in_sql(sql) == "mart"


def test_detect_serving_layer_case_insensitive() -> None:
    sql = "select * from MART.foo"
    assert detect_serving_layer_in_sql(sql) == "mart"


# ── 4.5d: enricher → catalog_resources projection ────────────────────────


def test_resolve_resource_identity_for_table() -> None:
    from app.infrastructure.celery.tasks.catalog_enrichment_tasks import (
        _resolve_resource_identity_for_table,
    )

    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    row = MagicMock()
    row.portal = "datos_gob_ar"
    row.source_id = "dataset-abc"
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    conn.execute.return_value = cursor

    result = _resolve_resource_identity_for_table(engine, "cache_xyz")
    assert result == "datos_gob_ar::dataset-abc"


def test_resolve_resource_identity_returns_none_when_missing() -> None:
    from app.infrastructure.celery.tasks.catalog_enrichment_tasks import (
        _resolve_resource_identity_for_table,
    )

    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    conn.execute.return_value = cursor

    result = _resolve_resource_identity_for_table(engine, "cache_orphan")
    assert result is None


def test_resolve_resource_identity_handles_qualified_name() -> None:
    """For a `raw."<bare>"` input, the resolver should:
      1. First try `raw_table_versions` with (schema=raw, table=<bare>)
         and return `resource_identity` directly when found.
      2. Only fall back to `cached_datasets` -> `datasets` if the rtv
         lookup misses.
    Without the rtv-first hop, raw tables that have no `cached_datasets`
    row (cleanup_invariants registered them under backfill_postauto::*)
    would resolve onto a different dataset that happens to share the
    same bare name in cached_datasets.
    """
    from app.infrastructure.celery.tasks.catalog_enrichment_tasks import (
        _resolve_resource_identity_for_table,
    )

    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    # rtv lookup hits with the canonical resource_identity.
    rtv_row = MagicMock()
    rtv_row.resource_identity = "portal_x::source_id_y"
    rtv_cursor = MagicMock()
    rtv_cursor.fetchone.return_value = rtv_row
    # cd cursor is unused on the happy-path; provide one anyway in case
    # the implementation queries it defensively.
    cd_cursor = MagicMock()
    cd_cursor.fetchone.return_value = None
    conn.execute.side_effect = [rtv_cursor, cd_cursor]

    result = _resolve_resource_identity_for_table(engine, 'raw."foo__v1"')
    assert result == "portal_x::source_id_y"

    # First call must be the rtv query — schema='raw', tn='foo__v1'.
    first_call_kwargs = conn.execute.call_args_list[0][0][1]
    assert first_call_kwargs["sch"] == "raw"
    assert first_call_kwargs["tn"] == "foo__v1"


def test_project_enrichment_skips_when_no_resource_identity() -> None:
    from app.infrastructure.celery.tasks.catalog_enrichment_tasks import (
        _project_enrichment_to_catalog_resources,
    )

    engine = MagicMock()
    with patch(
        "app.infrastructure.celery.tasks.catalog_enrichment_tasks._resolve_resource_identity_for_table",
        return_value=None,
    ):
        # Should not raise — returns silently.
        _project_enrichment_to_catalog_resources(
            engine,
            table_name="cache_orphan",
            metadata={"display_name": "Orphan"},
            row_count=0,
            embedding_str=None,
            quality_score=0.5,
        )
    # No engine.begin() call when we early-out.
    engine.begin.assert_not_called()


def test_project_enrichment_executes_update() -> None:
    from app.infrastructure.celery.tasks.catalog_enrichment_tasks import (
        _project_enrichment_to_catalog_resources,
    )

    engine = MagicMock()
    txn_conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = txn_conn

    with patch(
        "app.infrastructure.celery.tasks.catalog_enrichment_tasks._resolve_resource_identity_for_table",
        return_value="bcra::tasa-x",
    ):
        _project_enrichment_to_catalog_resources(
            engine,
            table_name="cache_bcra_tasa",
            metadata={
                "display_name": "Tasa BCRA",
                "domain": "economia",
                "subdomain": "tasas",
                "tags": ["bcra", "monetario"],
            },
            row_count=1000,
            embedding_str="[0.1,0.2]",
            quality_score=0.9,
        )
    txn_conn.execute.assert_called_once()
    bind_params = txn_conn.execute.call_args[0][1]
    assert bind_params["rid"] == "bcra::tasa-x"
    assert bind_params["dn"] == "Tasa BCRA"
    assert bind_params["dom"] == "economia"
    assert bind_params["tk"] == "bcra"  # taxonomy_key derived from first short tag
    assert bind_params["emb"] == "[0.1,0.2]"
