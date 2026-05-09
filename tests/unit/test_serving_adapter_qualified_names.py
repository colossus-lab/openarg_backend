"""Unit tests for `LegacyServingAdapter` name handling and mart serving.

The adapter must transparently accept three shapes that appear in
`catalog_resources.materialized_table_name`:
  - `raw."<bare>"`  (Phase 1.5 raw layer, quoted bare name)
  - `<schema>.<bare>`  (generic qualified)
  - `<bare>`  (legacy unqualified — defaults to `public`)
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.domain.entities.serving import Schema, ServingLayer
from app.domain.ports.serving.serving_port import QueryResourceMismatchError
from app.infrastructure.adapters.serving.legacy_serving_adapter import (
    LegacyServingAdapter,
    _parse_qualified_name,
    _sql_references_relation,
)


def test_legacy_unqualified_defaults_to_public() -> None:
    schema, bare = _parse_qualified_name("cache_caba_test")
    assert schema == "public"
    assert bare == "cache_caba_test"


def test_quoted_raw_qualified() -> None:
    schema, bare = _parse_qualified_name('raw."caba__test__abcd1234__v1"')
    assert schema == "raw"
    assert bare == "caba__test__abcd1234__v1"


def test_unquoted_raw_qualified() -> None:
    schema, bare = _parse_qualified_name("raw.caba__test__abcd1234__v1")
    assert schema == "raw"
    assert bare == "caba__test__abcd1234__v1"


def test_other_schema_qualified() -> None:
    schema, bare = _parse_qualified_name("staging.foo_bar")
    assert schema == "staging"
    assert bare == "foo_bar"


def test_empty_input_safe() -> None:
    schema, bare = _parse_qualified_name("")
    assert schema == "public"
    assert bare == ""


def test_whitespace_stripped() -> None:
    schema, bare = _parse_qualified_name("  raw.foo  ")
    assert schema == "raw"
    assert bare == "foo"


def test_sql_reference_match_accepts_public_unqualified() -> None:
    assert _sql_references_relation(
        "SELECT * FROM cache_demo",
        schema_name="public",
        bare_name="cache_demo",
    )


def test_sql_reference_match_requires_schema_for_raw() -> None:
    assert _sql_references_relation(
        'SELECT * FROM raw."cache_demo__v1"',
        schema_name="raw",
        bare_name="cache_demo__v1",
    )
    assert not _sql_references_relation(
        'SELECT * FROM "cache_demo__v1"',
        schema_name="raw",
        bare_name="cache_demo__v1",
    )


@pytest.mark.asyncio
async def test_query_uses_resource_schema_layer() -> None:
    mart_def = SimpleNamespace(mart_schema="mart", mart_view_name="series_economicas")
    row_cursor = SimpleNamespace(
        fetchmany=lambda _n: [(1,)],
        keys=lambda: ["valor"],
    )
    conn = AsyncMock()
    conn.execute.side_effect = [SimpleNamespace(fetchone=lambda: mart_def), None, row_cursor]
    engine = MagicMock()
    engine.connect.return_value.__aenter__.return_value = conn

    adapter = LegacyServingAdapter(engine)
    adapter.get_schema = AsyncMock(  # type: ignore[method-assign]
        return_value=Schema(columns=["valor"], column_types={"valor": "integer"}, layer=ServingLayer.MART)
    )

    rows = await adapter.query(
        "mart::series_economicas", "SELECT * FROM mart.series_economicas"
    )

    adapter.get_schema.assert_awaited_once_with("mart::series_economicas")
    assert rows.layer == ServingLayer.MART
    assert rows.columns == ["valor"]
    assert rows.data == [[1]]


@pytest.mark.asyncio
async def test_explain_mart_reads_mart_definitions() -> None:
    row = SimpleNamespace(
        mart_id="series_economicas",
        description="Series económicas consolidadas",
        domain="economia",
        yaml_version="v1",
        updated_at="2026-05-05T00:00:00Z",
    )
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=SimpleNamespace(fetchone=lambda: row))
    engine = MagicMock()
    engine.connect.return_value.__aenter__.return_value = conn

    adapter = LegacyServingAdapter(engine)
    adapter.get_schema = AsyncMock(  # type: ignore[method-assign]
        return_value=Schema(columns=["valor"], column_types={"valor": "integer"}, layer=ServingLayer.MART)
    )

    entry = await adapter.explain("mart::series_economicas")

    adapter.get_schema.assert_awaited_once_with("mart::series_economicas")
    assert entry.resource.resource_id == "mart::series_economicas"
    assert entry.resource.layer == ServingLayer.MART
    assert entry.resource.domain == "economia"
    assert entry.parser_version == "v1"


@pytest.mark.asyncio
async def test_get_schema_accepts_raw_resource_id() -> None:
    rows = [
        SimpleNamespace(column_name="nombre", data_type="text"),
        SimpleNamespace(column_name="edad", data_type="integer"),
    ]
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=SimpleNamespace(fetchall=lambda: rows))
    engine = MagicMock()
    engine.connect.return_value.__aenter__.return_value = conn

    adapter = LegacyServingAdapter(engine)
    schema = await adapter.get_schema("raw::caba__padron__abcd1234__v1")

    assert schema.layer == ServingLayer.RAW
    assert schema.columns == ["nombre", "edad"]
    assert schema.column_types == {"nombre": "text", "edad": "integer"}


@pytest.mark.asyncio
async def test_query_rejects_relation_mismatch() -> None:
    adapter = LegacyServingAdapter(MagicMock())
    adapter.get_schema = AsyncMock(  # type: ignore[method-assign]
        return_value=Schema(columns=["valor"], column_types={"valor": "integer"}, layer=ServingLayer.RAW)
    )
    adapter._expected_relation_for_resource = AsyncMock(  # type: ignore[attr-defined]
        return_value=("raw", "caba__padron__abcd1234__v1")
    )

    with pytest.raises(QueryResourceMismatchError):
        await adapter.query("raw::caba__padron__abcd1234__v1", "SELECT * FROM raw.otra_tabla")


# ---------- DEBT-016-003 regression coverage ----------
# Closing the three "dormant bugs" documented in spec 016 §8 DEBT-016-003.
# Each test below pins the post-fix behaviour so a regression in the
# adapter or the NL2SQL routing trips a unit failure.


@pytest.mark.asyncio
async def test_debt_016_003_bug1_legacy_layer_propagated() -> None:
    """`query()` for a public/legacy resource MUST report `CACHE_LEGACY`,
    NOT a hardcoded value. Pinned counterpart of the MART case in
    `test_query_uses_resource_schema_layer`."""
    row_cursor = SimpleNamespace(
        fetchmany=lambda _n: [(42,)],
        keys=lambda: ["valor"],
    )
    conn = AsyncMock()
    conn.execute.side_effect = [None, row_cursor]
    engine = MagicMock()
    engine.connect.return_value.__aenter__.return_value = conn

    adapter = LegacyServingAdapter(engine)
    adapter._expected_relation_for_resource = AsyncMock(  # type: ignore[attr-defined]
        return_value=("public", "cache_indec_ipc")
    )
    adapter.get_schema = AsyncMock(  # type: ignore[method-assign]
        return_value=Schema(
            columns=["valor"],
            column_types={"valor": "integer"},
            layer=ServingLayer.CACHE_LEGACY,
        )
    )

    rows = await adapter.query(
        "indec::ipc-aperturas",
        "SELECT * FROM cache_indec_ipc",
    )

    assert rows.layer == ServingLayer.CACHE_LEGACY
    assert rows.data == [[42]]


@pytest.mark.asyncio
async def test_debt_016_003_bug2_explain_mart_short_circuits() -> None:
    """`explain('mart::<id>')` MUST branch to `mart_definitions` BEFORE
    touching `catalog_resources`. Spec 016 originally raised
    `ResourceNotFoundError` because it always queried catalog_resources
    first — this test pins the fix.

    Asserts the SQL fired against the engine references `mart_definitions`,
    NOT `catalog_resources`.
    """
    captured_sqls: list[str] = []

    async def _execute(stmt, *args, **kwargs):
        captured_sqls.append(str(stmt))
        return SimpleNamespace(
            fetchone=lambda: SimpleNamespace(
                mart_id="bcra_principales_indicadores",
                description="BCRA",
                domain="economia",
                yaml_version="v0.1",
                updated_at="2026-05-09",
            )
        )

    conn = AsyncMock()
    conn.execute = _execute
    engine = MagicMock()
    engine.connect.return_value.__aenter__.return_value = conn

    adapter = LegacyServingAdapter(engine)
    adapter.get_schema = AsyncMock(  # type: ignore[method-assign]
        return_value=Schema(
            columns=["valor"],
            column_types={"valor": "integer"},
            layer=ServingLayer.MART,
        )
    )

    entry = await adapter.explain("mart::bcra_principales_indicadores")

    assert entry.resource.layer == ServingLayer.MART
    assert any("mart_definitions" in s for s in captured_sqls), captured_sqls
    assert not any("catalog_resources" in s for s in captured_sqls), (
        "explain(mart::*) leaked into catalog_resources — DEBT-016-003 "
        "regression"
    )


# ---------- DEBT-016-001 — HNSW vector search wiring (2026-05-09) ----------


@pytest.mark.asyncio
async def test_discover_marts_uses_hnsw_when_embedding_provided() -> None:
    """When `query_embedding` is passed, `_discover_marts` MUST use HNSW
    cosine similarity over `mart_definitions.embedding` and bypass the
    lexical word-level ILIKE path.
    """
    captured_sqls: list[str] = []

    async def _execute(stmt, *args, **kwargs):
        captured_sqls.append(str(stmt))
        return SimpleNamespace(
            fetchall=lambda: [
                SimpleNamespace(
                    mart_id="bcra_principales_indicadores",
                    description="BCRA",
                    domain="economia",
                    mart_schema="mart",
                    mart_view_name="bcra_principales_indicadores",
                    sim=0.85,
                )
            ]
        )

    conn = AsyncMock()
    conn.execute = _execute
    engine = MagicMock()
    engine.connect.return_value.__aenter__.return_value = conn

    adapter = LegacyServingAdapter(engine)
    fake_embedding = [0.1] * 1024

    results = await adapter._discover_marts(
        "evolución reservas BCRA",
        limit=5,
        domain=None,
        portal=None,
        query_embedding=fake_embedding,
    )

    assert len(results) == 1
    assert results[0].resource_id == "mart::bcra_principales_indicadores"
    # The vector path SQL must reference embedding HNSW operator `<=>`
    assert any("embedding <=> CAST" in s for s in captured_sqls), captured_sqls
    # And MUST NOT fall back to ILIKE when vector returned rows
    assert not any("ILIKE" in s for s in captured_sqls), (
        "vector path returned rows but ILIKE fired anyway — wasteful fallback"
    )


@pytest.mark.asyncio
async def test_discover_marts_falls_back_to_ilike_without_embedding() -> None:
    """Without `query_embedding`, _discover_marts uses the lexical path
    (word-level ILIKE) — preserves backward compat.
    """
    captured_sqls: list[str] = []

    async def _execute(stmt, *args, **kwargs):
        captured_sqls.append(str(stmt))
        return SimpleNamespace(fetchall=lambda: [])

    conn = AsyncMock()
    conn.execute = _execute
    engine = MagicMock()
    engine.connect.return_value.__aenter__.return_value = conn

    adapter = LegacyServingAdapter(engine)
    await adapter._discover_marts(
        "compras y contrataciones",
        limit=5,
        domain=None,
        portal=None,
        query_embedding=None,
    )
    # No vector SQL fired
    assert not any("embedding <=>" in s for s in captured_sqls)
    # Lexical path fired
    assert any("ILIKE" in s for s in captured_sqls), captured_sqls
