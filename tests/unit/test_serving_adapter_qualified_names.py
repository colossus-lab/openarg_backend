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
from app.infrastructure.adapters.serving.legacy_serving_adapter import (
    LegacyServingAdapter,
    _parse_qualified_name,
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


@pytest.mark.asyncio
async def test_query_uses_resource_schema_layer() -> None:
    row_cursor = SimpleNamespace(
        fetchmany=lambda _n: [(1,)],
        keys=lambda: ["valor"],
    )
    conn = AsyncMock()
    conn.execute.side_effect = [None, row_cursor]
    engine = MagicMock()
    engine.connect.return_value.__aenter__.return_value = conn

    adapter = LegacyServingAdapter(engine)
    adapter.get_schema = AsyncMock(  # type: ignore[method-assign]
        return_value=Schema(columns=["valor"], column_types={"valor": "integer"}, layer=ServingLayer.MART)
    )

    rows = await adapter.query("mart::series_economicas", "SELECT 1")

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
