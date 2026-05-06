"""Unit tests for `ServingResolver` (MASTERPLAN Fase 4).

Covers caching behavior, layer counting for the planner, and the
`OPENARG_PIPELINE_USE_SERVING_PORT` rollback flag.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.application.pipeline.connectors.serving_resolver import (
    ServingResolver,
    serving_port_enabled,
)
from app.domain.entities.serving import (
    CatalogEntry,
    Resource,
    Rows,
    Schema,
    ServingLayer,
)


def _resource(rid: str, layer: ServingLayer = ServingLayer.MART) -> Resource:
    return Resource(
        resource_id=rid, title=rid.replace("_", " "), layer=layer
    )


def _schema(rid: str, layer: ServingLayer = ServingLayer.MART) -> Schema:
    return Schema(
        columns=["a", "b"],
        column_types={"a": "int", "b": "text"},
        layer=layer,
    )


@pytest.mark.asyncio
async def test_discover_proxies_to_port() -> None:
    port = AsyncMock()
    port.discover.return_value = [_resource("mart::x")]
    resolver = ServingResolver(port)
    result = await resolver.discover("query", limit=5)
    port.discover.assert_awaited_once_with(
        "query", limit=5, portal=None, domain=None
    )
    assert len(result) == 1
    assert result[0].resource_id == "mart::x"


@pytest.mark.asyncio
async def test_get_schema_caches() -> None:
    port = AsyncMock()
    port.get_schema.return_value = _schema("mart::x")
    resolver = ServingResolver(port)

    s1 = await resolver.get_schema("mart::x")
    s2 = await resolver.get_schema("mart::x")
    assert s1 is s2  # same object → cache hit
    port.get_schema.assert_awaited_once()  # only called once


@pytest.mark.asyncio
async def test_get_schema_distinguishes_resources() -> None:
    port = AsyncMock()
    port.get_schema.side_effect = [_schema("a"), _schema("b")]
    resolver = ServingResolver(port)
    s1 = await resolver.get_schema("a")
    s2 = await resolver.get_schema("b")
    assert s1 is not s2
    assert port.get_schema.await_count == 2


@pytest.mark.asyncio
async def test_query_proxies_to_port() -> None:
    port = AsyncMock()
    port.query.return_value = Rows(columns=["x"], data=[[1]], layer=ServingLayer.MART)
    resolver = ServingResolver(port)
    result = await resolver.query("rid", "SELECT 1", max_rows=10, timeout_seconds=5)
    port.query.assert_awaited_once_with("rid", "SELECT 1", max_rows=10, timeout_seconds=5)
    assert result.data == [[1]]


@pytest.mark.asyncio
async def test_explain_proxies_to_port() -> None:
    port = AsyncMock()
    port.explain.return_value = CatalogEntry(
        resource=_resource("rid"), schema=_schema("rid")
    )
    resolver = ServingResolver(port)
    result = await resolver.explain("rid")
    port.explain.assert_awaited_once_with("rid")
    assert result.resource.resource_id == "rid"


@pytest.mark.asyncio
async def test_discover_for_planner_counts_layers() -> None:
    port = AsyncMock()
    port.discover.return_value = [
        _resource("mart::a", ServingLayer.MART),
        _resource("mart::b", ServingLayer.MART),
        _resource("rid_x", ServingLayer.RAW),
        _resource("legacy", ServingLayer.CACHE_LEGACY),
    ]
    resolver = ServingResolver(port)
    resources, layer_counts = await resolver.discover_for_planner("test")
    assert len(resources) == 4
    assert layer_counts == {"mart": 2, "raw": 1, "cache_legacy": 1}


@pytest.mark.asyncio
async def test_disabled_flag_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "0")
    port = AsyncMock()
    resolver = ServingResolver(port)
    with pytest.raises(RuntimeError, match="resolver bypassed"):
        await resolver.discover("x")
    with pytest.raises(RuntimeError):
        await resolver.get_schema("x")
    with pytest.raises(RuntimeError):
        await resolver.query("x", "SELECT 1")
    with pytest.raises(RuntimeError):
        await resolver.explain("x")


def test_flag_recognizes_truthy_values(monkeypatch: pytest.MonkeyPatch) -> None:
    for v in ("1", "true", "TRUE", "yes"):
        monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", v)
        assert serving_port_enabled() is True

    for v in ("0", "false", "no", ""):
        monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", v)
        assert serving_port_enabled() is False

    monkeypatch.delenv("OPENARG_PIPELINE_USE_SERVING_PORT", raising=False)
    assert serving_port_enabled() is True  # default on
