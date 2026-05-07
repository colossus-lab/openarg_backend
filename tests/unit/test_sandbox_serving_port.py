"""Unit tests for the serving-port path in `discover_catalog_hints_for_planner`
(MASTERPLAN Fase 4.5a).

The new path:
  - When `serving_port` is provided AND `OPENARG_PIPELINE_USE_SERVING_PORT=1`,
    mart/staging hits are surfaced as a planner block ABOVE the legacy
    `table_catalog` block.
  - When `serving_port` is None, behavior is bit-for-bit legacy.
  - When the flag is OFF, the serving block is empty even if the port is
    provided (rollback escape hatch).
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.application.pipeline.connectors.sandbox import (
    _join_hint_blocks,
    _serving_port_planner_hints,
)
from app.domain.entities.serving import Resource, ServingLayer


def _r(rid: str, layer: ServingLayer, title: str = "") -> Resource:
    return Resource(
        resource_id=rid, title=title or rid, layer=layer, domain="test"
    )


# ── _join_hint_blocks ─────────────────────────────────────────────────────


def test_join_hint_blocks_drops_empty() -> None:
    assert _join_hint_blocks("", "x") == "x"
    assert _join_hint_blocks("x", "") == "x"
    assert _join_hint_blocks("a", "b") == "a\n\nb"


def test_join_hint_blocks_drops_whitespace_only() -> None:
    assert _join_hint_blocks("  \n", "x") == "x"


def test_join_hint_blocks_three_args() -> None:
    assert _join_hint_blocks("a", "b", "c") == "a\n\nb\n\nc"


# ── _serving_port_planner_hints ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_marts_appear_first(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.discover.return_value = [
        _r("mart::a", ServingLayer.MART, "Mart A"),
        _r("staging.b", ServingLayer.STAGING, "Staging B"),
        _r("raw.c", ServingLayer.RAW, "Raw C"),
        _r("legacy.c", ServingLayer.CACHE_LEGACY, "Legacy C"),
    ]
    block = await _serving_port_planner_hints("query", port, limit=5)
    # Mart label appears
    assert "MARTS DISPONIBLES" in block
    assert "Mart A" in block
    # Staging label appears
    assert "STAGING" in block
    assert "Staging B" in block
    # Raw label appears
    assert "RAW DISPONIBLE" in block
    assert "Raw C" in block
    # cache_legacy is intentionally suppressed (covered by the legacy block)
    assert "Legacy C" not in block


@pytest.mark.asyncio
async def test_returns_empty_when_no_preferred_layer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.discover.return_value = [
        _r("legacy.a", ServingLayer.CACHE_LEGACY),
    ]
    block = await _serving_port_planner_hints("query", port, limit=5)
    assert block == ""


@pytest.mark.asyncio
async def test_flag_off_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "0")
    port = AsyncMock()
    port.discover.return_value = [_r("mart::a", ServingLayer.MART)]
    block = await _serving_port_planner_hints("query", port, limit=5)
    assert block == ""


@pytest.mark.asyncio
async def test_port_failure_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.discover.side_effect = RuntimeError("DB down")
    block = await _serving_port_planner_hints("query", port, limit=5)
    assert block == ""


@pytest.mark.asyncio
async def test_marts_only_no_staging_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.discover.return_value = [_r("mart::a", ServingLayer.MART, "Mart A")]
    block = await _serving_port_planner_hints("query", port, limit=5)
    assert "MARTS DISPONIBLES" in block
    assert "STAGING" not in block


@pytest.mark.asyncio
async def test_staging_only_no_mart_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.discover.return_value = [_r("staging.x", ServingLayer.STAGING, "S X")]
    block = await _serving_port_planner_hints("query", port, limit=5)
    assert "STAGING" in block
    assert "MARTS DISPONIBLES" not in block


@pytest.mark.asyncio
async def test_raw_only_renders_raw_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.discover.return_value = [_r("raw.a", ServingLayer.RAW, "Raw A")]
    block = await _serving_port_planner_hints("query", port, limit=5)
    assert "RAW DISPONIBLE" in block
    assert "Raw A" in block
    assert "MARTS DISPONIBLES" not in block
    assert "STAGING" not in block


# ── discover_catalog_hints_for_planner integration ────────────────────────


@pytest.mark.asyncio
async def test_discover_catalog_hints_serving_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When sandbox=None, only the serving block appears."""
    from app.application.pipeline.connectors.sandbox import (
        discover_catalog_hints_for_planner,
    )

    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.discover.return_value = [_r("mart::a", ServingLayer.MART, "Mart A")]
    embedding = AsyncMock()
    embedding.embed.return_value = [0.0] * 1024

    result = await discover_catalog_hints_for_planner(
        "query", sandbox=None, embedding=embedding, serving_port=port
    )
    assert "MARTS DISPONIBLES" in result
    assert "Mart A" in result


@pytest.mark.asyncio
async def test_discover_catalog_hints_no_serving_port_falls_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When serving_port=None, no serving block is added — legacy path only."""
    from app.application.pipeline.connectors.sandbox import (
        discover_catalog_hints_for_planner,
    )

    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    embedding = AsyncMock()
    embedding.embed.return_value = [0.0] * 1024

    result = await discover_catalog_hints_for_planner(
        "query", sandbox=None, embedding=embedding, serving_port=None
    )
    # Without sandbox AND without serving_port, the function returns "".
    assert result == ""
