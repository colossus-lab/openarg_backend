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
from app.infrastructure.monitoring.metrics import MetricsCollector


def _r(rid: str, layer: ServingLayer, title: str = "") -> Resource:
    return Resource(resource_id=rid, title=title or rid, layer=layer, domain="test")


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


@pytest.mark.asyncio
async def test_raw_and_staging_render_exact_resource_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    port = AsyncMock()
    port.discover.return_value = [
        _r("staging.dataset_x", ServingLayer.STAGING, "Dataset X"),
        _r("raw.portal__dataset_y", ServingLayer.RAW, "Dataset Y"),
    ]
    block = await _serving_port_planner_hints("query", port, limit=5)
    assert "staging.dataset_x (Dataset X)" in block
    assert "raw.portal__dataset_y (Dataset Y)" in block


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


@pytest.mark.asyncio
async def test_discover_catalog_hints_reranks_legacy_block_when_flag_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.application.pipeline.connectors.sandbox import (
        discover_catalog_hints_for_planner,
    )
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    MetricsCollector._instance = None
    monkeypatch.setenv("OPENARG_ENABLE_LLM_RERANKER", "1")
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "0")

    sandbox = PgSandboxAdapter()
    # Use a sync mock connection because sandbox._get_engine() is sync.
    from unittest.mock import MagicMock

    sync_conn = MagicMock()
    sync_conn.execute.side_effect = [
        type(
            "_Rows",
            (),
            {
                "fetchall": lambda self: [
                    type(
                        "_R",
                        (),
                        {
                            "table_name": "cache_ipc",
                            "display_name": "IPC",
                            "description": "Inflación mensual",
                            "row_count": 100,
                            "score": 0.90,
                        },
                    )(),
                    type(
                        "_R",
                        (),
                        {
                            "table_name": "cache_dolar_blue",
                            "display_name": "Dólar Blue",
                            "description": "Cotización diaria",
                            "row_count": 50,
                            "score": 0.70,
                        },
                    )(),
                ]
            },
        )(),
        # 2nd execute = mart vector search (added to discover_catalog_hints
        # to surface marts for the LLM reranker). Returning empty rows keeps
        # this test focused on the legacy table_catalog → reranker path.
        type("_NoMarts", (), {"fetchall": lambda self: []})(),
    ]
    sync_conn.rollback.return_value = None
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = sync_conn
    engine.connect.return_value.__exit__.return_value = False
    sandbox._engine = engine

    embedding = AsyncMock()
    embedding.embed.return_value = [0.0] * 1024
    llm = AsyncMock()
    llm.chat.return_value = type("Resp", (), {"content": "[1, 0]"})()

    result = await discover_catalog_hints_for_planner(
        "dólar blue",
        sandbox=sandbox,
        embedding=embedding,
        serving_port=None,
        llm=llm,
        limit=5,
    )

    first_data_line = next(line for line in result.splitlines() if line.startswith("  - "))
    assert "cache_dolar_blue" in first_data_line

    metrics = MetricsCollector().get_metrics()
    assert metrics["connectors"]["planner_reranker"]["calls"] >= 1
    assert metrics["connectors"]["planner_reranker"]["errors"] == 0


@pytest.mark.asyncio
async def test_discover_catalog_hints_shadow_mode_preserves_base_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.application.pipeline.connectors.sandbox import (
        discover_catalog_hints_for_planner,
    )
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    MetricsCollector._instance = None
    monkeypatch.setenv("OPENARG_ENABLE_LLM_RERANKER", "1")
    monkeypatch.setenv("OPENARG_LLM_RERANKER_SHADOW_MODE", "1")
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "0")

    sandbox = PgSandboxAdapter()
    from unittest.mock import MagicMock

    sync_conn = MagicMock()
    sync_conn.execute.side_effect = [
        type(
            "_Rows",
            (),
            {
                "fetchall": lambda self: [
                    type(
                        "_R",
                        (),
                        {
                            "table_name": "cache_ipc",
                            "display_name": "IPC",
                            "description": "Inflación mensual",
                            "row_count": 100,
                            "score": 0.90,
                        },
                    )(),
                    type(
                        "_R",
                        (),
                        {
                            "table_name": "cache_dolar_blue",
                            "display_name": "Dólar Blue",
                            "description": "Cotización diaria",
                            "row_count": 50,
                            "score": 0.70,
                        },
                    )(),
                ]
            },
        )(),
        # 2nd execute = mart vector search; empty for this shadow-mode test.
        type("_NoMarts", (), {"fetchall": lambda self: []})(),
    ]
    sync_conn.rollback.return_value = None
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = sync_conn
    engine.connect.return_value.__exit__.return_value = False
    sandbox._engine = engine

    embedding = AsyncMock()
    embedding.embed.return_value = [0.0] * 1024
    llm = AsyncMock()
    llm.chat.return_value = type("Resp", (), {"content": "[1, 0]"})()

    result = await discover_catalog_hints_for_planner(
        "dólar blue",
        sandbox=sandbox,
        embedding=embedding,
        serving_port=None,
        llm=llm,
        limit=5,
    )

    first_data_line = next(line for line in result.splitlines() if line.startswith("  - "))
    assert "cache_ipc" in first_data_line

    metrics = MetricsCollector().get_metrics()
    assert metrics["connectors"]["planner_reranker"]["calls"] >= 1
