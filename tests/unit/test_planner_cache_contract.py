from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.pipeline.connectors.sandbox import (
    discover_catalog_hints_for_planner,
)
from app.application.pipeline.plan_cache import invalidate_query_plan_cache
from app.domain.entities.connectors.data_result import ExecutionPlan
from app.infrastructure.celery.tasks.ops_fixes import _warm_query_plan_candidate


@pytest.mark.asyncio
async def test_discover_reuses_precomputed_embedding_for_serving(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1")
    serving_port = AsyncMock()
    serving_port.discover.return_value = []
    embedding = AsyncMock()
    precomputed = [0.1, 0.2, 0.3]

    result = await discover_catalog_hints_for_planner(
        "inflacion",
        sandbox=None,
        embedding=embedding,
        serving_port=serving_port,
        precomputed_embedding=precomputed,
    )

    assert result == ""
    embedding.embed.assert_not_awaited()
    assert serving_port.discover.await_args.kwargs["query_embedding"] == precomputed


@pytest.mark.asyncio
async def test_warm_query_plan_candidate_uses_online_planner_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = MagicMock()
    read_conn = MagicMock()
    read_conn.execute.return_value.fetchone.return_value = None
    read_conn.rollback.return_value = None
    engine.connect.return_value.__enter__.return_value = read_conn
    engine.connect.return_value.__exit__.return_value = False

    write_conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = write_conn
    engine.begin.return_value.__exit__.return_value = False

    embedder = AsyncMock()
    embedder.embed.return_value = [0.1, 0.2]
    llm = AsyncMock()
    sandbox = object()
    serving_port = object()

    discover_mock = AsyncMock(return_value="CATALOG HINTS")
    generate_mock = AsyncMock(
        return_value=ExecutionPlan(query="inflacion", intent="answer", steps=[])
    )
    monkeypatch.setattr(
        "app.infrastructure.celery.tasks.ops_fixes.discover_catalog_hints_for_planner",
        discover_mock,
    )
    monkeypatch.setattr(
        "app.infrastructure.adapters.connectors.query_planner.generate_plan",
        generate_mock,
    )

    status = await _warm_query_plan_candidate(
        question="inflacion",
        engine=engine,
        embedder=embedder,
        llm=llm,
        sandbox=sandbox,
        serving_port=serving_port,
    )

    assert status == "warmed"
    discover_mock.assert_awaited_once()
    assert discover_mock.await_args.kwargs["precomputed_embedding"] == [0.1, 0.2]
    generate_mock.assert_awaited_once()
    assert generate_mock.await_args.kwargs["catalog_hints"] == "CATALOG HINTS"
    assert generate_mock.await_args.kwargs["skip_classifier"] is False


def test_invalidate_query_plan_cache_deletes_all_rows() -> None:
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.rowcount = 7
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False

    deleted = invalidate_query_plan_cache(engine, reason="test")

    assert deleted == 7
    conn.execute.assert_called_once()
