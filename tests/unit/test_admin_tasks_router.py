from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.presentation.http.controllers.admin.tasks_router import TASK_REGISTRY, get_task_status


def test_task_registry_includes_new_operational_tasks():
    expected = {
        "catalog_backfill": ("openarg.catalog_backfill", "ingest"),
        "populate_catalog_embeddings": ("openarg.populate_catalog_embeddings", "embedding"),
        "seed_connector_endpoints": ("openarg.seed_connector_endpoints", "ingest"),
        "ingest_censo2022": ("openarg.ingest_censo2022", "ingest"),
        "refresh_curated_sources": ("openarg.refresh_curated_sources", "ingest"),
        "ws0_retrospective_sweep": ("openarg.ws0_retrospective_sweep", "ingest"),
        "ws0_5_state_invariants_sweep": ("openarg.ws0_5_state_invariants_sweep", "default"),
        "ops_portal_health": ("openarg.ops_portal_health", "ingest"),
        "report_failed_tasks": ("openarg.report_failed_tasks", "default"),
    }

    for task_id, (celery_name, queue) in expected.items():
        assert task_id in TASK_REGISTRY
        assert TASK_REGISTRY[task_id]["celery_name"] == celery_name
        assert TASK_REGISTRY[task_id]["queue"] == queue


@pytest.mark.asyncio
@patch("app.presentation.http.controllers.admin.tasks_router._inspect_task_snapshot")
@patch("app.presentation.http.controllers.admin.tasks_router.AsyncResult")
async def test_get_task_status_promotes_pending_active_task(
    mock_async_result,
    mock_inspect_snapshot,
):
    mock_async_result.return_value = SimpleNamespace(state="PENDING", result=None, info=None)
    mock_inspect_snapshot.return_value = {
        "bucket": "active",
        "worker": "celery@test",
        "name": "openarg.collect_data",
        "args": ["dataset-id"],
        "started": 123.0,
    }

    response = await get_task_status("abc-123")

    assert response["state"] == "STARTED"
    assert response["completed"] is False
    assert response["info"] == {
        "status": "Task is active",
        "worker": "celery@test",
        "name": "openarg.collect_data",
        "args": ["dataset-id"],
        "started": 123.0,
    }


@pytest.mark.asyncio
@patch("app.presentation.http.controllers.admin.tasks_router._inspect_task_snapshot")
@patch("app.presentation.http.controllers.admin.tasks_router.AsyncResult")
async def test_get_task_status_keeps_pending_when_task_not_found_live(
    mock_async_result,
    mock_inspect_snapshot,
):
    mock_async_result.return_value = SimpleNamespace(state="PENDING", result=None, info=None)
    mock_inspect_snapshot.return_value = None

    response = await get_task_status("abc-123")

    assert response["state"] == "PENDING"
    assert response["completed"] is False
    assert response["info"] == "Task is queued or unknown"
