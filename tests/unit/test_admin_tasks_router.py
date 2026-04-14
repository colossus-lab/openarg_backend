from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.presentation.http.controllers.admin.tasks_router import get_task_status


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
