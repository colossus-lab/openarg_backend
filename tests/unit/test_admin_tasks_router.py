from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from app.presentation.http.controllers.admin.tasks_router import (
    TASK_REGISTRY,
    _get_admin_key,
    get_task_status,
    verify_admin_key,
)


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


# ── H7 fix: admin key fail-closed, no fallback to BACKEND_API_KEY ──


def test_get_admin_key_returns_empty_when_unset(monkeypatch):
    """Missing ADMIN_API_KEY must NOT fall back to BACKEND_API_KEY."""
    monkeypatch.delenv("ADMIN_API_KEY", raising=False)
    monkeypatch.setenv("BACKEND_API_KEY", "backend-secret-xyz")
    assert _get_admin_key() == ""


def test_get_admin_key_returns_admin_value_when_set(monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", "admin-secret-abc")
    monkeypatch.setenv("BACKEND_API_KEY", "backend-secret-xyz")
    assert _get_admin_key() == "admin-secret-abc"


def test_verify_admin_key_503_when_unconfigured(monkeypatch):
    """Fail-closed: missing ADMIN_API_KEY blocks every request."""
    monkeypatch.delenv("ADMIN_API_KEY", raising=False)
    monkeypatch.setenv("BACKEND_API_KEY", "backend-secret-xyz")
    with pytest.raises(HTTPException) as excinfo:
        verify_admin_key(x_admin_key="anything")
    assert excinfo.value.status_code == 503
    assert "not configured" in excinfo.value.detail.lower()


def test_verify_admin_key_rejects_backend_key_when_admin_unset(monkeypatch):
    """Regression for H7: a holder of BACKEND_API_KEY must NOT be admin.

    Pre-fix behaviour: `_get_admin_key()` fell back to BACKEND_API_KEY and
    `verify_admin_key("backend-secret")` returned the key string (200 OK).
    Post-fix: 503 because ADMIN_API_KEY isn't set.
    """
    monkeypatch.delenv("ADMIN_API_KEY", raising=False)
    monkeypatch.setenv("BACKEND_API_KEY", "backend-secret-xyz")
    with pytest.raises(HTTPException) as excinfo:
        verify_admin_key(x_admin_key="backend-secret-xyz")
    assert excinfo.value.status_code == 503


def test_verify_admin_key_rejects_wrong_value(monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", "admin-secret-abc")
    with pytest.raises(HTTPException) as excinfo:
        verify_admin_key(x_admin_key="wrong-key")
    assert excinfo.value.status_code == 401


def test_verify_admin_key_accepts_correct_value(monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", "admin-secret-abc")
    assert verify_admin_key(x_admin_key="admin-secret-abc") == "admin-secret-abc"
