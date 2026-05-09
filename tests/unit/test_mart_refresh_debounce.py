from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.infrastructure.celery.tasks._db import (
    _mart_refresh_recently_succeeded,
    _trigger_marts_for_portal,
)


def test_mart_refresh_recently_succeeded_true_for_recent_success() -> None:
    assert _mart_refresh_recently_succeeded(
        last_refresh_status="refreshed",
        last_refreshed_at=datetime.now(UTC) - timedelta(seconds=30),
    )


def test_mart_refresh_recently_succeeded_false_for_old_success() -> None:
    assert not _mart_refresh_recently_succeeded(
        last_refresh_status="built",
        last_refreshed_at=datetime.now(UTC) - timedelta(seconds=600),
    )


def test_mart_refresh_recently_succeeded_false_for_failed_status() -> None:
    assert not _mart_refresh_recently_succeeded(
        last_refresh_status="refresh_failed",
        last_refreshed_at=datetime.now(UTC) - timedelta(seconds=30),
    )


@patch("app.infrastructure.celery.tasks.mart_tasks.refresh_mart.apply_async")
def test_trigger_marts_skips_recent_success(mock_apply_async) -> None:
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = [
        SimpleNamespace(
            mart_id="series_economicas",
            last_refresh_status="refreshed",
            last_refreshed_at=datetime.now(UTC),
        )
    ]

    _trigger_marts_for_portal(engine, "bcra::cotizaciones")

    mock_apply_async.assert_not_called()


@patch("app.infrastructure.celery.tasks.mart_tasks.refresh_mart.apply_async")
def test_trigger_marts_enqueues_after_failed_refresh(mock_apply_async) -> None:
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = [
        SimpleNamespace(
            mart_id="series_economicas",
            last_refresh_status="refresh_failed",
            last_refreshed_at=datetime.now(UTC),
        )
    ]

    _trigger_marts_for_portal(engine, "bcra::cotizaciones")

    mock_apply_async.assert_called_once()
