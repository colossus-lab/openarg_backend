from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.presentation.http.controllers.admin.monitoring_router import (
    get_cache_drops,
    get_portal_health,
    get_state_violations,
    get_validation_findings,
)


class _FetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


def _row(**kwargs):
    return SimpleNamespace(_mapping=kwargs)


@pytest.mark.asyncio
@patch("app.presentation.http.controllers.admin.monitoring_router.get_sync_engine")
async def test_get_validation_findings_returns_recent_rows(mock_get_engine):
    mock_conn = MagicMock()
    mock_conn.execute.return_value = _FetchAllResult(
        [
            _row(
                resource_id="rid-1",
                detector_name="html_as_data",
                detector_version="1",
                severity="critical",
                mode="retrospective",
                should_redownload=True,
                message="bad payload",
                found_at="2026-04-25T10:00:00Z",
                resolved_at=None,
                portal="datos_gob_ar",
                source_id="abc",
            )
        ]
    )
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    mock_engine.dispose = MagicMock()
    mock_get_engine.return_value = mock_engine

    response = await get_validation_findings(limit=10)

    assert response["limit"] == 10
    assert len(response["items"]) == 1
    assert response["items"][0]["detector_name"] == "html_as_data"


@pytest.mark.asyncio
@patch("app.presentation.http.controllers.admin.monitoring_router.get_sync_engine")
async def test_get_state_violations_filters_state_mode(mock_get_engine):
    mock_conn = MagicMock()
    mock_conn.execute.return_value = _FetchAllResult(
        [
            _row(
                resource_id="rid-2",
                violation_kind="invariant_retry_max_status_error",
                severity="critical",
                message="stuck",
                found_at="2026-04-25T10:05:00Z",
                resolved_at=None,
                portal="bcra",
                source_id="series",
            )
        ]
    )
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    mock_engine.dispose = MagicMock()
    mock_get_engine.return_value = mock_engine

    response = await get_state_violations(limit=5)

    assert response["limit"] == 5
    assert response["items"][0]["violation_kind"] == "invariant_retry_max_status_error"


@pytest.mark.asyncio
@patch("app.presentation.http.controllers.admin.monitoring_router._table_exists")
async def test_get_cache_drops_reports_unavailable_when_table_missing(mock_table_exists):
    mock_table_exists.return_value = False

    response = await get_cache_drops(limit=20)

    assert response == {"items": [], "available": False, "limit": 20}


@pytest.mark.asyncio
@patch("app.presentation.http.controllers.admin.monitoring_router._table_exists")
@patch("app.presentation.http.controllers.admin.monitoring_router.get_sync_engine")
async def test_get_portal_health_returns_rows(mock_get_engine, mock_table_exists):
    mock_table_exists.return_value = True
    mock_conn = MagicMock()
    mock_conn.execute.return_value = _FetchAllResult(
        [
            _row(
                portal="datos_gob_ar",
                host="datos.gob.ar",
                is_down=False,
                last_status=200,
                last_check="2026-04-25T10:10:00Z",
                consecutive_failures=0,
                last_error=None,
            )
        ]
    )
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    mock_engine.dispose = MagicMock()
    mock_get_engine.return_value = mock_engine

    response = await get_portal_health(limit=25)

    assert response["available"] is True
    assert response["items"][0]["portal"] == "datos_gob_ar"
