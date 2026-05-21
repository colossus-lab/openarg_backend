from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.infrastructure.celery.tasks.catalog_backfill import run_seed_connector_endpoints


@patch("app.infrastructure.celery.tasks.catalog_backfill.get_sync_engine")
def test_run_seed_connector_endpoints_upserts_all_live_connectors(mock_get_engine):
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_engine.dispose = MagicMock()
    mock_get_engine.return_value = mock_engine

    result = run_seed_connector_endpoints()

    assert result["written"] == 9
    assert mock_conn.execute.call_count == 9


@patch("app.infrastructure.celery.tasks.catalog_backfill.get_sync_engine")
def test_run_seed_connector_endpoints_supports_dry_run(mock_get_engine):
    mock_engine = MagicMock()
    mock_engine.dispose = MagicMock()
    mock_get_engine.return_value = mock_engine

    result = run_seed_connector_endpoints(dry_run=True)

    assert result == {"written": 9, "dry_run": True}
