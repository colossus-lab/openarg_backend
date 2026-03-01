"""Tests for StaffAdapter with mocked DB sessions."""
from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.infrastructure.adapters.connectors.staff_adapter import StaffAdapter


def _make_row(**kwargs):
    """Create a mock row that supports ._mapping and attribute access."""
    row = MagicMock()
    row._mapping = kwargs
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


@pytest.fixture
def mock_session_factory():
    """Return an async_sessionmaker mock that yields a mock session."""
    session = AsyncMock()
    factory = MagicMock()

    # Make factory() return an async context manager that yields session
    ctx = AsyncMock()
    ctx.__aenter__.return_value = session
    ctx.__aexit__.return_value = None
    factory.return_value = ctx

    return factory, session


@pytest.fixture
def adapter(mock_session_factory):
    factory, _ = mock_session_factory
    return StaffAdapter(session_factory=factory)


class TestGetByLegislator:
    async def test_returns_empty_when_no_snapshot(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        # First call: latest snapshot date → None
        result_mock = MagicMock()
        result_mock.scalar.return_value = None
        session.execute.return_value = result_mock

        result = await adapter.get_by_legislator("Yeza")
        assert result.source == "staff:hcdn"
        assert result.records == []

    async def test_returns_records(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        # First call: snapshot date
        date_result = MagicMock()
        date_result.scalar.return_value = date(2026, 3, 1)

        # Second call: actual rows
        rows = [
            _make_row(
                legajo="1234",
                apellido="GOMEZ",
                nombre="JUAN",
                escalafon="A",
                area_desempeno="YEZA",
                convenio="LEY",
            )
        ]
        rows_result = MagicMock()
        rows_result.__iter__ = lambda self: iter(rows)

        session.execute.side_effect = [date_result, rows_result]

        result = await adapter.get_by_legislator("Yeza")
        assert result.source == "staff:hcdn"
        assert len(result.records) == 1
        assert result.records[0]["legajo"] == "1234"


class TestCountByLegislator:
    async def test_count_with_snapshot(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        date_result = MagicMock()
        date_result.scalar.return_value = date(2026, 3, 1)

        count_result = MagicMock()
        count_result.scalar.return_value = 15

        session.execute.side_effect = [date_result, count_result]

        result = await adapter.count_by_legislator("Yeza")
        assert result.records[0]["cantidad_asesores"] == 15


class TestGetChanges:
    async def test_changes_with_name(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        from datetime import datetime, UTC

        rows = [
            _make_row(
                legajo="5678",
                apellido="PEREZ",
                nombre="MARIA",
                area_desempeno="YEZA",
                tipo="alta",
                detected_at=datetime(2026, 3, 1, tzinfo=UTC),
            )
        ]
        rows_result = MagicMock()
        rows_result.__iter__ = lambda self: iter(rows)
        session.execute.return_value = rows_result

        result = await adapter.get_changes(name="Yeza")
        assert len(result.records) == 1
        assert result.records[0]["tipo"] == "alta"


class TestSearch:
    async def test_search_with_snapshot(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        date_result = MagicMock()
        date_result.scalar.return_value = date(2026, 3, 1)

        rows = [
            _make_row(
                legajo="9999",
                apellido="LOPEZ",
                nombre="CARLOS",
                escalafon="B",
                area_desempeno="SECRETARIA",
                convenio="CCT",
            )
        ]
        rows_result = MagicMock()
        rows_result.__iter__ = lambda self: iter(rows)

        session.execute.side_effect = [date_result, rows_result]

        result = await adapter.search("LOPEZ")
        assert len(result.records) == 1
        assert result.records[0]["apellido"] == "LOPEZ"


class TestStats:
    async def test_stats_with_snapshot(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        date_result = MagicMock()
        date_result.scalar.return_value = date(2026, 3, 1)

        stats_row = _make_row(total=3600, areas=200, escalafones=5)
        stats_result = MagicMock()
        stats_result.fetchone.return_value = stats_row

        session.execute.side_effect = [date_result, stats_result]

        result = await adapter.stats()
        assert result.records[0]["total_empleados"] == 3600
        assert result.records[0]["areas_distintas"] == 200
