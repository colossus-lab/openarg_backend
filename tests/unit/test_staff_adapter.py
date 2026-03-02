"""Tests for StaffAdapter with mocked DB sessions."""
from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.domain.exceptions.connector_errors import ConnectorError
from app.infrastructure.adapters.connectors.staff_adapter import StaffAdapter, _escape_like


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

    ctx = AsyncMock()
    ctx.__aenter__.return_value = session
    ctx.__aexit__.return_value = None
    factory.return_value = ctx

    return factory, session


@pytest.fixture
def adapter(mock_session_factory):
    factory, _ = mock_session_factory
    return StaffAdapter(session_factory=factory)


# ── _escape_like helper ──────────────────────────────────────


class TestEscapeLike:
    def test_no_metacharacters(self):
        assert _escape_like("Yeza") == "Yeza"

    def test_percent(self):
        assert _escape_like("100%") == "100\\%"

    def test_underscore(self):
        assert _escape_like("my_name") == "my\\_name"

    def test_backslash(self):
        assert _escape_like("path\\value") == "path\\\\value"

    def test_combined(self):
        assert _escape_like("a%b_c\\d") == "a\\%b\\_c\\\\d"


# ── get_by_legislator ────────────────────────────────────────


class TestGetByLegislator:
    async def test_returns_empty_when_no_snapshot(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        result_mock = MagicMock()
        result_mock.scalar.return_value = None
        session.execute.return_value = result_mock

        result = await adapter.get_by_legislator("Yeza")
        assert result.source == "staff:hcdn"
        assert result.records == []

    async def test_returns_records(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        date_result = MagicMock()
        date_result.scalar.return_value = date(2026, 3, 1)

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

    async def test_empty_name_returns_empty(self, adapter):
        result = await adapter.get_by_legislator("  ")
        assert result.records == []
        assert "sin especificar" in result.dataset_title

    async def test_limit_clamped(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        date_result = MagicMock()
        date_result.scalar.return_value = date(2026, 3, 1)

        rows_result = MagicMock()
        rows_result.__iter__ = lambda self: iter([])
        session.execute.side_effect = [date_result, rows_result]

        await adapter.get_by_legislator("Yeza", limit=9999)
        # The second execute call should have lim=500 (clamped)
        call_args = session.execute.call_args_list[1]
        assert call_args[0][1]["lim"] == 500

    async def test_db_error_raises_connector_error(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        session.execute.side_effect = RuntimeError("connection lost")

        with pytest.raises(ConnectorError) as exc_info:
            await adapter.get_by_legislator("Yeza")
        assert exc_info.value.details["action"] == "get_by_legislator"


# ── count_by_legislator ──────────────────────────────────────


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

    async def test_count_no_snapshot(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        result_mock = MagicMock()
        result_mock.scalar.return_value = None
        session.execute.return_value = result_mock

        result = await adapter.count_by_legislator("Yeza")
        assert result.records == []

    async def test_count_empty_name(self, adapter):
        result = await adapter.count_by_legislator("")
        assert result.records == []

    async def test_count_db_error(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        session.execute.side_effect = RuntimeError("timeout")

        with pytest.raises(ConnectorError):
            await adapter.count_by_legislator("Yeza")


# ── get_changes ──────────────────────────────────────────────


class TestGetChanges:
    async def test_changes_with_name(self, adapter, mock_session_factory):
        _, session = mock_session_factory

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
        assert "Yeza" in result.dataset_title

    async def test_changes_without_name(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        rows_result = MagicMock()
        rows_result.__iter__ = lambda self: iter([])
        session.execute.return_value = rows_result

        result = await adapter.get_changes()
        assert result.records == []
        assert "Últimos cambios" in result.dataset_title

    async def test_changes_detected_at_serialized(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        dt = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)
        rows = [
            _make_row(
                legajo="1",
                apellido="A",
                nombre="B",
                area_desempeno="C",
                tipo="baja",
                detected_at=dt,
            )
        ]
        rows_result = MagicMock()
        rows_result.__iter__ = lambda self: iter(rows)
        session.execute.return_value = rows_result

        result = await adapter.get_changes(name="test")
        assert result.records[0]["detected_at"] == dt.isoformat()

    async def test_changes_detected_at_none(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        rows = [
            _make_row(
                legajo="1",
                apellido="A",
                nombre="B",
                area_desempeno="C",
                tipo="baja",
                detected_at=None,
            )
        ]
        rows_result = MagicMock()
        rows_result.__iter__ = lambda self: iter(rows)
        session.execute.return_value = rows_result

        result = await adapter.get_changes(name="test")
        assert result.records[0]["detected_at"] is None

    async def test_changes_db_error(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        session.execute.side_effect = RuntimeError("fail")

        with pytest.raises(ConnectorError):
            await adapter.get_changes()


# ── search ───────────────────────────────────────────────────


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

    async def test_search_no_snapshot(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        result_mock = MagicMock()
        result_mock.scalar.return_value = None
        session.execute.return_value = result_mock

        result = await adapter.search("LOPEZ")
        assert result.records == []

    async def test_search_empty_query(self, adapter):
        result = await adapter.search("  ")
        assert result.records == []
        assert "sin consulta" in result.dataset_title

    async def test_search_db_error(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        session.execute.side_effect = RuntimeError("fail")

        with pytest.raises(ConnectorError):
            await adapter.search("test")


# ── stats ────────────────────────────────────────────────────


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
        assert result.records[0]["escalafones_distintos"] == 5
        assert result.records[0]["snapshot_date"] == "2026-03-01"

    async def test_stats_no_snapshot(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        result_mock = MagicMock()
        result_mock.scalar.return_value = None
        session.execute.return_value = result_mock

        result = await adapter.stats()
        assert result.records == []

    async def test_stats_db_error(self, adapter, mock_session_factory):
        _, session = mock_session_factory
        session.execute.side_effect = RuntimeError("fail")

        with pytest.raises(ConnectorError):
            await adapter.stats()
