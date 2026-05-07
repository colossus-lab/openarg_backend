from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter
from app.infrastructure.celery.tasks._db import register_via_b_table
from app.infrastructure.celery.tasks.catalog_backfill import (
    _qualified_materialized_table_name,
)
from app.infrastructure.celery.tasks.collector_tasks import (
    _current_write_schema,
    _resolve_physical_table_ref,
    _table_exists,
)


def test_qualified_materialized_table_name_preserves_raw_schema():
    assert (
        _qualified_materialized_table_name(
            "cache_demo__abcd1234__v1",
            schema_name="raw",
            fallback_name="cache_fallback",
        )
        == 'raw."cache_demo__abcd1234__v1"'
    )
    assert (
        _qualified_materialized_table_name(
            "cache_demo",
            schema_name="public",
            fallback_name="cache_fallback",
        )
        == "cache_demo"
    )


def test_resolve_physical_table_ref_uses_context_schema_for_bare_name():
    token = _current_write_schema.set("raw")
    try:
        assert _resolve_physical_table_ref("cache_demo") == ("raw", "cache_demo")
    finally:
        _current_write_schema.reset(token)


def test_table_exists_checks_current_raw_schema_for_bare_name():
    observed = {}

    class _Conn:
        def execute(self, _stmt, params=None):
            observed.update(params or {})
            return SimpleNamespace(scalar=lambda: True)

        def rollback(self):
            return None

    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=_Conn())
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    token = _current_write_schema.set("raw")
    try:
        assert _table_exists(engine, "cache_demo") is True
    finally:
        _current_write_schema.reset(token)

    assert observed["sch"] == "raw"
    assert observed["tn"] == "cache_demo"


@patch("app.infrastructure.celery.tasks._db._trigger_marts_for_portal")
def test_register_via_b_table_triggers_refresh_even_when_registry_row_is_unchanged(
    mock_trigger,
):
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    register_via_b_table(
        engine,
        resource_identity="bcra::cotizaciones",
        table_name="cache_bcra_cotizaciones",
        schema_name="public",
        row_count=10,
    )

    mock_trigger.assert_called_once_with(engine, "bcra::cotizaciones")


def test_list_tables_sync_does_not_expose_unregistered_public_cache_tables():
    ready_rows = [
        SimpleNamespace(
            dataset_id="11111111-1111-1111-1111-111111111111",
            table_name="cache_ready",
            row_count=10,
            columns_json='["a"]',
        )
    ]
    raw_rows: list[SimpleNamespace] = []

    class _Conn:
        def __init__(self):
            self.calls = 0

        def execute(self, _stmt, _params=None):
            self.calls += 1
            if self.calls == 1:
                return SimpleNamespace(fetchall=lambda: ready_rows)
            if self.calls == 2:
                return SimpleNamespace(fetchall=lambda: raw_rows)
            raise AssertionError(f"Unexpected execute call #{self.calls}")

        def rollback(self):
            return None

    adapter = PgSandboxAdapter()
    adapter._engine = MagicMock()
    adapter._engine.connect.return_value.__enter__ = MagicMock(return_value=_Conn())
    adapter._engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    tables = adapter._list_tables_sync()

    assert [t.table_name for t in tables] == ["cache_ready"]
