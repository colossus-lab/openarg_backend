"""Unit tests for `_resolve_collect_destination` (MASTERPLAN Fase 1.5).

The destination resolver is the single switch that decides where a
materialization lands. Off the flag = legacy `cache_*` in `public`. On =
versioned `<bare>__v<N>` in schema `raw`. Tests cover both paths plus the
shape of the destination DTO so downstream callers don't get surprised.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.celery.tasks.collector_tasks import (
    _CollectDestination,
    _resolve_collect_destination,
)


def _mock_engine_with_max_version(max_version: int) -> MagicMock:
    """Build an engine mock whose connect()→execute() and begin()→execute()
    both return a row with `v=max_version`, simulating
    `SELECT MAX(version) FROM raw_table_versions`.

    `_resolve_raw_table_for_dataset` uses `engine.begin()` for the
    advisory-lock-protected version bump (the read happens inside the
    transaction). It still uses `engine.connect()` for the dataset lookup.
    """
    engine = MagicMock()

    # The `with engine.begin() as conn:` branch — used for the version bump.
    txn_conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = txn_conn
    result_row = MagicMock()
    result_row.v = max_version
    cursor = MagicMock()
    cursor.fetchone.return_value = result_row
    txn_conn.execute.return_value = cursor

    # The `with engine.connect() as conn:` branch — used for dataset lookup.
    plain_conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = plain_conn
    plain_conn.execute.return_value = cursor
    plain_conn.rollback = MagicMock()
    return engine


def test_legacy_path_when_flag_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENARG_USE_RAW_LAYER", raising=False)
    engine = _mock_engine_with_max_version(0)
    dest = _resolve_collect_destination(
        engine,
        "550e8400-e29b-41d4-a716-446655440000",
        portal="caba",
        source_id="datasets-123",
        title="Test dataset",
    )
    assert dest.schema == "public"
    assert dest.bare_name.startswith("cache_")
    assert dest.version == 0
    assert dest.resource_identity == "caba::datasets-123"
    assert dest.qualified_name == dest.bare_name  # public stays unqualified


def test_raw_path_when_flag_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENARG_USE_RAW_LAYER", "1")
    engine = _mock_engine_with_max_version(0)
    dest = _resolve_collect_destination(
        engine,
        "550e8400-e29b-41d4-a716-446655440000",
        portal="caba",
        source_id="datasets-123",
        title="Test dataset",
    )
    assert dest.schema == "raw"
    assert dest.bare_name.endswith("__v1")
    assert dest.version == 1
    assert dest.resource_identity == "caba::datasets-123"
    assert dest.qualified_name == f"raw.{dest.bare_name}"


def test_raw_path_bumps_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENARG_USE_RAW_LAYER", "1")
    engine = _mock_engine_with_max_version(7)  # 7 prior versions in registry
    dest = _resolve_collect_destination(
        engine,
        "550e8400-e29b-41d4-a716-446655440000",
        portal="entre_rios",
        source_id="src-x",
        title="something",
    )
    assert dest.version == 8
    assert dest.bare_name.endswith("__v8")


def test_flag_recognizes_truthy_values(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.infrastructure.celery.tasks.collector_tasks import _use_raw_layer

    for v in ("1", "true", "TRUE", "yes", "True"):
        monkeypatch.setenv("OPENARG_USE_RAW_LAYER", v)
        assert _use_raw_layer() is True

    for v in ("0", "false", "no", "", "garbage"):
        monkeypatch.setenv("OPENARG_USE_RAW_LAYER", v)
        assert _use_raw_layer() is False


def test_destination_dto_qualified_name() -> None:
    raw_dest = _CollectDestination(
        schema="raw",
        bare_name="caba__test__abcd1234__v1",
        version=1,
        resource_identity="caba::test",
    )
    assert raw_dest.qualified_name == "raw.caba__test__abcd1234__v1"

    legacy_dest = _CollectDestination(
        schema="public",
        bare_name="cache_caba_test",
        version=0,
        resource_identity="caba::test",
    )
    assert legacy_dest.qualified_name == "cache_caba_test"
