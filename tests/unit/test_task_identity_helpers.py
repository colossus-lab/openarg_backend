from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.infrastructure.celery.tasks._db import register_via_b_error, register_via_b_table
from app.infrastructure.celery.tasks.catalog_enrichment_tasks import (
    _project_enrichment_to_catalog_resources,
    _resolve_resource_identity_for_table,
)
from app.infrastructure.celery.tasks.ingestion_findings_sweep import _load_batch


def test_load_batch_sql_keeps_full_source_id_after_first_separator() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    engine.connect.return_value.__exit__.return_value = False

    _load_batch(engine, offset=0, limit=10, portals=None)

    stmt = conn.execute.call_args.args[0]
    sql = str(stmt)
    assert "substring(rtv.resource_identity FROM POSITION('::' IN rtv.resource_identity) + 2)" in sql


def test_resolve_resource_identity_for_mart_table() -> None:
    conn = MagicMock()
    conn.execute.side_effect = [
        SimpleNamespace(fetchone=lambda: SimpleNamespace(mart_id="series_economicas")),
    ]
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    engine.connect.return_value.__exit__.return_value = False

    rid = _resolve_resource_identity_for_table(engine, "mart.series_economicas")

    assert rid == "mart::series_economicas"


@patch("app.infrastructure.celery.tasks.catalog_enrichment_tasks._resolve_resource_identity_for_table")
def test_catalog_projection_skips_marts(mock_resolve) -> None:
    mock_resolve.return_value = "mart::series_economicas"
    engine = MagicMock()

    _project_enrichment_to_catalog_resources(
        engine,
        table_name="mart.series_economicas",
        metadata={"display_name": "Series Económicas", "tags": ["economia"]},
        row_count=10,
        embedding_str="[0.1,0.2]",
        quality_score=1.0,
    )

    engine.begin.assert_not_called()


@patch("app.infrastructure.celery.tasks.collector_tasks._apply_cached_outcome")
def test_register_via_b_error_marks_internal_failures_as_materialization(mock_apply) -> None:
    register_via_b_error(
        MagicMock(),
        dataset_id="11111111-1111-1111-1111-111111111111",
        table_name="cache_bac_test",
        error_message="psycopg.errors.UniqueViolation: duplicate key value",
    )

    outcome = mock_apply.call_args.kwargs["outcome"]
    assert outcome.result_kind == "retryable_materialization"


@patch("app.infrastructure.celery.tasks._db._trigger_marts_for_portal")
def test_register_via_b_table_skips_refresh_when_upsert_is_noop(mock_trigger) -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = None
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False

    register_via_b_table(
        engine,
        resource_identity="bcra::cotizaciones",
        table_name="cache_bcra_cotizaciones",
        row_count=100,
    )

    mock_trigger.assert_not_called()
