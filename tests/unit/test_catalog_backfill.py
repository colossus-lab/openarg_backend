from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.infrastructure.celery.tasks.catalog_backfill import (
    _build_catalog_embedding_text,
    _derived_header_quality,
    _derived_layout_profile,
    _materialization_status,
    _release_backfill_lock,
    _resource_kind,
    _try_backfill_lock,
    backfill_batch,
    catalog_backfill_task,
    run_populate_catalog_embeddings,
)


class _FetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FetchOneResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


def _row(**kwargs):
    return SimpleNamespace(_mapping=kwargs)


def test_build_catalog_embedding_text_dedupes_titles():
    assert (
        _build_catalog_embedding_text("Reservas BCRA", "Reservas BCRA")
        == "Reservas BCRA"
    )
    assert (
        _build_catalog_embedding_text("Reservas BCRA", "Serie histórica")
        == "Reservas BCRA - Serie histórica"
    )


def test_materialization_status_maps_terminal_rows():
    assert _materialization_status("ready") == "ready"
    assert _materialization_status("pending") == "pending"
    assert _materialization_status("error", "timeout") == "failed"
    assert _materialization_status("permanently_failed", "zip_document_bundle") == "non_tabular"
    assert _materialization_status("permanently_failed", "403 forbidden") == "failed"


def test_resource_kind_marks_document_bundles():
    assert _resource_kind("permanently_failed", "zip_document_bundle") == "document_bundle"
    assert _resource_kind("permanently_failed", "bad_zip_file") == "file"


def test_catalog_metadata_derivers_keep_only_explicit_signals():
    """The derivers used to fabricate `simple_tabular`/`good` defaults
    when status='ready' but the row had no real layout/header_quality.
    That made backfilled rows look like they had been processed by the
    phase4 parser. We now return None when there's no explicit signal,
    so dashboards can distinguish "tagged by phase4" from "unknown"."""
    assert _derived_layout_profile("header_multiline", "ready") == "header_multiline"
    # No real layout_profile -> None, regardless of cached_status. The
    # caller is responsible for re-running the parser if it wants a
    # phase4 tag.
    assert _derived_layout_profile(None, "ready") is None
    assert _derived_layout_profile(None, "error") is None

    assert _derived_header_quality("degraded", "ready", None) == "degraded"
    # No explicit quality -> None for ready (was 'good' before — see
    # docstring on the function for why that was a lie).
    assert _derived_header_quality(None, "ready", None) is None
    assert (
        _derived_header_quality(None, "ready", "header_quality:degraded;layout_profile:wide_csv")
        == "degraded"
    )
    assert _derived_header_quality(None, "error", "timeout") is None


def test_backfill_lock_helpers():
    """Lock acquisition + release should keep the same connection open
    between the two SELECTs so the session-scoped advisory lock survives.
    `_try_backfill_lock` returns the connection from `engine.connect()`
    directly (no context manager) and stashes it in a module-level dict
    until release; that's the only shape that makes the lock outlive the
    acquire call."""
    # Reset the module-level dict so this test is independent of order.
    from app.infrastructure.celery.tasks import catalog_backfill as _cb

    _cb._HELD_BACKFILL_LOCK.clear()

    engine = MagicMock()
    conn = MagicMock()
    conn.in_transaction.return_value = False
    conn.execute.return_value = _FetchOneResult(True)
    engine.connect.return_value = conn  # NOT a context manager — real conn

    assert _try_backfill_lock(engine) is True
    # Connection should be stashed (not closed) — the lock survives
    # because this very connection holds it.
    conn.close.assert_not_called()
    assert _cb._CATALOG_BACKFILL_LOCK_KEY in _cb._HELD_BACKFILL_LOCK

    _release_backfill_lock(engine)

    # After release: connection closed, dict empty.
    conn.close.assert_called_once()
    assert _cb._CATALOG_BACKFILL_LOCK_KEY not in _cb._HELD_BACKFILL_LOCK
    # Two SQL roundtrips: try_advisory_lock + advisory_unlock.
    assert conn.execute.call_count == 2
    assert "pg_try_advisory_lock" in str(conn.execute.call_args_list[0].args[0])
    assert "pg_advisory_unlock" in str(conn.execute.call_args_list[1].args[0])


@patch("app.infrastructure.celery.tasks.catalog_backfill.get_sync_engine")
@patch("app.infrastructure.celery.tasks.catalog_backfill.run_backfill")
def test_catalog_backfill_task_skips_when_lock_is_held(mock_run_backfill, mock_get_engine):
    from app.infrastructure.celery.tasks import catalog_backfill as _cb

    _cb._HELD_BACKFILL_LOCK.clear()

    lock_conn = MagicMock()
    lock_conn.in_transaction.return_value = False
    lock_conn.execute.return_value = _FetchOneResult(False)  # lock NOT acquired
    lock_engine = MagicMock()
    lock_engine.connect.return_value = lock_conn  # direct, not via __enter__
    mock_get_engine.return_value = lock_engine

    result = catalog_backfill_task.run()

    assert result == {"status": "skipped_already_running"}
    mock_run_backfill.assert_not_called()
    # Failed acquire should close the probe connection so we don't leak.
    lock_conn.close.assert_called_once()


@patch("app.infrastructure.celery.tasks.catalog_backfill.get_sync_engine")
@patch("boto3.client")
def test_run_populate_catalog_embeddings_updates_missing_rows(mock_boto_client, mock_get_engine):
    select_rows = [
        _row(
            id="11111111-1111-1111-1111-111111111111",
            display_name="Reservas BCRA",
            canonical_title="Serie histórica",
        )
    ]

    mock_select_conn = MagicMock()
    mock_select_conn.execute.side_effect = [
        _FetchAllResult(select_rows),
        _FetchAllResult([]),
    ]
    mock_update_conn = MagicMock()

    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_select_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_update_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_engine.dispose = MagicMock()
    mock_get_engine.return_value = mock_engine

    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps({"embeddings": [[0.1, 0.2, 0.3]]}).encode()
    mock_bedrock = MagicMock()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}
    mock_boto_client.return_value = mock_bedrock

    result = run_populate_catalog_embeddings(max_batches=2)

    assert result["selected"] == 1
    assert result["updated"] == 1
    assert result["batches"] == 1
    payload = mock_update_conn.execute.call_args.args[1]
    assert payload[0]["id"] == "11111111-1111-1111-1111-111111111111"
    assert payload[0]["embedding"] == "[0.1,0.2,0.3]"


@patch("app.infrastructure.celery.tasks.catalog_backfill.PhysicalNamer")
@patch("app.infrastructure.celery.tasks.catalog_backfill.TitleExtractor")
def test_backfill_batch_persists_layout_and_header_metadata(mock_extractor_cls, mock_namer_cls):
    row = SimpleNamespace(
        dataset_id="11111111-1111-1111-1111-111111111111",
        portal="datos_gob_ar",
        source_id="ipc-1",
        raw_title="IPC Nacional",
        organization="INDEC",
        url="https://example.com/ipc.csv",
        format="csv",
        table_name="cache_ipc_r111",
        s3_key="datasets/datos_gob_ar/ipc.csv",
        cached_status="ready",
        error_message=None,
        layout_profile="header_multiline",
        header_quality="degraded",
    )

    select_conn = MagicMock()
    select_conn.execute.return_value = _FetchAllResult([row])
    write_conn = MagicMock()
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=select_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=write_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    mock_extractor = MagicMock()
    mock_extractor.extract.return_value = SimpleNamespace(
        canonical_title="IPC Nacional",
        display_name="IPC Nacional",
        title_source=SimpleNamespace(value="metadata"),
        title_confidence=0.9,
    )
    mock_extractor_cls.return_value = mock_extractor
    mock_namer = MagicMock()
    mock_namer.build.return_value = SimpleNamespace(table_name="cache_ipc_r111")
    mock_namer_cls.return_value = mock_namer

    read, written = backfill_batch(
        engine,
        offset=0,
        limit=10,
        dry_run=False,
        extractor=mock_extractor,
        namer=mock_namer,
    )

    assert (read, written) == (1, 1)
    params = write_conn.execute.call_args.args[1]
    assert params["layout_profile"] == "header_multiline"
    assert params["header_quality"] == "degraded"
    # Backfilled rows are NOT marked phase4-v1: doing so would falsely
    # claim the data was parsed with the new parser. The catalog uses
    # `legacy:unknown` so dashboards / reingest filters can tell apart
    # genuine phase4-v1 ingestions from backfilled metadata.
    assert params["parser_version"] == "legacy:unknown"
    assert params["normalization_version"] == "legacy:unknown"
