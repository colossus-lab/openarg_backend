from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.celery.tasks.collector_tasks import (
    _make_unique_columns,
    _resource_table_name,
    _route_table_for_schema,
    _sanitize_columns,
    _schema_table_name,
    collect_dataset,
)
from app.infrastructure.celery.tasks.embedding_tasks import index_sesiones_chunks
from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding


class _FetchOneResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class TestCollectorP2:
    def test_sanitize_columns_compacts_overflow_into_json_column(self):
        data = {f"col_{i}": [i] for i in range(1505)}

        import pandas as pd

        df = pd.DataFrame(data)

        compacted = _sanitize_columns(df)

        assert len(compacted.columns) == 1400
        assert "_overflow_json" in compacted.columns
        overflow = json.loads(compacted.loc[0, "_overflow_json"])
        assert overflow["col_1399"] == 1399
        assert overflow["col_1504"] == 1504

    def test_make_unique_columns_deduplicates_colliding_headers(self):
        assert _make_unique_columns(["DEPARTAMENTOS", "DEPARTAMENTOS", " DEPARTAMENTOS "]) == [
            "DEPARTAMENTOS",
            "DEPARTAMENTOS_2",
            "DEPARTAMENTOS_3",
        ]

    @patch("app.infrastructure.celery.tasks.collector_tasks._ensure_cached_entry")
    @patch("app.infrastructure.celery.tasks.collector_tasks._get_table_row_count")
    @patch("app.infrastructure.celery.tasks.collector_tasks._table_exists")
    @patch("app.infrastructure.celery.tasks.collector_tasks._check_schema_compat_columns")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    def test_route_table_for_schema_uses_schema_variant_when_base_is_incompatible(
        self,
        mock_get_engine,
        mock_check_schema_compat_columns,
        mock_table_exists,
        mock_get_table_row_count,
        mock_ensure_cached_entry,
    ):
        mock_check_schema_compat_columns.return_value = False
        mock_table_exists.side_effect = [True, False]
        mock_get_table_row_count.return_value = 0
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        target_table, append_mode, status = _route_table_for_schema(
            mock_engine,
            "11111111-1111-1111-1111-111111111111",
            "cache_energia_canon_hidrocarburifero",
            ["periodo", "valor", "_source_dataset_id"],
            append_mode=True,
        )

        assert target_table == _schema_table_name(
            "cache_energia_canon_hidrocarburifero",
            ["periodo", "valor", "_source_dataset_id"],
        )
        assert append_mode is False
        assert status is None
        mock_ensure_cached_entry.assert_called_once()

    @patch("app.infrastructure.celery.tasks.collector_tasks._ensure_cached_entry")
    @patch("app.infrastructure.celery.tasks.collector_tasks._get_table_row_count")
    @patch("app.infrastructure.celery.tasks.collector_tasks._table_exists")
    @patch("app.infrastructure.celery.tasks.collector_tasks._check_schema_compat_columns")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    def test_route_table_for_schema_falls_back_to_resource_table_when_target_is_full(
        self,
        mock_get_engine,
        mock_check_schema_compat_columns,
        mock_table_exists,
        mock_get_table_row_count,
        mock_ensure_cached_entry,
    ):
        mock_check_schema_compat_columns.return_value = True
        mock_table_exists.side_effect = [True, True, False]
        mock_get_table_row_count.return_value = 500_000
        conn = MagicMock()
        conn.execute.return_value = _ScalarResult(False)
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_engine.return_value = mock_engine
        dataset_id = "11111111-1111-1111-1111-111111111111"

        target_table, append_mode, status = _route_table_for_schema(
            mock_engine,
            dataset_id,
            "cache_energia_canon_hidrocarburifero",
            ["periodo", "valor", "_source_dataset_id"],
            append_mode=True,
        )

        assert target_table == _resource_table_name(
            "cache_energia_canon_hidrocarburifero",
            dataset_id,
        )
        assert append_mode is False
        assert status is None
        assert mock_ensure_cached_entry.call_count == 1

    @patch("app.infrastructure.celery.tasks.collector_tasks._ensure_cached_entry")
    @patch("app.infrastructure.celery.tasks.collector_tasks._set_error_status")
    @patch("app.infrastructure.celery.tasks.collector_tasks._has_temp_space")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    def test_collect_dataset_aborts_when_temp_space_is_insufficient(
        self,
        mock_get_engine,
        mock_has_temp_space,
        mock_set_error_status,
        _mock_ensure_cached_entry,
    ):
        mock_has_temp_space.return_value = False

        dataset_row = SimpleNamespace(
            title="IPC Nacional",
            download_url="https://example.com/ipc.csv",
            format="csv",
            portal="datos_gob_ar",
            source_id="ipc-1",
        )
        retry_row = SimpleNamespace(retry_count=0)

        mock_conn = MagicMock()

        def execute_side_effect(stmt, params=None):
            query = str(stmt)
            if "SELECT title, download_url, format, portal, source_id" in query:
                return _FetchOneResult(dataset_row)
            if "SELECT retry_count FROM cached_datasets" in query:
                return _FetchOneResult(retry_row)
            if "WHERE table_name = :tn AND status = 'ready'" in query:
                return _FetchOneResult(None)
            return MagicMock()

        mock_conn.execute.side_effect = execute_side_effect

        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        result = collect_dataset.run("11111111-1111-1111-1111-111111111111")

        assert result == {"error": "insufficient_temp_space"}
        mock_set_error_status.assert_called_once()

    @patch("app.infrastructure.celery.tasks.collector_tasks._ensure_cached_entry")
    @patch("app.infrastructure.celery.tasks.collector_tasks._ensure_postgis_geom")
    @patch("app.infrastructure.celery.tasks.collector_tasks._upload_file_to_s3")
    @patch("app.infrastructure.celery.tasks.collector_tasks._load_csv_chunked")
    @patch("app.infrastructure.celery.tasks.collector_tasks._stream_download")
    @patch("httpx.Client")
    @patch("app.infrastructure.celery.tasks.collector_tasks._has_temp_space")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    def test_collect_dataset_no_longer_uses_head_probe(
        self,
        mock_get_engine,
        mock_has_temp_space,
        mock_httpx_client,
        mock_stream_download,
        mock_load_csv_chunked,
        mock_upload_file_to_s3,
        _mock_ensure_postgis,
        _mock_ensure_cached_entry,
    ):
        mock_has_temp_space.return_value = True
        mock_stream_download.return_value = 123
        mock_load_csv_chunked.return_value = (10, ["col1"], False)
        mock_upload_file_to_s3.return_value = "datasets/datos_gob_ar/id/ipc.csv"

        dataset_row = SimpleNamespace(
            title="IPC Nacional",
            download_url="https://example.com/ipc.csv",
            format="csv",
            portal="datos_gob_ar",
            source_id="ipc-1",
        )
        retry_row = SimpleNamespace(retry_count=0)
        prev_cached_row = SimpleNamespace(is_cached=False)

        mock_conn = MagicMock()

        def execute_side_effect(stmt, params=None):
            query = str(stmt)
            if "SELECT title, download_url, format, portal, source_id" in query:
                return _FetchOneResult(dataset_row)
            if "SELECT retry_count FROM cached_datasets" in query:
                return _FetchOneResult(retry_row)
            if "WHERE table_name = :tn AND status = 'ready'" in query:
                return _FetchOneResult(None)
            if "UPDATE cached_datasets SET" in query:
                return MagicMock()
            if "SELECT is_cached FROM datasets" in query:
                return _FetchOneResult(prev_cached_row)
            if "UPDATE datasets SET is_cached = true, row_count = :rows" in query:
                return MagicMock()
            return MagicMock()

        mock_conn.execute.side_effect = execute_side_effect

        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        with (
            patch("app.infrastructure.celery.tasks.scraper_tasks.index_dataset_embedding.delay"),
            patch(
                "app.infrastructure.celery.tasks.catalog_enrichment_tasks.enrich_single_table.delay"
            ),
        ):
            result = collect_dataset.run("11111111-1111-1111-1111-111111111111")

        assert result["rows"] == 10
        assert result["table_name"] == _resource_table_name(
            "cache_datos_gob_ar_ipc_nacional",
            "11111111-1111-1111-1111-111111111111",
        )
        mock_httpx_client.assert_not_called()

    @patch("app.infrastructure.celery.tasks.collector_tasks._read_excel_frame")
    @patch("app.infrastructure.celery.tasks.collector_tasks._set_error_status")
    @patch("app.infrastructure.celery.tasks.collector_tasks._has_temp_space")
    @patch("app.infrastructure.celery.tasks.collector_tasks._upload_file_to_s3")
    @patch("app.infrastructure.celery.tasks.collector_tasks._stream_download")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    @patch("app.infrastructure.celery.tasks.collector_tasks._ensure_cached_entry")
    def test_collect_dataset_marks_excel_without_sheets_as_terminal_error(
        self,
        _mock_ensure_cached_entry,
        mock_get_engine,
        mock_stream_download,
        mock_upload_file_to_s3,
        mock_has_temp_space,
        mock_set_error_status,
        mock_read_excel_frame,
    ):
        mock_has_temp_space.return_value = True
        mock_stream_download.return_value = 123
        mock_upload_file_to_s3.return_value = "datasets/datos_gob_ar/id/ipc.xlsx"
        mock_read_excel_frame.side_effect = ValueError("excel_no_worksheets")

        dataset_row = SimpleNamespace(
            title="IPC Nacional",
            download_url="https://example.com/ipc.xlsx",
            format="xlsx",
            portal="datos_gob_ar",
            source_id="ipc-1",
        )
        retry_row = SimpleNamespace(retry_count=0)

        mock_conn = MagicMock()

        def execute_side_effect(stmt, params=None):
            query = str(stmt)
            if "SELECT title, download_url, format, portal, source_id" in query:
                return _FetchOneResult(dataset_row)
            if "SELECT retry_count FROM cached_datasets" in query:
                return _FetchOneResult(retry_row)
            if "WHERE table_name = :tn AND status = 'ready'" in query:
                return _FetchOneResult(None)
            return MagicMock()

        mock_conn.execute.side_effect = execute_side_effect

        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        result = collect_dataset.run("11111111-1111-1111-1111-111111111111")

        assert result == {"error": "excel_no_worksheets"}
        mock_set_error_status.assert_called_once_with(
            mock_engine,
            "11111111-1111-1111-1111-111111111111",
            "excel_no_worksheets",
            table_name=_resource_table_name(
                "cache_datos_gob_ar_ipc_nacional",
                "11111111-1111-1111-1111-111111111111",
            ),
        )

    @patch("app.infrastructure.celery.tasks.collector_tasks._ensure_cached_entry")
    @patch("app.infrastructure.celery.tasks.collector_tasks._ensure_postgis_geom")
    @patch("app.infrastructure.celery.tasks.collector_tasks._upload_file_to_s3")
    @patch("app.infrastructure.celery.tasks.collector_tasks._load_csv_chunked")
    @patch("app.infrastructure.celery.tasks.collector_tasks._stream_download")
    @patch("app.infrastructure.celery.tasks.collector_tasks._has_temp_space")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    def test_collect_dataset_uses_resource_staging_table_as_primary_materialization(
        self,
        mock_get_engine,
        mock_has_temp_space,
        mock_stream_download,
        mock_load_csv_chunked,
        mock_upload_file_to_s3,
        _mock_ensure_postgis,
        _mock_ensure_cached_entry,
    ):
        mock_has_temp_space.return_value = True
        mock_stream_download.return_value = 123
        mock_load_csv_chunked.return_value = (10, ["periodo", "valor", "_source_dataset_id"], False)
        mock_upload_file_to_s3.return_value = "datasets/energia/id/canon.csv"

        dataset_row = SimpleNamespace(
            title="Canon hidrocarburifero",
            download_url="https://example.com/canon.csv",
            format="csv",
            portal="energia",
            source_id="canon-2",
        )
        retry_row = SimpleNamespace(retry_count=0)
        cached_row = SimpleNamespace(id="cached-id", dataset_id="22222222-2222-2222-2222-222222222222")
        prev_cached_row = SimpleNamespace(is_cached=False)

        mock_conn = MagicMock()

        def execute_side_effect(stmt, params=None):
            query = str(stmt)
            if "SELECT title, download_url, format, portal, source_id" in query:
                return _FetchOneResult(dataset_row)
            if "SELECT retry_count FROM cached_datasets" in query:
                return _FetchOneResult(retry_row)
            if "WHERE table_name = :tn AND status = 'ready'" in query:
                return _FetchOneResult(cached_row)
            if "UPDATE cached_datasets SET" in query:
                return MagicMock()
            if "SELECT is_cached FROM datasets" in query:
                return _FetchOneResult(prev_cached_row)
            if "UPDATE datasets SET is_cached = true, row_count = :rows" in query:
                return MagicMock()
            return MagicMock()

        mock_conn.execute.side_effect = execute_side_effect

        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        with (
            patch("app.infrastructure.celery.tasks.scraper_tasks.index_dataset_embedding.delay"),
            patch(
                "app.infrastructure.celery.tasks.catalog_enrichment_tasks.enrich_single_table.delay"
            ),
        ):
            result = collect_dataset.run("11111111-1111-1111-1111-111111111111")

        assert result["table_name"] == _resource_table_name(
            "cache_energia_canon_hidrocarburifero",
            "11111111-1111-1111-1111-111111111111",
        )
        assert result["rows"] == 10


class TestEmbeddingP2:
    @patch("app.infrastructure.celery.tasks.scraper_tasks._get_sample_rows_text")
    @patch("app.infrastructure.celery.tasks.scraper_tasks._get_data_statistics")
    @patch("app.infrastructure.celery.tasks.scraper_tasks.get_sync_engine")
    @patch("boto3.client")
    def test_index_dataset_embedding_inserts_chunks_in_batch(
        self,
        mock_boto_client,
        mock_get_engine,
        mock_get_data_statistics,
        mock_get_sample_rows_text,
    ):
        mock_get_data_statistics.return_value = None
        mock_get_sample_rows_text.return_value = None

        dataset_row = SimpleNamespace(
            title="IPC Nacional",
            description="Inflacion mensual",
            columns='["mes","valor"]',
            tags="ipc,precios",
            organization="INDEC",
            portal="datos_gob_ar",
            format="csv",
            download_url="https://example.com/ipc.csv",
            is_cached=False,
            row_count=10,
        )

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [
            _FetchOneResult(dataset_row),
            MagicMock(),  # delete
            MagicMock(),  # batch insert
        ]
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps({"embeddings": [[0.1] * 3, [0.2] * 3, [0.3] * 3]}).encode()
        mock_bedrock = MagicMock()
        mock_bedrock.invoke_model.return_value = {"body": mock_body}
        mock_boto_client.return_value = mock_bedrock

        result = index_dataset_embedding.run("11111111-1111-1111-1111-111111111111")

        assert result["chunks_created"] == 3
        assert mock_conn.execute.call_count == 3
        batch_rows = mock_conn.execute.call_args_list[-1].args[1]
        assert isinstance(batch_rows, list)
        assert len(batch_rows) == 3

    @patch("boto3.client")
    @patch("app.infrastructure.celery.tasks.embedding_tasks.get_sync_engine")
    def test_index_sesiones_chunks_inserts_batch_rows(
        self,
        mock_get_engine,
        mock_boto_client,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        chunk_path = tmp_path / "chunks.json"
        chunk_path.write_text(
            json.dumps(
                [
                    {
                        "periodo": "141",
                        "reunion": "1",
                        "fecha": "2026-01-01",
                        "tipoSesion": "ordinaria",
                        "pdfUrl": "https://example.com/sesion.pdf",
                        "totalPages": 10,
                        "speaker": "Diputado A",
                        "chunkIndex": 1,
                        "text": "Texto uno",
                    },
                    {
                        "periodo": "141",
                        "reunion": "1",
                        "fecha": "2026-01-01",
                        "tipoSesion": "ordinaria",
                        "pdfUrl": "https://example.com/sesion.pdf",
                        "totalPages": 10,
                        "speaker": "Diputado B",
                        "chunkIndex": 2,
                        "text": "Texto dos",
                    },
                ]
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr("app.infrastructure.celery.tasks.embedding_tasks._CHUNKS_DIR", tmp_path)

        count_conn = MagicMock()
        count_conn.execute.return_value = _FetchAllResult([])
        insert_conn = MagicMock()
        insert_conn.execute.return_value = MagicMock()

        mock_engine = MagicMock()
        mock_engine.begin.side_effect = [
            MagicMock(__enter__=MagicMock(return_value=count_conn), __exit__=MagicMock(return_value=False)),
            MagicMock(__enter__=MagicMock(return_value=insert_conn), __exit__=MagicMock(return_value=False)),
        ]
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps({"embeddings": [[0.1] * 3, [0.2] * 3]}).encode()
        mock_bedrock = MagicMock()
        mock_bedrock.invoke_model.return_value = {"body": mock_body}
        mock_boto_client.return_value = mock_bedrock

        result = index_sesiones_chunks.run(batch_size=50)

        assert result["total_chunks"] == 2
        assert insert_conn.execute.call_count == 1
        rows = insert_conn.execute.call_args.args[1]
        assert isinstance(rows, list)
        assert len(rows) == 2
