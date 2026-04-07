from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.celery.tasks.embedding_tasks import index_sesiones_chunks


class _FetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class TestSesionesIncrementalP3:
    @patch("boto3.client")
    @patch("app.infrastructure.celery.tasks.embedding_tasks.get_sync_engine")
    def test_index_sesiones_skips_when_all_chunks_already_exist(
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
                        "pdfUrl": "https://example.com/sesion.pdf",
                        "chunkIndex": 1,
                        "text": "Texto uno",
                    },
                    {
                        "pdfUrl": "https://example.com/sesion.pdf",
                        "chunkIndex": 2,
                        "text": "Texto dos",
                    },
                ]
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr("app.infrastructure.celery.tasks.embedding_tasks._CHUNKS_DIR", tmp_path)

        count_conn = MagicMock()
        count_conn.execute.return_value = _FetchAllResult(
            [
                ("https://example.com/sesion.pdf", 1),
                ("https://example.com/sesion.pdf", 2),
            ]
        )

        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=count_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        result = index_sesiones_chunks.run(batch_size=50)

        assert result["status"] == "already_indexed"
        assert result["count"] == 2
        assert result["skipped"] == 2
        mock_boto_client.assert_not_called()

    @patch("boto3.client")
    @patch("app.infrastructure.celery.tasks.embedding_tasks.get_sync_engine")
    def test_index_sesiones_embeds_only_new_chunks(
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

        existing_conn = MagicMock()
        existing_conn.execute.return_value = _FetchAllResult(
            [("https://example.com/sesion.pdf", 1)]
        )
        insert_conn = MagicMock()
        insert_conn.execute.return_value = MagicMock()

        mock_engine = MagicMock()
        mock_engine.begin.side_effect = [
            MagicMock(__enter__=MagicMock(return_value=existing_conn), __exit__=MagicMock(return_value=False)),
            MagicMock(__enter__=MagicMock(return_value=insert_conn), __exit__=MagicMock(return_value=False)),
        ]
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps({"embeddings": [[0.2] * 3]}).encode()
        mock_bedrock = MagicMock()
        mock_bedrock.invoke_model.return_value = {"body": mock_body}
        mock_boto_client.return_value = mock_bedrock

        result = index_sesiones_chunks.run(batch_size=50)

        assert result["status"] == "indexed"
        assert result["total_chunks"] == 1
        assert result["skipped"] == 1
        mock_bedrock.invoke_model.assert_called_once()
        rows = insert_conn.execute.call_args.args[1]
        assert len(rows) == 1
        assert rows[0]["chunk_index"] == 2
