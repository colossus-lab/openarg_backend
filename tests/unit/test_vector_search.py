from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter


@pytest.fixture
def mock_session():
    session = AsyncMock()
    return session


@pytest.fixture
def adapter(mock_session):
    return PgVectorSearchAdapter(mock_session)


class TestPgVectorSearchAdapter:
    async def test_search_returns_empty_list_when_no_results(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        results = await adapter.search_datasets([0.1] * 1536, limit=5)
        assert results == []

    async def test_search_passes_embedding_and_limit(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        embedding = [0.5] * 1536
        await adapter.search_datasets(embedding, limit=3)

        call_args = mock_session.execute.call_args
        params = call_args[0][1]
        assert params["limit"] == 3
        assert "0.5" in params["embedding"]

    async def test_search_with_portal_filter(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets([0.1] * 1536, portal_filter="caba")

        call_args = mock_session.execute.call_args
        params = call_args[0][1]
        assert params["portal"] == "caba"

    async def test_search_deduplicates_by_dataset_in_sql(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets([0.1] * 1536, limit=5)

        call_args = mock_session.execute.call_args
        query = str(call_args[0][0])
        assert "ROW_NUMBER() OVER (" in query
        assert "PARTITION BY d.id" in query
        assert "WHERE dataset_rank = 1" in query

    async def test_search_maps_rows_to_search_results(self, adapter, mock_session):
        mock_row = MagicMock()
        mock_row.dataset_id = "abc-123"
        mock_row.title = "Test Dataset"
        mock_row.description = "Test description"
        mock_row.portal = "datos_gob_ar"
        mock_row.download_url = "https://example.com/data.csv"
        mock_row.columns = '["col1", "col2"]'
        mock_row.score = 0.85

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [mock_row]
        mock_session.execute.return_value = mock_result

        results = await adapter.search_datasets([0.1] * 1536)
        assert len(results) == 1
        assert results[0].dataset_id == "abc-123"
        assert results[0].title == "Test Dataset"
        assert results[0].score == 0.85

    async def test_delete_dataset_chunks(self, adapter, mock_session):
        await adapter.delete_dataset_chunks("abc-123")
        mock_session.execute.assert_awaited_once()
        mock_session.commit.assert_awaited_once()
