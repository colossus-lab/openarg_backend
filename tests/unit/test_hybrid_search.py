"""Tests for hybrid search (BM25 + vector + RRF) in PgVectorSearchAdapter."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter


@pytest.fixture
def mock_session() -> AsyncMock:
    session = AsyncMock()
    return session


@pytest.fixture
def adapter(mock_session: AsyncMock) -> PgVectorSearchAdapter:
    return PgVectorSearchAdapter(mock_session)


@pytest.mark.asyncio
async def test_search_datasets_hybrid_returns_results(adapter: PgVectorSearchAdapter, mock_session: AsyncMock) -> None:
    """Test that hybrid search executes the fusion query and maps results."""
    mock_row = MagicMock()
    mock_row.dataset_id = "123"
    mock_row.title = "Test Dataset"
    mock_row.description = "A test dataset"
    mock_row.portal = "datos.gob.ar"
    mock_row.download_url = "https://example.com/data.csv"
    mock_row.columns = "col1,col2"
    mock_row.score = 0.032

    mock_result = MagicMock()
    mock_result.fetchall.return_value = [mock_row]
    mock_session.execute.return_value = mock_result

    embedding = [0.1] * 768
    results = await adapter.search_datasets_hybrid(
        query_embedding=embedding,
        query_text="inflación argentina",
        limit=5,
    )

    assert len(results) == 1
    assert results[0].dataset_id == "123"
    assert results[0].title == "Test Dataset"
    assert results[0].score == 0.032
    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_search_datasets_hybrid_empty_results(adapter: PgVectorSearchAdapter, mock_session: AsyncMock) -> None:
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_session.execute.return_value = mock_result

    results = await adapter.search_datasets_hybrid(
        query_embedding=[0.1] * 768,
        query_text="nonexistent query",
        limit=5,
    )
    assert results == []


@pytest.mark.asyncio
async def test_search_datasets_hybrid_with_portal_filter(adapter: PgVectorSearchAdapter, mock_session: AsyncMock) -> None:
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_session.execute.return_value = mock_result

    await adapter.search_datasets_hybrid(
        query_embedding=[0.1] * 768,
        query_text="educación",
        limit=3,
        portal_filter="caba",
    )

    call_args = mock_session.execute.call_args
    params = call_args[0][1]
    assert params["portal"] == "caba"


@pytest.mark.asyncio
async def test_search_datasets_still_works(adapter: PgVectorSearchAdapter, mock_session: AsyncMock) -> None:
    """Ensure the original search_datasets method still works (backward compat)."""
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_session.execute.return_value = mock_result

    results = await adapter.search_datasets(
        query_embedding=[0.1] * 768,
        limit=5,
    )
    assert results == []
