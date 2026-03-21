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
async def test_search_datasets_hybrid_returns_results(
    adapter: PgVectorSearchAdapter,
    mock_session: AsyncMock,
) -> None:
    """Test that hybrid search executes the fusion query and maps results."""
    mock_row = MagicMock()
    mock_row.dataset_id = "123"
    mock_row.title = "Test Dataset"
    mock_row.description = "A test dataset"
    mock_row.portal = "datos.gob.ar"
    mock_row.download_url = "https://example.com/data.csv"
    mock_row.columns = "col1,col2"
    mock_row.score = 0.06

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
    assert results[0].score == 0.06
    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_search_datasets_hybrid_empty_results(
    adapter: PgVectorSearchAdapter,
    mock_session: AsyncMock,
) -> None:
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
async def test_search_datasets_hybrid_with_portal_filter(
    adapter: PgVectorSearchAdapter,
    mock_session: AsyncMock,
) -> None:
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
async def test_search_datasets_still_works(
    adapter: PgVectorSearchAdapter,
    mock_session: AsyncMock,
) -> None:
    """Ensure the original search_datasets method still works (backward compat)."""
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_session.execute.return_value = mock_result

    results = await adapter.search_datasets(
        query_embedding=[0.1] * 768,
        limit=5,
    )
    assert results == []


@pytest.mark.asyncio
async def test_hybrid_search_uses_websearch_to_tsquery(
    adapter: PgVectorSearchAdapter,
    mock_session: AsyncMock,
) -> None:
    """Verify the SQL uses websearch_to_tsquery instead of plainto_tsquery."""
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_session.execute.return_value = mock_result

    await adapter.search_datasets_hybrid(
        query_embedding=[0.1] * 768,
        query_text="inflación mensual",
        limit=5,
    )

    call_args = mock_session.execute.call_args
    sql_text = str(call_args[0][0])
    assert "websearch_to_tsquery" in sql_text
    assert "plainto_tsquery" not in sql_text


@pytest.mark.asyncio
async def test_hybrid_search_rrf_k_param(
    adapter: PgVectorSearchAdapter,
    mock_session: AsyncMock,
) -> None:
    """Verify rrf_k is passed as a SQL parameter."""
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_session.execute.return_value = mock_result

    await adapter.search_datasets_hybrid(
        query_embedding=[0.1] * 768,
        query_text="test",
        limit=5,
        rrf_k=30,
    )

    call_args = mock_session.execute.call_args
    params = call_args[0][1]
    assert params["rrf_k"] == 30


@pytest.mark.asyncio
async def test_hybrid_search_min_score_filters(
    adapter: PgVectorSearchAdapter,
    mock_session: AsyncMock,
) -> None:
    """Verify low-score results are filtered out by min_score."""
    mock_row_high = MagicMock()
    mock_row_high.dataset_id = "1"
    mock_row_high.title = "High Score"
    mock_row_high.description = ""
    mock_row_high.portal = "test"
    mock_row_high.download_url = ""
    mock_row_high.columns = ""
    mock_row_high.score = 0.05

    mock_row_low = MagicMock()
    mock_row_low.dataset_id = "2"
    mock_row_low.title = "Low Score"
    mock_row_low.description = ""
    mock_row_low.portal = "test"
    mock_row_low.download_url = ""
    mock_row_low.columns = ""
    mock_row_low.score = 0.001

    mock_result = MagicMock()
    mock_result.fetchall.return_value = [mock_row_high, mock_row_low]
    mock_session.execute.return_value = mock_result

    results = await adapter.search_datasets_hybrid(
        query_embedding=[0.1] * 768,
        query_text="test",
        limit=5,
        min_score=0.01,
    )

    assert len(results) == 1
    assert results[0].dataset_id == "1"
