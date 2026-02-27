from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.infrastructure.adapters.llm.openai_embedding_adapter import OpenAIEmbeddingAdapter


@pytest.fixture
def mock_client():
    client = AsyncMock()
    return client


@pytest.fixture
def adapter(mock_client):
    a = OpenAIEmbeddingAdapter.__new__(OpenAIEmbeddingAdapter)
    a._client = mock_client
    a._model = "text-embedding-3-small"
    a._dimensions = 1536
    return a


class TestOpenAIEmbeddingAdapter:
    async def test_embed_returns_vector(self, adapter, mock_client):
        expected = [0.1] * 1536
        mock_response = MagicMock()
        mock_response.data = [MagicMock(embedding=expected)]
        mock_client.embeddings.create.return_value = mock_response

        result = await adapter.embed("test query")
        assert result == expected
        mock_client.embeddings.create.assert_awaited_once()

    async def test_embed_batch_returns_list_of_vectors(self, adapter, mock_client):
        v1 = [0.1] * 1536
        v2 = [0.2] * 1536
        item1 = MagicMock()
        item1.embedding = v1
        item1.index = 0
        item2 = MagicMock()
        item2.embedding = v2
        item2.index = 1
        mock_response = MagicMock()
        mock_response.data = [item2, item1]  # reversed order to test sorting
        mock_client.embeddings.create.return_value = mock_response

        result = await adapter.embed_batch(["query1", "query2"])
        assert len(result) == 2
        assert result[0] == v1
        assert result[1] == v2
