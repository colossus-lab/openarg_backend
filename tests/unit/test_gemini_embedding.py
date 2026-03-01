from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.infrastructure.adapters.llm.gemini_embedding_adapter import GeminiEmbeddingAdapter


@pytest.fixture
def adapter():
    with patch("app.infrastructure.adapters.llm.gemini_embedding_adapter.genai"):
        a = GeminiEmbeddingAdapter(api_key="fake-key", model="gemini-embedding-001", dimensions=768)
        yield a


class TestGeminiEmbeddingAdapter:
    async def test_embed_returns_vector(self, adapter):
        expected = [0.1] * 768
        mock_to_thread = AsyncMock(return_value={"embedding": expected})
        with patch("app.infrastructure.adapters.llm.gemini_embedding_adapter.asyncio") as mock_asyncio:
            mock_asyncio.to_thread = mock_to_thread
            result = await adapter.embed("test query")

        assert result == expected
        mock_to_thread.assert_awaited_once()

    async def test_embed_batch_returns_list_of_vectors(self, adapter):
        v1 = [0.1] * 768
        v2 = [0.2] * 768
        mock_to_thread = AsyncMock(return_value={"embedding": [v1, v2]})
        with patch("app.infrastructure.adapters.llm.gemini_embedding_adapter.asyncio") as mock_asyncio:
            mock_asyncio.to_thread = mock_to_thread
            result = await adapter.embed_batch(["query1", "query2"])

        assert len(result) == 2
        assert result[0] == v1
        assert result[1] == v2
