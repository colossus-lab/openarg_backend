from __future__ import annotations

import os
from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

# Ensure test env
os.environ.setdefault("APP_ENV", "test")


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ── Mock LLM ──────────────────────────────────────────────────


@dataclass
class FakeLLMResponse:
    content: str = "Respuesta de prueba"
    tokens_used: int = 100
    model: str = "test-model"


@pytest.fixture
def mock_llm():
    llm = AsyncMock()
    llm.chat.return_value = FakeLLMResponse()

    async def fake_stream(*args, **kwargs):
        for chunk in ["Respuesta ", "de ", "prueba"]:
            yield chunk

    llm.chat_stream = fake_stream
    return llm


@pytest.fixture
def mock_embedding():
    emb = AsyncMock()
    emb.embed.return_value = [0.1] * 1536
    emb.embed_batch.return_value = [[0.1] * 1536]
    return emb


# ── Mock connectors ──────────────────────────────────────────


@pytest.fixture
def mock_series():
    connector = AsyncMock()
    connector.search.return_value = [
        {
            "id": "148.3_INIVELNAL_DICI_M_26",
            "title": "IPC Nacional",
            "description": "Inflación Nacional",
        },
    ]
    connector.fetch.return_value = None
    return connector


@pytest.fixture
def mock_ckan():
    connector = AsyncMock()
    connector.search_datasets.return_value = []
    connector.query_datastore.return_value = []
    return connector


@pytest.fixture
def mock_cache():
    cache = AsyncMock()
    cache.get.return_value = None
    cache.set.return_value = None
    cache.delete.return_value = None
    cache.exists.return_value = False
    return cache
