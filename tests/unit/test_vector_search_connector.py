from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.application.pipeline.connectors.vector_search import execute_search_datasets_step
from app.domain.entities.connectors.data_result import PlanStep
from app.domain.ports.search.vector_search import SearchResult


@pytest.mark.asyncio
async def test_execute_search_datasets_step_uses_hybrid_retrieval() -> None:
    embedding = AsyncMock()
    embedding.embed.return_value = [0.1, 0.2, 0.3]

    vector_search = AsyncMock()
    vector_search.search_datasets_hybrid.return_value = [
        SearchResult(
            dataset_id="ds-1",
            title="IPC mensual",
            description="Serie de inflación",
            portal="datos.gob.ar",
            download_url="https://example.com/ipc.csv",
            columns='["fecha","valor"]',
            score=0.91,
        )
    ]

    step = PlanStep(
        id="step_vector",
        action="search_datasets",
        description="Buscar datasets relevantes",
        params={"query": "inflacion mensual", "limit": 7},
        depends_on=[],
    )

    results = await execute_search_datasets_step(step, embedding, vector_search)

    embedding.embed.assert_awaited_once_with("inflacion mensual")
    vector_search.search_datasets_hybrid.assert_awaited_once_with(
        [0.1, 0.2, 0.3],
        "inflacion mensual",
        limit=7,
    )
    assert len(results) == 1
    assert results[0].source == "pgvector:datos.gob.ar"
    assert results[0].dataset_title == "IPC mensual"
    assert results[0].metadata["score"] == 0.91


@pytest.mark.asyncio
async def test_execute_search_datasets_step_falls_back_to_description_query() -> None:
    embedding = AsyncMock()
    embedding.embed.return_value = [0.4, 0.5]

    vector_search = AsyncMock()
    vector_search.search_datasets_hybrid.return_value = []

    step = PlanStep(
        id="step_vector",
        action="search_datasets",
        description="hospitales cordoba",
        params={},
        depends_on=[],
    )

    results = await execute_search_datasets_step(step, embedding, vector_search)

    embedding.embed.assert_awaited_once_with("hospitales cordoba")
    vector_search.search_datasets_hybrid.assert_awaited_once_with(
        [0.4, 0.5],
        "hospitales cordoba",
        limit=10,
    )
    assert results == []
