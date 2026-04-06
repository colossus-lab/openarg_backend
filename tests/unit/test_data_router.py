"""Unit tests for the /data/* endpoints (data_router).

Tests cover authentication, POST /data/query, GET /data/tables,
and POST /data/search using mocked ISQLSandbox, IVectorSearch,
and IEmbeddingProvider dependencies.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from dishka import Provider, Scope, make_async_container, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.domain.ports.llm.llm_provider import IEmbeddingProvider
from app.domain.ports.sandbox.sql_sandbox import (
    CachedTableInfo,
    ISQLSandbox,
    SandboxResult,
)
from app.domain.ports.search.vector_search import IVectorSearch, SearchResult
from app.presentation.http.controllers.data.data_router import router as data_router

SERVICE_TOKEN = "svc_test_token_12345"


# ---------------------------------------------------------------------------
# Dishka mock provider
# ---------------------------------------------------------------------------


def _make_mock_provider(
    sandbox: ISQLSandbox | None = None,
    vector_search: IVectorSearch | None = None,
    embedding: IEmbeddingProvider | None = None,
) -> Provider:
    _sandbox = sandbox or _default_sandbox()
    _vector_search = vector_search or _default_vector_search()
    _embedding = embedding or _default_embedding()

    class TestProvider(Provider):
        scope = Scope.REQUEST

        @provide
        def sandbox(self) -> ISQLSandbox:
            return _sandbox

        @provide
        def vector_search(self) -> IVectorSearch:
            return _vector_search

        @provide
        def embedding(self) -> IEmbeddingProvider:
            return _embedding

    return TestProvider()


def _default_sandbox() -> AsyncMock:
    mock = AsyncMock(spec=ISQLSandbox)
    mock.execute_readonly.return_value = SandboxResult(
        columns=["id", "name"],
        rows=[{"id": 1, "name": "test"}],
        row_count=1,
        truncated=False,
        error=None,
    )
    mock.list_cached_tables.return_value = [
        CachedTableInfo(
            table_name="cache_ipc",
            dataset_id="ds-001",
            row_count=100,
            columns=["fecha", "valor"],
        ),
    ]
    return mock


def _default_vector_search() -> AsyncMock:
    mock = AsyncMock(spec=IVectorSearch)
    mock.search_datasets.return_value = [
        SearchResult(
            dataset_id="ds-001",
            title="IPC Nacional",
            description="Indice de precios al consumidor",
            portal="datos_gob_ar",
            download_url="https://example.com/ipc.csv",
            columns='["fecha", "valor"]',
            score=0.92,
        ),
    ]
    return mock


def _default_embedding() -> AsyncMock:
    mock = AsyncMock(spec=IEmbeddingProvider)
    mock.embed.return_value = [0.1] * 1536
    return mock


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("DATA_SERVICE_TOKEN", SERVICE_TOKEN)
    fast_app = FastAPI()
    fast_app.include_router(data_router)
    container = make_async_container(_make_mock_provider())
    setup_dishka(container=container, app=fast_app)
    return fast_app


@pytest.fixture
async def client(app) -> AsyncClient:
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _auth_header(token: str = SERVICE_TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ===================================================================
# 1. Auth tests
# ===================================================================


class TestAuthVerification:
    async def test_valid_token_allows_request(self, client):
        resp = await client.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200

    async def test_missing_auth_header_returns_401(self, client):
        resp = await client.get("/data/tables")
        assert resp.status_code == 401

    async def test_wrong_token_returns_401(self, client):
        resp = await client.get("/data/tables", headers=_auth_header("wrong_token"))
        assert resp.status_code == 401

    async def test_bearer_prefix_required(self, client):
        resp = await client.get(
            "/data/tables",
            headers={"Authorization": SERVICE_TOKEN},
        )
        assert resp.status_code == 401

    async def test_empty_bearer_returns_401(self, client):
        resp = await client.get(
            "/data/tables",
            headers={"Authorization": "Bearer "},
        )
        assert resp.status_code == 401

    async def test_empty_service_token_returns_503(self, monkeypatch):
        monkeypatch.setenv("DATA_SERVICE_TOKEN", "")
        fast_app = FastAPI()
        fast_app.include_router(data_router)
        container = make_async_container(_make_mock_provider())
        setup_dishka(container=container, app=fast_app)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/data/tables", headers=_auth_header())
            assert resp.status_code == 503
            assert "not configured" in resp.json()["detail"]


# ===================================================================
# 2. POST /data/query
# ===================================================================


class TestDataQuery:
    async def test_valid_sql_returns_result(self, client):
        resp = await client.post(
            "/data/query",
            json={"sql": "SELECT * FROM cache_ipc"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["columns"] == ["id", "name"]
        assert body["row_count"] == 1
        assert body["truncated"] is False
        assert body["error"] is None

    async def test_empty_sql_returns_422(self, client):
        resp = await client.post(
            "/data/query",
            json={"sql": ""},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_missing_sql_field_returns_422(self, client):
        resp = await client.post(
            "/data/query",
            json={},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_sandbox_error_propagated(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=[], rows=[], row_count=0, truncated=False,
            error="Only SELECT statements are allowed",
        )
        monkeypatch.setenv("DATA_SERVICE_TOKEN", SERVICE_TOKEN)

        fast_app = FastAPI()
        fast_app.include_router(data_router)
        container = make_async_container(_make_mock_provider(sandbox=sandbox))
        setup_dishka(container=container, app=fast_app)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "DROP TABLE evil"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert resp.json()["error"] == "Only SELECT statements are allowed"


# ===================================================================
# 3. GET /data/tables
# ===================================================================


class TestDataTables:
    async def test_returns_table_list(self, client):
        resp = await client.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1
        assert body[0]["table_name"] == "cache_ipc"
        assert body[0]["dataset_id"] == "ds-001"
        assert body[0]["row_count"] == 100

    async def test_empty_table_list(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = []
        monkeypatch.setenv("DATA_SERVICE_TOKEN", SERVICE_TOKEN)

        fast_app = FastAPI()
        fast_app.include_router(data_router)
        container = make_async_container(_make_mock_provider(sandbox=sandbox))
        setup_dishka(container=container, app=fast_app)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200
        assert resp.json() == []


# ===================================================================
# 4. POST /data/search
# ===================================================================


class TestDataSearch:
    async def test_valid_query_returns_results(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "inflacion", "limit": 5},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1
        assert body[0]["table_name"] == "cache_ipc"
        assert body[0]["relevance"] == 0.92

    async def test_no_cached_tables_match_returns_empty(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            CachedTableInfo("cache_other", "ds-other", 10, ["x"]),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            SearchResult(
                dataset_id="ds-not-cached", title="Uncached",
                description="Not cached", portal="test",
                download_url="", columns='["a"]', score=0.95,
            ),
        ]
        monkeypatch.setenv("DATA_SERVICE_TOKEN", SERVICE_TOKEN)

        fast_app = FastAPI()
        fast_app.include_router(data_router)
        container = make_async_container(
            _make_mock_provider(sandbox=sandbox, vector_search=vector_search),
        )
        setup_dishka(container=container, app=fast_app)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/search",
                json={"query": "something"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_empty_query_returns_422(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": ""},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_limit_above_max_returns_422(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "datos", "limit": 100},
            headers=_auth_header(),
        )
        assert resp.status_code == 422
