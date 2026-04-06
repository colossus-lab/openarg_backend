"""Response format contract tests for the /data/* endpoints.

These tests verify the exact JSON response format that external
consumers (e.g. TerritorIA) receive. The /data/* API is a public
contract and must remain stable across releases. Tests here focus
on response structure, Python-to-JSON type handling, serialization
edge cases (Unicode, emoji, special characters), and HTTP headers.
"""

from __future__ import annotations

import json
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
            table_name="cache_sample",
            dataset_id="ds-001",
            row_count=100,
            columns=["col_a", "col_b"],
        ),
    ]
    return mock


def _default_vector_search() -> AsyncMock:
    mock = AsyncMock(spec=IVectorSearch)
    mock.search_datasets.return_value = [
        SearchResult(
            dataset_id="ds-001",
            title="Sample Dataset",
            description="A sample dataset description",
            portal="sample_portal",
            download_url="https://example.com/sample.csv",
            columns='["col_a", "col_b"]',
            score=0.92,
        ),
    ]
    return mock


def _default_embedding() -> AsyncMock:
    mock = AsyncMock(spec=IEmbeddingProvider)
    mock.embed.return_value = [0.1] * 1536
    return mock


# ---------------------------------------------------------------------------
# Helpers: build app with custom mocks
# ---------------------------------------------------------------------------


def _build_app(
    monkeypatch: pytest.MonkeyPatch,
    sandbox: ISQLSandbox | None = None,
    vector_search: IVectorSearch | None = None,
    embedding: IEmbeddingProvider | None = None,
) -> FastAPI:
    monkeypatch.setenv("DATA_SERVICE_TOKEN", SERVICE_TOKEN)
    fast_app = FastAPI()
    fast_app.include_router(data_router)
    container = make_async_container(
        _make_mock_provider(
            sandbox=sandbox,
            vector_search=vector_search,
            embedding=embedding,
        ),
    )
    setup_dishka(container=container, app=fast_app)
    return fast_app


def _auth_header(token: str = SERVICE_TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _sandbox_with_rows(rows: list[dict], columns: list[str]) -> AsyncMock:
    mock = AsyncMock(spec=ISQLSandbox)
    mock.execute_readonly.return_value = SandboxResult(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        truncated=False,
        error=None,
    )
    mock.list_cached_tables.return_value = []
    return mock


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def app(monkeypatch):
    return _build_app(monkeypatch)


@pytest.fixture
async def client(app) -> AsyncClient:
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ===================================================================
# 1. POST /data/query — response structure contract
# ===================================================================


class TestQueryResponseContract:
    async def test_response_has_exact_fields(self, client):
        resp = await client.post(
            "/data/query",
            json={"sql": "SELECT * FROM cache_sample"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert set(body.keys()) == {
            "columns",
            "rows",
            "row_count",
            "truncated",
            "error",
        }

    async def test_columns_is_list_of_strings(self, client):
        resp = await client.post(
            "/data/query",
            json={"sql": "SELECT * FROM cache_sample"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body["columns"], list)
        assert all(isinstance(c, str) for c in body["columns"])

    async def test_rows_is_list_of_dicts(self, client):
        resp = await client.post(
            "/data/query",
            json={"sql": "SELECT * FROM cache_sample"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body["rows"], list)
        assert all(isinstance(r, dict) for r in body["rows"])

    async def test_row_count_is_integer(self, client):
        resp = await client.post(
            "/data/query",
            json={"sql": "SELECT * FROM cache_sample"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body["row_count"], int)
        assert not isinstance(body["row_count"], bool)

    async def test_truncated_is_boolean(self, client):
        resp = await client.post(
            "/data/query",
            json={"sql": "SELECT * FROM cache_sample"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body["truncated"], bool)
        # Verify raw JSON text contains the literal "false"/"true"
        raw = resp.text
        assert '"truncated":false' in raw or '"truncated": false' in raw

    async def test_error_can_be_null(self, client):
        resp = await client.post(
            "/data/query",
            json={"sql": "SELECT * FROM cache_sample"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["error"] is None
        # And the raw JSON should contain "null"
        raw = resp.text
        assert '"error":null' in raw or '"error": null' in raw

    async def test_error_is_string_when_set(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=[],
            rows=[],
            row_count=0,
            truncated=False,
            error="Only SELECT statements are allowed",
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "DROP TABLE evil"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body["error"], str)
        assert len(body["error"]) > 0


# ===================================================================
# 2. POST /data/query — Python → JSON data types
# ===================================================================


class TestQueryDataTypes:
    async def test_integer_values(self, monkeypatch):
        sandbox = _sandbox_with_rows(
            rows=[{"id": 42, "count": 0, "neg": -7}],
            columns=["id", "count", "neg"],
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM t"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["id"] == 42
        assert isinstance(row["id"], int)
        assert row["count"] == 0
        assert row["neg"] == -7

    async def test_float_values(self, monkeypatch):
        sandbox = _sandbox_with_rows(
            rows=[{"pi": 3.14, "big": 1.5e10, "small": 0.001}],
            columns=["pi", "big", "small"],
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM t"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["pi"] == 3.14
        assert isinstance(row["pi"], float)
        assert row["big"] == 1.5e10
        assert row["small"] == 0.001

    async def test_string_values(self, monkeypatch):
        sandbox = _sandbox_with_rows(
            rows=[{"name": "hello world", "code": "AB-123"}],
            columns=["name", "code"],
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM t"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["name"] == "hello world"
        assert row["code"] == "AB-123"
        assert isinstance(row["name"], str)

    async def test_null_values(self, monkeypatch):
        sandbox = _sandbox_with_rows(
            rows=[{"a": None, "b": "present"}],
            columns=["a", "b"],
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM t"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["a"] is None
        assert row["b"] == "present"
        raw = resp.text
        assert '"a":null' in raw or '"a": null' in raw

    async def test_boolean_values(self, monkeypatch):
        sandbox = _sandbox_with_rows(
            rows=[{"active": True, "deleted": False}],
            columns=["active", "deleted"],
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM t"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["active"] is True
        assert row["deleted"] is False
        assert isinstance(row["active"], bool)
        raw = resp.text
        assert "true" in raw
        assert "false" in raw

    async def test_unicode_strings(self, monkeypatch):
        sandbox = _sandbox_with_rows(
            rows=[{"char": "ñ", "city": "Córdoba", "word": "Año"}],
            columns=["char", "city", "word"],
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM t"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["char"] == "ñ"
        assert row["city"] == "Córdoba"
        assert row["word"] == "Año"

    async def test_emoji_in_values(self, monkeypatch):
        sandbox = _sandbox_with_rows(
            rows=[{"label": "🇦🇷 datos"}],
            columns=["label"],
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM t"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["label"] == "🇦🇷 datos"

    async def test_special_chars_escaped(self, monkeypatch):
        raw_value = 'has "quotes" and \\backslash\\ and\nnewline\tand\ttab'
        sandbox = _sandbox_with_rows(
            rows=[{"text": raw_value}],
            columns=["text"],
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM t"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        # Re-parse the raw body to ensure it is valid JSON
        parsed = json.loads(resp.text)
        assert parsed["rows"][0]["text"] == raw_value

    async def test_empty_strings(self, monkeypatch):
        sandbox = _sandbox_with_rows(
            rows=[{"a": "", "b": "not empty"}],
            columns=["a", "b"],
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM t"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["a"] == ""
        assert row["b"] == "not empty"

    async def test_numeric_columns_names(self, monkeypatch):
        sandbox = _sandbox_with_rows(
            rows=[{"0": "zero", "1": "one", "column1": "named"}],
            columns=["0", "1", "column1"],
        )
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM t"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        row = resp.json()["rows"][0]
        assert row["0"] == "zero"
        assert row["1"] == "one"
        assert row["column1"] == "named"


# ===================================================================
# 3. POST /data/search — response structure contract
# ===================================================================


class TestSearchResponseContract:
    async def test_search_response_is_list(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "something"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)

    async def test_search_item_has_exact_fields(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "something"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) >= 1
        assert set(body[0].keys()) == {
            "table_name",
            "title",
            "description",
            "relevance",
        }

    async def test_search_table_name_is_string(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "something"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body[0]["table_name"], str)

    async def test_search_relevance_is_float(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "something"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body[0]["relevance"], float)
        assert not isinstance(body[0]["relevance"], bool)

    async def test_search_relevance_rounded_3_decimals(self, monkeypatch):
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            SearchResult(
                dataset_id="ds-001",
                title="Sample",
                description="A sample",
                portal="p",
                download_url="",
                columns='["x"]',
                score=0.123456,
            ),
        ]
        fast_app = _build_app(monkeypatch, vector_search=vector_search)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/search",
                json={"query": "something"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body[0]["relevance"] == 0.123

    async def test_empty_search_returns_empty_array(self, monkeypatch):
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = []
        fast_app = _build_app(monkeypatch, vector_search=vector_search)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/data/search",
                json={"query": "no matches"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body == []
        assert isinstance(body, list)
        assert resp.text.strip() == "[]"


# ===================================================================
# 4. GET /data/tables — response structure contract
# ===================================================================


class TestTablesResponseContract:
    async def test_tables_response_is_list(self, client):
        resp = await client.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)

    async def test_tables_item_has_exact_fields(self, client):
        resp = await client.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) >= 1
        assert set(body[0].keys()) == {
            "table_name",
            "dataset_id",
            "row_count",
            "columns",
        }

    async def test_row_count_can_be_null(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            CachedTableInfo(
                table_name="cache_sample",
                dataset_id="ds-001",
                row_count=None,
                columns=["col_a"],
            ),
        ]
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200
        body = resp.json()
        assert body[0]["row_count"] is None
        raw = resp.text
        assert '"row_count":null' in raw or '"row_count": null' in raw

    async def test_dataset_id_is_string_always(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            CachedTableInfo(
                table_name="cache_sample",
                dataset_id=None,  # type: ignore[arg-type]
                row_count=10,
                columns=["col_a"],
            ),
        ]
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200
        body = resp.json()
        assert body[0]["dataset_id"] == ""
        assert isinstance(body[0]["dataset_id"], str)

    async def test_columns_always_list_of_strings(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            CachedTableInfo(
                table_name="cache_sample",
                dataset_id="ds-001",
                row_count=10,
                columns=["name", 59, "value"],  # type: ignore[list-item]
            ),
        ]
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200
        body = resp.json()
        assert body[0]["columns"] == ["name", "59", "value"]
        assert all(isinstance(c, str) for c in body[0]["columns"])

    async def test_empty_tables_returns_empty_array(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = []
        fast_app = _build_app(monkeypatch, sandbox=sandbox)

        transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200
        body = resp.json()
        assert body == []
        assert isinstance(body, list)
        assert resp.text.strip() == "[]"


# ===================================================================
# 5. Content-Type headers
# ===================================================================


class TestContentTypeHeaders:
    async def test_query_returns_json_content_type(self, client):
        resp = await client.post(
            "/data/query",
            json={"sql": "SELECT 1"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/json")

    async def test_tables_returns_json_content_type(self, client):
        resp = await client.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/json")

    async def test_search_returns_json_content_type(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "datos"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/json")
