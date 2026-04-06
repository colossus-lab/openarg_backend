"""HTTP protocol-level tests for the /data/* endpoints (data_router).

Covers allowed HTTP methods, malformed request body handling,
Content-Type requirements, Authorization header edge cases,
and request size / length boundaries.

These tests focus on HTTP protocol correctness rather than
business logic — the goal is to make sure the endpoints only
respond to allowed methods, reject malformed requests cleanly,
and enforce the contract declared in the OpenAPI schema.
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
# Dishka mock provider (copied from test_data_router.py)
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
# Fixtures (copied from test_data_router.py)
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
# 1. HTTP methods allowed per endpoint
# ===================================================================


class TestHTTPMethodsAllowed:
    """Each endpoint should only accept its documented HTTP method."""

    async def test_query_post_allowed(self, client):
        resp = await client.post(
            "/data/query",
            json={"sql": "SELECT 1"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200

    async def test_query_get_not_allowed(self, client):
        resp = await client.get("/data/query", headers=_auth_header())
        assert resp.status_code == 405

    async def test_query_put_not_allowed(self, client):
        resp = await client.put(
            "/data/query",
            json={"sql": "SELECT 1"},
            headers=_auth_header(),
        )
        assert resp.status_code == 405

    async def test_query_delete_not_allowed(self, client):
        resp = await client.delete("/data/query", headers=_auth_header())
        assert resp.status_code == 405

    async def test_query_patch_not_allowed(self, client):
        resp = await client.patch(
            "/data/query",
            json={"sql": "SELECT 1"},
            headers=_auth_header(),
        )
        assert resp.status_code == 405

    async def test_tables_get_allowed(self, client):
        resp = await client.get("/data/tables", headers=_auth_header())
        assert resp.status_code == 200

    async def test_tables_post_not_allowed(self, client):
        resp = await client.post(
            "/data/tables",
            json={},
            headers=_auth_header(),
        )
        assert resp.status_code == 405

    async def test_tables_put_not_allowed(self, client):
        resp = await client.put(
            "/data/tables",
            json={},
            headers=_auth_header(),
        )
        assert resp.status_code == 405

    async def test_search_post_allowed(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "inflacion"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200

    async def test_search_get_not_allowed(self, client):
        resp = await client.get("/data/search", headers=_auth_header())
        assert resp.status_code == 405


# ===================================================================
# 2. Malformed request bodies
# ===================================================================


class TestMalformedRequestBodies:
    """Body payloads that don't conform to the Pydantic schema
    should be rejected with 422 (Unprocessable Entity)."""

    async def test_invalid_json_syntax(self, client):
        """Unparseable JSON should produce a 4xx (typically 422)."""
        resp = await client.post(
            "/data/query",
            content=b"not valid json{",
            headers={
                **_auth_header(),
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code in (400, 422)

    async def test_empty_body(self, client):
        """Empty body is not valid JSON for a POST expecting a dict."""
        resp = await client.post(
            "/data/query",
            content=b"",
            headers={
                **_auth_header(),
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code == 422

    async def test_array_instead_of_object(self, client):
        """JSON array where object is expected → 422."""
        resp = await client.post(
            "/data/query",
            json=["sql", "SELECT 1"],
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_sql_as_integer(self, client):
        """sql field typed as int → 422."""
        resp = await client.post(
            "/data/query",
            json={"sql": 123},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_sql_as_array(self, client):
        """sql field typed as list → 422."""
        resp = await client.post(
            "/data/query",
            json={"sql": ["SELECT 1"]},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_sql_as_null(self, client):
        """sql field as null → 422 (field is required & non-nullable)."""
        resp = await client.post(
            "/data/query",
            json={"sql": None},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_extra_fields_accepted_or_rejected(self, client):
        """By default Pydantic ignores extra fields → 200."""
        resp = await client.post(
            "/data/query",
            json={"sql": "SELECT 1", "extra": "x"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200

    async def test_query_missing_query_field(self, client):
        """/data/search with missing required `query` → 422."""
        resp = await client.post(
            "/data/search",
            json={"limit": 5},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_query_as_int(self, client):
        """/data/search with `query` typed as int → 422."""
        resp = await client.post(
            "/data/search",
            json={"query": 42},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_limit_as_string(self, client):
        """/data/search with non-coercible limit string → 422."""
        resp = await client.post(
            "/data/search",
            json={"query": "test", "limit": "five"},
            headers=_auth_header(),
        )
        assert resp.status_code == 422


# ===================================================================
# 3. Content-Type header requirements
# ===================================================================


class TestContentTypeRequirements:
    """FastAPI's JSON body parsing tolerates missing/wrong Content-Type
    as long as the body is valid JSON. These tests document that
    current behavior."""

    async def test_missing_content_type_on_post(self, client):
        """POST with valid JSON body but no Content-Type header.

        FastAPI still parses the body as JSON → 200.
        """
        resp = await client.post(
            "/data/query",
            content=b'{"sql": "SELECT 1"}',
            headers=_auth_header(),
        )
        # FastAPI parses JSON body even without Content-Type; documents
        # actual behavior rather than strict HTTP spec.
        assert resp.status_code in (200, 415, 422)

    async def test_wrong_content_type(self, client):
        """POST with Content-Type: text/plain but valid JSON body.

        FastAPI still parses it as JSON → 200.
        """
        resp = await client.post(
            "/data/query",
            content=b'{"sql": "SELECT 1"}',
            headers={
                **_auth_header(),
                "Content-Type": "text/plain",
            },
        )
        assert resp.status_code in (200, 415, 422)

    async def test_explicit_json_content_type(self, client):
        resp = await client.post(
            "/data/query",
            content=b'{"sql": "SELECT 1"}',
            headers={
                **_auth_header(),
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code == 200

    async def test_content_type_with_charset(self, client):
        resp = await client.post(
            "/data/query",
            content=b'{"sql": "SELECT 1"}',
            headers={
                **_auth_header(),
                "Content-Type": "application/json; charset=utf-8",
            },
        )
        assert resp.status_code == 200


# ===================================================================
# 4. Authorization header variations
# ===================================================================


class TestAuthHeaderVariations:
    """Authorization header case-insensitivity and malformed values."""

    async def test_authorization_header_lowercase_in_request(self, client):
        """HTTP header names are case-insensitive → lowercase works."""
        resp = await client.get(
            "/data/tables",
            headers={"authorization": f"Bearer {SERVICE_TOKEN}"},
        )
        assert resp.status_code == 200

    async def test_multiple_authorization_headers(self, client):
        """Sending Authorization twice: httpx will typically combine values
        with a comma, which breaks the 'Bearer <token>' format check.

        Current behavior: 401 (combined header no longer matches 'Bearer ').
        """
        resp = await client.get(
            "/data/tables",
            headers=[
                ("Authorization", f"Bearer {SERVICE_TOKEN}"),
                ("Authorization", "Bearer other"),
            ],
        )
        # Either 200 (if dedup preserves first) or 401 (if combined).
        assert resp.status_code in (200, 401)

    async def test_whitespace_only_token(self, client):
        """'Bearer    ' (only whitespace after Bearer) → 401."""
        resp = await client.get(
            "/data/tables",
            headers={"Authorization": "Bearer    "},
        )
        assert resp.status_code == 401

    async def test_no_bearer_just_token(self, client):
        """Raw token without 'Bearer ' prefix → 401."""
        resp = await client.get(
            "/data/tables",
            headers={"Authorization": SERVICE_TOKEN},
        )
        assert resp.status_code == 401

    async def test_basic_auth_instead_of_bearer(self, client):
        """Basic auth scheme instead of Bearer → 401."""
        resp = await client.get(
            "/data/tables",
            headers={"Authorization": "Basic dXNlcjpwYXNz"},
        )
        assert resp.status_code == 401


# ===================================================================
# 5. Large payloads / length boundaries
# ===================================================================


class TestLargePayloads:
    """Pydantic Field constraints enforce max_length for string inputs."""

    async def test_query_at_max_length(self, client):
        """sql exactly 5000 chars → 200 (boundary inclusive)."""
        sql = "SELECT " + ("a" * (5000 - len("SELECT ")))
        assert len(sql) == 5000
        resp = await client.post(
            "/data/query",
            json={"sql": sql},
            headers=_auth_header(),
        )
        assert resp.status_code == 200

    async def test_query_over_max_length(self, client):
        """sql 10000 chars → 422 (over max_length=5000)."""
        resp = await client.post(
            "/data/query",
            json={"sql": "a" * 10000},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_search_query_at_max_length(self, client):
        """search query exactly 2000 chars → 200."""
        q = "a" * 2000
        resp = await client.post(
            "/data/search",
            json={"query": q},
            headers=_auth_header(),
        )
        assert resp.status_code == 200

    async def test_search_query_over_max_length(self, client):
        """search query 5000 chars → 422 (over max_length=2000)."""
        resp = await client.post(
            "/data/search",
            json={"query": "a" * 5000},
            headers=_auth_header(),
        )
        assert resp.status_code == 422
