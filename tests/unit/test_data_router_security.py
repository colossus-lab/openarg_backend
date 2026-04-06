"""Security and SQL-injection tests for the /data/query endpoint.

These tests simulate how an attacker with a valid service token would try to
bypass the sandbox. Since the sandbox is mocked, we verify that when the
sandbox returns error strings (e.g. "Access denied", "Only SELECT allowed"),
the router propagates them correctly to the client.

We also cover Pydantic validation limits, auth edge cases, and timeout/
truncation propagation.
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
# Dishka mock provider (duplicated from test_data_router.py by design)
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
            table_name="cache_example",
            dataset_id="ds-001",
            row_count=100,
            columns=["col_a", "col_b"],
        ),
    ]
    return mock


def _default_vector_search() -> AsyncMock:
    mock = AsyncMock(spec=IVectorSearch)
    mock.search_datasets.return_value = []
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


def _build_client_with_sandbox(monkeypatch, sandbox: AsyncMock) -> AsyncClient:
    """Build a fresh app/client with a custom sandbox mock."""
    monkeypatch.setenv("DATA_SERVICE_TOKEN", SERVICE_TOKEN)
    fast_app = FastAPI()
    fast_app.include_router(data_router)
    container = make_async_container(_make_mock_provider(sandbox=sandbox))
    setup_dishka(container=container, app=fast_app)
    transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
    return AsyncClient(transport=transport, base_url="http://test")


def _blocked_sandbox(error: str) -> AsyncMock:
    """Build a sandbox mock that rejects every query with the given error."""
    mock = AsyncMock(spec=ISQLSandbox)
    mock.execute_readonly.return_value = SandboxResult(
        columns=[],
        rows=[],
        row_count=0,
        truncated=False,
        error=error,
    )
    return mock


# ===================================================================
# 1. SQL Injection attack vectors
# ===================================================================


class TestSQLInjectionAttacks:
    """SQL that should be blocked by the sandbox's validation layers."""

    async def test_union_attack_on_protected_table(self, monkeypatch):
        sandbox = _blocked_sandbox("Access denied: table 'users' is not allowed")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM cache_sample UNION SELECT * FROM users"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["error"] == "Access denied: table 'users' is not allowed"
        assert body["rows"] == []
        assert body["row_count"] == 0

    async def test_subquery_on_protected_table(self, monkeypatch):
        sandbox = _blocked_sandbox("Access denied: subquery references forbidden table")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={
                    "sql": (
                        "SELECT * FROM cache_sample "
                        "WHERE x IN (SELECT email FROM users)"
                    ),
                },
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert resp.json()["error"] == (
            "Access denied: subquery references forbidden table"
        )

    async def test_cte_on_protected_table(self, monkeypatch):
        sandbox = _blocked_sandbox("Access denied: CTE references forbidden table")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={
                    "sql": (
                        "WITH x AS (SELECT * FROM users) "
                        "SELECT * FROM cache_sample LIMIT 1"
                    ),
                },
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert "CTE" in resp.json()["error"]

    async def test_stacked_statements_blocked(self, monkeypatch):
        sandbox = _blocked_sandbox("Only a single SELECT statement is allowed")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT 1; DROP TABLE cache_sample"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert resp.json()["error"] == "Only a single SELECT statement is allowed"

    async def test_information_schema_probe_blocked(self, monkeypatch):
        sandbox = _blocked_sandbox("Access denied: information_schema is forbidden")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM information_schema.tables"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert "information_schema" in resp.json()["error"]

    async def test_pg_catalog_probe_blocked(self, monkeypatch):
        sandbox = _blocked_sandbox("Access denied: pg_catalog is forbidden")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM pg_catalog.pg_user"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert "pg_catalog" in resp.json()["error"]


# ===================================================================
# 2. Forbidden non-SELECT operations
# ===================================================================


class TestForbiddenOperations:
    """Non-SELECT statements that the sandbox must reject."""

    async def test_drop_table_blocked(self, monkeypatch):
        sandbox = _blocked_sandbox("Only SELECT statements are allowed")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "DROP TABLE cache_sample"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert "Only SELECT" in resp.json()["error"]

    async def test_delete_blocked(self, monkeypatch):
        sandbox = _blocked_sandbox("Only SELECT statements are allowed")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "DELETE FROM cache_sample WHERE 1=1"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert "Only SELECT" in resp.json()["error"]

    async def test_update_blocked(self, monkeypatch):
        sandbox = _blocked_sandbox("Only SELECT statements are allowed")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "UPDATE cache_sample SET x=1"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert "Only SELECT" in resp.json()["error"]

    async def test_insert_blocked(self, monkeypatch):
        sandbox = _blocked_sandbox("Only SELECT statements are allowed")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "INSERT INTO cache_sample VALUES (1,2)"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert "Only SELECT" in resp.json()["error"]

    async def test_truncate_blocked(self, monkeypatch):
        sandbox = _blocked_sandbox("Only SELECT statements are allowed")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "TRUNCATE cache_sample"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert "Only SELECT" in resp.json()["error"]

    async def test_alter_blocked(self, monkeypatch):
        sandbox = _blocked_sandbox("Only SELECT statements are allowed")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "ALTER TABLE cache_sample DROP COLUMN x"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert "Only SELECT" in resp.json()["error"]

    async def test_grant_blocked(self, monkeypatch):
        sandbox = _blocked_sandbox("Only SELECT statements are allowed")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "GRANT ALL ON cache_sample TO public"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert "Only SELECT" in resp.json()["error"]


# ===================================================================
# 3. Pydantic length limits
# ===================================================================


class TestSQLLengthLimits:
    """Schema-level validation for the `sql` field (min_length=1, max_length=5000)."""

    async def test_sql_at_exact_max_length_is_valid(self, client):
        # Build a valid-looking SELECT of exactly 5000 chars by padding the WHERE clause.
        prefix = "SELECT * FROM cache_example WHERE col_a = '"
        suffix = "'"
        padding_len = 5000 - len(prefix) - len(suffix)
        assert padding_len > 0
        sql = prefix + ("a" * padding_len) + suffix
        assert len(sql) == 5000

        resp = await client.post(
            "/data/query",
            json={"sql": sql},
            headers=_auth_header(),
        )
        # Passes Pydantic → default sandbox returns the happy-path result.
        assert resp.status_code == 200
        assert resp.json()["error"] is None

    async def test_sql_above_max_length_returns_422(self, client):
        sql = "S" + ("a" * 5000)  # 5001 chars
        assert len(sql) == 5001
        resp = await client.post(
            "/data/query",
            json={"sql": sql},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_sql_with_single_char_passes_validation(self, monkeypatch):
        # 1 char passes Pydantic (min_length=1). The sandbox will reject it with
        # its own error, which must be propagated.
        sandbox = _blocked_sandbox("Invalid SQL: parse error")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "S"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        assert resp.json()["error"] == "Invalid SQL: parse error"


# ===================================================================
# 4. Auth edge cases
# ===================================================================


class TestAuthEdgeCases:
    """Edge cases around the Bearer-token parser in verify_service_token()."""

    async def test_token_with_trailing_whitespace_is_stripped(self, client):
        # The router does `auth_header[7:].strip()`, so surrounding spaces are removed
        # and the token still matches via secrets.compare_digest.
        resp = await client.get(
            "/data/tables",
            headers={"Authorization": f"Bearer  {SERVICE_TOKEN}  "},
        )
        assert resp.status_code == 200

    async def test_lowercase_bearer_prefix_rejected(self, client):
        # The check `startswith("Bearer ")` is case-sensitive.
        resp = await client.get(
            "/data/tables",
            headers={"Authorization": f"bearer {SERVICE_TOKEN}"},
        )
        assert resp.status_code == 401

    async def test_multi_word_after_bearer_rejected(self, client):
        # "token1 token2" (after stripping) does not equal the expected token.
        resp = await client.get(
            "/data/tables",
            headers={"Authorization": "Bearer token1 token2"},
        )
        assert resp.status_code == 401

    async def test_very_long_token_rejected(self, client):
        very_long = "a" * 10000
        resp = await client.get(
            "/data/tables",
            headers={"Authorization": f"Bearer {very_long}"},
        )
        assert resp.status_code == 401

    async def test_unicode_token_rejected(self, client):
        # secrets.compare_digest will reject any non-matching string; unicode
        # bytes do not crash the comparison.
        resp = await client.get(
            "/data/tables",
            headers={"Authorization": "Bearer svc_emoji_unicode_key"},
        )
        assert resp.status_code == 401

    async def test_wrong_token_uses_constant_time_comparison(self, client):
        # We can't measure timing here, but we can verify that a token with the
        # same length as SERVICE_TOKEN but different content is rejected with
        # the same 401 code (via secrets.compare_digest).
        same_length_wrong = "x" * len(SERVICE_TOKEN)
        assert len(same_length_wrong) == len(SERVICE_TOKEN)
        resp = await client.get(
            "/data/tables",
            headers=_auth_header(same_length_wrong),
        )
        assert resp.status_code == 401

    async def test_prefix_match_of_token_rejected(self, client):
        # A prefix of the real token must not pass (compare_digest is strict).
        prefix = SERVICE_TOKEN[:-2]
        resp = await client.get(
            "/data/tables",
            headers=_auth_header(prefix),
        )
        assert resp.status_code == 401


# ===================================================================
# 5. Timeout and truncation propagation
# ===================================================================


class TestQueryTimeout:
    """Sandbox timeout / truncation signals must surface in the response."""

    async def test_timeout_error_propagated(self, monkeypatch):
        sandbox = _blocked_sandbox("Query timed out after 10 seconds")
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM cache_example"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["error"] == "Query timed out after 10 seconds"
        assert body["rows"] == []
        assert body["row_count"] == 0

    async def test_truncated_flag_propagated(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["id"],
            rows=[{"id": i} for i in range(3)],
            row_count=3,
            truncated=True,
            error=None,
        )
        async with _build_client_with_sandbox(monkeypatch, sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT id FROM cache_example"},
                headers=_auth_header(),
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["truncated"] is True
        assert body["row_count"] == 3
        assert body["error"] is None
