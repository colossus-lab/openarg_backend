"""End-to-end flow tests for the /data/* endpoints from Territoria's POV.

Territoria is a GovTech platform for Argentine municipal governments that
consumes OpenArg's /data/* API to discover relevant tables, query them with
hardcoded SQL, and feed the results into its own LLM policy generator.

These tests simulate the real request sequences Territoria performs:
    1. POST /data/search   -> find relevant cached tables
    2. POST /data/query    -> run hardcoded SELECT against a table
    3. GET  /data/tables   -> inspect schema when needed

All upstream dependencies (sandbox, vector search, embeddings) are mocked.
"""

from __future__ import annotations

from decimal import Decimal
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
            table_name="cache_budget_sample",
            dataset_id="ds-budget-001",
            row_count=100,
            columns=["fecha", "monto"],
        ),
    ]
    return mock


def _default_vector_search() -> AsyncMock:
    mock = AsyncMock(spec=IVectorSearch)
    mock.search_datasets.return_value = [
        SearchResult(
            dataset_id="ds-budget-001",
            title="Budget Sample",
            description="Sample national budget data",
            portal="sample_portal",
            download_url="https://example.com/budget.csv",
            columns='["fecha", "monto"]',
            score=0.91,
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


def _build_client(
    monkeypatch,
    sandbox: ISQLSandbox | None = None,
    vector_search: IVectorSearch | None = None,
    embedding: IEmbeddingProvider | None = None,
) -> AsyncClient:
    """Build an AsyncClient with a custom provider configuration."""
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
    transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
    return AsyncClient(transport=transport, base_url="http://test")


# ===================================================================
# 1. TestTerritoriaPolicyFlow — full flow simulation
# ===================================================================


class TestTerritoriaPolicyFlow:
    async def test_discover_and_query_flow(self, monkeypatch):
        """Territoria discovers tables then queries the first one."""
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            CachedTableInfo("cache_budget_sample", "ds-budget-001", 500, ["fecha", "monto"]),
            CachedTableInfo("cache_education_metrics", "ds-edu-001", 200, ["year", "indicator"]),
            CachedTableInfo("cache_health_indicators", "ds-health-001", 300, ["fecha", "value"]),
        ]
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["fecha", "monto"],
            rows=[
                {"fecha": "2026-01-01", "monto": 1000},
                {"fecha": "2026-02-01", "monto": 1200},
            ],
            row_count=2,
            truncated=False,
            error=None,
        )
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            SearchResult(
                "ds-budget-001", "Budget Sample", "National budget", "p", "", '["fecha"]', 0.92
            ),
            SearchResult(
                "ds-edu-001", "Education Metrics", "Edu indicators", "p", "", '["year"]', 0.85
            ),
            SearchResult(
                "ds-health-001", "Health Indicators", "Health data", "p", "", '["fecha"]', 0.78
            ),
        ]

        async with _build_client(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        ) as c:
            # Step 1: search
            search_resp = await c.post(
                "/data/search",
                json={"query": "budget education municipal", "limit": 5},
                headers=_auth_header(),
            )
            assert search_resp.status_code == 200
            results = search_resp.json()["results"]
            assert len(results) == 3
            first_table = results[0]["table_name"]
            assert first_table == "cache_budget_sample"

            # Step 2: query first table
            query_resp = await c.post(
                "/data/query",
                json={"sql": f"SELECT * FROM {first_table} LIMIT 10"},
                headers=_auth_header(),
            )
            assert query_resp.status_code == 200
            body = query_resp.json()
            assert body["columns"] == ["fecha", "monto"]
            assert body["row_count"] == 2
            assert body["truncated"] is False
            assert body["error"] is None
            assert len(body["rows"]) == 2

    async def test_multiple_searches_for_context(self, monkeypatch):
        """Territoria runs 3 separate searches to build policy context."""
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            CachedTableInfo("cache_budget_sample", "ds-budget-001", 500, ["fecha"]),
            CachedTableInfo("cache_education_metrics", "ds-edu-001", 200, ["year"]),
            CachedTableInfo("cache_transport_data", "ds-transport-001", 150, ["line"]),
        ]

        # Return different datasets per query
        calls = {"count": 0}

        async def fake_search(**kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                return [SearchResult("ds-budget-001", "Budget", "Budget data", "p", "", "[]", 0.9)]
            if calls["count"] == 2:
                return [SearchResult("ds-edu-001", "Edu", "Education data", "p", "", "[]", 0.88)]
            return [
                SearchResult("ds-transport-001", "Transport", "Transport data", "p", "", "[]", 0.82)
            ]

        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.side_effect = fake_search

        async with _build_client(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        ) as c:
            tables_seen = []
            for q in ["budget", "education", "municipal"]:
                resp = await c.post(
                    "/data/search",
                    json={"query": q, "limit": 3},
                    headers=_auth_header(),
                )
                assert resp.status_code == 200
                results = resp.json()["results"]
                assert len(results) >= 1
                tables_seen.append(results[0]["table_name"])

            # Each search returned a distinct table
            assert len(set(tables_seen)) == 3

    async def test_query_specific_columns(self, monkeypatch):
        """SELECT specific columns returns only those columns in response."""
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["fecha", "monto"],
            rows=[{"fecha": "2026-01-01", "monto": 500}],
            row_count=1,
            truncated=False,
            error=None,
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT fecha, monto FROM cache_budget_sample LIMIT 1"},
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["columns"] == ["fecha", "monto"]
            assert "id" not in body["columns"]
            assert set(body["rows"][0].keys()) == {"fecha", "monto"}


# ===================================================================
# 2. TestTerritoriaHardcodedQueries — SQL patterns Territoria uses
# ===================================================================


class TestTerritoriaHardcodedQueries:
    async def test_select_top_n_by_date(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["fecha", "monto"],
            rows=[
                {"fecha": "2026-12-01", "monto": 900},
                {"fecha": "2026-11-01", "monto": 850},
            ],
            row_count=2,
            truncated=False,
            error=None,
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM cache_budget_sample ORDER BY fecha DESC LIMIT 10"},
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["row_count"] == 2
            assert body["error"] is None
            assert body["rows"][0]["fecha"] == "2026-12-01"

    async def test_filter_by_province(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["provincia", "valor"],
            rows=[
                {"provincia": "Buenos Aires", "valor": 100},
                {"provincia": "Buenos Aires", "valor": 120},
            ],
            row_count=2,
            truncated=False,
            error=None,
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={
                    "sql": (
                        "SELECT * FROM cache_budget_sample WHERE provincia = 'Buenos Aires' LIMIT 5"
                    ),
                },
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["row_count"] == 2
            assert all(r["provincia"] == "Buenos Aires" for r in body["rows"])

    async def test_aggregation_query(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["categoria", "total"],
            rows=[
                {"categoria": "salud", "total": 5000},
                {"categoria": "educacion", "total": 8000},
                {"categoria": "infraestructura", "total": 3000},
            ],
            row_count=3,
            truncated=False,
            error=None,
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={
                    "sql": (
                        "SELECT categoria, SUM(monto) as total "
                        "FROM cache_budget_sample GROUP BY categoria"
                    ),
                },
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["columns"] == ["categoria", "total"]
            assert body["row_count"] == 3
            assert all("categoria" in r and "total" in r for r in body["rows"])

    async def test_join_between_cache_tables_allowed(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["x"],
            rows=[{"x": 1}, {"x": 2}],
            row_count=2,
            truncated=False,
            error=None,
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT a.x FROM cache_a a JOIN cache_b b ON a.id=b.id"},
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["error"] is None
            assert body["row_count"] == 2


# ===================================================================
# 3. TestTerritoriaErrorRecovery — how Territoria handles errors
# ===================================================================


class TestTerritoriaErrorRecovery:
    async def test_invalid_table_name_error(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=[],
            rows=[],
            row_count=0,
            truncated=False,
            error='relation "nonexistent_table" does not exist',
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROM nonexistent_table"},
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["error"] is not None
            assert "does not exist" in body["error"]
            assert body["row_count"] == 0
            assert body["rows"] == []

    async def test_syntax_error_handling(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=[],
            rows=[],
            row_count=0,
            truncated=False,
            error='syntax error at or near "FROMMM"',
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT * FROMMM cache_x"},
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["error"] is not None
            assert "syntax error" in body["error"].lower()

    async def test_timeout_during_aggregation(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=[],
            rows=[],
            row_count=0,
            truncated=False,
            error="Query timed out after 10 seconds",
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={
                    "sql": (
                        "SELECT categoria, SUM(monto) FROM cache_budget_sample GROUP BY categoria"
                    ),
                },
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["error"] is not None
            assert "timed out" in body["error"].lower()
            assert body["row_count"] == 0


# ===================================================================
# 4. TestTerritoriaDataShapes — data format expectations
# ===================================================================


class TestTerritoriaDataShapes:
    async def test_response_matches_contract(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["a"],
            rows=[{"a": 1}],
            row_count=1,
            truncated=False,
            error=None,
        )
        sandbox.list_cached_tables.return_value = [
            CachedTableInfo("cache_budget_sample", "ds-budget-001", 42, ["a", "b"]),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            SearchResult(
                "ds-budget-001",
                "Budget",
                "Budget description",
                "p",
                "",
                "[]",
                0.77,
            ),
        ]

        expected_query_fields = {"columns", "rows", "row_count", "truncated", "error"}
        expected_search_fields = {"table_name", "name", "title", "description", "relevance"}
        expected_table_fields = {"table_name", "name", "dataset_id", "row_count", "columns"}

        async with _build_client(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        ) as c:
            # Query contract
            q = await c.post(
                "/data/query",
                json={"sql": "SELECT a FROM cache_budget_sample"},
                headers=_auth_header(),
            )
            assert q.status_code == 200
            assert set(q.json().keys()) == expected_query_fields

            # Search contract
            s = await c.post(
                "/data/search",
                json={"query": "budget", "limit": 5},
                headers=_auth_header(),
            )
            assert s.status_code == 200
            search_items = s.json()["results"]
            assert len(search_items) >= 1
            assert set(search_items[0].keys()) == expected_search_fields

            # Tables contract
            t = await c.get("/data/tables", headers=_auth_header())
            assert t.status_code == 200
            table_items = t.json()["tables"]
            assert len(table_items) >= 1
            assert set(table_items[0].keys()) == expected_table_fields

    async def test_rows_are_dicts_not_arrays(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["col1", "col2"],
            rows=[
                {"col1": "val1", "col2": 123},
                {"col1": "val2", "col2": 456},
            ],
            row_count=2,
            truncated=False,
            error=None,
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT col1, col2 FROM cache_transport_data"},
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            for row in body["rows"]:
                assert isinstance(row, dict)
                assert "col1" in row
                assert "col2" in row

    async def test_null_values_preserved(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["id", "optional_field"],
            rows=[
                {"id": 1, "optional_field": None},
                {"id": 2, "optional_field": "present"},
            ],
            row_count=2,
            truncated=False,
            error=None,
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT id, optional_field FROM cache_health_indicators"},
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["rows"][0]["optional_field"] is None
            assert body["rows"][1]["optional_field"] == "present"

    async def test_numeric_types_preserved(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["int_val", "float_val", "decimal_val"],
            rows=[
                {
                    "int_val": 42,
                    "float_val": 3.14,
                    "decimal_val": float(Decimal("99.95")),
                },
            ],
            row_count=1,
            truncated=False,
            error=None,
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT int_val, float_val, decimal_val FROM cache_education_metrics"},
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            row = body["rows"][0]
            assert row["int_val"] == 42
            assert isinstance(row["int_val"], int)
            assert row["float_val"] == pytest.approx(3.14)
            assert isinstance(row["float_val"], float)
            assert row["decimal_val"] == pytest.approx(99.95)


# ===================================================================
# 5. TestTerritoriaBatchScenarios — realistic batch patterns
# ===================================================================


class TestTerritoriaBatchScenarios:
    async def test_sequential_searches_and_queries(self, monkeypatch):
        """Simulate 5 sequential requests: 3 searches + 2 queries all succeed."""
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            CachedTableInfo("cache_budget_sample", "ds-budget-001", 500, ["fecha"]),
            CachedTableInfo("cache_health_indicators", "ds-health-001", 300, ["fecha"]),
        ]
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["fecha"],
            rows=[{"fecha": "2026-01-01"}],
            row_count=1,
            truncated=False,
            error=None,
        )
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            SearchResult("ds-budget-001", "Budget", "B", "p", "", "[]", 0.9),
            SearchResult("ds-health-001", "Health", "H", "p", "", "[]", 0.85),
        ]

        async with _build_client(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        ) as c:
            # 3 searches
            for q in ["budget for education", "waste management", "health indicators"]:
                resp = await c.post(
                    "/data/search",
                    json={"query": q, "limit": 5},
                    headers=_auth_header(),
                )
                assert resp.status_code == 200
                assert len(resp.json()["results"]) >= 1

            # 2 queries
            for table in ["cache_budget_sample", "cache_health_indicators"]:
                resp = await c.post(
                    "/data/query",
                    json={"sql": f"SELECT fecha FROM {table} LIMIT 1"},
                    headers=_auth_header(),
                )
                assert resp.status_code == 200
                body = resp.json()
                assert body["error"] is None
                assert body["row_count"] == 1

    async def test_large_result_truncation(self, monkeypatch):
        """Sandbox returns a truncated result; Territoria must handle it."""
        big_rows = [{"id": i, "value": i * 10} for i in range(1000)]
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["id", "value"],
            rows=big_rows,
            row_count=1000,
            truncated=True,
            error=None,
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={"sql": "SELECT id, value FROM cache_budget_sample"},
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["truncated"] is True
            assert body["row_count"] == 1000
            assert body["error"] is None
            assert len(body["rows"]) == 1000

    async def test_empty_result_sets(self, monkeypatch):
        """Empty result sets are returned as 200 with empty rows array."""
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.execute_readonly.return_value = SandboxResult(
            columns=["id", "value"],
            rows=[],
            row_count=0,
            truncated=False,
            error=None,
        )

        async with _build_client(monkeypatch, sandbox=sandbox) as c:
            resp = await c.post(
                "/data/query",
                json={
                    "sql": "SELECT id, value FROM cache_budget_sample WHERE id = -999",
                },
                headers=_auth_header(),
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["rows"] == []
            assert body["row_count"] == 0
            assert body["error"] is None
            assert body["truncated"] is False
