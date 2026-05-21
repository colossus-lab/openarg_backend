"""Unit tests for POST /data/search — semantic search deduplication + edge cases.

Covers the dedup fix applied to data_router.data_search: vector_search may
return multiple chunks (main/columns/contextual) for the same dataset, but
only ONE entry per cached table should appear in the response. The fetch
limit was increased to body.limit * 4 to compensate.

Public repo — no real API keys or production data. Generic dataset IDs only.
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
            table_name="cache_dataset_a",
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
            title="Dataset A",
            description="Generic description",
            portal="test_portal",
            download_url="https://example.com/a.csv",
            columns='["fecha", "valor"]',
            score=0.92,
        ),
    ]
    return mock


def _default_embedding() -> AsyncMock:
    mock = AsyncMock(spec=IEmbeddingProvider)
    mock.embed.return_value = [0.1] * 1536
    return mock


def _make_search_result(
    dataset_id: str,
    score: float,
    title: str = "Dataset",
    description: str = "Description",
) -> SearchResult:
    return SearchResult(
        dataset_id=dataset_id,
        title=title,
        description=description,
        portal="test_portal",
        download_url=f"https://example.com/{dataset_id}.csv",
        columns='["col_a", "col_b"]',
        score=score,
    )


def _make_cached(dataset_id: str, table_name: str) -> CachedTableInfo:
    return CachedTableInfo(
        table_name=table_name,
        dataset_id=dataset_id,
        row_count=100,
        columns=["col_a", "col_b"],
    )


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


def _build_app(
    monkeypatch,
    *,
    sandbox: ISQLSandbox | None = None,
    vector_search: IVectorSearch | None = None,
    embedding: IEmbeddingProvider | None = None,
) -> FastAPI:
    """Build a FastAPI app with the given mocked dependencies."""
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


async def _post_search(
    fast_app: FastAPI,
    payload: dict,
) -> tuple[int, object]:
    transport = ASGITransport(app=fast_app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        resp = await c.post(
            "/data/search",
            json=payload,
            headers=_auth_header(),
        )
    body: object
    try:
        raw = resp.json()
        body = raw["results"] if isinstance(raw, dict) and "results" in raw else raw
    except Exception:
        body = None
    return resp.status_code, body


# ===================================================================
# 1. Deduplication (the fix)
# ===================================================================


class TestSearchDeduplication:
    async def test_same_dataset_different_chunks_deduplicated(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            _make_cached("ds-001", "cache_dataset_a"),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        # Simulate 3 chunks (main, columns, contextual) for the same dataset
        vector_search.search_datasets.return_value = [
            _make_search_result("ds-001", 0.95, title="Dataset A (main)"),
            _make_search_result("ds-001", 0.90, title="Dataset A (columns)"),
            _make_search_result("ds-001", 0.85, title="Dataset A (contextual)"),
        ]

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "dataset a"})

        assert status == 200
        assert isinstance(body, list)
        assert len(body) == 1
        assert body[0]["table_name"] == "cache_dataset_a"

    async def test_different_datasets_not_deduplicated(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            _make_cached("ds-001", "cache_dataset_a"),
            _make_cached("ds-002", "cache_dataset_b"),
            _make_cached("ds-003", "cache_dataset_c"),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            _make_search_result("ds-001", 0.95),
            _make_search_result("ds-002", 0.90),
            _make_search_result("ds-003", 0.85),
        ]

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "datasets"})

        assert status == 200
        assert len(body) == 3
        tables = {row["table_name"] for row in body}
        assert tables == {"cache_dataset_a", "cache_dataset_b", "cache_dataset_c"}

    async def test_dedup_preserves_highest_score(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            _make_cached("ds-001", "cache_dataset_a"),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        # Results come sorted descending from search_datasets
        vector_search.search_datasets.return_value = [
            _make_search_result("ds-001", 0.95),
            _make_search_result("ds-001", 0.90),
            _make_search_result("ds-001", 0.85),
        ]

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "q"})

        assert status == 200
        assert len(body) == 1
        # Highest score (first encountered) is preserved
        assert body[0]["relevance"] == 0.95

    async def test_dedup_with_mix_cached_uncached(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        # ds-001 and ds-003 have cached tables; ds-002 does NOT
        sandbox.list_cached_tables.return_value = [
            _make_cached("ds-001", "cache_dataset_a"),
            _make_cached("ds-003", "cache_dataset_c"),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        # 5 results: ds-001 (dup x3 interleaved), ds-002 (skip, not cached), ds-003
        vector_search.search_datasets.return_value = [
            _make_search_result("ds-001", 0.95),
            _make_search_result("ds-001", 0.92),
            _make_search_result("ds-002", 0.88),  # not cached → skipped
            _make_search_result("ds-003", 0.80),
            _make_search_result("ds-001", 0.75),
        ]

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "mixed"})

        assert status == 200
        assert len(body) == 2
        tables = [row["table_name"] for row in body]
        assert tables == ["cache_dataset_a", "cache_dataset_c"]
        # Top-scoring ds-001 chunk kept
        assert body[0]["relevance"] == 0.95

    async def test_dedup_respects_limit(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            _make_cached("ds-001", "cache_dataset_a"),
            _make_cached("ds-002", "cache_dataset_b"),
            _make_cached("ds-003", "cache_dataset_c"),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        # 20 results with 3 unique dataset_ids — many duplicates per dataset
        results: list[SearchResult] = []
        score = 0.99
        for i in range(20):
            dataset_id = f"ds-00{(i % 3) + 1}"
            results.append(_make_search_result(dataset_id, round(score, 3)))
            score -= 0.01
        vector_search.search_datasets.return_value = results

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(
            fast_app,
            {"query": "q", "limit": 2},
        )

        assert status == 200
        assert len(body) == 2
        tables = [row["table_name"] for row in body]
        # All returned tables are unique
        assert len(set(tables)) == 2

    async def test_raw_table_catalog_hits_appear_without_dataset_id(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = []

        class _FakeConn:
            def execute(self, *_args, **_kwargs):
                return type(
                    "_Rows",
                    (),
                    {
                        "fetchall": lambda self: [
                            type(
                                "_Row",
                                (),
                                {
                                    "table_name": "raw.energia__produccion__abcd1234__v1",
                                    "display_name": "Producción de energía",
                                    "description": "Serie raw de producción",
                                    "score": 0.88,
                                },
                            )()
                        ]
                    },
                )()

            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return False

        class _FakeEngine:
            def connect(self):
                return _FakeConn()

        sandbox._get_engine = lambda: _FakeEngine()

        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = []

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(
            fast_app,
            {"query": "producción de energía", "limit": 5},
        )

        assert status == 200
        assert len(body) == 1
        assert body[0]["table_name"] == "raw.energia__produccion__abcd1234__v1"


# ===================================================================
# 2. Relevance threshold
# ===================================================================


class TestSearchRelevanceThreshold:
    async def test_results_below_threshold_not_shown(self, monkeypatch):
        """Assumes search_datasets applied min_similarity=0.40 filter.

        When it returns mixed scores at/above the threshold, all are kept.
        """
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            _make_cached("ds-001", "cache_dataset_a"),
            _make_cached("ds-002", "cache_dataset_b"),
            _make_cached("ds-003", "cache_dataset_c"),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            _make_search_result("ds-001", 0.90),
            _make_search_result("ds-002", 0.45),
            _make_search_result("ds-003", 0.41),
        ]

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "q"})

        assert status == 200
        assert len(body) == 3
        scores = [row["relevance"] for row in body]
        assert 0.90 in scores
        assert 0.45 in scores
        assert 0.41 in scores

    async def test_rounding_relevance(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            _make_cached("ds-001", "cache_dataset_a"),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            _make_search_result("ds-001", 0.923456),
        ]

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "q"})

        assert status == 200
        assert len(body) == 1
        assert body[0]["relevance"] == 0.923

    async def test_very_low_score_still_returned(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            _make_cached("ds-001", "cache_dataset_a"),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        # search_datasets did the min_similarity filtering already
        vector_search.search_datasets.return_value = [
            _make_search_result("ds-001", 0.401),
        ]

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "q"})

        assert status == 200
        assert len(body) == 1
        assert body[0]["relevance"] == 0.401


# ===================================================================
# 3. Limit parameter
# ===================================================================


class TestSearchLimit:
    async def test_limit_default_is_10(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            _make_cached(f"ds-{i:03d}", f"cache_dataset_{i:03d}") for i in range(1, 16)
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            _make_search_result(f"ds-{i:03d}", round(0.99 - i * 0.01, 3)) for i in range(1, 16)
        ]

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "q"})

        assert status == 200
        assert len(body) == 10

    async def test_limit_1_returns_single_result(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            _make_cached("ds-001", "cache_dataset_a"),
            _make_cached("ds-002", "cache_dataset_b"),
            _make_cached("ds-003", "cache_dataset_c"),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            _make_search_result("ds-001", 0.95),
            _make_search_result("ds-002", 0.90),
            _make_search_result("ds-003", 0.85),
        ]

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "q", "limit": 1})

        assert status == 200
        assert len(body) == 1

    async def test_limit_50_is_max_allowed(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "datos", "limit": 50},
            headers=_auth_header(),
        )
        assert resp.status_code == 200

    async def test_limit_51_rejected(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "datos", "limit": 51},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_limit_0_rejected(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "datos", "limit": 0},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_limit_negative_rejected(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "datos", "limit": -5},
            headers=_auth_header(),
        )
        assert resp.status_code == 422


# ===================================================================
# 4. Query validation
# ===================================================================


class TestSearchQueryValidation:
    async def test_query_length_exact_2000(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "a" * 2000},
            headers=_auth_header(),
        )
        assert resp.status_code == 200

    async def test_query_length_2001_rejected(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "a" * 2001},
            headers=_auth_header(),
        )
        assert resp.status_code == 422

    async def test_query_with_special_chars(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "datos ñoños & <tags> \U0001f1e6\U0001f1f7"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200

    async def test_query_with_sql_injection_chars(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "' OR 1=1 --"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200

    async def test_query_only_whitespace(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "    "},
            headers=_auth_header(),
        )
        assert resp.status_code == 200

    async def test_query_tab_newline_chars(self, client):
        resp = await client.post(
            "/data/search",
            json={"query": "data\n\tnews"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200


# ===================================================================
# 5. Empty-results edge cases
# ===================================================================


class TestSearchEmptyResults:
    async def test_no_search_results(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        sandbox.list_cached_tables.return_value = [
            _make_cached("ds-001", "cache_dataset_a"),
        ]
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = []

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "nothing matches"})

        assert status == 200
        assert body == []

    async def test_all_results_uncached(self, monkeypatch):
        sandbox = AsyncMock(spec=ISQLSandbox)
        # no cached tables at all
        sandbox.list_cached_tables.return_value = []
        vector_search = AsyncMock(spec=IVectorSearch)
        vector_search.search_datasets.return_value = [
            _make_search_result("ds-001", 0.95),
            _make_search_result("ds-002", 0.90),
            _make_search_result("ds-003", 0.85),
            _make_search_result("ds-004", 0.80),
            _make_search_result("ds-005", 0.75),
        ]

        fast_app = _build_app(
            monkeypatch,
            sandbox=sandbox,
            vector_search=vector_search,
        )
        status, body = await _post_search(fast_app, {"query": "uncached"})

        assert status == 200
        assert body == []

    async def test_embedding_failure_returns_502(self, monkeypatch):
        embedding = AsyncMock(spec=IEmbeddingProvider)
        embedding.embed.side_effect = Exception("bedrock unavailable")

        fast_app = _build_app(monkeypatch, embedding=embedding)
        status, body = await _post_search(fast_app, {"query": "anything"})

        assert status == 502
        assert isinstance(body, dict)
        assert body.get("detail") == "Servicio de búsqueda no disponible"
