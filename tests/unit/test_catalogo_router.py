"""HTTP del modo datos de la API pública (`/catalogo/buscar|tabla|datos`)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from dishka import Provider, Scope, make_async_container, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.application.api_key_service import generate_api_key
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.llm.llm_provider import IEmbeddingProvider
from app.domain.ports.sandbox.sql_sandbox import (
    CachedTableInfo,
    ISQLSandbox,
    SandboxResult,
    TableSource,
)
from app.domain.ports.search.vector_search import IVectorSearch, SearchResult
from app.presentation.http.controllers.public_api.catalogo_router import router

_T = "raw.datos_gob_ar__principales_tasas_de_interes__6335b6d1__v1"
_BARE = _T.split(".")[1]
_CSV = "https://infra.datos.gob.ar/x/principales-tasas-interes-diarias.csv"


class FakeCache:
    def __init__(self) -> None:
        self.counters: dict[str, int] = {}

    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]


class FakeSandbox:
    def __init__(self) -> None:
        self.sql: list[str] = []
        self.error: str | None = None

    async def list_cached_tables(self) -> list[CachedTableInfo]:
        return [CachedTableInfo(table_name=_T, dataset_id="ds-1", row_count=8569, columns=[])]

    async def get_column_types(self, names: list[str]) -> dict[str, list[tuple[str, str]]]:
        cols = [
            ("indice_tiempo", "text"),
            ("tasas_interes_call", "double precision"),
            ("tasas_interes_badlar", "double precision"),
            ("_source_url", "text"),
        ]
        return {n: cols for n in names}

    async def execute_readonly(self, sql: str, timeout_seconds: int = 10) -> SandboxResult:
        self.sql.append(sql)
        if self.error:
            return SandboxResult(
                columns=[], rows=[], row_count=0, truncated=False, error=self.error
            )
        if "min(left(" in sql:
            rows = [{"desde": "2003-01-02", "hasta": "2026-06-18"}]
        else:
            rows = [
                {
                    "indice_tiempo": "2026-06-18",
                    "tasas_interes_call": 33.14,
                    "tasas_interes_badlar": 21,
                },
                {
                    "indice_tiempo": "2026-06-17",
                    "tasas_interes_call": 33.0,
                    "tasas_interes_badlar": 21,
                },
            ]
        return SandboxResult(columns=list(rows[0]), rows=rows, row_count=len(rows), truncated=False)

    async def get_table_sources(self, names: list[str]) -> dict[str, TableSource]:
        return {
            _BARE: TableSource(
                title="Principales tasas de interés", portal="datos_gob_ar", url=_CSV
            )
        }


@pytest.fixture
def key() -> tuple[str, ApiKey]:
    raw, key_hash = generate_api_key()
    return raw, ApiKey(
        user_id=__import__("uuid").uuid4(), key_hash=key_hash, plan="free", is_active=True
    )


@pytest.fixture
def sandbox() -> FakeSandbox:
    return FakeSandbox()


@pytest.fixture
def cache() -> FakeCache:
    return FakeCache()


@pytest.fixture
async def client(key: tuple[str, ApiKey], sandbox: FakeSandbox, cache: FakeCache):
    repo = AsyncMock(spec=IApiKeyRepository)
    repo.get_by_key_hash.return_value = key[1]
    search = AsyncMock(spec=IVectorSearch)
    search.search_datasets_hybrid.return_value = [
        SearchResult(
            dataset_id="ds-1",
            title="Principales tasas de interés",
            description="Tasas diarias del BCRA",
            portal="datos_gob_ar",
            download_url=_CSV,
            columns="",
            score=0.9,
        ),
        SearchResult(
            dataset_id="ds-sin-tabla",
            title="Otro",
            description="",
            portal="caba",
            download_url="",
            columns="",
            score=0.5,
        ),
    ]
    embedding = AsyncMock(spec=IEmbeddingProvider)
    embedding.embed.return_value = [0.1] * 4

    class P(Provider):
        scope = Scope.REQUEST

        @provide
        def s(self) -> ISQLSandbox:
            return sandbox  # type: ignore[return-value]

        @provide
        def c(self) -> ICacheService:
            return cache  # type: ignore[return-value]

        @provide
        def r(self) -> IApiKeyRepository:
            return repo

        @provide
        def v(self) -> IVectorSearch:
            return search

        @provide
        def e(self) -> IEmbeddingProvider:
            return embedding

    app = FastAPI()
    app.include_router(router)
    setup_dishka(container=make_async_container(P()), app=app)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        c.headers["Authorization"] = f"Bearer {key[0]}"
        yield c


async def test_without_key_is_401(client: AsyncClient) -> None:
    del client.headers["Authorization"]
    assert (await client.get("/catalogo/buscar", params={"q": "tasas"})).status_code == 401
    assert (await client.post("/catalogo/datos", json={"tabla": _T})).status_code == 401


async def test_buscar_lists_datasets_with_their_queryable_tables(client: AsyncClient) -> None:
    r = await client.get("/catalogo/buscar", params={"q": "tasas de interés"})
    assert r.status_code == 200, r.text
    first, second = r.json()["resultados"]
    assert first["titulo"] == "Principales tasas de interés"
    assert first["url"] == _CSV
    assert first["tablas"] == [{"tabla": _T, "filas": 8569}]
    assert second["tablas"] == []  # está en el catálogo pero no tiene tabla consultable


async def test_tabla_describes_columns_period_and_sample(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    r = await client.get("/catalogo/tabla", params={"nombre": _BARE})
    assert r.status_code == 200, r.text
    body: dict[str, Any] = r.json()
    assert body["tabla"] == _T
    assert [c["nombre"] for c in body["columnas"]] == [
        "indice_tiempo",
        "tasas_interes_call",
        "tasas_interes_badlar",
    ]
    assert body["columna_fecha"] == "indice_tiempo"
    assert (body["desde"], body["hasta"]) == ("2003-01-02", "2026-06-18")
    assert body["titulo"] == "Principales tasas de interés" and body["url"] == _CSV
    assert len(body["muestra"]) == 2
    assert all('"_source_url"' not in s for s in sandbox.sql)


async def test_unknown_table_is_404(client: AsyncClient) -> None:
    r = await client.get("/catalogo/tabla", params={"nombre": "api_keys"})
    assert r.status_code == 404
    assert "buscar_datasets" in r.json()["detail"]


async def test_datos_returns_rows_and_source(client: AsyncClient, sandbox: FakeSandbox) -> None:
    r = await client.post(
        "/catalogo/datos",
        json={
            "tabla": _T,
            "columnas": ["indice_tiempo", "tasas_interes_call"],
            "desde": "2026-06",
            "orden": "desc",
            "limite": 2,
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["cantidad"] == 2 and body["truncado"] is True
    assert body["fuente"] == "Principales tasas de interés"
    assert "left(\"indice_tiempo\"::text, 7) >= '2026-06'" in sandbox.sql[-1]


async def test_datos_rejects_invented_columns(client: AsyncClient, sandbox: FakeSandbox) -> None:
    r = await client.post("/catalogo/datos", json={"tabla": _T, "columnas": ["password"]})
    assert r.status_code == 400
    assert "no existen" in r.json()["detail"]
    assert sandbox.sql == []  # no llegó a la base


async def test_datos_rejects_unknown_fields(client: AsyncClient) -> None:
    r = await client.post("/catalogo/datos", json={"tabla": _T, "sql": "SELECT 1"})
    assert r.status_code == 422


async def test_sandbox_rejection_is_a_400_not_a_500(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    sandbox.error = "Forbidden SQL operation: DO"
    r = await client.post("/catalogo/datos", json={"tabla": _T})
    assert r.status_code == 400


async def test_catalog_quota_is_enforced(
    client: AsyncClient, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PUBLIC_API_CATALOG_DAILY_LIMIT", "2")
    assert (await client.get("/catalogo/tabla", params={"nombre": _T})).status_code == 200
    assert (await client.get("/catalogo/tabla", params={"nombre": _T})).status_code == 200
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    assert r.status_code == 429
    assert not any(
        ":day:" in k and "catalog" not in k for k in cache.counters
    )  # no toca las 10 preguntas
