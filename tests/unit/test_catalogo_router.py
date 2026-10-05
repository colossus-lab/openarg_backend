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
from app.domain.ports.credits.credit_repository import ICreditRepository
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
        self.params: list[Any] = []
        self.error: str | None = None
        self.error_kind: str | None = None
        # Si está, el error es sólo para las consultas que contienen este texto.
        self.error_only_for: str | None = None
        # La consulta principal de /datos no devuelve filas.
        self.empty = False

    tables = [
        CachedTableInfo(table_name=_T, dataset_id="ds-1", row_count=8569, columns=[]),
        # Versión vieja del mismo dataset: 0 filas, no se tiene que ofrecer.
        CachedTableInfo(
            table_name="raw.tasas_vieja__v1", dataset_id="ds-1b", row_count=0, columns=[]
        ),
    ]
    find_calls: list[dict] = []

    async def list_cached_tables(self) -> list[CachedTableInfo]:
        raise AssertionError("el modo datos no tiene que listar las ~32.000 tablas")

    async def find_tables(self, *, dataset_ids=None, table_names=None) -> list[CachedTableInfo]:
        self.find_calls.append({"dataset_ids": dataset_ids, "table_names": table_names})
        ids = set(dataset_ids or [])
        names = {n.split(".")[-1] for n in table_names or []}
        return [
            t for t in self.tables if t.dataset_id in ids or t.table_name.split(".")[-1] in names
        ]

    async def get_column_types(self, names: list[str]) -> dict[str, list[tuple[str, str]]]:
        cols = [
            ("indice_tiempo", "text"),
            ("tasas_interes_call", "double precision"),
            ("tasas_interes_badlar", "double precision"),
            ("_source_url", "text"),
        ]
        return {n: cols for n in names}

    async def execute_readonly(
        self, sql: str, timeout_seconds: int = 10, *, params: Any = None
    ) -> SandboxResult:
        self.sql.append(sql)
        self.params.append(params)
        if self.error and not (self.error_only_for and self.error_only_for not in sql):
            return SandboxResult(
                columns=[],
                rows=[],
                row_count=0,
                truncated=False,
                error=self.error,
                error_kind=self.error_kind,
            )
        if "AS reconocidas" in sql:
            rows = [
                {"desde": "2003-01-02", "hasta": "2026-06-18", "reconocidas": 9, "con_valor": 9}
            ]
        elif self.empty and "LIMIT 100" in sql:
            return SandboxResult(columns=[], rows=[], row_count=0, truncated=False)
        elif "GROUP BY 1" in sql:
            rows = [{"valor": "Principales tasas", "filas": 12}, {"valor": "Otra cosa", "filas": 3}]
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
def search() -> AsyncMock:
    search = AsyncMock(spec=IVectorSearch)
    search.search_datasets_ann.return_value = [
        SearchResult(
            dataset_id="ds-1",
            title="Principales tasas de interés",
            description="Tasas diarias del BCRA",
            portal="datos_gob_ar",
            download_url=_CSV,
            columns="",
            score=0.9,
        ),
        # El mismo dataset duplicado en el catálogo (otro ID, mismo título y URL).
        SearchResult(
            dataset_id="ds-1b",
            title="Principales tasas de interés",
            description="Tasas diarias del BCRA",
            portal="datos_gob_ar",
            download_url=_CSV,
            columns="",
            score=0.89,
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
    return search


@pytest.fixture
async def client(
    key: tuple[str, ApiKey], sandbox: FakeSandbox, cache: FakeCache, search: AsyncMock
):
    repo = AsyncMock(spec=IApiKeyRepository)
    repo.get_by_key_hash.return_value = key[1]
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

        @provide
        def cr(self) -> ICreditRepository:
            credits = AsyncMock(spec=ICreditRepository)
            credits.get_active_supporter.return_value = None
            credits.balance.return_value = {"preguntas": 0, "datos": 0}
            credits.debit.return_value = False
            return credits

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
    # El duplicado se agrupa: dos resultados, no tres.
    first, second = r.json()["resultados"]
    assert first["titulo"] == "Principales tasas de interés"
    assert first["url"] == _CSV
    # La tabla de 0 filas (versión vieja) no se ofrece.
    assert first["tablas"] == [{"tabla": _T, "filas": 8569}]
    assert second["tablas"] == []  # está en el catálogo pero no tiene tabla consultable


async def test_buscar_goes_through_the_hnsw_index(client: AsyncClient, search: AsyncMock) -> None:
    """La búsqueda exacta recorre los 76k chunks (de 1 a más de 60 s en staging)."""
    r = await client.get(
        "/catalogo/buscar", params={"q": "tasas de interés", "portal": "caba", "limite": 5}
    )
    assert r.status_code == 200, r.text
    search.search_datasets.assert_not_awaited()
    kwargs = search.search_datasets_ann.await_args.kwargs
    assert kwargs["portal_filter"] == "caba"
    assert kwargs["limit"] == 10  # el doble: después se agrupan los duplicados
    assert kwargs["min_similarity"] == 0.40


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
    assert ">= :p0" in sandbox.sql[-1] and "2026-06" not in sandbox.sql[-1]
    assert sandbox.params[-1] == {"p0": "2026-06-01"}
    assert body["aviso"] is None


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


async def test_a_timeout_says_the_table_is_too_big(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    """Antes: 'Probá con otros filtros o menos columnas' también para un timeout."""
    sandbox.error, sandbox.error_kind = "Query timed out after 10 seconds.", "timeout"
    r = await client.post("/catalogo/datos", json={"tabla": _T})
    assert r.status_code == 400
    assert "muy grande" in r.json()["detail"]


async def test_tabla_survives_a_failing_period_query(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    """ok.1: el 400 genérico de describir_tabla venía de las consultas auxiliares."""
    sandbox.error, sandbox.error_kind = "Query timed out after 10 seconds.", "timeout"
    sandbox.error_only_for = "AS reconocidas"
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["columna_fecha"] == "indice_tiempo"
    assert body["desde"] is None and body["aviso_fecha"]
    assert len(body["muestra"]) == 2


async def test_datos_with_no_rows_explains_and_suggests(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    """3.2: cero filas venía sin ninguna pista."""
    sandbox.empty = True
    sandbox.get_column_types = AsyncMock(  # type: ignore[method-assign]
        return_value={_T: [("indice_tiempo", "text"), ("serie", "text")]}
    )
    r = await client.post(
        "/catalogo/datos",
        json={"tabla": _T, "filtros": [{"columna": "serie", "operador": "=", "valor": "tasas"}]},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["cantidad"] == 0
    assert "Ninguna fila" in body["aviso"]
    assert body["sugerencias"]["serie"][0] == {"valor": "Principales tasas", "filas": 12}
    probe = next(s for s in sandbox.sql if "GROUP BY 1" in s)
    assert '"serie" IS NOT NULL' in probe


async def test_datos_accepts_operator_filters(client: AsyncClient, sandbox: FakeSandbox) -> None:
    r = await client.post(
        "/catalogo/datos",
        json={
            "tabla": _T,
            "filtros": [
                {"columna": "tasas_interes_call", "operador": "mayor_que", "valor": 30.5},
                {"columna": "indice_tiempo", "operador": "en", "valores": ["2026-06-18"]},
            ],
        },
    )
    assert r.status_code == 200, r.text
    assert '"tasas_interes_call" > :p0' in sandbox.sql[-1]
    assert str(sandbox.params[-1]["p0"]) == "30.5"


async def test_datos_accepts_numbers_in_filters(client: AsyncClient, sandbox: FakeSandbox) -> None:
    """Revisión del PR #133: `valores: [2020, 2021]` daba un 422 genérico."""
    r = await client.post(
        "/catalogo/datos",
        json={
            "tabla": _T,
            "filtros": [{"columna": "indice_tiempo", "operador": "en", "valores": [2020, 2021]}],
        },
    )
    assert r.status_code == 200, r.text
    assert sandbox.params[-1] == {"p0": ["2020", "2021"]}
    r = await client.post("/catalogo/datos", json={"tabla": _T, "filtros": {"indice_tiempo": 2020}})
    assert r.status_code == 200, r.text


async def test_tabla_blocked_says_why_instead_of_a_period(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    sandbox.error = "La tabla tiene un problema de calidad sin resolver"
    sandbox.error_kind, sandbox.error_only_for = "blocked", "AS reconocidas"
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["desde"] is None and "problema de calidad" in body["aviso_fecha"]


async def test_catalog_quota_is_enforced(
    client: AsyncClient, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PUBLIC_API_MONTHLY_DATOS", "2")
    assert (await client.get("/catalogo/tabla", params={"nombre": _T})).status_code == 200
    assert (await client.get("/catalogo/tabla", params={"nombre": _T})).status_code == 200
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    assert r.status_code == 402
    assert "per month" in r.json()["detail"]
    assert not any(
        ":month:" in k and "catalog" not in k for k in cache.counters
    )  # no toca las preguntas


async def test_lookups_are_targeted_not_a_full_listing(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    sandbox.find_calls.clear()
    await client.get("/catalogo/buscar", params={"q": "tasas"})
    await client.get("/catalogo/tabla", params={"nombre": _T})
    await client.post("/catalogo/datos", json={"tabla": _T})
    assert sandbox.find_calls[0]["dataset_ids"] == ["ds-1", "ds-1b", "ds-sin-tabla"]
    assert sandbox.find_calls[1]["table_names"] == [_T]
    assert sandbox.find_calls[2]["table_names"] == [_T]
