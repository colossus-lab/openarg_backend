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
    TableProfile,
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

    profiles: dict[str, TableProfile] = {}

    async def table_profiles(self, names: list[str]) -> dict[str, TableProfile]:
        return {n.split(".")[-1]: p for n in names if (p := self.profiles.get(n.split(".")[-1]))}

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
    # Cuatro por resultado: después se juntan las copias del mismo archivo.
    assert kwargs["limit"] == 20
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


async def test_buscar_shows_one_copy_of_a_file_even_when_both_have_rows(
    client: AsyncClient, sandbox: FakeSandbox, search: AsyncMock
) -> None:
    """Prod, 04-oct: los gemelos de la migración de datos.gob.ar tienen las dos
    tablas con filas (8.467 la vieja, 8.582 la nueva). El filtro `filas != 0`
    sólo las separaba en staging, donde la vieja quedó con row_count=0 por un
    artefacto; en prod el resultado salía con las dos y el modelo adivinaba."""
    url = "https://infra.datos.gob.ar/catalog/sspm/dataset/92/distribution/92.2/download/r.csv"
    search.search_datasets_ann.return_value = [
        SearchResult("vieja", "Reservas internacionales", "", "datos_gob_ar", url, "", 0.77),
        SearchResult("nueva", "Reservas internacionales", "", "datos_gob_ar", url, "", 0.76),
    ]
    sandbox.tables = [
        CachedTableInfo("raw.reservas__bebd015a__v1", "vieja", 8467, []),
        CachedTableInfo("reservas_rf3719b94d1", "nueva", 8582, []),
    ]

    r = await client.get("/catalogo/buscar", params={"q": "reservas internacionales"})

    assert r.status_code == 200, r.text
    [only] = r.json()["resultados"]
    assert only["tablas"] == [{"tabla": "reservas_rf3719b94d1", "filas": 8582}]
    assert only["archivo"] == "r.csv" and only["formato"] == "CSV"


async def test_buscar_uses_the_live_version_rows_and_sends_tableless_to_the_bottom(
    client: AsyncClient, sandbox: FakeSandbox, search: AsyncMock
) -> None:
    """Staging tiene `row_count=0` en 18.134 tablas con filas (Votaciones
    Nominales: 231.043), y el filtro las escondía como "sin tabla"."""
    search.search_datasets_ann.return_value = [
        SearchResult("sin", "Elecciones 2025 CABA", "", "caba", "https://c/e.csv", "", 0.70),
        SearchResult("vn", "Votaciones Nominales", "", "diputados", "https://d/vn.csv", "", 0.67),
    ]
    sandbox.tables = [CachedTableInfo("raw.vn__v3", "vn", 0, [])]
    sandbox.profiles = {"vn__v3": TableProfile("vn__v3", rows=231_043)}

    r = await client.get("/catalogo/buscar", params={"q": "votaciones"})

    first, second = r.json()["resultados"]
    assert first["titulo"] == "Votaciones Nominales"
    assert first["tablas"] == [{"tabla": "raw.vn__v3", "filas": 231_043}]
    assert second["tablas"] == []


async def test_an_unknown_portal_is_a_400_with_the_valid_ones(
    client: AsyncClient, search: AsyncMock
) -> None:
    """Un portal que no existe vaciaba el filtro y la respuesta era una lista
    vacía con 200: el MCP decía "No encontré datasets" (4.4.5)."""
    search.search_datasets_ann.return_value = []
    search.known_portals.return_value = ["caba", "datos_gob_ar", "indec"]

    r = await client.get("/catalogo/buscar", params={"q": "soja", "portal": "INDEC"})

    assert r.status_code == 400
    assert "'INDEC'" in r.json()["detail"] and "indec" in r.json()["detail"]


async def test_a_real_portal_with_nothing_similar_is_still_an_empty_list(
    client: AsyncClient, search: AsyncMock
) -> None:
    search.search_datasets_ann.return_value = []
    search.known_portals.return_value = ["caba", "indec"]

    r = await client.get("/catalogo/buscar", params={"q": "soja", "portal": "caba"})

    assert r.status_code == 200 and r.json()["resultados"] == []
