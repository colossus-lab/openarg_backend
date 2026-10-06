"""HTTP del modo datos de la API pública (`/catalogo/buscar|tabla|datos`)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest
from dishka import Provider, Scope, make_async_container, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.application.api_key_service import generate_api_key
from app.application.public_catalog import MAX_OFFSET
from app.application.quality.data_age import DataAge
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.domain.ports.llm.llm_provider import IEmbeddingProvider
from app.domain.ports.sandbox.sql_sandbox import (
    CachedTableInfo,
    ColumnValueStats,
    ISQLSandbox,
    SandboxResult,
    TableProfile,
    TableSource,
    TableValueStats,
)
from app.domain.ports.search.vector_search import IVectorSearch, SearchResult
from app.presentation.http.controllers.public_api import catalogo_router
from app.presentation.http.controllers.public_api.catalogo_router import router

_T = "raw.datos_gob_ar__principales_tasas_de_interes__6335b6d1__v1"
_BARE = _T.split(".")[1]
_CSV = "https://infra.datos.gob.ar/x/principales-tasas-interes-diarias.csv"


class FakeCache:
    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.ttls: dict[str, int] = {}

    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    async def ttl(self, key: str) -> int | None:
        return self.ttls.get(key)


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
        # Filas de la consulta principal de /datos (None: las dos de siempre).
        self.data_rows: list[dict[str, Any]] | None = None
        # Filas de la consulta de un agregado (`AS valor`).
        self.agg_rows: list[dict[str, Any]] = []
        # `pg_class.reltuples` que devuelven las estadísticas.
        self.estimated_rows: int | None = None

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
        elif "GROUP BY 1" in sql:
            # Los valores que existen (`sugerencias`).
            rows = [{"valor": "Principales tasas", "filas": 12}, {"valor": "Otra cosa", "filas": 3}]
        elif "AS valor" in sql:
            rows = self.agg_rows
            if not rows:
                return SandboxResult(columns=[], rows=[], row_count=0, truncated=False)
        elif self.empty and "LIMIT 101" in sql:
            return SandboxResult(columns=[], rows=[], row_count=0, truncated=False)
        elif self.data_rows is not None and "LIMIT 5" not in sql:
            rows = self.data_rows
            if not rows:
                return SandboxResult(columns=[], rows=[], row_count=0, truncated=False)
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

    async def get_value_stats(self, table: str, columns: list[str]) -> TableValueStats | None:
        if self.estimated_rows is None:
            return None
        return TableValueStats(estimated_rows=self.estimated_rows)


_LEIDA = datetime(2026, 9, 5, 3, 6, tzinfo=UTC)


@pytest.fixture(autouse=True)
def edad(monkeypatch: pytest.MonkeyPatch) -> dict[str, DataAge | None]:
    """La fecha de lectura sale de la base (`data_age_for`): acá, fija."""
    state: dict[str, DataAge | None] = {"age": DataAge(as_of=_LEIDA, days=30, source="cached")}

    async def fake(table_name: str) -> DataAge | None:
        return state["age"]

    monkeypatch.setattr(catalogo_router, "_edad_de_los_datos", fake)
    return state


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
    # Pidió 2 y hay exactamente 2: no hay más (antes decía truncado=True).
    assert body["cantidad"] == 2 and body["truncado"] is False
    assert body["siguiente_offset"] is None
    assert sandbox.sql[-1].endswith("LIMIT 3")
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


# ── /datos: truncado exacto, orden estable y offset (QW12 / ok.3) ──────────


def _filas(n: int) -> list[dict[str, Any]]:
    return [
        {"indice_tiempo": f"2026-06-{d:02d}", "tasas_interes_call": 30.0 + d}
        for d in range(1, n + 1)
    ]


async def test_datos_truncado_exacto_y_pagina_siguiente(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    sandbox.data_rows = _filas(3)  # limite + 1: hay más
    r = await client.post("/catalogo/datos", json={"tabla": _T, "limite": 2, "offset": 4})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["cantidad"] == 2 and len(body["filas"]) == 2
    assert body["truncado"] is True and body["siguiente_offset"] == 6
    assert sandbox.sql[-1].endswith("LIMIT 3 OFFSET 4")


async def test_datos_sugiere_orden_desc_si_trajo_lo_mas_viejo(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    """El orden por defecto es ascendente: las primeras filas de una serie
    larga son las de 2003, y el modelo las leía como "lo último"."""
    sandbox.data_rows = _filas(3)
    r = await client.post("/catalogo/datos", json={"tabla": _T, "limite": 2})
    notas = r.json()["filtros_aplicados"]
    assert any('orden="desc"' in n for n in notas)
    # Si eligió el orden, ya sabe.
    r = await client.post("/catalogo/datos", json={"tabla": _T, "limite": 2, "orden": "asc"})
    assert not r.json()["filtros_aplicados"]


async def test_datos_desempata_por_posicion_fisica(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    await client.post("/catalogo/datos", json={"tabla": _T, "orden": "desc"})
    assert "DESC NULLS LAST, ctid LIMIT 101" in sandbox.sql[-1]


@pytest.fixture
def sin_fecha(sandbox: FakeSandbox) -> FakeSandbox:
    sandbox.get_column_types = AsyncMock(  # type: ignore[method-assign]
        return_value={_T: [("jurisdiccion_desc", "text"), ("credito_devengado", "text")]}
    )
    sandbox.data_rows = [
        {"jurisdiccion_desc": "A", "credito_devengado": "1"},
        {"jurisdiccion_desc": "B", "credito_devengado": "2"},
        {"jurisdiccion_desc": "C", "credito_devengado": "3"},
    ]
    return sandbox


async def test_datos_sin_fecha_en_tabla_chica_ordena_por_ctid(
    client: AsyncClient, sin_fecha: FakeSandbox
) -> None:
    """Sin columna de fecha no había ORDER BY: dos pedidos iguales podían
    traer filas distintas y la página siguiente repetir filas."""
    sin_fecha.estimated_rows = 4_905
    r = await client.post("/catalogo/datos", json={"tabla": _T, "limite": 2})
    assert sin_fecha.sql[-1].endswith("ORDER BY ctid LIMIT 3")
    assert r.json()["truncado"] is True and not r.json()["filtros_aplicados"]


async def test_datos_sin_fecha_en_tabla_grande_no_la_ordena_entera_y_lo_dice(
    client: AsyncClient, sin_fecha: FakeSandbox
) -> None:
    sin_fecha.estimated_rows = 8_427_643  # molinetes del subte
    r = await client.post("/catalogo/datos", json={"tabla": _T, "limite": 2})
    assert "ORDER BY" not in sin_fecha.sql[-1]
    (nota,) = r.json()["filtros_aplicados"]
    assert "demasiado grande" in nota and "no está garantizado" in nota


async def test_datos_sin_fecha_y_tamano_desconocido_no_arriesga_el_orden(
    client: AsyncClient, sin_fecha: FakeSandbox
) -> None:
    """Staging tiene `row_count=0` en tablas de millones de filas. La nota no
    dice que sea grande (revisión del PR #139): no se sabe."""
    sin_fecha.tables = [CachedTableInfo(table_name=_T, dataset_id="ds-1", row_count=0, columns=[])]
    r = await client.post("/catalogo/datos", json={"tabla": _T, "limite": 2})
    assert "ORDER BY" not in sin_fecha.sql[-1]
    (nota,) = r.json()["filtros_aplicados"]
    assert "no se sabe cuántas filas" in nota and "demasiado grande" not in nota


async def test_datos_no_ofrece_una_pagina_mas_alla_del_tope_de_offset(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    """offset 9.900 + 500 filas daba siguiente_offset=10.400, que el propio
    servidor rechaza con un 422 (revisión del PR #139)."""
    sandbox.data_rows = _filas(3)
    r = await client.post(
        "/catalogo/datos", json={"tabla": _T, "limite": 2, "offset": MAX_OFFSET - 1}
    )
    body = r.json()
    assert body["truncado"] is True and body["siguiente_offset"] is None
    assert any(f"offset={MAX_OFFSET}" in n for n in body["filtros_aplicados"])
    # Justo en el tope todavía se puede pedir.
    r = await client.post(
        "/catalogo/datos", json={"tabla": _T, "limite": 2, "offset": MAX_OFFSET - 2}
    )
    assert r.json()["siguiente_offset"] == MAX_OFFSET


async def test_datos_un_numeric_sale_como_numero(client: AsyncClient, sandbox: FakeSandbox) -> None:
    sandbox.data_rows = [{"indice_tiempo": "2026-06-18", "tasas_interes_call": Decimal("33.14")}]
    r = await client.post("/catalogo/datos", json={"tabla": _T})
    assert r.json()["filas"][0]["tasas_interes_call"] == 33.14


async def test_datos_offset_mas_alla_del_final(client: AsyncClient, sandbox: FakeSandbox) -> None:
    sandbox.data_rows = []
    r = await client.post("/catalogo/datos", json={"tabla": _T, "offset": 500})
    body = r.json()
    assert body["cantidad"] == 0 and "offset=500" in body["aviso"]


# ── errores del sandbox, cada uno con su mensaje ───────────────────────────


@pytest.mark.parametrize(
    ("kind", "error", "status", "code", "texto"),
    [
        ("timeout", "Query timed out after 10 seconds.", 400, "timeout", "muy grande"),
        ("validation", "Forbidden SQL operation: DO", 400, "validador", "validador"),
        (
            "blocked",
            "La tabla 'x' tiene un problema de calidad sin resolver detectado por 'html_as_data'",
            400,
            "tabla_bloqueada",
            "problema de calidad",
        ),
        (
            "missing_column",
            "One or more referenced columns do not exist.",
            400,
            "columna_inexistente",
            "describir_tabla",
        ),
        (
            "missing_table",
            "The requested table does not exist.",
            404,
            "tabla_inexistente",
            "buscar",
        ),
        ("execution", "Query execution failed.", 400, "ejecucion", "no se pudo ejecutar"),
    ],
)
async def test_cada_rechazo_del_sandbox_tiene_su_mensaje(
    client: AsyncClient,
    sandbox: FakeSandbox,
    kind: str,
    error: str,
    status: int,
    code: str,
    texto: str,
) -> None:
    """Antes todo era un 400 con "Probá con otros filtros o menos columnas"."""
    sandbox.error, sandbox.error_kind = error, kind
    r = await client.post("/catalogo/datos", json={"tabla": _T})
    assert r.status_code == status
    assert r.headers["x-openarg-error"] == code
    assert texto in r.json()["detail"]
    if code != "ejecucion":
        assert "Probá con otros filtros o menos columnas" not in r.json()["detail"]


# ── /tabla: frescura (3.4) ─────────────────────────────────────────────────


async def test_tabla_dice_cuando_se_leyo_y_el_ultimo_dato(client: AsyncClient) -> None:
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    frescura = r.json()["frescura"]
    assert frescura["actualizada"] == "2026-09-05"
    assert frescura["ultimo_dato"] == "2026-06-18"
    assert frescura["serie"] is True and frescura["fecha_corte"] is None


async def test_tabla_sin_serie_es_una_foto_con_fecha_de_corte(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    sandbox.get_column_types = AsyncMock(  # type: ignore[method-assign]
        return_value={_T: [("jurisdiccion_desc", "text"), ("credito_devengado", "text")]}
    )
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    frescura = r.json()["frescura"]
    assert frescura["serie"] is False and frescura["ultimo_dato"] is None
    assert frescura["fecha_corte"] == "2026-09-05"
    assert "foto" in frescura["nota"]


async def test_tabla_con_fecha_y_periodo_desconocido_no_es_una_foto(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    """El rango exacto pasó el timeout y la muestra no tiene fechas: antes
    salía «Es una foto… vigentes al» día de lectura (revisión del PR #139)."""
    sandbox.error, sandbox.error_kind = "Query timed out after 10 seconds.", "timeout"
    sandbox.error_only_for = "AS reconocidas"
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    assert r.status_code == 200, r.text
    frescura = r.json()["frescura"]
    assert frescura["serie"] is None
    assert frescura["fecha_corte"] is None and frescura["ultimo_dato"] is None
    assert "foto" not in frescura["nota"] and "no se pudo determinar" in frescura["nota"]
    assert frescura["actualizada"] == "2026-09-05"


async def test_tabla_con_periodo_de_la_muestra_lo_marca_aproximado(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    sandbox.error, sandbox.error_kind = "Query timed out after 10 seconds.", "timeout"
    sandbox.error_only_for = "AS reconocidas"
    stats = TableValueStats(
        estimated_rows=11_600_000,
        columns={
            "indice_tiempo": ColumnValueStats(
                column="indice_tiempo", histogram_bounds=["2019-01-01", "2023-05-01", "2025-11-01"]
            )
        },
    )
    sandbox.get_value_stats = AsyncMock(return_value=stats)  # type: ignore[method-assign]
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    frescura = r.json()["frescura"]
    assert frescura["serie"] is True and frescura["aproximado"] is True
    assert frescura["ultimo_dato"] == "2025-11-01" and "aproximado" in frescura["nota"]


async def test_tabla_leida_hace_mucho_lo_avisa(
    client: AsyncClient, edad: dict[str, DataAge | None]
) -> None:
    edad["age"] = DataAge(as_of=datetime(2026, 5, 9, tzinfo=UTC), days=149, source="cached")
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    assert "hace 149 días" in r.json()["frescura"]["nota"]


async def test_tabla_bloqueada_sin_fecha_lo_dice(client: AsyncClient, sandbox: FakeSandbox) -> None:
    """Sin columna de fecha, una tabla oculta por calidad sólo se notaba en que
    la muestra venía vacía."""
    sandbox.get_column_types = AsyncMock(  # type: ignore[method-assign]
        return_value={_T: [("jurisdiccion_desc", "text")]}
    )
    sandbox.error = "La tabla tiene un problema de calidad sin resolver"
    sandbox.error_kind = "blocked"
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    assert r.status_code == 200, r.text
    assert "problema de calidad" in r.json()["aviso"] and r.json()["frescura"] is None


async def test_tabla_de_defunciones_no_avisa_que_su_fecha_no_es_la_del_dato(
    client: AsyncClient, sandbox: FakeSandbox
) -> None:
    """Revisión del PR #154 (H044): en caba__defunciones `FECHA_DEFUNCION` es la
    fecha del dato, pero describir_tabla decía «es una fecha de defunción, no la
    del dato»."""
    tabla = "raw.caba__defunciones__91003a9e__v1"
    sandbox.tables = [  # type: ignore[misc]
        CachedTableInfo(table_name=tabla, dataset_id="ds-def", row_count=3000, columns=[])
    ]
    sandbox.get_column_types = AsyncMock(  # type: ignore[method-assign]
        return_value={tabla: [("FECHA_DEFUNCION", "text"), ("GENERO", "text")]}
    )
    r = await client.get("/catalogo/tabla", params={"nombre": tabla})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["columna_fecha"] == "FECHA_DEFUNCION"
    assert not body.get("aviso_fecha")


# ── /agregar (3.1 / RC7) ───────────────────────────────────────────────────

_PRESUPUESTO_TIPOS = [
    ("funcion_desc", "text"),
    ("jurisdiccion_desc", "text"),
    ("credito_devengado", "double precision"),
    ("ejercicio_presupuestario", "bigint"),
]


@pytest.fixture
def presupuesto(sandbox: FakeSandbox) -> FakeSandbox:
    sandbox.get_column_types = AsyncMock(  # type: ignore[method-assign]
        return_value={_T: _PRESUPUESTO_TIPOS}
    )
    return sandbox


async def test_agregar_devuelve_el_total_y_sobre_cuantas_filas(
    client: AsyncClient, presupuesto: FakeSandbox
) -> None:
    presupuesto.agg_rows = [{"valor": 4570405.98, "__filas": 493, "__filas_con_valor": 493}]
    r = await client.post(
        "/catalogo/agregar",
        json={
            "tabla": _T,
            "operacion": "suma",
            "columna": "credito_devengado",
            "filtros": {"funcion_desc": "educacion y cultura"},
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["filas"] == [{"valor": 4570405.98, "filas_usadas": 493}]
    assert body["filas_usadas"] == 493 and body["truncado"] is False
    assert body["calculo"] == "suma de credito_devengado"
    assert body["fuente"] == "Principales tasas de interés" and body["url"] == _CSV
    sql = next(s for s in presupuesto.sql if "AS valor" in s)
    assert 'sum("credito_devengado")' in sql and "educacion" not in sql


async def test_agregar_ranking_por_jurisdiccion_de_mayor_a_menor(
    client: AsyncClient, presupuesto: FakeSandbox
) -> None:
    presupuesto.agg_rows = [
        {
            "jurisdiccion_desc": f"J{i}",
            "valor": 100 - i,
            "__filas": 10,
            "__filas_con_valor": 10,
            "__filas_total": 160,
            "__filas_con_valor_total": 160,
        }
        for i in range(4)  # limite 3 + 1: hay más grupos
    ]
    r = await client.post(
        "/catalogo/agregar",
        json={
            "tabla": _T,
            "operacion": "suma",
            "columna": "credito_devengado",
            "agrupar_por": ["jurisdiccion_desc"],
            "limite": 3,
        },
    )
    body = r.json()
    assert [f["jurisdiccion_desc"] for f in body["filas"]] == ["J0", "J1", "J2"]
    assert body["truncado"] is True and body["filas_usadas"] == 160
    assert body["columnas"] == ["jurisdiccion_desc", "valor", "filas_usadas"]
    assert any("Hay más de 3 grupos" in n for n in body["notas"])
    sql = next(s for s in presupuesto.sql if "AS valor" in s)
    assert "ORDER BY valor DESC NULLS LAST LIMIT 4" in sql


async def test_agregar_sin_coincidencias_no_es_un_valor(
    client: AsyncClient, presupuesto: FakeSandbox
) -> None:
    presupuesto.agg_rows = [{"valor": None, "__filas": 0, "__filas_con_valor": 0}]
    r = await client.post(
        "/catalogo/agregar",
        json={
            "tabla": _T,
            "operacion": "suma",
            "columna": "credito_devengado",
            "filtros": [{"columna": "funcion_desc", "operador": "=", "valor": "tasas"}],
        },
    )
    body = r.json()
    assert body["filas"] == [] and body["filas_usadas"] == 0
    assert "Ninguna fila" in body["aviso"]
    assert body["sugerencias"]["funcion_desc"][0]["valor"] == "Principales tasas"


async def test_agregar_acepta_min_y_max(client: AsyncClient, presupuesto: FakeSandbox) -> None:
    presupuesto.agg_rows = [{"valor": 1, "__filas": 5, "__filas_con_valor": 5}]
    r = await client.post(
        "/catalogo/agregar",
        json={"tabla": _T, "operacion": "max", "columna": "credito_devengado"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["calculo"] == "maximo de credito_devengado"
    assert 'max("credito_devengado")' in next(s for s in presupuesto.sql if "AS valor" in s)


async def test_agregar_rechaza_columnas_inventadas_antes_de_consultar(
    client: AsyncClient, presupuesto: FakeSandbox
) -> None:
    r = await client.post(
        "/catalogo/agregar", json={"tabla": _T, "operacion": "suma", "columna": "password"}
    )
    assert r.status_code == 400 and "describir_tabla" in r.json()["detail"]
    assert presupuesto.sql == []
    r = await client.post("/catalogo/agregar", json={"tabla": _T, "operacion": "mediana"})
    assert r.status_code == 400 and "operacion" in r.json()["detail"]


async def test_agregar_usa_el_cupo_del_modo_datos(
    client: AsyncClient,
    presupuesto: FakeSandbox,
    cache: FakeCache,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PUBLIC_API_MONTHLY_DATOS", "1")
    presupuesto.agg_rows = [{"valor": 1, "__filas": 1}]
    body = {"tabla": _T, "operacion": "conteo"}
    assert (await client.post("/catalogo/agregar", json=body)).status_code == 200
    r = await client.post("/catalogo/agregar", json=body)
    assert r.status_code == 402 and "catalog requests per month" in r.json()["detail"]
    assert any(":catalog:min" in k for k in cache.counters)


async def test_agregar_timeout_tiene_su_mensaje(
    client: AsyncClient, presupuesto: FakeSandbox
) -> None:
    presupuesto.error, presupuesto.error_kind = "Query timed out after 10 seconds.", "timeout"
    presupuesto.error_only_for = "AS valor"
    r = await client.post("/catalogo/agregar", json={"tabla": _T, "operacion": "conteo"})
    assert r.status_code == 400 and r.headers["x-openarg-error"] == "timeout"
    # No le sugiere la herramienta que acaba de usar (un modelo reintentaba en
    # bucle): acotar o agrupar por menos columnas.
    detail = r.json()["detail"]
    assert "agregar_datos" not in detail and "agrupá por menos columnas" in detail


async def test_agregar_el_valor_es_un_numero_en_el_json(
    client: AsyncClient, presupuesto: FakeSandbox
) -> None:
    """Los números se leen como ::numeric y psycopg devuelve Decimal, que
    Pydantic serializaba como texto: "valor": "5793524.174913833" (revisión
    del PR #139). Medido en staging: Educación y Cultura 2026."""
    presupuesto.agg_rows = [
        {
            "funcion_desc": "Educación y Cultura",
            "valor": Decimal("5793524.174913833"),
            "__filas": 505,
            "__filas_con_valor": 505,
        },
        {
            "funcion_desc": "Salud",
            "valor": Decimal("1200.000000000"),
            "__filas": 3,
            "__filas_con_valor": 3,
        },
    ]
    r = await client.post(
        "/catalogo/agregar",
        json={
            "tabla": _T,
            "operacion": "suma",
            "columna": "credito_devengado",
            "agrupar_por": ["funcion_desc"],
        },
    )
    assert r.status_code == 200, r.text
    educacion, salud = r.json()["filas"]
    assert isinstance(educacion["valor"], float) and educacion["valor"] == 5793524.174913833
    # Un total entero sale entero, no 1200.0.
    assert salud["valor"] == 1200 and isinstance(salud["valor"], int)
    assert '"valor":5793524.174913833' in r.text.replace(" ", "")


async def test_el_limite_por_minuto_dice_cuanto_falta(
    client: AsyncClient, cache: FakeCache, key: tuple[str, ApiKey]
) -> None:
    """QW10: `Retry-After` era siempre 60 aunque faltaran 5 segundos."""
    minute = f"rl:user:{key[1].user_id}:catalog:min"
    cache.counters[minute] = 30
    cache.ttls[minute] = 17
    r = await client.get("/catalogo/tabla", params={"nombre": _T})
    assert r.status_code == 429
    assert r.headers["retry-after"] == "17"
