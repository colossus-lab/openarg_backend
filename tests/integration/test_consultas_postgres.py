"""Los constructores de consultas contra Postgres de verdad.

Lo que un test con dobles no ve:

- que las expresiones SQL de fechas y números den lo mismo que sus espejos en
  Python (``fecha_iso``, ``leer_numero``), con el motor de regex de Postgres;
- que una consulta armada, con sus valores como parámetros ligados, pase por
  el sandbox real (validador, gates, ``text()`` de SQLAlchemy) y devuelva lo
  correcto: igualdad sin acentos, «Banco do Brasil» como valor de filtro,
  ``> 1000000.5`` (el auto-fix lo rompía) y un período sobre fechas d/m/aaaa.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text

from app.application.consultas.fechas import expresion_fecha, fecha_iso
from app.application.consultas.numeros import (
    clase_valor,
    expresion_ambiguo,
    expresion_numero,
    leer_numero,
)
from app.application.public_catalog import DataRequest, build_data_query

# A nivel de módulo: dishka resuelve las anotaciones de los providers (con
# `from __future__ import annotations` son texto) en los globales del módulo.
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox

FECHAS = [
    "2026-03-05T00:00:00",
    "2024-03",
    "2024/3/5",
    "1/10/2017",
    "05/06/2017",
    "1/4/2025 00:00",
    "201801",
    "20180115",
    "2020",
    "2020.0",
    "Junio de 2026",
    "2018 SEPTIEMBRE",
    "Nocturno",
    "13/13/2020",
    "31/02/2020",
    "",
]
NUMEROS = [
    "1234",
    "12.500",
    "-59.796",
    "1,250",
    "1.234.567",
    "1.234,56",
    "1,234.5",
    "0.125",
    "s/d",
    # Los cinco del pedido original que faltaban.
    "12,5",
    "1,234.56",
    "12.5",
    # H042: Python sacaba con strip() nbsp, tab, CR y LF; btrim(x), sólo espacios.
    "\xa012.500\xa0",
    "12.500\t",
    "0,82\r\r\n",
    " 1.234,56 ",
    "\t-12,5\n",
    "12\u2007",
]


def _engine():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    engine = create_engine(url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")
    return engine


def _values(items: list[str]) -> tuple[str, dict[str, str]]:
    return (
        ", ".join(f"(CAST(:v{i} AS text))" for i in range(len(items))),
        {f"v{i}": v for i, v in enumerate(items)},
    )


@pytest.mark.parametrize("modo", ["iso", "inicio", "fin"])
def test_la_expresion_de_fechas_coincide_con_python(modo: str) -> None:
    engine = _engine()
    values, params = _values(FECHAS)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT v, {expresion_fecha('v', 'text', modo)} AS iso "
                f"FROM (VALUES {values}) AS t(v)"
            ),
            params,
        ).fetchall()
    assert {v: iso for v, iso in rows} == {v: fecha_iso(v, modo) for v in FECHAS}


def test_los_blancos_se_pliegan_igual_que_en_python() -> None:
    """H011 (revisión independiente del 05-oct): `plegar_sql` sólo hacía `btrim`.

    Verificado también contra el Postgres de staging (sólo lectura) el 06-oct.
    """
    from app.application.consultas.texto import plegar, plegar_sql

    blancos = [chr(c) for c in range(0x110000) if chr(c).isspace()]
    textos = ["Hosp. Zonal Gral. de Ag.  Prof. Dr. R. Carrillo", "  EDUCACIÓN y   Cultura "]
    textos += [f"a{c}b{c}{c}c" for c in blancos]
    engine = _engine()
    values, params = _values(textos)
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT v, {plegar_sql('v')} AS p FROM (VALUES {values}) AS t(v)"), params
        ).fetchall()
    assert {v: p for v, p in rows} == {v: plegar(v) for v in textos}


# Revisión independiente del 05-oct: fechas m/d (H003) y el mes de una columna
# aparte (H002). Verificado también contra el Postgres de staging (sólo
# lectura) el 06-oct.
FECHAS_CON_BARRAS = [
    *FECHAS,
    "5/31/2021",
    "3/4/2025",
    "12/31/2021 10:00",
    "31/5/2021",
    "10/31/2022",
    "01/06/2014",
    "15/01/2020",
]


@pytest.mark.parametrize("formato", [None, "case_mdy", "case_mixta"])
@pytest.mark.parametrize("modo", ["iso", "inicio", "fin"])
def test_las_lecturas_dia_mes_coinciden_con_python(modo: str, formato: str | None) -> None:
    from app.application.consultas.fechas import lectura_de

    engine = _engine()
    values, params = _values(FECHAS_CON_BARRAS)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT v, {expresion_fecha('v', 'text', modo, formato)} AS iso "
                f"FROM (VALUES {values}) AS t(v)"
            ),
            params,
        ).fetchall()
    lectura = lectura_de(formato)
    assert {v: iso for v, iso in rows} == {
        v: fecha_iso(v, modo, lectura) for v in FECHAS_CON_BARRAS
    }


MESES = ["6", "06", "6.0", "12", "13", "0", "99", "Junio", "ENERO", "Diciembre*", "ene-17"]
MESES += ["Setiembre", "sept.", "Marcas", "Total", "10/2020", "2024-07-01 00:00:00", "", " 7 "]


def test_la_expresion_del_mes_coincide_con_python() -> None:
    from app.application.consultas.fechas import expresion_mes, mes_de

    engine = _engine()
    values, params = _values(MESES)
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT v, {expresion_mes('v')} AS m FROM (VALUES {values}) AS t(v)"), params
        ).fetchall()
    assert {v: m for v, m in rows} == {v: mes_de(v) for v in MESES}


@pytest.mark.parametrize("formato", [None, "ar", "en"])
def test_la_expresion_de_numeros_coincide_con_python(formato: str | None) -> None:
    engine = _engine()
    values, params = _values(NUMEROS)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT v, {expresion_numero('v', 'text', formato)} AS n "
                f"FROM (VALUES {values}) AS t(v)"
            ),
            params,
        ).fetchall()
    assert {v: n for v, n in rows} == {v: leer_numero(v, formato) for v in NUMEROS}


def test_la_condicion_de_ambiguo_coincide_con_python() -> None:
    engine = _engine()
    values, params = _values(NUMEROS)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT v, {expresion_ambiguo('v', 'text')} AS amb FROM (VALUES {values}) AS t(v)"
            ),
            params,
        ).fetchall()
    assert {v: a for v, a in rows} == {
        v: (clase_valor(v) or "").startswith("ambiguo") for v in NUMEROS
    }


FILAS_DE_PRUEBA = (
    "('Educación y Cultura', 'Banco do Brasil', '1.500.000,50', '1/10/2017'),"
    "('Educación y Cultura', 'Banco Nación', '900.000', '15/1/2018'),"
    "('Salud', 'Call Center', '2.000.000', '1/9/2017'),"
    "('Defensa', 'Otro', '12.500', 'sin fecha')"
)
# Para los cálculos sobre `monto`: con «1.500.000,50» y «2.000.000» son cinco
# valores distintos que sólo se leen en formato argentino, el mínimo para
# decidir el formato (H009). Con las cuatro filas de arriba la columna se
# rechaza, y los tests de `agregar` fallaban (revisión del PR #148).
FILAS_TURISMO = (
    "('Turismo', 'Otro', '1.250.000', '1/3/2016'),"
    "('Turismo', 'Otro', '350.000,25', '1/4/2016'),"
    "('Turismo', 'Otro', '75,5', '1/5/2016')"
)


@contextmanager
def _tabla_de_prueba(filas: str) -> Iterator[str]:
    engine = _engine()
    name = f"cache_test_consultas_{uuid.uuid4().hex[:8]}"
    with engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE TABLE public."{name}" (funcion_desc text, entidad text, monto text, fecha text)'
            )
        )
        conn.execute(text(f'INSERT INTO public."{name}" VALUES {filas}'))
    try:
        yield name
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))


@pytest.fixture
def tabla():
    """Una tabla `cache_*` de prueba: el validador del sandbox sólo deja leer esas."""
    with _tabla_de_prueba(FILAS_DE_PRUEBA) as name:
        yield name


@pytest.fixture
def tabla_montos():
    """La misma, con las tres filas de `FILAS_TURISMO`: `monto` es argentina."""
    with _tabla_de_prueba(f"{FILAS_DE_PRUEBA},{FILAS_TURISMO}") as name:
        yield name


def _run(name: str, **kw):
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    q = build_data_query(
        DataRequest(
            table=name,
            available_columns=["funcion_desc", "entidad", "monto", "fecha"],
            column_types=[(c, "text") for c in ("funcion_desc", "entidad", "monto", "fecha")],
            **kw,
        )
    )
    result = PgSandboxAdapter()._execute_sync(q.sql, 10, q.params)
    assert result.error is None, (result.error, q.sql)
    return result.rows


def test_igualdad_sin_mayusculas_ni_acentos(tabla: str) -> None:
    rows = _run(tabla, filtros={"funcion_desc": "  EDUCACION y cultura "})
    assert len(rows) == 2


def test_un_valor_con_palabras_de_sql_es_un_filtro_valido(tabla: str) -> None:
    assert [r["entidad"] for r in _run(tabla, filtros={"entidad": "Banco do Brasil"})] == [
        "Banco do Brasil"
    ]
    assert len(_run(tabla, filtros={"entidad": "Call Center"})) == 1


def test_mayor_que_con_decimales_y_formato_argentino(tabla: str) -> None:
    rows = _run(
        tabla,
        filtros=[{"columna": "monto", "operador": ">", "valor": "1000000.5"}],
        formatos={"monto": "ar"},
    )
    assert sorted(r["funcion_desc"] for r in rows) == ["Educación y Cultura", "Salud"]


def test_periodo_y_orden_sobre_fechas_d_m_aaaa(tabla: str) -> None:
    """Con `left(col, 7) >= '2017-09'` estas fechas daban 0 filas."""
    rows = _run(tabla, desde="2017-09", hasta="2017-12", columns=["fecha"])
    assert sorted(r["fecha"] for r in rows) == ["1/10/2017", "1/9/2017"]
    ultimo = _run(tabla, orden="desc", limite=1, columns=["fecha"])
    assert ultimo == [{"fecha": "15/1/2018"}]  # no "sin fecha" ni "1/9/2017"


def test_en_es_una_lista(tabla: str) -> None:
    rows = _run(
        tabla,
        filtros=[{"columna": "funcion_desc", "operador": "en", "valores": ["Salud", "Defensa"]}],
    )
    assert len(rows) == 2


def test_tabla_grande_suma_lo_pedido_a_los_valores_de_pg_stats(tabla: str) -> None:
    """Revisión del PR #133: en el camino de tablas grandes (`tolerante=False`)
    el filtro se reemplazaba por los valores frecuentes de `pg_stats` y lo
    pedido que no estaba entre ellos se perdía («Salud» acá: 2 filas en vez
    de 3, sin aviso)."""
    from app.application.consultas.filtros import Filter

    filtro = Filter(
        "funcion_desc",
        "en",
        ("EDUCACION Y CULTURA", "Salud"),
        # Lo que `resolver_canonicos` saca de un most_common_vals sin «Salud».
        canonicos=("Educación y Cultura",),
    )
    rows = _run(tabla, filtros=[filtro], tolerante=False)
    assert sorted(r["funcion_desc"] for r in rows) == [
        "Educación y Cultura",
        "Educación y Cultura",
        "Salud",
    ]
    distinto = Filter(
        "funcion_desc", "!=", "EDUCACION Y CULTURA", canonicos=("Educación y Cultura",)
    )
    assert sorted(r["funcion_desc"] for r in _run(tabla, filtros=[distinto], tolerante=False)) == [
        "Defensa",
        "Salud",
    ]


def test_calcular_agrupado_informa_el_total_de_todos_los_grupos(tabla: str) -> None:
    """Con más grupos que `limite`, la suma de los mostrados no es el total."""
    from app.application.answers.aggregates import (
        FILAS,
        FILAS_TOTAL,
        AggregateRequest,
        build_aggregate_query,
    )
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    q = build_aggregate_query(
        AggregateRequest(
            table=tabla,
            column_types=[(c, "text") for c in ("funcion_desc", "entidad", "monto", "fecha")],
            operacion="conteo",
            agrupar_por=["funcion_desc"],
            limite=1,
        )
    )
    result = PgSandboxAdapter()._execute_sync(q.sql, 10, q.params)
    assert result.error is None, (result.error, q.sql)
    assert len(result.rows) == 2  # limite + 1: se sabe que hay más grupos
    assert result.rows[0][FILAS] == 2  # Educación y Cultura, el grupo más grande
    assert {int(r[FILAS_TOTAL]) for r in result.rows} == {4}  # las 4 filas de la tabla


def test_leer_numero_es_decimal() -> None:
    assert leer_numero("1.500.000,50") == Decimal("1500000.50")


# ── modo datos: truncado exacto, orden estable y offset (QW12 / ok.3) ──────


def test_desempate_por_ctid_y_offset_pasan_el_sandbox(tabla: str) -> None:
    """El validador real acepta `ctid` y `OFFSET`; las páginas no se pisan."""
    todas = _run(tabla, orden="desc", columns=["fecha", "entidad"], una_de_mas=True)
    primera = _run(tabla, orden="desc", limite=2, columns=["fecha", "entidad"], una_de_mas=True)
    segunda = _run(
        tabla, orden="desc", limite=2, offset=2, columns=["fecha", "entidad"], una_de_mas=True
    )
    assert len(primera) == 3  # limite + 1: hay más
    assert primera[:2] + segunda[:2] == todas[:4]


def test_sin_fecha_orden_fisico_es_el_del_archivo(tabla: str) -> None:
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    sin_fecha = ["funcion_desc", "entidad", "monto"]  # como si la tabla no tuviera `fecha`
    q = build_data_query(
        DataRequest(
            table=tabla,
            available_columns=sin_fecha,
            column_types=[(c, "text") for c in sin_fecha],
            columns=["entidad"],
            orden_fisico=True,
            offset=1,
        )
    )
    assert q.orden == "fisico"
    result = PgSandboxAdapter()._execute_sync(q.sql, 10, q.params)
    assert result.error is None, (result.error, q.sql)
    assert [r["entidad"] for r in result.rows] == ["Banco Nación", "Call Center", "Otro"]


def test_una_columna_que_ya_no_existe_tiene_su_propio_error(tabla: str) -> None:
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    result = PgSandboxAdapter()._execute_sync(f'SELECT "no_existe" FROM public."{tabla}"', 10, {})
    assert result.error_kind == "missing_column"
    result = PgSandboxAdapter()._execute_sync('SELECT 1 FROM public."cache_no_existe_xyz"', 10, {})
    assert result.error_kind == "missing_table"


# ── agregar_datos (3.1): la cuenta en la base, de punta a punta ────────────


async def _agregar(tabla: str, **kw):  # type: ignore[no-untyped-def]
    from app.application.consultas.agregar import PedidoAgregado, agregar
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    sandbox = PgSandboxAdapter()

    async def run(sql, params):  # type: ignore[no-untyped-def]
        result = await sandbox.execute_readonly(sql, params=params)
        assert result.error is None, (result.error, sql)
        return result.rows

    return await agregar(
        sandbox,
        PedidoAgregado(
            tabla=tabla,
            tipos=[(c, "text") for c in ("funcion_desc", "entidad", "monto", "fecha")],
            **kw,
        ),
        run,
    )


async def test_agregar_suma_un_monto_argentino_filtrado(tabla_montos: str) -> None:
    """Educación y Cultura: 1.500.000,50 + 900.000, con la igualdad sin acentos."""
    res = await _agregar(
        tabla_montos,
        operacion="suma",
        columna="monto",
        filtros={"funcion_desc": "educacion y cultura"},
    )
    assert res.grupos == [{"valor": Decimal("2400000.50")}]
    assert res.filas_usadas == 2 and res.filas_con_valor == 2 and not res.truncado


async def test_agregar_ranking_dice_el_total_de_todos_los_grupos(tabla_montos: str) -> None:
    res = await _agregar(
        tabla_montos, operacion="suma", columna="monto", agrupar_por=["funcion_desc"], limite=2
    )
    assert [g["funcion_desc"] for g in res.grupos] == ["Educación y Cultura", "Salud"]
    assert res.truncado and res.filas_usadas == 7  # las 4 filas de `tabla` y las 3 de Turismo
    assert any("Hay más de 2 grupos" in a for a in res.avisos)


async def test_agregar_rechaza_la_columna_sin_evidencia_suficiente(tabla: str) -> None:
    """Con las cuatro filas de `tabla`, «900.000» y «12.500» no se pueden leer y
    sólo dos valores dicen el formato: no se calcula (H009)."""
    from app.application.public_catalog import CatalogRequestError

    with pytest.raises(CatalogRequestError, match="sólo 2 valores distintos"):
        await _agregar(tabla, operacion="suma", columna="monto")


def test_la_cuenta_de_ambiguos_corre_con_los_filtros_del_calculo(tabla: str) -> None:
    """H041: las filas con un número ambiguo se cuentan en otra consulta
    (revisión del PR #148), con los parámetros ligados del cálculo. En `tabla`
    son «900.000» y «12.500»; con el filtro queda «900.000»."""
    from app.application.answers.aggregates import (
        FILAS_AMBIGUAS,
        AggregateRequest,
        build_aggregate_query,
    )
    from app.application.consultas.filtros import Filter
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    tipos = [(c, "text") for c in ("funcion_desc", "entidad", "monto", "fecha")]
    for filtros, esperado in (
        ([], 2),
        ([Filter("funcion_desc", "=", "educacion y cultura")], 1),
    ):
        q = build_aggregate_query(
            AggregateRequest(
                table=tabla,
                column_types=tipos,
                operacion="suma",
                columna="monto",
                filtros=filtros,
                formatos={},
            )
        )
        assert q.sql_ambiguas is not None
        result = PgSandboxAdapter()._execute_sync(q.sql_ambiguas, 10, q.params)
        assert result.error is None, (result.error, q.sql_ambiguas)
        assert [int(r[FILAS_AMBIGUAS]) for r in result.rows] == [esperado]


async def test_agregar_sin_coincidencias_explica_en_vez_de_dar_cero(tabla: str) -> None:
    res = await _agregar(tabla, operacion="conteo", filtros={"funcion_desc": "Educacion"})
    assert res.vacio and res.filas_usadas == 0 and res.grupos == []
    assert res.aviso is not None and "Ninguna fila" in res.aviso
    assert "Educación y Cultura" in [s["valor"] for s in res.sugerencias["funcion_desc"]]


async def test_agregar_por_http_devuelve_el_valor_como_numero(tabla_montos: str) -> None:
    """Revisión del PR #139: la suma sale de Postgres como Decimal y el router la
    pasaba cruda a `list[dict[str, Any]]`, que Pydantic serializa como texto
    ("valor": "2400000.50"). Los tests del router usan floats y la prueba en
    staging fue con la función: acá es el endpoint con el sandbox real."""
    from unittest.mock import AsyncMock

    from dishka import Provider, Scope, make_async_container, provide
    from dishka.integrations.fastapi import setup_dishka
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    from app.application.api_key_service import generate_api_key
    from app.domain.entities.api_key.api_key import ApiKey
    from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter
    from app.presentation.http.controllers.public_api.catalogo_router import router

    class Sandbox(PgSandboxAdapter):
        # El catálogo (`cached_datasets`) no conoce la tabla de prueba: el
        # resto (consulta, estadísticas) es el adaptador real.
        async def find_tables(self, *, dataset_ids=None, table_names=None):  # type: ignore[no-untyped-def,override]
            return [
                CachedTableInfo(table_name=tabla_montos, dataset_id="ds", row_count=7, columns=[])
            ]

        async def get_column_types(self, table_names):  # type: ignore[no-untyped-def,override]
            cols = [(c, "text") for c in ("funcion_desc", "entidad", "monto", "fecha")]
            return {n: cols for n in table_names}

        async def get_table_sources(self, table_names):  # type: ignore[no-untyped-def,override]
            return {}

    class Cache:
        async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
            return 1

        async def ttl(self, key: str) -> int | None:
            return None

    raw, key_hash = generate_api_key()
    repo = AsyncMock(spec=IApiKeyRepository)
    repo.get_by_key_hash.return_value = ApiKey(
        user_id=uuid.uuid4(), key_hash=key_hash, plan="free", is_active=True
    )
    credits = AsyncMock(spec=ICreditRepository)
    credits.get_active_supporter.return_value = None
    sandbox = Sandbox()

    class P(Provider):
        scope = Scope.REQUEST

        @provide
        def s(self) -> ISQLSandbox:
            return sandbox

        @provide
        def c(self) -> ICacheService:
            return Cache()  # type: ignore[return-value]

        @provide
        def r(self) -> IApiKeyRepository:
            return repo

        @provide
        def cr(self) -> ICreditRepository:
            return credits

    app = FastAPI()
    app.include_router(router)
    setup_dishka(container=make_async_container(P()), app=app)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        r = await client.post(
            "/catalogo/agregar",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "tabla": tabla_montos,
                "operacion": "suma",
                "columna": "monto",
                "agrupar_por": ["funcion_desc"],
            },
        )
    assert r.status_code == 200, r.text
    filas = {f["funcion_desc"]: f for f in r.json()["filas"]}
    educacion = filas["Educación y Cultura"]
    assert educacion["valor"] == 2400000.5 and isinstance(educacion["valor"], float)
    assert educacion["filas_usadas"] == 2
    # Una suma entera sale entera.
    assert filas["Salud"]["valor"] == 2000000 and isinstance(filas["Salud"]["valor"], int)


# ── revisión independiente del 05-oct, de punta a punta ─────────────────────


async def _agregar_en(tabla: str, tipos: list[tuple[str, str]], **kw):  # type: ignore[no-untyped-def]
    from app.application.consultas.agregar import PedidoAgregado, agregar
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    sandbox = PgSandboxAdapter()

    async def run(sql, params):  # type: ignore[no-untyped-def]
        result = await sandbox.execute_readonly(sql, params=params)
        assert result.error is None, (result.error, sql)
        return result.rows

    return await agregar(sandbox, PedidoAgregado(tabla=tabla, tipos=tipos, **kw), run)


async def test_un_valor_con_dos_espacios_se_encuentra_copiado_tal_cual() -> None:
    """H011: el filtro plegaba lo pedido («ag. prof») pero no la columna («ag.  prof»)."""
    engine = _engine()
    name = f"cache_test_blancos_{uuid.uuid4().hex[:8]}"
    nbsp = chr(0xA0)
    valores = ["Hosp. Zonal Gral. de Ag.  Prof. Dr. R. Carrillo", f"Hosp. Zonal{nbsp}Gral.", "Otro"]
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE public."{name}" (establecimiento text)'))
        for v in valores:
            conn.execute(text(f'INSERT INTO public."{name}" VALUES (:v)'), {"v": v})
    try:
        tipos = [("establecimiento", "text")]
        for pedido in valores[:2]:
            res = await _agregar_en(
                name, tipos, operacion="conteo", filtros={"establecimiento": pedido}
            )
            assert res.grupos == [{"valor": 1}], pedido
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))


@pytest.fixture
def tabla_mensual():
    """Como teus_movilizados (staging): `anio` y `mes` separados, un dato por mes."""
    engine = _engine()
    name = f"cache_test_anio_mes_{uuid.uuid4().hex[:8]}"
    with engine.begin() as conn:
        conn.execute(
            text(f'CREATE TABLE public."{name}" (anio bigint, mes text, puerto text, teus text)')
        )
        filas = [f"(2024, '{m}', 'Dock Sud', '{m * 10}')" for m in range(1, 13)]
        filas += [f"(2025, '{m}', 'Dock Sud', '{m * 100}')" for m in range(1, 10)]
        # Un mes escrito con el nombre y una fila de total, que no es de ningún mes.
        filas += ["(2025, 'Junio', 'La Plata', '7')", "(2025, 'Total', 'Dock Sud', '4500')"]
        conn.execute(text(f'INSERT INTO public."{name}" VALUES ' + ", ".join(filas)))
    yield name
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))


_TIPOS_MENSUAL = [("anio", "bigint"), ("mes", "text"), ("puerto", "text"), ("teus", "text")]


async def test_un_mes_sobre_anio_y_mes_suma_solo_ese_mes(tabla_mensual: str) -> None:
    """H002: antes entraba el año entero (4500 + 7 + la suma de los nueve meses)."""
    res = await _agregar_en(
        tabla_mensual,
        _TIPOS_MENSUAL,
        operacion="suma",
        columna="teus",
        desde="2025-06",
        hasta="2025-06",
    )
    assert res.grupos == [{"valor": Decimal("607")}]  # 600 de Dock Sud y 7 de La Plata
    assert res.filas_usadas == 2


async def test_el_anio_entero_sigue_entrando_entero(tabla_mensual: str) -> None:
    res = await _agregar_en(
        tabla_mensual, _TIPOS_MENSUAL, operacion="conteo", desde="2025-01", hasta="2025-12"
    )
    assert res.grupos == [{"valor": 11}]  # nueve meses, «Junio» y la fila de total


def test_orden_desc_trae_el_ultimo_mes(tabla_mensual: str) -> None:
    """H010: con `ORDER BY anio DESC, ctid` salía enero de 2025."""
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    q = build_data_query(
        DataRequest(
            table=tabla_mensual,
            available_columns=[c for c, _ in _TIPOS_MENSUAL],
            column_types=_TIPOS_MENSUAL,
            columns=["anio", "mes"],
            orden="desc",
            limite=1,
        )
    )
    result = PgSandboxAdapter()._execute_sync(q.sql, 10, q.params)
    assert result.error is None, (result.error, q.sql)
    assert result.rows == [{"anio": 2025, "mes": "9"}]


@pytest.fixture
def tabla_mes_dia():
    """Como la Pauta publicitaria de CABA (staging, c7fd6b9c): fechas m/d/aaaa."""
    engine = _engine()
    name = f"cache_test_mes_dia_{uuid.uuid4().hex[:8]}"
    fechas = [f"5/{d}/2021" for d in range(1, 32)]  # mayo entero
    fechas += [f"{m}/15/2021" for m in (3, 4, 6, 8)] + ["3/4/2021", "8/5/2021", "12/30/2021"]
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE public."{name}" (fecha_pauta text, monto text)'))
        conn.execute(
            text(
                f'INSERT INTO public."{name}" VALUES ' + ", ".join(f"('{f}', '1')" for f in fechas)
            )
        )
        # La lectura d/m o m/d sale de la muestra de pg_stats.
        conn.execute(text(f'ANALYZE public."{name}"'))
    yield name
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))


async def test_un_mes_sobre_fechas_mes_dia(tabla_mes_dia: str) -> None:
    """H003: leídas d/m, mayo de 2021 daba 2 filas: «5/5/2021» y «8/5/2021» (de agosto)."""
    res = await _agregar_en(
        tabla_mes_dia,
        [("fecha_pauta", "text"), ("monto", "text")],
        operacion="conteo",
        desde="2021-05",
        hasta="2021-05",
    )
    assert res.grupos == [{"valor": 31}]


# ── revisión del PR #154, de punta a punta ──────────────────────────────────


@pytest.fixture
def tabla_mensual_texto():
    """Como mart.estadistica_mediaciones (staging): `anio` y `mes` de texto."""
    engine = _engine()
    name = f"cache_test_anio_mes_texto_{uuid.uuid4().hex[:8]}"
    filas = [f"('{a}', '{m}', '{m}')" for a in (2022, 2023, 2024) for m in range(1, 13)]
    # Un mes con blancos (sigue por las expresiones regulares) y una fila de total.
    filas += ["('2024', ' 7 ', '5')", "('2024', 'Total', '999')"]
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE public."{name}" (anio text, mes text, cantidad text)'))
        conn.execute(text(f'INSERT INTO public."{name}" VALUES ' + ", ".join(filas)))
        conn.execute(text(f'ANALYZE public."{name}"'))
    yield name
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))


_TIPOS_MENSUAL_TEXTO = [("anio", "text"), ("mes", "text"), ("cantidad", "text")]


@pytest.mark.parametrize(("mes", "suma"), [("2023-06", 6), ("2024-07", 12), ("2024-12", 12)])
async def test_un_mes_sobre_anio_y_mes_de_texto(
    tabla_mensual_texto: str, mes: str, suma: int
) -> None:
    """Con `anio` de forma conocida se filtra primero el año (más barato) y después el mes."""
    res = await _agregar_en(
        tabla_mensual_texto,
        _TIPOS_MENSUAL_TEXTO,
        operacion="suma",
        columna="cantidad",
        desde=mes,
        hasta=mes,
    )
    assert res.query.fecha is not None and res.query.fecha.formato == "anio"
    assert res.grupos == [{"valor": suma}]


def test_orden_desc_sobre_anio_y_mes_de_texto(tabla_mensual_texto: str) -> None:
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    q = build_data_query(
        DataRequest(
            table=tabla_mensual_texto,
            available_columns=[c for c, _ in _TIPOS_MENSUAL_TEXTO],
            column_types=_TIPOS_MENSUAL_TEXTO,
            columns=["anio", "mes"],
            orden="desc",
            limite=1,
            formato_fecha="anio",
        )
    )
    result = PgSandboxAdapter()._execute_sync(q.sql, 10, q.params)
    assert result.error is None, (result.error, q.sql)
    assert result.rows == [{"anio": "2024", "mes": "12"}]


async def test_el_periodo_de_anio_y_mes_llega_al_ultimo_mes(
    tabla_mensual: str, tabla_mensual_texto: str
) -> None:
    """Revisión de ola 3 (#144 × #154): el período de describir_tabla iba por el
    año solo, y una tabla de un año con meses hasta marzo era «2026 a 2026»:
    una foto vigente al día de lectura. La fila «Total» cuenta por su año."""
    from app.application.consultas.fechas import resolver_columna_fecha
    from app.application.consultas.preparar import describir_periodo
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    sandbox = PgSandboxAdapter()
    for tabla, tipos, esperado in [
        (tabla_mensual, _TIPOS_MENSUAL, ("2024-01", "2025-09")),
        (tabla_mensual_texto, _TIPOS_MENSUAL_TEXTO, ("2022-01", "2024-12")),
    ]:
        fecha = resolver_columna_fecha(tipos, tabla=tabla)
        assert fecha is not None and fecha.mes == "mes"
        periodo = await describir_periodo(sandbox, tabla, fecha)
        assert (periodo.desde, periodo.hasta) == esperado, periodo
        # Todas las filas tienen año: ningún aviso de valores sin fecha.
        assert periodo.aviso is None, periodo


@pytest.fixture
def tabla_mensual_mes_dia():
    """Como biodiésel y bioetanol 26bc8483 (staging): «M/1/AAAA», ningún día mayor que 12."""
    engine = _engine()
    name = f"cache_test_mensual_md_{uuid.uuid4().hex[:8]}"
    filas = [
        f"('{m}/1/{a}', '{10 * m if a == 2017 else 1}')" for a in (2016, 2017) for m in range(1, 13)
    ]
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE public."{name}" (fecha text, produccion_ton text)'))
        conn.execute(text(f'INSERT INTO public."{name}" VALUES ' + ", ".join(filas)))
        conn.execute(text(f'ANALYZE public."{name}"'))
    yield name
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))


@pytest.mark.parametrize(("mes", "suma"), [("2017-10", 100), ("2017-01", 10)])
async def test_un_mes_sobre_una_serie_mensual_mes_dia(
    tabla_mensual_mes_dia: str, mes: str, suma: int
) -> None:
    """Leída d/m, cada mes caía en enero: enero sumaba el año y octubre daba 0 filas."""
    res = await _agregar_en(
        tabla_mensual_mes_dia,
        [("fecha", "text"), ("produccion_ton", "text")],
        operacion="suma",
        columna="produccion_ton",
        desde=mes,
        hasta=mes,
    )
    assert res.grupos == [{"valor": suma}]
    assert res.filas_usadas == 1


@pytest.fixture
def tabla_periodo_anual():
    """Como consultas_medicas_ambulatorias a29f6e30 (staging): `periodo` con años."""
    engine = _engine()
    name = f"cache_test_periodo_anual_{uuid.uuid4().hex[:8]}"
    filas = [f"('{a}', '{a}')" for a in range(2013, 2022) for _ in range(3)]
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE public."{name}" (periodo text, consultas text)'))
        conn.execute(text(f'INSERT INTO public."{name}" VALUES ' + ", ".join(filas)))
        conn.execute(text(f'ANALYZE public."{name}"'))
    yield name
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))


async def test_un_mes_sobre_un_periodo_de_anios_se_rechaza(tabla_periodo_anual: str) -> None:
    """H002 en una columna de años que no se llama `anio`: junio daba el año entero."""
    from app.application.consultas.sql import CatalogRequestError

    tipos = [("periodo", "text"), ("consultas", "text")]
    with pytest.raises(CatalogRequestError, match="años enteros"):
        await _agregar_en(
            tabla_periodo_anual, tipos, operacion="conteo", desde="2019-06", hasta="2019-06"
        )
    res = await _agregar_en(
        tabla_periodo_anual, tipos, operacion="conteo", desde="2019", hasta="2019"
    )
    assert res.grupos == [{"valor": 3}]
