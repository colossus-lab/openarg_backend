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
    expresion_otro_formato,
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


@pytest.mark.parametrize(("formato", "otro"), [("ar", "en"), ("en", "ar")])
def test_la_condicion_del_otro_formato_coincide_con_python(formato: str, otro: str) -> None:
    """Lo que cuenta la confirmación en la columna entera: los valores que
    `clase_valor` lee sólo en el otro formato, sin los ambiguos."""
    engine = _engine()
    values, params = _values(NUMEROS)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT v, {expresion_otro_formato('v', formato)} AS otro "
                f"FROM (VALUES {values}) AS t(v)"
            ),
            params,
        ).fetchall()
    assert {v: o for v, o in rows} == {v: clase_valor(v) == otro for v in NUMEROS}


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
def _tabla_de_prueba(filas: str, *, autovacuum: bool = True) -> Iterator[str]:
    engine = _engine()
    name = f"cache_test_consultas_{uuid.uuid4().hex[:8]}"
    # Sin autovacuum no hay ANALYZE a mitad del test: la muestra de
    # `perfiles_numericos` son sólo las primeras filas, sin `pg_stats`.
    opciones = "" if autovacuum else " WITH (autovacuum_enabled = false)"
    with engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE TABLE public."{name}" (funcion_desc text, entidad text, monto text, fecha text)'
                + opciones
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


async def test_agregar_rechaza_una_mezcla_que_la_muestra_no_ve() -> None:
    """Segunda revisión del PR #148: consultas_medicas (staging `a7ce7a82`). Las
    primeras 200 filas, que son la muestra, dicen «inglés»; «1.005.915», al
    final de la tabla, sólo puede ser argentino. Sin la cuenta en la columna
    entera, «120.813» consultas se sumaban como 120,813."""
    from app.application.public_catalog import CatalogRequestError
    from tests.unit.test_consultas_numeros import (
        CONSULTAS_MEDICAS_FILAS,
        CONSULTAS_MEDICAS_PG_STATS,
    )

    def filas(valores: list[str]) -> str:
        return ",".join(f"('x', 'x', '{v}', 'x')" for v in valores)

    valores = [*CONSULTAS_MEDICAS_FILAS, *CONSULTAS_MEDICAS_PG_STATS]
    with _tabla_de_prueba(filas(valores), autovacuum=False) as limpia:
        res = await _agregar(limpia, operacion="suma", columna="monto")
        # La cuenta pasó por el sandbox real y no encontró argentinos.
        assert res.req.formatos == {"monto": "en"}
    with _tabla_de_prueba(filas([*valores, "1.005.915"]), autovacuum=False) as mezclada:
        with pytest.raises(CatalogRequestError, match="columna entera"):
            await _agregar(mezclada, operacion="suma", columna="monto")


async def test_agregar_busca_los_ambiguos_de_una_mezcla_que_la_muestra_no_ve() -> None:
    """Tercera revisión del PR #148: la muestra (las primeras 200 filas) es
    inglesa y no trae ambiguos; al final hay un argentino y un «12.500». Sin
    formato, el filtro `monto > 5` dejaba afuera el «12.500» sin aviso. Sin el
    «12.500» no hay nada que leer de dos formas: queda sin formato y cuenta
    todas las filas que pasan el filtro."""
    from app.application.public_catalog import CatalogRequestError

    def filas(valores: list[str]) -> str:
        return ",".join(f"('x', 'x', '{v}', 'x')" for v in valores)

    muestra = ["1,234.5", "7.25", "1,000,000", "3.5", "12,345.67"] * 40
    filtro = [{"columna": "monto", "operador": ">", "valor": "5"}]
    with _tabla_de_prueba(filas([*muestra, "1.234,5"]), autovacuum=False) as limpia:
        res = await _agregar(limpia, operacion="conteo", filtros=filtro)
        assert res.req.formatos == {"monto": None}
        assert res.grupos == [{"valor": 161}]  # 4 de cada 5 de la muestra, y «1.234,5»
    with _tabla_de_prueba(filas([*muestra, "1.234,5", "12.500"]), autovacuum=False) as mezclada:
        with pytest.raises(CatalogRequestError, match="«12.500»"):
            await _agregar(mezclada, operacion="conteo", filtros=filtro)


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
