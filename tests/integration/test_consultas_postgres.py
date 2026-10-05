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
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text

from app.application.consultas.fechas import expresion_fecha, fecha_iso
from app.application.consultas.numeros import expresion_numero, leer_numero
from app.application.public_catalog import DataRequest, build_data_query

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
NUMEROS = ["1234", "12.500", "-59.796", "1,250", "1.234.567", "1.234,56", "1,234.5", "0.125", "s/d"]


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


@pytest.fixture
def tabla():
    """Una tabla `cache_*` de prueba: el validador del sandbox sólo deja leer esas."""
    engine = _engine()
    name = f"cache_test_consultas_{uuid.uuid4().hex[:8]}"
    with engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE TABLE public."{name}" (funcion_desc text, entidad text, monto text, fecha text)'
            )
        )
        conn.execute(
            text(
                f'INSERT INTO public."{name}" VALUES '
                "('Educación y Cultura', 'Banco do Brasil', '1.500.000,50', '1/10/2017'),"
                "('Educación y Cultura', 'Banco Nación', '900.000', '15/1/2018'),"
                "('Salud', 'Call Center', '2.000.000', '1/9/2017'),"
                "('Defensa', 'Otro', '12.500', 'sin fecha')"
            )
        )
    yield name
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))


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


async def test_agregar_suma_un_monto_argentino_filtrado(tabla: str) -> None:
    """Educación y Cultura: 1.500.000,50 + 900.000, con la igualdad sin acentos."""
    res = await _agregar(
        tabla,
        operacion="suma",
        columna="monto",
        filtros={"funcion_desc": "educacion y cultura"},
    )
    assert res.grupos == [{"valor": Decimal("2400000.50")}]
    assert res.filas_usadas == 2 and res.filas_con_valor == 2 and not res.truncado


async def test_agregar_ranking_dice_el_total_de_todos_los_grupos(tabla: str) -> None:
    res = await _agregar(
        tabla, operacion="suma", columna="monto", agrupar_por=["funcion_desc"], limite=2
    )
    assert [g["funcion_desc"] for g in res.grupos] == ["Educación y Cultura", "Salud"]
    assert res.truncado and res.filas_usadas == 4
    assert any("Hay más de 2 grupos" in a for a in res.avisos)


async def test_agregar_sin_coincidencias_explica_en_vez_de_dar_cero(tabla: str) -> None:
    res = await _agregar(tabla, operacion="conteo", filtros={"funcion_desc": "Educacion"})
    assert res.vacio and res.filas_usadas == 0 and res.grupos == []
    assert res.aviso is not None and "Ninguna fila" in res.aviso
    assert "Educación y Cultura" in [s["valor"] for s in res.sugerencias["funcion_desc"]]
