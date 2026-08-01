"""DB-level: la extraccion de version de `cleanup_invariants` no debe explotar.

El pase de "two-pass orphan registration" deriva
`raw_table_versions.version` del nombre fisico de la tabla. La expresion
original era

    COALESCE(NULLIF(regexp_replace(t.table_name, '^.*__v', ''), '')::int, 1)

y `regexp_replace` sin coincidencia devuelve el nombre **entero**, no vacio:
`NULLIF(..., '')` no lo anulaba y el `::int` levantaba
`InvalidTextRepresentation` con la primera tabla sin sufijo `__vN`. Como
`cleanup_invariants` corre entero dentro de un solo `engine.begin()`, eso
abortaba los seis arreglos de invariantes, no solo el registro.

Medido el 2026-08-01: 6.395 de 26.862 tablas del esquema `raw` en produccion
no tienen sufijo `__vN` (el patron viejo `cache_*_r<hex>`), asi que la tarea
venia fallando cada hora desde que se introdujo el 2026-05-05.

Estos tests solo necesitan una conexion a Postgres — no tocan tablas de la
aplicacion — asi que corren en el CI de integracion tal como esta.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

# La expresion tal como vive en `ops_fixes.cleanup_invariants`.
VERSION_EXPR = "COALESCE(NULLIF(substring(:name from '__v([0-9]+)$'), '')::int, 1)"


def _engine_or_skip():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — este test necesita un Postgres vivo")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — ambiental
        pytest.skip(f"DB inalcanzable: {exc}")


@pytest.fixture(scope="module")
def engine():
    return _engine_or_skip()


@pytest.mark.parametrize(
    ("table_name", "esperado"),
    [
        # Con sufijo: se extrae el numero.
        ("portal__source__abc123__v3", 3),
        ("portal__source__abc123__v1", 1),
        ("termina_en__v007", 7),
        ("indec__eph__2971d412__v12", 12),
        # Sin sufijo: cae al default 1 en vez de explotar. Estas son las que
        # rompian la tarea entera.
        ("cache_datos_gob_ar_archivos_recibidos_de_los_podere_ra1c6cd654d", 1),
        ("cache_presupuesto_credito_2016", 1),
        ("datos_gob_ar__presupuesto_de_la_administracion_pu__03_s641f6a21", 1),
        # Sufijo malformado: tampoco debe explotar.
        ("raro__v", 1),
        ("raro__v12x", 1),
        ("__v", 1),
        (" ", 1),
    ],
)
def test_version_extraction_never_raises(engine, table_name: str, esperado: int) -> None:
    with engine.connect() as conn:
        got = conn.execute(text(f"SELECT {VERSION_EXPR}"), {"name": table_name}).scalar()
    assert got == esperado, f"{table_name!r} dio {got}, esperaba {esperado}"


def test_la_expresion_vieja_si_explota(engine) -> None:
    """Control negativo: sin esto, el test de arriba no prueba nada.

    Si la forma vieja dejara de fallar, la parametrizacion anterior pasaria
    por accidente y el test no estaria vigilando lo que cree vigilar.
    """
    vieja = "COALESCE(NULLIF(regexp_replace(:name, '^.*__v', ''), '')::int, 1)"
    from sqlalchemy.exc import DataError

    with engine.connect() as conn, pytest.raises(DataError):
        conn.execute(text(f"SELECT {vieja}"), {"name": "cache_algo_r1c6cd654d"}).scalar()


def test_evaluacion_forzada_sobre_muchos_nombres(engine) -> None:
    """Evaluar la expresion fila por fila, no en un `count(*)` que la descarte.

    `SELECT count(*) FROM (SELECT <expr> ...)` deja al planner podar la
    columna que nadie consume, asi que una expresion rota igual "pasa".
    Un `sum()` sobre la expresion la obliga a evaluarse en cada fila.
    """
    nombres = [
        "cache_algo_r1c6cd654d",
        "portal__source__aaaaaaaa__v2",
        "otra_sin_version",
        "x__v10",
    ]
    expr = "COALESCE(NULLIF(substring(n from '__v([0-9]+)$'), '')::int, 1)"
    sql = f"SELECT sum({expr}) FROM unnest(CAST(:nombres AS text[])) AS t(n)"  # noqa: S608
    with engine.connect() as conn:
        total = conn.execute(text(sql), {"nombres": nombres}).scalar()
    # 1 + 2 + 1 + 10
    assert int(total) == 14
