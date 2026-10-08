"""Cotizaciones del BCRA con fecha e historia, contra Postgres de verdad.

Ítem 2.4 de la auditoría del 04-oct: ``raw.cache_bcra_cotizaciones`` era una
foto sin fecha que la tarea pisaba cada día. Ahora la tarea:

- le agrega ``fecha`` y un índice único ``(fecha, "codigoMoneda")`` a la tabla
  que creó ``to_sql`` (Alembic nunca la tuvo), de forma idempotente;
- borra la foto vieja sin fecha en la misma transacción que inserta la nueva;
- inserta con ``ON CONFLICT DO NOTHING``: correr dos veces el mismo día (o un
  sábado, cuando la API repite el viernes) no duplica.

Y el mart ``series_economicas`` v0.4 sigue siendo una fila por moneda: la de
la última fecha. Mientras la tabla sea la foto vieja (todas las fechas NULL),
sigue sirviendo esa foto en vez de quedar vacío.

Corre contra Postgres porque lo que decide es el índice único y el SQL del
mart: con un doble en memoria no se probaría nada.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from app.application.marts.mart import load_mart
from app.application.marts.sql_macros import _LiveRow, resolve_macros
from app.infrastructure.celery.tasks import bcra_tasks

TABLE = "cache_bcra_cotizaciones_it"
INDEX = "uq_cache_bcra_cotizaciones_it"
MART_YAML = Path(__file__).resolve().parents[2] / "config" / "marts" / "series_economicas.yaml"


def _engine_or_skip():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


@pytest.fixture
def engine(monkeypatch):
    """Una tabla descartable con otro nombre: la real no se toca."""
    engine = _engine_or_skip()
    monkeypatch.setattr(bcra_tasks, "TABLE_NAME", TABLE)
    monkeypatch.setattr(bcra_tasks, "UNIQUE_INDEX", INDEX)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text(f"DROP TABLE IF EXISTS raw.{TABLE} CASCADE"))
    yield engine
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS raw.{TABLE} CASCADE"))
    engine.dispose()


def _legacy_snapshot(engine) -> None:
    """La tabla como la dejaba `to_sql`: 4 columnas y una foto sin fecha."""
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE raw.{TABLE} (
                    "codigoMoneda" text, descripcion text,
                    "tipoPase" double precision, "tipoCotizacion" double precision
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                INSERT INTO raw.{TABLE} VALUES
                    ('USD', 'DOLAR E.E.U.U.', 0, 1520),
                    ('REF', 'DOLAR REFERENCIA COM 3500', 0, 1523.0868),
                    ('EUR', 'EURO', 1.17, 1710.76)
                """
            )
        )


def _rows(fecha: str, usd: float) -> list[dict]:
    return bcra_tasks._normalize(
        [
            {"fecha": fecha, "codigoMoneda": "USD", "descripcion": "DOLAR", "tipoCotizacion": usd},
            {"fecha": fecha, "codigoMoneda": "REF", "descripcion": "REF", "tipoCotizacion": 1523.0},
            {
                "fecha": fecha,
                "codigoMoneda": "EUR",
                "descripcion": "EURO",
                "tipoCotizacion": 1710.0,
            },
        ]
    )


def test_la_tabla_vieja_pasa_a_tener_fecha_e_historia(engine) -> None:
    _legacy_snapshot(engine)

    inserted, total, last = bcra_tasks._upsert(engine, _rows("2026-10-01", 1524.5))

    assert (inserted, total, last) == (3, 3, date(2026, 10, 1))
    with engine.connect() as conn:
        tipo = conn.execute(
            text(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema = 'raw' AND table_name = :t AND column_name = 'fecha'"
            ),
            {"t": TABLE},
        ).scalar()
        indexdef = conn.execute(
            text("SELECT indexdef FROM pg_indexes WHERE schemaname = 'raw' AND indexname = :i"),
            {"i": INDEX},
        ).scalar()
        sin_fecha = conn.execute(
            text(f"SELECT count(*) FROM raw.{TABLE} WHERE fecha IS NULL")
        ).scalar()
    assert tipo == "date"
    assert "UNIQUE" in indexdef and "fecha" in indexdef and "codigoMoneda" in indexdef
    assert sin_fecha == 0  # la foto vieja sin fecha se fue con la primera corrida


def test_dos_corridas_el_mismo_dia_no_duplican_y_la_historia_crece(engine) -> None:
    assert bcra_tasks._upsert(engine, _rows("2026-10-01", 1524.5))[:2] == (3, 3)
    # Otra corrida el mismo día, o un sábado que repite el viernes.
    assert bcra_tasks._upsert(engine, _rows("2026-10-01", 1524.5))[:2] == (0, 3)
    inserted, total, last = bcra_tasks._upsert(engine, _rows("2026-10-02", 1520.0))
    assert (inserted, total, last) == (3, 6, date(2026, 10, 2))
    # Un valor distinto para un día ya cargado no pisa el que estaba.
    assert bcra_tasks._upsert(engine, _rows("2026-10-01", 9999.0))[:2] == (0, 6)
    with engine.connect() as conn:
        usd = conn.execute(
            text(
                f"""SELECT "tipoCotizacion" FROM raw.{TABLE}
                    WHERE fecha = '2026-10-01' AND "codigoMoneda" = 'USD'"""
            )
        ).scalar()
    assert usd == 1524.5


def test_sin_tabla_la_crea(engine) -> None:
    inserted, total, _ = bcra_tasks._upsert(engine, _rows("2026-10-02", 1520.0))
    assert (inserted, total) == (3, 3)


# ── el mart ────────────────────────────────────────────────


def _mart_rows(engine, monkeypatch) -> list[tuple]:
    """El SQL del YAML real, resuelto contra la tabla de prueba."""
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_identities",
        lambda _e, ids: {
            "bcra::cotizaciones": _LiveRow("bcra::cotizaciones", "raw", TABLE),
        },
    )
    for name in (
        "_query_live_by_portals",
        "_query_live_by_identity_patterns",
        "_query_live_by_table_patterns",
    ):
        monkeypatch.setattr(f"app.application.marts.sql_macros.{name}", lambda _e, _a: [])
    mart = load_mart(MART_YAML)
    sql = resolve_macros(mart.sql, engine)
    with engine.connect() as conn:
        return [
            tuple(r)
            for r in conn.execute(
                text(f"SELECT serie_id, valor, fecha FROM ({sql}) m ORDER BY serie_id")  # noqa: S608
            )
        ]


def test_el_mart_muestra_solo_la_ultima_fecha(engine, monkeypatch) -> None:
    bcra_tasks._upsert(engine, _rows("2026-10-01", 1524.5))
    bcra_tasks._upsert(engine, _rows("2026-10-02", 1520.0))

    rows = _mart_rows(engine, monkeypatch)

    assert [r[0] for r in rows] == ["EUR", "REF", "USD"]  # una fila por moneda
    assert {r[2] for r in rows} == {date(2026, 10, 2)}
    assert dict((r[0], r[1]) for r in rows)["USD"] == 1520.0


def test_el_mart_sigue_sirviendo_la_foto_vieja_hasta_la_primera_corrida(
    engine, monkeypatch
) -> None:
    """El deploy puede reconstruir el mart antes de que corra el snapshot."""
    _legacy_snapshot(engine)

    rows = _mart_rows(engine, monkeypatch)

    assert [r[0] for r in rows] == ["EUR", "REF", "USD"]
    assert {r[2] for r in rows} == {None}


def test_el_yaml_subio_de_version_y_expone_la_fecha() -> None:
    mart = load_mart(MART_YAML)
    assert tuple(int(p) for p in mart.version.split(".")) >= (0, 4, 0)
    assert "fecha" in [c.name for c in mart.canonical_columns]
