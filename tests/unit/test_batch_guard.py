"""Tests for the write-time check every vía-B connector passes through.

The two shapes it exists for are both real incidents: a resource that went from
500,000 rows to an empty version without anyone hearing about it, and a payroll
that became one blank row while every status stayed green.

What matters as much as detecting them is not firing otherwise — this runs on
every vía-B ingest, and a check that cries wolf on ordinary variation is a check
people route to a folder.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.application.collection.batch_guard import (
    COLLAPSE_MIN_PREVIOUS,
    BatchVerdict,
    check_after_write,
)


class _Col:
    def __init__(self, name):
        self.name = name


def _engine(*, previous=None, columns=("a", "b"), vistas=10, con_datos=10, raises=False):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    if raises:
        conn.execute.side_effect = RuntimeError("db caída")
        return engine

    sample = MagicMock()
    sample.vistas = vistas
    sample.con_datos = con_datos

    calls = {"n": 0}

    def _execute(stmt, params=None):
        calls["n"] += 1
        res = MagicMock()
        res.scalar.return_value = previous
        res.fetchall.return_value = [_Col(c) for c in columns]
        res.fetchone.return_value = sample
        return res

    conn.execute.side_effect = _execute
    return engine


def _check(engine, **kw):
    return check_after_write(
        engine,
        resource_identity=kw.get("rid", "portal::x"),
        schema_name="raw",
        table_name="t",
        version=kw.get("version", 2),
        row_count=kw.get("row_count", 10),
    )


# ── colapso contra la versión anterior ─────────────────────────


def test_a_collapse_against_the_previous_version_is_reported():
    # El caso real: 500.000 filas → 0 en la versión siguiente.
    v = _check(_engine(previous=500_000), row_count=0)
    assert not v.ok
    assert "500000" in v.reason and "0" in v.reason
    assert v.previous_rows == 500_000


def test_ordinary_shrinkage_is_not_a_collapse():
    v = _check(_engine(previous=1000), row_count=950)
    assert v.ok, "un dataset que encoge un poco no es una alarma"


def test_growth_is_never_a_collapse():
    assert _check(_engine(previous=100), row_count=5000).ok


def test_a_tiny_previous_version_says_nothing():
    # De 8 filas a 1 es ruido, no una señal.
    v = _check(_engine(previous=COLLAPSE_MIN_PREVIOUS - 1), row_count=1)
    assert v.ok


def test_the_first_version_has_nothing_to_compare_against():
    assert _check(_engine(previous=None), row_count=0).ok


def test_an_unknown_row_count_cannot_be_compared():
    assert _check(_engine(previous=500_000), row_count=None).ok


# ── filas sin contenido ────────────────────────────────────────


def test_rows_with_no_content_anywhere_are_reported():
    v = _check(_engine(previous=None, vistas=200, con_datos=0))
    assert not v.ok
    assert "ninguna columna con contenido" in v.reason


def test_one_populated_row_is_enough_to_pass():
    assert _check(_engine(previous=None, vistas=200, con_datos=1)).ok


def test_an_empty_table_is_not_a_finding_here():
    # Cero filas es visible por otros medios; esta comprobación es para las
    # tablas que SÍ tienen filas y no dicen nada.
    assert _check(_engine(previous=None, vistas=0, con_datos=0)).ok


def test_a_single_column_table_is_skipped():
    assert _check(_engine(previous=None, columns=("solo",), vistas=10, con_datos=0)).ok


# ── no romper lo que vigila ────────────────────────────────────


def test_a_database_error_is_swallowed():
    v = _check(_engine(raises=True))
    assert v.ok, "una comprobación no puede romper la ingesta que vigila"


def test_the_verdict_carries_the_table_it_judged():
    assert _check(_engine(previous=None)).table == "raw.t"


def test_an_empty_reason_means_ok():
    assert BatchVerdict(resource_identity="a", table="raw.t").ok
