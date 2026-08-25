"""Tests for the sweeps and the write-time guard, which shipped without any.

`_report_if_degenerate` runs on **every vía-B ingest** — thirteen connectors —
and had no test. The sweeps had none either, which for `find_empty_content_tables`
matters more than usual: two earlier versions of its candidate query silently
excluded most of the corpus, and a green run looked identical either way.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.infrastructure.celery.tasks import _db
from app.infrastructure.celery.tasks import empty_content_tasks as ect
from app.infrastructure.celery.tasks import staleness_tasks as st


class _Verdict:
    def __init__(self, ok=True, reason=""):
        self.ok = ok
        self.reason = reason
        self.table = "raw.t"


# ── el guardián en el punto único ──────────────────────────────


def test_a_clean_batch_says_nothing(monkeypatch):
    enviados: list = []
    monkeypatch.setattr(
        "app.application.collection.batch_guard.check_after_write", lambda e, **kw: _Verdict()
    )
    monkeypatch.setattr(
        "app.application.quality.alerting.notify", lambda e, a, **kw: enviados.append(a)
    )
    _db._report_if_degenerate(
        MagicMock(),
        resource_identity="p::x",
        schema_name="raw",
        table_name="t",
        version=1,
        row_count=10,
    )
    assert enviados == []


def test_a_degenerate_batch_reaches_a_person(monkeypatch):
    enviados: list = []
    monkeypatch.setattr(
        "app.application.collection.batch_guard.check_after_write",
        lambda e, **kw: _Verdict(ok=False, reason="500000 → 0"),
    )
    monkeypatch.setattr(
        "app.application.quality.alerting.notify", lambda e, a, **kw: enviados.extend(a)
    )
    _db._report_if_degenerate(
        MagicMock(),
        resource_identity="p::x",
        schema_name="raw",
        table_name="t",
        version=2,
        row_count=0,
    )
    assert len(enviados) == 1
    assert enviados[0].kind == "via_b_degenerate"
    assert enviados[0].key == "p::x", "la identidad del recurso, no la de esta corrida"


def test_the_guard_never_breaks_the_ingest_it_watches(monkeypatch):
    def _boom(e, **kw):
        raise RuntimeError("db caída")

    monkeypatch.setattr("app.application.collection.batch_guard.check_after_write", _boom)
    _db._report_if_degenerate(
        MagicMock(),
        resource_identity="p::x",
        schema_name="raw",
        table_name="t",
        version=1,
        row_count=1,
    )  # no debe levantar


# ── el barrido de contenido ────────────────────────────────────


class _Cand:
    def __init__(self, name, filas=100, schema="raw"):
        self.schema_name = schema
        self.table_name = name
        self.filas = filas


class _Col:
    def __init__(self, name):
        self.name = name


def _sweep_engine(candidatas, columnas=("a", "b"), vistas=50, con_datos=50):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    muestra = MagicMock()
    muestra.vistas = vistas
    muestra.con_datos = con_datos
    respuestas = {"n": 0}

    def _execute(stmt, params=None):
        res = MagicMock()
        sql = str(stmt)
        if "pg_attribute" in sql:
            res.fetchall.return_value = [_Col(c) for c in columnas]
        elif "FILTER" in sql:
            res.fetchone.return_value = muestra
        else:
            res.fetchall.return_value = list(candidatas)
        respuestas["n"] += 1
        return res

    conn.execute.side_effect = _execute
    return engine


def test_a_table_with_rows_and_no_content_is_reported(monkeypatch):
    monkeypatch.setattr(ect, "get_sync_engine", lambda: _sweep_engine([_Cand("t")], con_datos=0))
    monkeypatch.setattr("app.application.quality.alerting.notify", lambda e, a, **kw: None)
    r = ect.find_empty_content_tables(limit=1)
    assert r["vacias"] == 1
    assert r["ejemplos"][0]["tabla"] == "raw.t"


def test_a_populated_table_is_not_reported(monkeypatch):
    monkeypatch.setattr(ect, "get_sync_engine", lambda: _sweep_engine([_Cand("t")], con_datos=1))
    r = ect.find_empty_content_tables(limit=1)
    assert r["vacias"] == 0


def test_a_sample_smaller_than_min_rows_says_nothing(monkeypatch):
    # `min_rows` se aplica a lo LEÍDO, no a una estadística que puede no existir.
    monkeypatch.setattr(
        ect, "get_sync_engine", lambda: _sweep_engine([_Cand("t")], vistas=2, con_datos=0)
    )
    r = ect.find_empty_content_tables(limit=1, min_rows=3)
    assert r["vacias"] == 0


def test_a_one_column_table_is_skipped(monkeypatch):
    monkeypatch.setattr(
        ect, "get_sync_engine", lambda: _sweep_engine([_Cand("t")], columnas=("sola",), con_datos=0)
    )
    r = ect.find_empty_content_tables(limit=1)
    assert r["saltadas"] == 1 and r["vacias"] == 0


# ── el aviso de fuentes tarde ──────────────────────────────────


class _Late:
    def __init__(self, rid="p::x"):
        self.resource_identity = rid
        self.days_late = 21.0
        self.cadence_days = 7.0
        self.times_seen = 9

    def phrase_es(self):
        return "llega cada ~7.0 día(s) y hace 21.0 que no llega"


def test_nothing_late_sends_nothing(monkeypatch):
    monkeypatch.setattr(st, "get_sync_engine", lambda: MagicMock())
    monkeypatch.setattr("app.application.quality.heartbeat.find_late", lambda e, **kw: [])
    r = st.alert_stale_ingests()
    assert r["late"] == 0 and "alerting" not in r


def test_a_late_source_is_reported_keyed_on_the_source(monkeypatch):
    enviados: list = []
    monkeypatch.setattr(st, "get_sync_engine", lambda: MagicMock())
    monkeypatch.setattr("app.application.quality.heartbeat.find_late", lambda e, **kw: [_Late()])
    monkeypatch.setattr(
        "app.application.quality.alerting.notify", lambda e, a, **kw: enviados.extend(a) or {}
    )
    r = st.alert_stale_ingests()
    assert r["late"] == 1
    assert enviados[0].kind == "ingest_late"
    assert enviados[0].key == "p::x", "una fuente que sigue tarde se reporta una vez"


def test_a_channel_that_is_down_does_not_cost_the_sweep(monkeypatch):
    def _boom(e, a, **kw):
        raise RuntimeError("telegram caído")

    monkeypatch.setattr(st, "get_sync_engine", lambda: MagicMock())
    monkeypatch.setattr("app.application.quality.heartbeat.find_late", lambda e, **kw: [_Late()])
    monkeypatch.setattr("app.application.quality.alerting.notify", _boom)
    r = st.alert_stale_ingests()
    assert r["late"] == 1
