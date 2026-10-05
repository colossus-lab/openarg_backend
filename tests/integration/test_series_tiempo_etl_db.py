"""El ETL de series contra Postgres de verdad: refresco, registro, swap atómico y alarma.

Corre contra Postgres porque lo que decide acá vive en la base: el índice UNIQUE
`uq_raw_table_versions_table_name` (con el que chocaba la identidad vieja), el
DDL transaccional del reemplazo (DROP + RENAME en una transacción, que con un
doble en memoria no existe). El umbral de `find_late` está en
`test_find_late_umbral_db.py`.

La API va en memoria (`httpx.MockTransport`). Cada test usa una clave propia
(`zz_prueba_<hex>`), así que no toca las tablas reales de series, y limpia lo
que escribió. `raw.cached_datasets` se crea si falta (la migración la deja en
`public`; en staging y prod vive en `raw`), igual que en
`test_unchanged_settles_row.py`.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock

import httpx
import pandas as pd
import pytest
from sqlalchemy import text

from app.infrastructure.celery.tasks import _db
from app.infrastructure.celery.tasks import series_tiempo_tasks as st


def _engine_or_skip():
    import os

    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = _db.get_sync_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


def _filas(n: int, desde: date) -> list[list[Any]]:
    return [[(desde + timedelta(days=i)).isoformat(), float(i)] for i in range(n)]


class _Api:
    def __init__(self, filas: list[list[Any]], descripcion: str = "Serie de prueba") -> None:
        self.filas = filas
        self.descripcion = descripcion
        self.pedidos: list[dict[str, str]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        p = dict(request.url.params)
        self.pedidos.append(p)
        start, limit = int(p.get("start", 0)), int(p.get("limit", 100))
        field: dict[str, Any] = {"description": self.descripcion, "id": p.get("ids")}
        if p.get("metadata") == "full":
            field.update(
                {"time_index_end": self.filas[-1][0], "is_updated": "True", "frequency": "R/P1D"}
            )
        return httpx.Response(
            200,
            json={
                "data": self.filas[start : start + limit],
                "count": len(self.filas),
                "meta": [{"frequency": "day"}, {"field": field, "dataset": {"title": "DS"}}],
            },
        )


@pytest.fixture
def entorno(monkeypatch):
    """Una serie de prueba con su tabla, su fila `ready` y su registro, como en staging."""
    engine = _engine_or_skip()
    clave = f"zz_prueba_{uuid.uuid4().hex[:8]}"
    serie = st.SerieETL(clave=clave, clave_catalogo="prueba", serie_id="TEST.1")
    monkeypatch.setattr(st, "SERIES_TABLAS", {clave: "prueba"})
    monkeypatch.setattr(st.series_tiempo_adapter, "SERIES_CATALOG", {"prueba": {"ids": ["TEST.1"]}})
    monkeypatch.setattr(st, "get_sync_engine", lambda: engine)
    # Sin broker: ni marts ni embeddings salen de acá.
    monkeypatch.setattr(_db, "_trigger_marts_for_portal", lambda *a, **k: None)
    from app.infrastructure.celery.tasks import scraper_tasks

    monkeypatch.setattr(scraper_tasks, "index_dataset_embedding", MagicMock())

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        creo_cached = not conn.execute(
            text("SELECT to_regclass('raw.cached_datasets') IS NOT NULL")
        ).scalar()
        if creo_cached:
            conn.execute(
                text(
                    "CREATE TABLE raw.cached_datasets (LIKE public.cached_datasets "
                    "INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"
                )
            )

    yield engine, serie

    with engine.begin() as conn:
        conn.execute(text(f'DROP VIEW IF EXISTS raw."{serie.tabla}_vista"'))
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{serie.tabla}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{serie.tabla}__nueva"'))
        conn.execute(
            text("DELETE FROM public.raw_table_versions WHERE table_name = :t"),
            {"t": serie.tabla},
        )
        conn.execute(
            text("DELETE FROM public.ingest_heartbeat WHERE resource_identity = :r"),
            {"r": serie.identidad},
        )
        if conn.execute(text("SELECT to_regclass('public.alert_log')")).scalar():
            conn.execute(
                text("DELETE FROM public.alert_log WHERE key LIKE :k"),
                {"k": f"{serie.identidad}%"},
            )
        conn.execute(
            text(
                "DELETE FROM public.ingestion_findings WHERE resource_id IN "
                "(SELECT CAST(id AS text) FROM datasets WHERE source_id = :s AND portal = :p)"
            ),
            {"s": serie.source_id, "p": st.PORTAL},
        )
        if creo_cached:
            conn.execute(text("DROP TABLE IF EXISTS raw.cached_datasets"))
        else:
            conn.execute(
                text("DELETE FROM raw.cached_datasets WHERE table_name = :t"),
                {"t": serie.tabla},
            )
        conn.execute(
            text("DELETE FROM datasets WHERE source_id = :s AND portal = :p"),
            {"s": serie.source_id, "p": st.PORTAL},
        )


def _sembrar(engine, serie: st.SerieETL, filas: list[list[Any]]) -> str:
    """El estado de staging: tabla vieja, fila `ready` y registro con la identidad buena."""
    df = pd.DataFrame(filas, columns=["fecha", "Serie de prueba"])
    df["fecha"] = pd.to_datetime(df["fecha"])
    df.to_sql(serie.tabla, engine, schema="raw", if_exists="replace", index=False)
    with engine.begin() as conn:
        did = conn.execute(
            text(
                "INSERT INTO datasets (source_id, title, portal, url, download_url, format, "
                "columns, is_cached, row_count) VALUES (:s, 'Series de Tiempo — vieja', :p, "
                ":u, '', 'json', :c, true, :n) RETURNING CAST(id AS text)"
            ),
            {
                "s": serie.source_id,
                "p": st.PORTAL,
                "u": serie.url,
                "c": json.dumps(["fecha", "Serie de prueba"]),
                "n": len(df),
            },
        ).scalar()
        conn.execute(
            text(
                "INSERT INTO raw.cached_datasets (dataset_id, table_name, status, row_count) "
                "VALUES (CAST(:d AS uuid), :t, 'ready', :n)"
            ),
            {"d": did, "t": serie.tabla, "n": len(df)},
        )
        conn.execute(
            text(
                "INSERT INTO public.raw_table_versions "
                "(resource_identity, version, schema_name, table_name, row_count) "
                "VALUES (:r, 1, 'raw', :t, :n)"
            ),
            {"r": serie.identidad, "t": serie.tabla, "n": len(df)},
        )
    return str(did)


def _con_api(monkeypatch, api: _Api) -> None:
    real = httpx.Client

    def _cliente(*a, **k):
        return real(*a, transport=httpx.MockTransport(api), **k)

    monkeypatch.setattr(st.httpx, "Client", _cliente)


def _tabla(engine, tabla: str) -> tuple[int, date | None]:
    with engine.connect() as conn:
        n, fin = conn.execute(text(f'SELECT count(*), max(fecha) FROM raw."{tabla}"')).one()
    return int(n), (fin.date() if fin else None)


def _oid(engine, tabla: str) -> int:
    with engine.connect() as conn:
        return int(
            conn.execute(text("SELECT to_regclass(:q)::oid"), {"q": f'raw."{tabla}"'}).scalar()
        )


# ── refresco ───────────────────────────────────────────────────────────────


def test_una_serie_ready_y_cortada_se_refresca_entera_y_queda_registrada(entorno, monkeypatch):
    """Tipo de cambio en staging: `ready`, 1.000 filas, hasta 2005. Antes se salteaba."""
    engine, serie = entorno
    _sembrar(engine, serie, _filas(1000, date(2003, 1, 2)))
    api = _Api(_filas(6001, date(2003, 1, 2)))
    _con_api(monkeypatch, api)

    resumen = st.ingest_series_tiempo.run(claves=[serie.clave])

    assert resumen["escritas"] == 1, resumen
    assert _tabla(engine, serie.tabla) == (6001, date(2003, 1, 2) + timedelta(days=6000))
    with engine.connect() as conn:
        cd = conn.execute(
            text("SELECT status, row_count FROM raw.cached_datasets WHERE table_name = :t"),
            {"t": serie.tabla},
        ).one()
        rtv = conn.execute(
            text(
                "SELECT resource_identity, row_count FROM public.raw_table_versions "
                "WHERE table_name = :t"
            ),
            {"t": serie.tabla},
        ).all()
        latidos = conn.execute(
            text("SELECT times_seen FROM public.ingest_heartbeat WHERE resource_identity = :r"),
            {"r": serie.identidad},
        ).scalar()
        titulo = conn.execute(
            text("SELECT title FROM datasets WHERE source_id = :s"), {"s": serie.source_id}
        ).scalar()
        huerfanas = conn.execute(
            text("SELECT to_regclass(:q)"), {"q": f'raw."{serie.tabla}__nueva"'}
        ).scalar()
    assert tuple(cd) == ("ready", 6001)
    assert [tuple(r) for r in rtv] == [(serie.identidad, 6001)], "una sola identidad por tabla"
    assert latidos == 1, "latido por serie"
    assert titulo == "Series de Tiempo — Serie de prueba"
    assert huerfanas is None


def test_sin_novedades_no_reescribe_pero_late(entorno, monkeypatch):
    engine, serie = entorno
    filas = _filas(1500, date(2003, 1, 2))
    _sembrar(engine, serie, filas)
    _con_api(monkeypatch, _Api(filas))
    antes = _oid(engine, serie.tabla)

    resumen = st.ingest_series_tiempo.run(claves=[serie.clave])

    assert resumen["al_dia"] == 1 and resumen["escritas"] == 0, resumen
    assert _oid(engine, serie.tabla) == antes, "la tabla no se recreó"
    with engine.connect() as conn:
        latidos = conn.execute(
            text("SELECT times_seen FROM public.ingest_heartbeat WHERE resource_identity = :r"),
            {"r": serie.identidad},
        ).scalar()
    assert latidos == 1


def test_el_guardian_deja_la_tabla_como_estaba(entorno, monkeypatch):
    engine, serie = entorno
    _sembrar(engine, serie, _filas(7000, date(1990, 1, 1)))  # más filas, termina antes
    _con_api(monkeypatch, _Api(_filas(6001, date(2003, 1, 2))))

    resumen = st.ingest_series_tiempo.run(claves=[serie.clave])

    assert resumen["rechazadas"] == 1, resumen
    assert "menos filas" in resumen["series"][serie.clave]["detalle"]
    assert _tabla(engine, serie.tabla)[0] == 7000
    with engine.connect() as conn:
        cd = conn.execute(
            text("SELECT status, row_count FROM raw.cached_datasets WHERE table_name = :t"),
            {"t": serie.tabla},
        ).one()
    assert tuple(cd) == ("ready", 7000), "la serie sigue servible"


def test_si_el_reemplazo_falla_queda_la_tabla_de_antes(entorno, monkeypatch):
    """El DROP falla (una vista depende de la tabla): rollback entero, nada a medias."""
    engine, serie = entorno
    _sembrar(engine, serie, _filas(1000, date(2003, 1, 2)))
    with engine.begin() as conn:
        conn.execute(
            text(f'CREATE VIEW raw."{serie.tabla}_vista" AS SELECT * FROM raw."{serie.tabla}"')
        )
    api = _Api(_filas(6001, date(2003, 1, 2)))

    with httpx.Client(transport=httpx.MockTransport(api)) as client:
        res = st.procesar_serie(engine, client, serie)

    assert res.estado == "fallida", res
    assert _tabla(engine, serie.tabla) == (1000, date(2003, 1, 2) + timedelta(days=999))
    with engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT to_regclass(:q)"), {"q": f'raw."{serie.tabla}__nueva"'}
            ).scalar()
            is None
        )
        assert (
            conn.execute(
                text("SELECT row_count FROM raw.cached_datasets WHERE table_name = :t"),
                {"t": serie.tabla},
            ).scalar()
            == 1000
        )


def test_la_alarma_ve_la_tabla_atrasada_en_la_base(entorno, monkeypatch):
    engine, serie = entorno
    _sembrar(engine, serie, _filas(1000, date(2003, 1, 2)))
    _con_api(monkeypatch, _Api(_filas(6001, date(2003, 1, 2))))

    informe = st.check_series_freshness.run(dry_run=True)

    assert [f["serie"] for f in informe["series_cache_stale"]] == [serie.clave]
    assert informe["series_source_stale"] == []
