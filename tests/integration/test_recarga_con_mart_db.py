"""Recargar una tabla con un mart encima, contra Postgres de verdad.

Lo que decide acá vive en la base: que el DROP se rechace, que una transacción
revierta filas y REFRESH juntos, que `pg_depend` nombre lo que depende de la
tabla. Con un doble en memoria nada de eso existe.

Cada test usa una tabla propia (`cache_indec_zz_<hex>`) y limpia lo que creó.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.application.quality import alerting
from app.infrastructure.celery.tasks import _db
from app.infrastructure.celery.tasks import indec_tasks as indec
from app.infrastructure.celery.tasks._db import (
    TableReplaceRefused,
    replace_table_keeping_dependents,
)


def _engine_or_skip() -> Engine:
    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = _db.get_sync_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


@dataclass
class Escena:
    engine: Engine
    tabla: str
    mart: str

    def filas(self) -> list[tuple[Any, ...]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(f'SELECT periodo, valor FROM raw."{self.tabla}" ORDER BY periodo')  # noqa: S608
            ).fetchall()
            conn.rollback()
        return [tuple(r) for r in rows]

    def mart_filas(self) -> list[tuple[Any, ...]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(f'SELECT periodo, valor FROM mart."{self.mart}" ORDER BY periodo')  # noqa: S608
            ).fetchall()
            conn.rollback()
        return [tuple(r) for r in rows]

    def oid(self) -> int:
        with self.engine.connect() as conn:
            valor = conn.execute(
                text("SELECT to_regclass(:q)::oid"), {"q": f'raw."{self.tabla}"'}
            ).scalar()
            conn.rollback()
        return int(valor)


VIEJAS = pd.DataFrame(
    {
        "periodo": ["2026-01-01 00:00:00", "2026-02-01 00:00:00"],
        "valor": ["2.9", "2.6"],
    }
)
NUEVAS = pd.DataFrame(
    {
        "periodo": ["2026-01-01 00:00:00", "2026-02-01 00:00:00", "2026-03-01 00:00:00"],
        "valor": ["2.9", "2.6", "3.4"],
    }
)


@pytest.fixture
def escena() -> Iterator[Escena]:
    """Una tabla con una matview encima, armada como `mart.inflacion_argentina`."""
    engine = _engine_or_skip()
    sufijo = uuid.uuid4().hex[:8]
    tabla = f"cache_indec_zz_{sufijo}"
    mart = f"zz_mart_{sufijo}"
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))
        VIEJAS.to_sql(tabla, conn, schema="raw", index=False)
        conn.execute(
            text(
                f"""
                CREATE MATERIALIZED VIEW mart."{mart}" AS
                SELECT periodo::timestamp AS periodo, valor::numeric AS valor
                FROM raw."{tabla}"
                WHERE valor ~ '^-?[0-9]+(\\.[0-9]+)?$'
                """
            )
        )
    yield Escena(engine, tabla, mart)
    with engine.begin() as conn:
        conn.execute(text(f'DROP MATERIALIZED VIEW IF EXISTS mart."{mart}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{tabla}"'))


def test_el_replace_de_antes_choca_con_el_mart(escena: Escena) -> None:
    """La causa, para que este test falle si Postgres o pandas cambian."""
    with pytest.raises(Exception, match="depend on it"):
        NUEVAS.to_sql(escena.tabla, escena.engine, schema="raw", if_exists="replace", index=False)

    assert len(escena.filas()) == 2


def test_recarga_las_filas_y_refresca_el_mart_sin_cambiar_la_tabla(escena: Escena) -> None:
    oid = escena.oid()

    refrescados = replace_table_keeping_dependents(escena.engine, NUEVAS, escena.tabla)

    assert refrescados == [f"mart.{escena.mart}"]
    assert escena.oid() == oid
    assert len(escena.filas()) == 3
    marts = escena.mart_filas()
    assert len(marts) == 3
    assert str(marts[-1][0]).startswith("2026-03-01")
    assert float(marts[-1][1]) == 3.4


def test_recargar_dos_veces_da_lo_mismo(escena: Escena) -> None:
    replace_table_keeping_dependents(escena.engine, NUEVAS, escena.tabla)
    primera = escena.mart_filas()

    replace_table_keeping_dependents(escena.engine, NUEVAS, escena.tabla)

    assert escena.mart_filas() == primera
    assert len(escena.filas()) == 3


def test_un_insert_que_falla_deja_tabla_y_mart_como_estaban(
    escena: Escena, monkeypatch: pytest.MonkeyPatch
) -> None:
    antes = (escena.filas(), escena.mart_filas())
    original = pd.DataFrame.to_sql

    def falla_despues_del_delete(self, *a, **k):
        original(self.head(1), *a, **k)
        raise RuntimeError("se cortó la conexión a mitad de la carga")

    monkeypatch.setattr(pd.DataFrame, "to_sql", falla_despues_del_delete)

    with pytest.raises(RuntimeError, match="a mitad"):
        replace_table_keeping_dependents(escena.engine, NUEVAS, escena.tabla)

    assert (escena.filas(), escena.mart_filas()) == antes


def test_un_refresh_que_falla_revierte_tambien_las_filas(escena: Escena) -> None:
    """Un dato que el mart no puede castear: el mart se queda con lo suyo y la tabla también."""
    antes = (escena.filas(), escena.mart_filas())
    rompe_el_cast = pd.DataFrame(
        {
            "periodo": ["2026-01-01 00:00:00", "no es una fecha", "2026-03-01 00:00:00"],
            "valor": ["2.9", "2.6", "3.4"],
        }
    )

    with pytest.raises(Exception, match="(?i)invalid input syntax"):
        replace_table_keeping_dependents(escena.engine, rompe_el_cast, escena.tabla)

    assert (escena.filas(), escena.mart_filas()) == antes


def test_no_deja_vacio_un_mart_que_tenia_filas(escena: Escena) -> None:
    antes = (escena.filas(), escena.mart_filas())
    nada_pasa_el_filtro = pd.DataFrame(
        {
            "periodo": ["2026-01-01 00:00:00", "2026-02-01 00:00:00"],
            "valor": ["s/d", "s/d"],
        }
    )

    with pytest.raises(TableReplaceRefused, match="empty"):
        replace_table_keeping_dependents(escena.engine, nada_pasa_el_filtro, escena.tabla)

    assert (escena.filas(), escena.mart_filas()) == antes


def test_rechaza_un_cambio_de_columnas(escena: Escena) -> None:
    antes = (escena.filas(), escena.mart_filas())

    with pytest.raises(TableReplaceRefused, match="columns changed"):
        replace_table_keeping_dependents(
            escena.engine, NUEVAS.rename(columns={"valor": "dato"}), escena.tabla
        )

    assert (escena.filas(), escena.mart_filas()) == antes


def test_rechaza_un_archivo_recortado(escena: Escena) -> None:
    with escena.engine.begin() as conn:
        pd.DataFrame({"periodo": ["2026-01-01"] * 8, "valor": ["1"] * 8}).to_sql(
            escena.tabla, conn, schema="raw", if_exists="append", index=False
        )
        conn.execute(text(f'REFRESH MATERIALIZED VIEW mart."{escena.mart}"'))
    antes = (escena.filas(), escena.mart_filas())

    with pytest.raises(TableReplaceRefused, match="rows would replace"):
        replace_table_keeping_dependents(escena.engine, VIEJAS, escena.tabla)

    assert (escena.filas(), escena.mart_filas()) == antes


def test_rechaza_un_dataframe_vacio(escena: Escena) -> None:
    antes = (escena.filas(), escena.mart_filas())

    with pytest.raises(TableReplaceRefused, match="no rows"):
        replace_table_keeping_dependents(escena.engine, VIEJAS.head(0), escena.tabla)

    assert (escena.filas(), escena.mart_filas()) == antes


def test_refresca_una_matview_sobre_una_vista_sobre_la_tabla(escena: Escena) -> None:
    vista = f"zz_vista_{uuid.uuid4().hex[:8]}"
    arriba = f"zz_arriba_{uuid.uuid4().hex[:8]}"
    with escena.engine.begin() as conn:
        conn.execute(text(f'CREATE VIEW raw."{vista}" AS SELECT * FROM raw."{escena.tabla}"'))
        conn.execute(
            text(f'CREATE MATERIALIZED VIEW mart."{arriba}" AS SELECT * FROM raw."{vista}"')
        )
    try:
        replace_table_keeping_dependents(escena.engine, NUEVAS, escena.tabla)

        with escena.engine.connect() as conn:
            arriba_filas = conn.execute(text(f'SELECT count(*) FROM mart."{arriba}"')).scalar()
            conn.rollback()
        assert arriba_filas == 3
    finally:
        with escena.engine.begin() as conn:
            conn.execute(text(f'DROP MATERIALIZED VIEW IF EXISTS mart."{arriba}"'))
            conn.execute(text(f'DROP VIEW IF EXISTS raw."{vista}"'))


def test_sin_dependientes_puede_cambiar_de_columnas() -> None:
    engine = _engine_or_skip()
    tabla = f"cache_indec_zz_{uuid.uuid4().hex[:8]}"
    try:
        assert replace_table_keeping_dependents(engine, VIEJAS, tabla) == []
        otra = pd.DataFrame({"Divisiones": ["Alimentos"], "GBA": ["26.9"]})

        assert replace_table_keeping_dependents(engine, otra, tabla) == []

        with engine.connect() as conn:
            columnas = (
                conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'raw' AND table_name = :t ORDER BY ordinal_position"
                    ),
                    {"t": tabla},
                )
                .scalars()
                .all()
            )
            conn.rollback()
        assert columnas == ["Divisiones", "GBA"]
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS raw."{tabla}"'))


# ── la tarea entera ──────────────────────────────────────────────────────────


@pytest.fixture
def tarea(escena: Escena, monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
    """`ingest_indec` con un archivo de dos hojas, la primera con el mart encima."""
    sufijo = escena.tabla.removeprefix("cache_indec_")
    identidad_duena = f"indec::dueña_{sufijo}"
    otra_tabla = f"cache_indec_{sufijo}_ponderaciones"
    with escena.engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO public.raw_table_versions
                    (resource_identity, version, schema_name, table_name, row_count)
                VALUES (:rid, 2, 'raw', :tn, 2)
                """
            ),
            {"rid": identidad_duena, "tn": escena.tabla},
        )

    estado: dict[str, Any] = {
        "identidad": identidad_duena,
        "otra_tabla": otra_tabla,
        "hojas": {"": NUEVAS, "Ponderaciones": pd.DataFrame({"Divisiones": ["Alimentos"]})},
        "alertas": [],
        "refrescos": [],
    }

    monkeypatch.setattr(indec, "get_sync_engine", lambda: escena.engine)
    monkeypatch.setattr(
        indec, "INDEC_DATASETS", [{"id": sufijo, "name": "Prueba", "url": "http://x/y.xls"}]
    )
    monkeypatch.setattr(
        indec,
        "_download_and_parse",
        lambda url: {("main" if k == "" else k): v for k, v in estado["hojas"].items()},
    )
    monkeypatch.setattr(indec, "_register_dataset", lambda *a, **k: "dataset-de-prueba")
    monkeypatch.setattr(
        "app.infrastructure.celery.tasks.scraper_tasks.index_dataset_embedding.delay",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(indec, "_refresh_indec_marts", lambda: estado["refrescos"].append(1))
    monkeypatch.setattr(_db, "_trigger_marts_for_portal", lambda *a, **k: None)
    monkeypatch.setattr(
        alerting, "notify", lambda engine, alerts, heading: estado["alertas"].extend(alerts)
    )
    monkeypatch.setattr(escena.engine, "dispose", MagicMock())

    yield estado

    with escena.engine.begin() as conn:
        conn.execute(
            text("DELETE FROM public.raw_table_versions WHERE table_name = ANY(:t)"),
            {"t": [escena.tabla, otra_tabla]},
        )
        conn.execute(
            text("DELETE FROM public.ingest_heartbeat WHERE resource_identity LIKE :p"),
            {"p": f"%{sufijo}%"},
        )
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{otra_tabla}"'))


def test_la_tarea_recarga_la_hoja_con_mart_y_la_que_sigue(
    escena: Escena, tarea: dict[str, Any]
) -> None:
    resultado = indec.ingest_indec.run()

    assert resultado == {"ingested": 2, "skipped": 0, "errors": 0}
    assert len(escena.mart_filas()) == 3
    with escena.engine.connect() as conn:
        assert (
            conn.execute(text(f'SELECT count(*) FROM raw."{tarea["otra_tabla"]}"')).scalar() == 1  # noqa: S608
        )
        conn.rollback()
    assert tarea["refrescos"] == [1]
    assert tarea["alertas"] == []


def test_la_tarea_respeta_la_identidad_que_ya_esta_en_el_registro(
    escena: Escena, tarea: dict[str, Any]
) -> None:
    indec.ingest_indec.run()

    with escena.engine.connect() as conn:
        filas = conn.execute(
            text(
                "SELECT resource_identity, version, row_count FROM public.raw_table_versions "
                "WHERE table_name = :t"
            ),
            {"t": escena.tabla},
        ).fetchall()
        latido = conn.execute(
            text("SELECT times_seen FROM public.ingest_heartbeat WHERE resource_identity = :r"),
            {"r": tarea["identidad"]},
        ).scalar()
        conn.rollback()
    assert [tuple(f) for f in filas] == [(tarea["identidad"], 2, 3)]
    assert latido == 1


def test_una_hoja_rechazada_no_le_cuesta_las_otras_y_avisa(
    escena: Escena, tarea: dict[str, Any]
) -> None:
    antes = escena.mart_filas()
    tarea["hojas"][""] = NUEVAS.rename(columns={"valor": "dato"})

    resultado = indec.ingest_indec.run()

    assert resultado == {"ingested": 1, "skipped": 0, "errors": 1}
    assert escena.mart_filas() == antes
    with escena.engine.connect() as conn:
        assert (
            conn.execute(text(f'SELECT count(*) FROM raw."{tarea["otra_tabla"]}"')).scalar() == 1  # noqa: S608
        )
        conn.rollback()
    assert [a.kind for a in tarea["alertas"]] == ["indec_load_failed"]
    assert "columns changed" in tarea["alertas"][0].detail


def test_una_tabla_sin_registro_usa_el_nombre_que_piden_los_marts() -> None:
    engine = _engine_or_skip()

    identidad, version = indec._registry_key(
        engine, "cache_indec_zz_no_existe", "ipc_Variación mensual aperturas"
    )

    assert identidad == "indec::ipc_variaci_n_mensual_aperturas"
    assert version == 1
