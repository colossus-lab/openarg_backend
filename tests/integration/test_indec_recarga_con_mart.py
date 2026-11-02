"""La recarga del INDEC con una matview encima, contra un Postgres real.

`to_sql(replace)` hace DROP sin CASCADE: Postgres lo rechaza si un mart lee la
tabla. Así quedó trabado el IPC, con la planilla en septiembre y las tablas en
marzo. Lo que importa acá lo decide Postgres y un doble no lo puede probar: que
el DROP se rechace, que la matview sobreviva y que una carga fallida deje los
datos de antes.
"""

from __future__ import annotations

import os
import uuid

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from app.infrastructure.celery.tasks.indec_tasks import (
    _DEPENDENT_MATVIEWS_SQL,
    _store_sheet,
)


@pytest.fixture
def engine():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set")
    engine = create_engine(url, pool_pre_ping=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")
    yield engine
    engine.dispose()


@pytest.fixture
def tabla(engine):
    """Una tabla como la del IPC, todo en text, con una matview que la lee."""
    sufijo = uuid.uuid4().hex[:8]
    nombre = f"cache_indec_prueba_{sufijo}"
    vista = f"prueba_{sufijo}"
    with engine.begin() as conn:
        conn.execute(
            text(f'CREATE TABLE raw."{nombre}" ("Región GBA" text, periodo text, valor text)')
        )
        conn.execute(
            text(f"""INSERT INTO raw."{nombre}" VALUES ('Nivel general', '2026-03-01', '3.4')""")
        )
        conn.execute(
            text(
                f'CREATE MATERIALIZED VIEW mart."{vista}" AS '
                f'SELECT "Región GBA" AS categoria, periodo::timestamp AS periodo, '
                f'valor::numeric AS valor FROM raw."{nombre}"'
            )
        )
    yield nombre, vista
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{nombre}" CASCADE'))


def _filas(engine, relacion: str) -> list[tuple]:
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(text(f"SELECT * FROM {relacion} ORDER BY 1, 2"))]


def _ipc(*filas: tuple) -> pd.DataFrame:
    return pd.DataFrame(list(filas), columns=["Región GBA", "periodo", "valor"])


def test_sin_dependientes_reemplaza_la_tabla_entera(engine) -> None:
    nombre = f"cache_indec_prueba_{uuid.uuid4().hex[:8]}"
    try:
        assert _store_sheet(engine, pd.DataFrame({"a": [1]}), nombre) is False
        # Cambiar columnas es justo lo que el reemplazo sí puede hacer.
        assert _store_sheet(engine, pd.DataFrame({"b": [2], "c": [3]}), nombre) is False
        assert _filas(engine, f'raw."{nombre}"') == [(2, 3)]
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS raw."{nombre}"'))


def test_con_una_matview_recarga_las_filas_y_la_vista_sobrevive(engine, tabla) -> None:
    nombre, vista = tabla
    nuevo = _ipc(
        ("Nivel general", "2026-08-01", 1.9),
        ("Nivel general", "2026-09-01", 2.1),
        ("Alimentos", "2026-09-01", "///"),
    )

    assert _store_sheet(engine, nuevo, nombre) is True

    # La tabla tiene sólo lo nuevo, con `valor` mixto guardado como text.
    assert _filas(engine, f'raw."{nombre}"') == [
        ("Alimentos", "2026-09-01", "///"),
        ("Nivel general", "2026-08-01", "1.9"),
        ("Nivel general", "2026-09-01", "2.1"),
    ]
    # La vista sigue ahí con lo viejo: sólo un REFRESH le trae los datos.
    assert len(_filas(engine, f'mart."{vista}"')) == 1


def test_el_refresh_ve_lo_recargado(engine, tabla) -> None:
    nombre, vista = tabla
    _store_sheet(engine, _ipc(("Nivel general", "2026-09-01", 2.1)), nombre)

    with engine.begin() as conn:
        conn.execute(text(f'REFRESH MATERIALIZED VIEW mart."{vista}"'))
        periodo = conn.execute(text(f'SELECT max(periodo) FROM mart."{vista}"')).scalar()

    assert periodo.year == 2026 and periodo.month == 9


def test_si_cambian_las_columnas_se_rechaza_y_quedan_los_datos(engine, tabla) -> None:
    nombre, _ = tabla
    distinto = pd.DataFrame({"Región GBA": ["x"], "periodo": ["2026-09-01"], "otra": [1]})

    with pytest.raises(ValueError, match="columns changed"):
        _store_sheet(engine, distinto, nombre)

    assert _filas(engine, f'raw."{nombre}"') == [("Nivel general", "2026-03-01", "3.4")]


def test_una_carga_que_falla_deja_los_datos_de_antes(engine, tabla) -> None:
    """El DELETE y la carga van juntos: sin esto, un error vacía la tabla."""
    nombre, _ = tabla
    with engine.begin() as conn:
        conn.execute(
            text(
                f'ALTER TABLE raw."{nombre}" ADD CONSTRAINT valor_chico '
                "CHECK (length(valor) < 5) NOT VALID"
            )
        )

    with pytest.raises(Exception, match="valor_chico"):
        _store_sheet(engine, _ipc(("Nivel general", "2026-09-01", "demasiado largo")), nombre)

    assert _filas(engine, f'raw."{nombre}"') == [("Nivel general", "2026-03-01", "3.4")]


def test_encuentra_las_matviews_que_leen_la_tabla(engine, tabla) -> None:
    nombre, vista = tabla
    with engine.connect() as conn:
        vistas = {r[0] for r in conn.execute(_DEPENDENT_MATVIEWS_SQL, {"tables": [nombre]})}

    assert vistas == {vista}
