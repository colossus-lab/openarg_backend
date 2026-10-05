"""El colector contra un Postgres real: un CSV por chunks llega entero y con su encabezado.

Hasta el 04-oct-2026 cada chunk volvía a inferir el encabezado. En
`proyectos_parlamentarios` (filas con `EXP_SENADO` vacío) cada chunk promovía
sus dos primeras filas a encabezado, el append del siguiente fallaba por
columnas distintas y `_to_sql_safe` hacía DROP + recreate: la tabla terminó con
el último chunk menos dos filas, 11.089 de 111.091, mientras el catálogo
anunciaba las 111.091. Con mocks eso no se ve — el DROP es del lado de
Postgres —, así que esto corre contra una base de verdad.

Se saltea limpio si no hay `DATABASE_URL`.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text

from app.infrastructure.celery.tasks import collector_tasks as ct

DATASET_ID = "350ea9f8-a3fb-47c5-9ef1-a66b0385fda8"
PROYECTOS_HEADER = [
    "PROYECTO_ID",
    "TITULO",
    "PUBLICACION_FECHA",
    "PUBLICACION_ID",
    "CAMARA_ORIGEN",
    "EXP_DIPUTADOS",
    "EXP_SENADO",
    "TIPO",
    "AUTOR",
]


def _engine_or_skip():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — needs a live Postgres")
    try:
        engine = create_engine(url.replace("+asyncpg", "+psycopg"), pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


@pytest.fixture
def table():
    engine = _engine_or_skip()
    name = f"test_colector_chunks_{uuid.uuid4().hex[:10]}"
    try:
        yield engine, name
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))  # noqa: S608
        engine.dispose()


def _write_proyectos(path, n_rows: int) -> None:
    lines = [",".join(PROYECTOS_HEADER)]
    for i in range(n_rows):
        lines.append(
            f"HCDN{289000 + i},PROYECTO DE LEY NUMERO {i} SOBRE TEMA {i % 7},"
            f'2026-03-05T00:00:00,HCDN144TP006,Diputados,{i:04d}-D-2026,,LEY,"TODERO, PABLO"'
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _columns(engine, name):
    with engine.connect() as conn:
        return [
            r[0]
            for r in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = :t ORDER BY ordinal_position"
                ),
                {"t": name},
            ).fetchall()
        ]


def _count(engine, name):
    with engine.connect() as conn:
        return conn.execute(text(f'SELECT count(*) FROM public."{name}"')).scalar()  # noqa: S608


def test_csv_por_chunks_llega_entero_y_con_su_encabezado(table, tmp_path):
    engine, name = table
    csv_path = tmp_path / "proyectos.csv"
    _write_proyectos(csv_path, 230)

    rows, columns, truncated = ct._load_csv_chunked(
        str(csv_path),
        name,
        engine,
        chunk_size=50,
        source_dataset_id=DATASET_ID,
        csv_params_override={"sep": ",", "encoding": "utf-8"},
    )

    assert (rows, truncated) == (230, False)
    assert columns == [*PROYECTOS_HEADER, "_source_dataset_id"]
    assert _columns(engine, name) == [*PROYECTOS_HEADER, "_source_dataset_id"]
    assert _count(engine, name) == 230


def test_un_chunk_con_texto_en_una_columna_numerica_recarga_como_texto(table, tmp_path):
    """El primer chunk crea `valor` como BIGINT; uno posterior trae `s/d`.

    Antes: o fallaba la colecta o, si el error mencionaba la columna, DROP +
    recreate y quedaba la cola del archivo. Ahora se recarga entero como texto.
    """
    engine, name = table
    lines = ["provincia,valor"]
    lines += [f"Provincia {i},{i}" for i in range(120)]
    lines += ["Provincia s/d,s/d"]
    lines += [f"Provincia {i},{i}" for i in range(120, 150)]
    csv_path = tmp_path / "valores.csv"
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    rows, columns, truncated = ct._load_csv_chunked(
        str(csv_path),
        name,
        engine,
        chunk_size=40,
        source_dataset_id=DATASET_ID,
        csv_params_override={"sep": ",", "encoding": "utf-8"},
    )

    assert (rows, truncated) == (151, False)
    assert columns == ["provincia", "valor", "_source_dataset_id"]
    assert _count(engine, name) == 151
    with engine.connect() as conn:
        tipo = conn.execute(
            text(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=:t AND column_name='valor'"
            ),
            {"t": name},
        ).scalar()
    assert tipo == "text"
