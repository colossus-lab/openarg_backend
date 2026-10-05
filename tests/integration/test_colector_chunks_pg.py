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
            conn.execute(text(f'DROP TABLE IF EXISTS public."{name}" CASCADE'))  # noqa: S608
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


def _types(engine, name) -> dict[str, str]:
    with engine.connect() as conn:
        return {
            r[0]: r[1]
            for r in conn.execute(
                text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = :t"
                ),
                {"t": name},
            ).fetchall()
        }


def _scalar(engine, sql: str):
    with engine.connect() as conn:
        return conn.execute(text(sql)).scalar()


def _write_csv(path, header: str, rows: list[str]) -> str:
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return str(path)


_PARAMS = {"sep": ",", "encoding": "utf-8"}


def test_un_chunk_con_texto_en_una_columna_numerica_relee_solo_esa_columna(table, tmp_path):
    """El primer chunk crea `valor` como BIGINT; uno posterior trae `s/d`.

    Antes del PR: o fallaba la colecta o, si el error mencionaba la columna,
    DROP + recreate y quedaba la cola del archivo. En la primera versión del PR
    se recargaba TODO el archivo como texto y `poblacion` también quedaba en
    TEXT (revisión, hallazgo menor). Ahora se relee sólo `valor` como texto.
    """
    engine, name = table
    rows_ = [f"Provincia {i},{i},{1000 + i}" for i in range(120)]
    rows_ += ["Provincia s/d,s/d,999"]
    rows_ += [f"Provincia {i},{i},{1000 + i}" for i in range(120, 150)]
    csv_path = _write_csv(tmp_path / "valores.csv", "provincia,valor,poblacion", rows_)

    rows, columns, truncated = ct._load_csv_chunked(
        csv_path,
        name,
        engine,
        chunk_size=40,
        source_dataset_id=DATASET_ID,
        csv_params_override=dict(_PARAMS),
    )

    assert (rows, truncated) == (151, False)
    assert columns == ["provincia", "valor", "poblacion", "_source_dataset_id"]
    assert _count(engine, name) == 151
    tipos = _types(engine, name)
    assert tipos["valor"] == "text"
    assert tipos["poblacion"] == "bigint"
    assert _scalar(engine, f"SELECT count(*) FROM public.\"{name}\" WHERE valor = 's/d'") == 1  # noqa: S608


def test_decimales_tarde_en_una_columna_entera_se_ensanchan_sin_redondear(table, tmp_path):
    """Postgres redondeaba `100.5` a `101` al apendear a la columna BIGINT que
    creó el primer chunk. Ahora la columna pasa a double en el lugar."""
    engine, name = table
    rows_ = [f"2024-01-01,prod {i},{100 + i}" for i in range(120)]
    rows_ += [f"2024-01-02,prod {i},{100 + i}.5" for i in range(30)]
    csv_path = _write_csv(tmp_path / "precios.csv", "fecha,producto,precio", rows_)

    rows, _columns, _truncated = ct._load_csv_chunked(
        csv_path,
        name,
        engine,
        chunk_size=40,
        source_dataset_id=DATASET_ID,
        csv_params_override=dict(_PARAMS),
    )

    assert rows == 150 and _count(engine, name) == 150
    tipos = _types(engine, name)
    assert tipos["precio"] == "double precision"
    assert tipos["producto"] == "text"
    expected = sum(100 + i for i in range(120)) + sum(100 + i + 0.5 for i in range(30))
    assert _scalar(engine, f'SELECT sum(precio) FROM public."{name}"') == expected  # noqa: S608


def _load_member(engine, name, csv_path, *, force_append):
    return ct._load_csv_chunked(
        csv_path,
        name,
        engine,
        chunk_size=40,
        source_dataset_id=DATASET_ID,
        force_append=force_append,
        csv_params_override=dict(_PARAMS),
    )


def test_miembro_de_zip_contra_una_columna_double_no_aborta(table, tmp_path):
    """Revisión del PR #130, hallazgo importante, con el escenario del revisor:
    el miembro 1 deja `precio` en double; el miembro 2 (force_append) trae
    enteros en su primer chunk y decimales después. La rama abortaba el ZIP
    entero con el primer chunk ya apendeado."""
    engine, name = table
    m1 = [f"2024-01-01,prod {i},{100 + i}.25" for i in range(50)]
    m2 = [f"2024-02-01,prod {i},{200 + i}" for i in range(60)]
    m2 += [f"2024-02-02,prod {i},{200 + i}.5" for i in range(60)]
    _load_member(
        engine,
        name,
        _write_csv(tmp_path / "m1.csv", "fecha,producto,precio", m1),
        force_append=False,
    )

    rows, _c, _t = _load_member(
        engine,
        name,
        _write_csv(tmp_path / "m2.csv", "fecha,producto,precio", m2),
        force_append=True,
    )

    assert rows == 120
    assert _count(engine, name) == 170
    assert _types(engine, name)["precio"] == "double precision"
    expected = (
        sum(100 + i + 0.25 for i in range(50))
        + sum(200 + i for i in range(60))
        + sum(200 + i + 0.5 for i in range(60))
    )
    assert _scalar(engine, f'SELECT sum(precio) FROM public."{name}"') == expected  # noqa: S608


def test_miembro_de_zip_con_decimales_contra_una_columna_bigint_la_ensancha(table, tmp_path):
    engine, name = table
    m1 = [f"2024-01-01,prod {i},{100 + i}" for i in range(50)]
    m2 = [f"2024-02-01,prod {i},{200 + i}" for i in range(60)]
    m2 += [f"2024-02-02,prod {i},{200 + i}.5" for i in range(60)]
    _load_member(
        engine,
        name,
        _write_csv(tmp_path / "m1.csv", "fecha,producto,precio", m1),
        force_append=False,
    )
    assert _types(engine, name)["precio"] == "bigint"

    rows, _c, _t = _load_member(
        engine,
        name,
        _write_csv(tmp_path / "m2.csv", "fecha,producto,precio", m2),
        force_append=True,
    )

    assert rows == 120 and _count(engine, name) == 170
    assert _types(engine, name)["precio"] == "double precision"
    fraccionarios = _scalar(
        engine,
        f'SELECT count(*) FROM public."{name}" WHERE precio <> trunc(precio)',  # noqa: S608
    )
    assert fraccionarios == 60  # ninguno redondeado


def test_miembro_de_zip_con_texto_contra_una_columna_bigint_la_pasa_a_texto(table, tmp_path):
    engine, name = table
    m1 = [f"Provincia {i},{i}" for i in range(50)]
    m2 = [f"Provincia {i},{i}" for i in range(60)] + ["Provincia s/d,s/d"]
    _load_member(
        engine, name, _write_csv(tmp_path / "m1.csv", "provincia,valor", m1), force_append=False
    )

    rows, _c, _t = _load_member(
        engine, name, _write_csv(tmp_path / "m2.csv", "provincia,valor", m2), force_append=True
    )

    assert rows == 61 and _count(engine, name) == 111
    assert _types(engine, name)["valor"] == "text"


def test_zip_saltea_el_miembro_que_no_entra_y_conserva_lo_anterior(table, tmp_path):
    """Revisión del PR #130, hallazgo menor: el primer chunk de un miembro N≥2
    todavía podía hacer DROP + recreate de la tabla compartida. Acá el ensanche
    no se puede (una vista depende de la columna): el miembro se saltea con una
    nota, y la tabla conserva la fila previa y el miembro 1."""
    import zipfile

    engine, name = table
    with engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE TABLE public."{name}" '  # noqa: S608
                '(codigo bigint, nombre text, "_source_dataset_id" text)'
            )
        )
        conn.execute(text(f"INSERT INTO public.\"{name}\" VALUES (1, 'previo', 'otro')"))  # noqa: S608
        conn.execute(text(f'CREATE VIEW public."{name}_v" AS SELECT codigo FROM public."{name}"'))  # noqa: S608
    zip_path = tmp_path / "bundle.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("a.csv", "codigo,nombre\n" + "".join(f"{i},n{i}\n" for i in range(10)))
        zf.writestr("b.csv", "codigo,nombre\nA12,x\nB13,y\n")

    with zipfile.ZipFile(zip_path) as zf:
        result = ct._parse_zip_archive(
            zf,
            zip_path=str(zip_path),
            dataset_id=DATASET_ID,
            table_name=name,
            engine=engine,
            append_mode=True,
        )

    assert result["parsed"] is True
    assert result["row_count"] == 10
    assert "b.csv" in (result["sampled_note"] or "")
    assert _count(engine, name) == 11
    assert _types(engine, name)["codigo"] == "bigint"
