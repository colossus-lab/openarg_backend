"""El retiro de las tablas de DDJJ que armaba el colector genérico, contra Postgres.

Se arman a mano las cuatro clases de tabla que hay en staging (08-oct):

- una del dataset de la OA en el catálogo;
- un pedazo de ZIP con las columnas de la OA y sin fila en el catálogo (lo
  registró el pase de huérfanas);
- una de CABA en formato largo;
- una de ARSAT con nombre parecido, que es otra planilla y se queda.

Y se prueba:

- la simulación no toca nada;
- el retiro borra con su registro (`raw_table_versions` reemplazado, fila del
  catálogo borrada, auditoría);
- deja las de ARSAT y las nuestras;
- se niega si las tablas propias todavía no se cargaron.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import text

from app.application.catalog import registry_reconcile
from app.infrastructure.celery.tasks import _db
from app.infrastructure.celery.tasks import ddjj_tasks as dt

_OA_TITULO = "Declaraciones Juradas Patrimoniales Integrales de carácter público"
_OA_CATALOGO = "zz_justicia__declaraciones_juradas_patrimoniales_int__test__v1"
_OA_HUERFANA = "zz_datos_gob_ar__declaraciones_juradas_patrimoniales__d6_stest"
_CABA = "zz_caba__declaraciones_juradas__test__v1"
_ARSAT = "zz_datos_gob_ar__declaraciones_juradas_patrimoniales__arsattest__v4"
# un pedazo viejo de actividades anteriores y posteriores, sin catálogo
_ACT_HUERFANA = "zz_datos_gob_ar__declaraciones_juradas_de_actividad__6fa_stest"
_TODAS = (_OA_CATALOGO, _OA_HUERFANA, _CABA, _ARSAT, _ACT_HUERFANA)


def _engine_or_skip():
    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = _db.get_sync_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


def _dataset(conn, portal: str, titulo: str) -> str:
    return str(
        conn.execute(
            text(
                "INSERT INTO datasets (source_id, title, organization, portal, url, download_url, "
                "format, columns, tags, is_cached) VALUES (:s, :t, 'x', :p, 'u', 'u', 'csv', '[]', "
                "'', true) RETURNING CAST(id AS text)"
            ),
            {"s": f"zz-{uuid.uuid4().hex[:8]}", "t": titulo, "p": portal},
        ).scalar()
    )


@pytest.fixture
def entorno(monkeypatch):
    engine = _engine_or_skip()
    monkeypatch.setattr(registry_reconcile, "_REGISTRY_MIN_ROWS", 0)
    oa_cols = "dj_id text, cuit text, funcionario_apellido_nombre text, total_bienes_final text"
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        if not conn.execute(text("SELECT to_regclass('raw.cached_datasets') IS NOT NULL")).scalar():
            conn.execute(
                text(
                    "CREATE TABLE raw.cached_datasets (LIKE public.cached_datasets "
                    "INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"
                )
            )
        for tabla in (
            dt.TABLA_DECLARACIONES,
            dt.TABLA_BIENES,
            dt.TABLA_DEUDAS,
            dt.TABLA_ACTIVIDADES,
        ):
            conn.execute(text(f'DROP TABLE IF EXISTS raw."{tabla}"'))
            conn.execute(text(f'CREATE TABLE raw."{tabla}" ({dt._DDL[tabla]})'))
        conn.execute(text(f'CREATE TABLE raw."{_OA_CATALOGO}" ({oa_cols})'))
        conn.execute(text(f'CREATE TABLE raw."{_OA_HUERFANA}" ({oa_cols})'))
        conn.execute(text(f'CREATE TABLE raw."{_CABA}" (informacion text, valor text)'))
        conn.execute(text(f'CREATE TABLE raw."{_ARSAT}" ("Apellido" text, "Nombre" text)'))
        conn.execute(
            text(f'CREATE TABLE raw."{_ACT_HUERFANA}" (cuit_cuil text, cargo_jurisdiccion text)')
        )
        oa = _dataset(conn, "justicia", _OA_TITULO)
        caba = _dataset(conn, "caba", "Declaraciones Juradas")
        arsat = _dataset(conn, "datos_gob_ar", "Declaraciones Juradas Patrimoniales")
        for dataset_id, tabla in ((oa, _OA_CATALOGO), (caba, _CABA), (arsat, _ARSAT)):
            conn.execute(
                text(
                    "INSERT INTO raw.cached_datasets (dataset_id, table_name, status, row_count) "
                    "VALUES (CAST(:d AS uuid), :t, 'ready', 1)"
                ),
                {"d": dataset_id, "t": tabla},
            )
        for tabla in _TODAS:
            conn.execute(
                text(
                    "INSERT INTO public.raw_table_versions "
                    "(resource_identity, version, schema_name, table_name) "
                    "VALUES (:r, 1, 'raw', :t)"
                ),
                {"r": f"zz::{tabla}", "t": tabla},
            )
    yield engine, {"oa": oa, "caba": caba, "arsat": arsat}
    with engine.begin() as conn:
        for tabla in (
            *_TODAS,
            dt.TABLA_DECLARACIONES,
            dt.TABLA_BIENES,
            dt.TABLA_DEUDAS,
            dt.TABLA_ACTIVIDADES,
        ):
            conn.execute(text(f'DROP TABLE IF EXISTS raw."{tabla}"'))
        conn.execute(
            text("DELETE FROM public.raw_table_versions WHERE resource_identity LIKE 'zz::%'")
        )
        conn.execute(text("DELETE FROM raw.cached_datasets WHERE table_name LIKE 'zz_%'"))
        conn.execute(text("DELETE FROM datasets WHERE source_id LIKE 'zz-%'"))
        conn.execute(text("DELETE FROM cache_drop_audit WHERE reason = 'reemplazada_por_ddjj'"))


def _existe(engine, tabla: str) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text("SELECT to_regclass(:q) IS NOT NULL"), {"q": f'raw."{tabla}"'}
            ).scalar()
        )


def test_la_simulacion_lista_y_no_toca_nada(entorno):
    engine, _ = entorno
    resumen = dt.retirar(engine, dry_run=True)
    nombres = " ".join(resumen["borraria"])
    for tabla in (_OA_CATALOGO, _OA_HUERFANA, _CABA, _ACT_HUERFANA):
        assert tabla in nombres
    assert _ARSAT not in nombres and "cache_ddjj" not in nombres
    assert resumen["por_firma"] == 2 and resumen["por_catalogo"] == 2
    assert all(_existe(engine, t) for t in _TODAS)


def test_el_retiro_borra_con_su_registro_y_deja_arsat(entorno):
    engine, ids = entorno
    resumen = dt.retirar(engine, dry_run=False)
    assert resumen["borradas"] == 4 and resumen["fallidas"] == []
    assert not any(_existe(engine, t) for t in (_OA_CATALOGO, _OA_HUERFANA, _CABA, _ACT_HUERFANA))
    assert _existe(engine, _ARSAT) and _existe(engine, dt.TABLA_DECLARACIONES)
    with engine.connect() as conn:
        vivas = (
            conn.execute(
                text(
                    "SELECT table_name FROM public.raw_table_versions "
                    "WHERE resource_identity LIKE 'zz::%' AND superseded_at IS NULL"
                )
            )
            .scalars()
            .all()
        )
        catalogo = (
            conn.execute(
                text("SELECT table_name FROM raw.cached_datasets WHERE table_name LIKE 'zz_%'")
            )
            .scalars()
            .all()
        )
        auditadas = conn.execute(
            text("SELECT count(*) FROM cache_drop_audit WHERE reason = 'reemplazada_por_ddjj'")
        ).scalar()
    assert vivas == [_ARSAT] and catalogo == [_ARSAT]
    assert auditadas == 4


def test_sin_las_tablas_propias_no_retira_nada(entorno):
    engine, _ = entorno
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE raw."{dt.TABLA_BIENES}"'))
    with pytest.raises(dt._Falla, match="primero cargar las DDJJ propias"):
        dt.retirar(engine, dry_run=False)
    assert all(_existe(engine, t) for t in _TODAS)


def test_el_colector_no_despacha_los_reemplazados():
    from app.application.ddjj.reemplazos import es_reemplazado, sql_sin_reemplazados

    assert es_reemplazado("justicia", f" {_OA_TITULO} ")
    assert es_reemplazado("caba", "Declaraciones Juradas")
    assert not es_reemplazado("datos_gob_ar", "Declaraciones Juradas Patrimoniales")  # ARSAT
    assert not es_reemplazado("caba", "Presupuesto")
    engine = _engine_or_skip()
    with engine.connect() as conn:
        # La condición es SQL válido y deja pasar a ARSAT.
        assert (
            conn.execute(
                text(
                    "SELECT count(*) FROM (VALUES ('datos_gob_ar', 'Declaraciones Juradas Patrimoniales'), "
                    f"('justicia', '{_OA_TITULO}')) AS d(portal, title) WHERE true"
                    + sql_sin_reemplazados("d")
                )
            ).scalar()
            == 1
        )
