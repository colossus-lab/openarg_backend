"""Cada tabla del INDEC tiene su hoja, contra Postgres.

`ingest_indec` registra un dataset por hoja con la misma `download_url`. Cuando la
puerta WS0 rechazaba una tabla, el colector tomaba el dataset como pendiente y
guardaba la primera hoja del archivo con su título. Medido en staging el
02-nov-2026: las tres `indec__indec_ipc_*` eran la variación mensual (`EXCEPT`
sin filas), y nivel general de febrero daba 2,6 en interanual y en índices.

El libro de estos tests tiene la misma forma que `sh_ipc_aperturas.xls`, con un
valor distinto por hoja, y pasa por el camino real: descarga, parseo y escritura.
"""

from __future__ import annotations

import io
import os
from datetime import datetime

import httpx
import pytest
from openpyxl import Workbook
from sqlalchemy import text

from app.application.catalog import registry_reconcile
from app.infrastructure.celery.tasks import _db, collector_tasks, indec_tasks
from app.infrastructure.celery.tasks import scraper_tasks as st

_URL = "https://www.indec.gob.ar/ftp/cuadros/economia/zz_aperturas.xlsx"
_MENSUAL, _INTERANUAL, _INDICES = (
    "Variación mensual aperturas",
    "Var. interanual aperturas",
    "Índices aperturas",
)
# Nivel general de febrero de 2026, los tres valores de `sh_ipc_aperturas.xls`.
_NIVEL_GENERAL = {_MENSUAL: 2.6, _INTERANUAL: 33.2, _INDICES: 10667.69}
_TABLAS = {
    _MENSUAL: "cache_indec_zz_hojas_variaci_n_mensual_aperturas",
    _INTERANUAL: "cache_indec_zz_hojas_var_interanual_aperturas",
    _INDICES: "cache_indec_zz_hojas_ndices_aperturas",
}
_DATASET = {"id": "zz_hojas", "name": "ZZ hojas", "url": _URL}


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


def _libro() -> bytes:
    """Un .xlsx como el del IPC: región, meses por columna y un valor propio por hoja."""
    libro = Workbook()
    libro.remove(libro.active)
    meses = [datetime(2025, m, 1) for m in range(1, 13)] + [
        datetime(2026, 1, 1),
        datetime(2026, 2, 1),
    ]
    for hoja, valor in _NIVEL_GENERAL.items():
        ws = libro.create_sheet(hoja)
        ws.append(["Región GBA", *meses])
        ws.append(["Nivel general", *[valor] * len(meses)])
        ws.append(["Frutas", *[valor + 1] * len(meses)])
        ws.append(["Verduras", *[valor + 2] * len(meses)])
    salida = io.BytesIO()
    libro.save(salida)
    return salida.getvalue()


def _tablas_indec(engine) -> list[str]:
    with engine.connect() as conn:
        return list(
            conn.execute(
                text(
                    "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = 'raw' AND c.relkind = 'r' "
                    "AND c.relname LIKE '%zz\\_%' ESCAPE '\\' "
                    "ORDER BY 1"
                )
            ).scalars()
        )


def _borrar_tablas(conn, *nombres: str) -> None:
    for nombre in nombres:
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{nombre}" CASCADE'))


@pytest.fixture
def base(monkeypatch):
    engine = _engine_or_skip()
    monkeypatch.setattr(registry_reconcile, "_REGISTRY_MIN_ROWS", 0)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        for tabla in ("cached_datasets", "cache_drop_audit"):
            if not conn.execute(
                text("SELECT to_regclass(:t) IS NOT NULL"), {"t": f"raw.{tabla}"}
            ).scalar():
                conn.execute(
                    text(
                        f"CREATE TABLE raw.{tabla} (LIKE public.{tabla} "
                        "INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"
                    )
                )
    yield engine
    with engine.begin() as conn:
        _borrar_tablas(conn, *_tablas_indec(engine))
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS mart.zz_lee_copia"))
        conn.execute(
            text(
                "DELETE FROM public.raw_table_versions WHERE table_name LIKE 'indec\\_\\_zz%' ESCAPE '\\' "
                "OR table_name LIKE '%zz\\_hojas%' ESCAPE '\\'"
            )
        )
        conn.execute(
            text(
                "DELETE FROM raw.cached_datasets WHERE table_name LIKE 'indec\\_\\_zz%' ESCAPE '\\' "
                "OR table_name LIKE '%zz\\_hojas%' ESCAPE '\\'"
            )
        )
        conn.execute(text("DELETE FROM catalog_resources WHERE source_id LIKE 'indec-%zz%'"))
        conn.execute(text("DELETE FROM datasets WHERE source_id LIKE 'indec-%zz%'"))
        conn.execute(
            text("DELETE FROM raw.cache_drop_audit WHERE reason = 'copia_del_colector_indec'")
        )


@pytest.fixture
def libro_servido(monkeypatch):
    """Todo `httpx.Client` responde con el libro: el del INDEC y el del colector."""
    contenido = _libro()

    def responder(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=contenido,
            headers={
                "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "content-length": str(len(contenido)),
            },
        )

    cliente_real = httpx.Client

    def cliente(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(responder)
        return cliente_real(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", cliente)
    # Celery sin broker: el embedding y el aviso a los marts no importan acá.
    monkeypatch.setattr(st.index_dataset_embedding, "delay", lambda *a, **k: None)
    monkeypatch.setattr(indec_tasks, "INDEC_DATASETS", [_DATASET])
    return contenido


def _primer_valor(engine, tabla: str) -> float:
    with engine.connect() as conn:
        columna = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'raw' AND table_name = :t ORDER BY ordinal_position LIMIT 1"
            ),
            {"t": tabla},
        ).scalar()
        return float(
            conn.execute(
                text(
                    f'SELECT valor FROM raw."{tabla}" WHERE trim("{columna}") = \'Nivel general\' '
                    "ORDER BY periodo DESC LIMIT 1"
                )
            ).scalar()
        )


def _datasets_de_prueba(engine) -> list[str]:
    with engine.connect() as conn:
        return list(
            conn.execute(
                text(
                    "SELECT CAST(id AS text) FROM datasets WHERE portal = 'indec' "
                    "AND source_id LIKE 'indec-cache_indec_zz_hojas%' ORDER BY source_id"
                )
            ).scalars()
        )


def test_ingest_indec_guarda_cada_hoja_en_su_tabla(base, libro_servido):
    resultado = indec_tasks.ingest_indec.run()
    assert resultado["errors"] == 0 and resultado["ingested"] == 3
    for hoja, tabla in _TABLAS.items():
        assert _primer_valor(base, tabla) == _NIVEL_GENERAL[hoja], hoja
    with base.connect() as conn:
        columnas = "periodo, valor"
        for tabla in (_TABLAS[_INTERANUAL], _TABLAS[_INDICES]):
            distintas = conn.execute(
                text(
                    f'SELECT count(*) FROM (SELECT {columnas} FROM raw."{tabla}" '
                    f'EXCEPT SELECT {columnas} FROM raw."{_TABLAS[_MENSUAL]}") x'
                )
            ).scalar()
            assert distintas > 0, tabla
        # La fila propia es lo que `retirar_copias` espera para retirar la copia.
        filas = conn.execute(
            text("SELECT table_name, status FROM raw.cached_datasets WHERE table_name = ANY(:t)"),
            {"t": list(_TABLAS.values())},
        ).all()
        assert {tuple(f) for f in filas} == {(t, "ready") for t in _TABLAS.values()}


def test_el_colector_no_pisa_las_hojas(base, libro_servido):
    indec_tasks.ingest_indec.run()
    antes = _tablas_indec(base)
    ids = _datasets_de_prueba(base)
    assert len(ids) == 3
    for dataset_id in ids:
        assert collector_tasks.collect_dataset.run(dataset_id) == {"skipped": "carga_propia"}
    assert _tablas_indec(base) == antes
    assert not [t for t in antes if t.startswith("indec__")]
    for hoja, tabla in _TABLAS.items():
        assert _primer_valor(base, tabla) == _NIVEL_GENERAL[hoja], hoja


def test_sin_la_guarda_el_colector_guarda_la_primera_hoja_en_todas(
    base, libro_servido, monkeypatch
):
    """La causa: lo que hacía el colector con estos datasets antes de la guarda."""
    indec_tasks.ingest_indec.run()
    monkeypatch.setattr(collector_tasks, "tiene_carga_propia", lambda _portal: False)
    monkeypatch.setenv("OPENARG_USE_RAW_LAYER", "1")
    ids = _datasets_de_prueba(base)
    for dataset_id in ids:
        collector_tasks.collect_dataset.run(dataset_id)
    with base.connect() as conn:
        copias = (
            conn.execute(
                text(
                    "SELECT cd.table_name FROM raw.cached_datasets cd "
                    "WHERE CAST(cd.dataset_id AS text) = ANY(:ids) AND cd.table_name LIKE 'indec\\_\\_%' ESCAPE '\\'"
                ),
                {"ids": ids},
            )
            .scalars()
            .all()
        )
    assert len(copias) == 3
    for tabla in copias:
        assert _primer_valor(base, tabla) == _NIVEL_GENERAL[_MENSUAL], tabla


# ── el retiro ────────────────────────────────────────────────────────────────


def _dataset(conn, source_id: str) -> str:
    return str(
        conn.execute(
            text(
                "INSERT INTO datasets (source_id, title, organization, portal, url, download_url, "
                "format, columns, tags, is_cached) VALUES (:s, :titulo, 'x', 'indec', :url, :descarga, 'xlsx', "
                "'[]', '', true) RETURNING CAST(id AS text)"
            ),
            {"s": source_id, "titulo": source_id, "url": _URL, "descarga": _URL},
        ).scalar()
    )


def _recurso(conn, dataset_id: str, source_id: str, tabla: str) -> None:
    conn.execute(
        text(
            """
            INSERT INTO catalog_resources (
                resource_identity, dataset_id, portal, source_id, canonical_title, display_name,
                title_source, title_confidence, resource_kind, materialization_status,
                materialized_table_name
            ) VALUES (
                :r, CAST(:d AS uuid), 'indec', :s, :titulo, :nombre, 'fallback', 0.1, 'file', 'ready', :t
            )
            """
        ),
        {
            "r": f"indec::{source_id}",
            "d": dataset_id,
            "s": source_id,
            "titulo": source_id,
            "nombre": source_id,
            "t": tabla,
        },
    )


def _fila_catalogo(
    conn, dataset_id: str, tabla: str, estado: str, error: str | None = None
) -> None:
    conn.execute(
        text(
            "INSERT INTO raw.cached_datasets (dataset_id, table_name, status, error_message, row_count) "
            "VALUES (CAST(:d AS uuid), :t, :e, :m, 1)"
        ),
        {"d": dataset_id, "t": tabla, "e": estado, "m": error},
    )


def _version(conn, tabla: str) -> None:
    conn.execute(
        text(
            "INSERT INTO public.raw_table_versions (resource_identity, version, schema_name, table_name) "
            "VALUES (:r, 1, 'raw', :t)"
        ),
        {"r": f"zz::{tabla}", "t": tabla},
    )


def _crear(conn, tabla: str, filas: int = 1) -> None:
    conn.execute(text(f'CREATE TABLE raw."{tabla}" (x int)'))
    conn.execute(text(f'INSERT INTO raw."{tabla}" SELECT generate_series(1, {filas})'))


@pytest.fixture
def copias(base):
    """Las cuatro situaciones de staging, armadas a mano."""
    ids: dict[str, str] = {}
    with base.begin() as conn:
        # Una hoja cuya tabla pasó la puerta: hoy publica la primera hoja.
        ids["lista"] = _dataset(conn, "indec-cache_indec_zz_hojas_lista")
        _crear(conn, "indec__zz_lista__v1")
        _crear(conn, "cache_indec_zz_hojas_lista", 7)
        _fila_catalogo(conn, ids["lista"], "indec__zz_lista__v1", "ready")
        _fila_catalogo(conn, ids["lista"], "cache_indec_zz_hojas_lista", "ready")
        _version(conn, "indec__zz_lista__v1")
        _recurso(conn, ids["lista"], "indec-cache_indec_zz_hojas_lista", "indec__zz_lista__v1")
        # Una hoja que la puerta rechaza: se queda sin publicar.
        ids["rechazada"] = _dataset(conn, "indec-cache_indec_zz_hojas_rechazada")
        _crear(conn, "indec__zz_rechazada__v1")
        _crear(conn, "cache_indec_zz_hojas_rechazada")
        _fila_catalogo(conn, ids["rechazada"], "indec__zz_rechazada__v1", "ready")
        _fila_catalogo(
            conn,
            ids["rechazada"],
            "cache_indec_zz_hojas_rechazada",
            "error",
            "ingestion_validation_failed:placeholder_headers",
        )
        _version(conn, "indec__zz_rechazada__v1")
        _recurso(
            conn,
            ids["rechazada"],
            "indec-cache_indec_zz_hojas_rechazada",
            "indec__zz_rechazada__v1",
        )
        # Su tabla existe pero `ingest_indec` todavía no la registró (el IPC, hoy).
        ids["sin_fila"] = _dataset(conn, "indec-cache_indec_zz_hojas_sin_fila")
        _crear(conn, "indec__zz_sin_fila__v1")
        _crear(conn, "cache_indec_zz_hojas_sin_fila")
        _fila_catalogo(conn, ids["sin_fila"], "indec__zz_sin_fila__v1", "ready")
        _recurso(
            conn, ids["sin_fila"], "indec-cache_indec_zz_hojas_sin_fila", "indec__zz_sin_fila__v1"
        )
        # Una versión vieja, sin fila en el catálogo.
        _crear(conn, "indec__zz_vieja__v1")
    return ids


def _existe(engine, tabla: str) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text("SELECT to_regclass(:q) IS NOT NULL"), {"q": f'raw."{tabla}"'}
            ).scalar()
        )


def test_la_simulacion_lista_y_no_toca_nada(base, copias):
    resumen = indec_tasks.retirar_copias(base, dry_run=True)
    borraria = " ".join(resumen["borraria"])
    assert "indec__zz_lista__v1" in borraria and "indec__zz_vieja__v1" in borraria
    assert "indec__zz_sin_fila__v1" not in borraria
    assert any("primero correr ingest_indec" in e for e in resumen["esperan"])
    assert resumen["sin_publicar"] == ["cache_indec_zz_hojas_rechazada"]
    assert _existe(base, "indec__zz_lista__v1")


def test_el_retiro_deja_al_catalogo_en_la_hoja_propia(base, copias):
    resumen = indec_tasks.retirar_copias(base, dry_run=False)
    assert resumen["borradas"] == 3 and resumen["fallidas"] == []
    assert not _existe(base, "indec__zz_lista__v1") and not _existe(base, "indec__zz_vieja__v1")
    assert _existe(base, "cache_indec_zz_hojas_lista")
    with base.connect() as conn:
        recurso = conn.execute(
            text(
                "SELECT materialized_table_name, materialization_status FROM catalog_resources "
                "WHERE resource_identity = 'indec::indec-cache_indec_zz_hojas_lista'"
            )
        ).one()
        assert recurso.materialized_table_name == 'raw."cache_indec_zz_hojas_lista"'
        assert recurso.materialization_status == "ready"
        assert conn.execute(
            text("SELECT is_cached, row_count FROM datasets WHERE CAST(id AS text) = :d"),
            {"d": copias["lista"]},
        ).one() == (True, 7)
        assert (
            conn.execute(
                text(
                    "SELECT superseded_at IS NOT NULL FROM public.raw_table_versions WHERE table_name = 'indec__zz_lista__v1'"
                )
            ).scalar()
            is True
        )
        assert (
            conn.execute(
                text(
                    "SELECT count(*) FROM raw.cached_datasets WHERE table_name LIKE 'indec\\_\\_zz\\_lista%' ESCAPE '\\'"
                )
            ).scalar()
            == 0
        )
        assert (
            conn.execute(
                text(
                    "SELECT count(*) FROM raw.cache_drop_audit WHERE reason = 'copia_del_colector_indec'"
                )
            ).scalar()
            == 3
        )


def test_una_hoja_rechazada_no_queda_publicada(base, copias):
    indec_tasks.retirar_copias(base, dry_run=False)
    assert not _existe(base, "indec__zz_rechazada__v1")
    with base.connect() as conn:
        recurso = conn.execute(
            text(
                "SELECT materialized_table_name, materialization_status FROM catalog_resources "
                "WHERE resource_identity = 'indec::indec-cache_indec_zz_hojas_rechazada'"
            )
        ).one()
        assert recurso.materialization_status != "ready"
        assert recurso.materialized_table_name == 'raw."cache_indec_zz_hojas_rechazada"'
        assert (
            conn.execute(
                text("SELECT is_cached FROM datasets WHERE CAST(id AS text) = :d"),
                {"d": copias["rechazada"]},
            ).scalar()
            is False
        )


def test_sin_la_fila_propia_se_queda_como_esta(base, copias):
    indec_tasks.retirar_copias(base, dry_run=False)
    assert _existe(base, "indec__zz_sin_fila__v1")
    with base.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT materialized_table_name FROM catalog_resources "
                    "WHERE resource_identity = 'indec::indec-cache_indec_zz_hojas_sin_fila'"
                )
            ).scalar()
            == "indec__zz_sin_fila__v1"
        )


def test_una_copia_que_lee_un_mart_espera_y_no_se_audita(base, copias):
    with base.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))
        conn.execute(
            text(
                'CREATE MATERIALIZED VIEW mart.zz_lee_copia AS SELECT x FROM raw."indec__zz_lista__v1"'
            )
        )
    resumen = indec_tasks.retirar_copias(base, dry_run=False)
    assert resumen["fallidas"] == []
    assert any("indec__zz_lista__v1: la lee zz_lee_copia" in e for e in resumen["esperan"])
    assert _existe(base, "indec__zz_lista__v1")
    with base.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT count(*) FROM raw.cache_drop_audit WHERE reason = 'copia_del_colector_indec' "
                    "AND object_name = 'raw.indec__zz_lista__v1'"
                )
            ).scalar()
            == 0
        )


# ── el mart que leía una copia ───────────────────────────────────────────────

_CUADRO_1 = "cache_indec_pobreza_informe_cuadro_1"


@pytest.fixture
def mart_pobreza(base):
    with base.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{_CUADRO_1}" CASCADE'))
        conn.execute(
            text(
                f'''CREATE TABLE raw."{_CUADRO_1}" ("Indicador" text, "2do. semestre 2016" double precision,
                "1er. semestre 2026*" double precision)'''
            )
        )
        conn.execute(
            text(
                f'''INSERT INTO raw."{_CUADRO_1}" VALUES ('Pobreza', NULL, NULL), ('Hogares', 21.5, 24.4),
                ('Personas', 30.3, 32.3), ('Indigencia', NULL, NULL), ('Hogares', 4.5, 5.6),
                ('Personas', 6.1, 7.5), ('Fuente: INDEC.', NULL, NULL)'''
            )
        )
        conn.execute(
            text(
                "INSERT INTO public.raw_table_versions (resource_identity, version, schema_name, table_name) "
                "VALUES ('indec::indec-cache_indec_pobreza_informe_cuadro_1', 1, 'raw', :t)"
            ),
            {"t": _CUADRO_1},
        )
    yield base
    with base.begin() as conn:
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS mart.pobreza_indec_aglomerados"))
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{_CUADRO_1}" CASCADE'))
        conn.execute(
            text("DELETE FROM public.raw_table_versions WHERE table_name = :t"), {"t": _CUADRO_1}
        )
        conn.execute(
            text("DELETE FROM mart_definitions WHERE mart_id = 'pobreza_indec_aglomerados'")
        )


def test_el_mart_de_pobreza_lee_el_cuadro_1(mart_pobreza, monkeypatch):
    from app.infrastructure.celery.tasks import mart_tasks

    monkeypatch.setattr(mart_tasks, "_embed_mart_definition", lambda *a, **k: None, raising=False)
    resultado = mart_tasks.build_mart.run("pobreza_indec_aglomerados")
    assert resultado.get("error") is None, resultado
    with mart_pobreza.connect() as conn:
        filas = conn.execute(
            text(
                "SELECT indicador, periodo, valor FROM mart.pobreza_indec_aglomerados "
                "ORDER BY indicador, periodo, valor"
            )
        ).all()
    assert [(i, p, float(v)) for i, p, v in filas] == [
        ("Hogares", "1er. semestre 2026", 5.6),
        ("Hogares", "1er. semestre 2026", 24.4),
        ("Hogares", "2do. semestre 2016", 4.5),
        ("Hogares", "2do. semestre 2016", 21.5),
        ("Personas", "1er. semestre 2026", 7.5),
        ("Personas", "1er. semestre 2026", 32.3),
        ("Personas", "2do. semestre 2016", 6.1),
        ("Personas", "2do. semestre 2016", 30.3),
    ]
