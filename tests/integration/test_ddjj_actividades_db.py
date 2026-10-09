"""DDJJ de actividades anteriores y posteriores contra Postgres de verdad.

Se prueba lo que vive en la base:

- la carga, con deduplicación, reemplazo y registro (`cached_datasets` en
  `ready`, `raw_table_versions`);
- el guardián, que frena si desaparece un momento;
- la consulta del adaptador por persona, por entidad (con alias: AFIP encuentra
  ARCA) y por momento.
"""

from __future__ import annotations

import csv
import io
import os
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from app.application.ddjj import actividades as act
from app.infrastructure.adapters.connectors import ddjj_adapter as da
from app.infrastructure.celery.tasks import _db
from app.infrastructure.celery.tasks import ddjj_tasks as dt

SI = "Sin información"
_ANT = [
    "documento_FODYW_fecha_hora_creacion", "documento_FODYW_nro_documento", "tramite_tipo",
    "apellidos_nombres", "cuit_cuil", "cargo_actual", "cargo_descripcion", "cargo_jurisdiccion",
    "cargo_fecha_inicio", "relacion_dependencia_empleador", "relacion_dependencia_empleador_cuit",
    "relacion_dependencia_sector", "relacion_dependencia_fecha_inicio",
    "relacion_dependencia_continuidad", "relacion_dependencia_fecha_cese",
    "ultimo_puesto_ocupado_descripcion",
]  # fmt: skip
_POS = [
    "documento_FOJWP_fecha_hora_creacion", "documento_FOJWP_nro_documento", "tramite_tipo",
    "apellidos_nombres", "cuit_cuil", "cargo_cese", "cargo_descripcion", "cargo_jurisdiccion",
    "cargo_fecha_cese", "relacion_dependencia_empleador", "relacion_dependencia_empleador_cuit",
    "relacion_dependencia_sector", "relacion_dependencia_fecha_inicio", "puesto_actual_descripcion",
]  # fmt: skip


def _csv(columnas: list[str], filas: list[list[str]]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(columnas)
    w.writerows(filas)
    return buf.getvalue()


def _ant(doc: str, fecha: str, nombre: str, cuit: str, empleador: str, desde: str) -> list[str]:
    return [
        fecha, doc, "GENE00588 - Inscripción", nombre, cuit, "Subsecretario/a", SI,
        "Ministerio de Economía", "2024-01-10", empleador, "30500000001",
        "Minero, petrolero y energético", desde, "No", "2023-12-31", "Gerente",
    ]  # fmt: skip


def _archivos(con_posteriores: bool = True) -> list[act.Archivo]:
    anteriores = _csv(
        _ANT,
        [
            _ant(
                "DOC-1",
                "2024-01-15 10:00:00",
                "Perez Juan",
                "20000000001",
                "YPF S.A.",
                "2015-03-01",
            ),
            # La misma actividad en una actualización: queda una.
            _ant(
                "DOC-2",
                "2025-02-01 10:00:00",
                "Perez Juan",
                "20000000001",
                "YPF S.A.",
                "2015-03-01",
            ),
            _ant("DOC-3", "2024-02-01 10:00:00", "Gomez Ana", "27000000002", "AFIP", "2005-05-01"),
            _ant("DOC-4", "2024-03-01 10:00:00", "Diaz Pedro", "20000000003", "ARCA", "2018-01-01"),
        ],
    )
    archivos = [act.Archivo(act.ANTERIOR, anteriores, "https://x/anteriores.csv")]
    if con_posteriores:
        posteriores = _csv(
            _POS,
            [
                [
                    "2025-12-20 09:00:00",
                    "DOC-9",
                    "GENE00590 - Inscripción al egreso",
                    "Perez Juan",
                    "20000000001",
                    "Subsecretario/a",
                    SI,
                    "Ministerio de Economía",
                    "2025-12-10",
                    "Techint S.A.",
                    "30500000009",
                    "Industria manufacturera",
                    "2026-01-05",
                    "Director",
                ]
            ],  # fmt: skip
        )
        archivos.append(act.Archivo(act.POSTERIOR, posteriores, "https://x/posteriores.csv"))
    return archivos


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


def _limpiar(engine) -> None:
    with engine.begin() as conn:
        for sufijo in ("", "__nueva", "__previa"):
            conn.execute(text(f'DROP TABLE IF EXISTS raw."{dt.TABLA_ACTIVIDADES}{sufijo}"'))
        conn.execute(
            text("DELETE FROM public.raw_table_versions WHERE table_name LIKE 'cache_ddjj_%'")
        )
        conn.execute(text("DELETE FROM raw.cached_datasets WHERE table_name LIKE 'cache_ddjj_%'"))
        conn.execute(text("DELETE FROM datasets WHERE portal = :p"), {"p": dt.PORTAL})


@pytest.fixture
def engine(monkeypatch):
    engine = _engine_or_skip()
    monkeypatch.setattr(_db, "_trigger_marts_for_portal", lambda *a, **k: None)
    from app.infrastructure.celery.tasks import scraper_tasks

    monkeypatch.setattr(scraper_tasks, "index_dataset_embedding", MagicMock())
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        if not conn.execute(text("SELECT to_regclass('raw.cached_datasets') IS NOT NULL")).scalar():
            conn.execute(
                text(
                    "CREATE TABLE raw.cached_datasets (LIKE public.cached_datasets "
                    "INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"
                )
            )
    _limpiar(engine)
    yield engine
    _limpiar(engine)


def test_carga_deduplica_y_registra(engine):
    resumen = dt.cargar_actividades(engine, _archivos(), permitir_menos_filas=False)
    assert resumen["filas_leidas"] == 5 and resumen["repetidas"] == 1
    assert resumen["por_momento"] == {"anterior": 3, "posterior": 1}
    with engine.connect() as conn:
        estado = conn.execute(
            text("SELECT status FROM raw.cached_datasets WHERE table_name = :t"),
            {"t": dt.TABLA_ACTIVIDADES},
        ).scalar()
        registro = conn.execute(
            text("SELECT resource_identity FROM public.raw_table_versions WHERE table_name = :t"),
            {"t": dt.TABLA_ACTIVIDADES},
        ).scalar()
        url = conn.execute(
            text("SELECT url FROM datasets WHERE source_id = 'ddjj-actividades'")
        ).scalar()
        documento = conn.execute(
            text(f"SELECT documento FROM raw.\"{dt.TABLA_ACTIVIDADES}\" WHERE entidad = 'YPF S.A.'")
        ).scalar()
    assert estado == "ready" and registro == "ddjj::ddjj-actividades"
    assert url == act.URL_DATASET
    assert documento == "DOC-2"  # la del documento más nuevo


def test_el_guardian_no_deja_perder_las_posteriores(engine):
    dt.cargar_actividades(engine, _archivos(), permitir_menos_filas=False)
    with pytest.raises(dt._Rechazo, match="desaparecen las actividades posterior"):
        dt.cargar_actividades(engine, _archivos(con_posteriores=False), permitir_menos_filas=False)


async def test_consultas_del_adaptador(engine):
    dt.cargar_actividades(engine, _archivos(), permitir_menos_filas=False)
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    async_engine = create_async_engine(os.environ["DATABASE_URL"].replace("+asyncpg", "+psycopg"))
    ddjj = da.DDJJAdapter(async_sessionmaker(async_engine, expire_on_commit=False))
    try:
        perez = await ddjj.actividades(persona="juan pérez")
        assert {(r["momento"], r["entidad"]) for r in perez.records} == {
            ("antes de asumir", "YPF S.A."),
            ("al irse", "Techint S.A."),
        }
        assert perez.metadata["personas"] == 1
        assert "no dice nada sobre conflictos de interés" in perez.metadata["description"]
        # AFIP encuentra también a quien escribió ARCA.
        afip = await ddjj.actividades(entidad="AFIP")
        assert sorted(r["nombre"] for r in afip.records) == ["DIAZ PEDRO", "GOMEZ ANA"]
        despues = await ddjj.actividades(entidad="techint", momento="posterior")
        assert [r["nombre"] for r in despues.records] == ["PEREZ JUAN"]
        antes = await ddjj.actividades(entidad="techint", momento="anterior")
        assert antes.records == []
        # Por el cargo público: quiénes dejaron Economía y adónde fueron.
        dejaron = await ddjj.actividades(organismo="economía", momento="posterior")
        assert [(r["nombre"], r["entidad"]) for r in dejaron.records] == [
            ("PEREZ JUAN", "Techint S.A.")
        ]
        assert (await ddjj.actividades(organismo="ANSES")).records == []
        con_cargo = await ddjj.actividades(organismo="economia", cargo="subsecretario")
        assert con_cargo.metadata["personas"] == 3
        # Con tope de filas, el total sigue siendo el real y va al principio.
        topadas = await ddjj.actividades(organismo="economia", limite=2)
        assert len(topadas.records) == 2 and topadas.metadata["total_records"] == 4
        assert topadas.metadata["description"].startswith(
            "4 actividades de 3 personas coinciden (se muestran 2)."
        )
        assert (await ddjj.actividades(entidad="YPF")).records[0]["tipo_actividad"] == (
            "empleo en relación de dependencia"
        )
    finally:
        await async_engine.dispose()
