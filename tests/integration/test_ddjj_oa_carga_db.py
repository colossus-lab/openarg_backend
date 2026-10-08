"""La carga de DDJJ de la OA contra Postgres de verdad.

Lo que se prueba vive en la base:

- el COPY;
- las sumas del detalle y la corrección ×10 en SQL;
- el reemplazo con RENAME de tablas e índices;
- que se conserven las filas de otras fuentes;
- el registro (`datasets`, `cached_datasets` en `ready`, `raw_table_versions`).

Los archivos son ZIP chicos armados acá, con los formatos de los reales.

Las tablas son las de verdad (`raw.cache_ddjj_*`): en CI la base es descartable
y localmente el test se saltea sin `DATABASE_URL`. Al terminar se borra todo lo
que el test escribió.
"""

from __future__ import annotations

import csv
import io
import zipfile
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from app.application.ddjj import oficina_anticorrupcion as oa
from app.infrastructure.celery.tasks import _db
from app.infrastructure.celery.tasks import ddjj_tasks as dt

_PRINCIPAL = [
    "dj_id",
    "cuit",
    "anio",
    "tipo_declaracion_jurada_descripcion",
    "rectificativa",
    "funcionario_apellido_nombre",
    "sector",
    "organismo",
    "cargo",
    "desde",
    "total_bienes_inicio",
    "deudas_inicio",
    "total_bienes_final",
    "total_deudas_final",
    "ingresos_neto_gastos",
    "gastos_personales",
]
_BIENES = [
    "dj_id",
    "cuit",
    "anio",
    "periodo_inicio_cierre",
    "bien_tipo",
    "bien_descripcion",
    " bien_origen_fondos",
    " bien_titularidad",
    " bien_importe",
]
_DEUDAS = [
    "dj_id",
    "cuit",
    "anio",
    "periodo_inicio_cierre",
    "deuda_tipo",
    " deuda_descripcion",
    " deuda_radicacion_localizacion",
    " deuda_clasificacion",
    " deuda_importe",
]


def _csv(columnas: list[str], filas: list[list[str]]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(columnas)
    w.writerows(filas)
    return ("﻿" + buf.getvalue()).encode("utf-8")


def _dj(dj_id, anio, nombre, organismo, bi, di, bc, dc, tipo="Anual", ingresos="0", gastos="0"):
    return [
        str(dj_id),
        f"20{dj_id:09d}",
        str(anio),
        tipo,
        "0",
        nombre,
        "PUBLICO",
        organismo,
        "Cargo",
        "202001",
        bi,
        di,
        bc,
        dc,
        ingresos,
        gastos,
    ]


def _armar_zip(tmp_path, nombre: str, miembros: dict[str, bytes]) -> list[oa.Archivo]:
    ruta = tmp_path / nombre
    with zipfile.ZipFile(ruta, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for n, contenido in miembros.items():
            z.writestr(n, contenido)
    return oa.archivos_del_zip(str(ruta), f"https://datos.jus.gob.ar/x/{nombre}")[0]


_HCDN = "HONORABLE CAMARA DE DIPUTADOS DE LA NACION"


def _archivos(tmp_path, *, sin_2023: bool = False) -> list[oa.Archivo]:
    principal_2024 = [
        # 1: sana, total igual al detalle.
        _dj(1, 2024, "ALFA", _HCDN, "100.00", "0.00", "300.00", "50.00"),
        # 2: total al cierre ×10 contra el detalle (el error del corte 20251222).
        _dj(2, 2024, "BETA", "ARCA", "100.00", "0.00", "2000.00", "0.00"),
        # 3: el total no cierra con su detalle por 50 veces: inconsistente.
        _dj(3, 2024, "GAMMA", "", "10.00", "0.00", "5000.00", "0.00"),
        # 4: inicial, lo declarado está al inicio.
        _dj(4, 2024, "DELTA", _HCDN, "700.00", "200.00", "0.00", "0.00", tipo="Inicial"),
        # 5: ingresos imposibles para su patrimonio.
        _dj(5, 2024, "EPSILON", "ARCA", "10.00", "0.00", "10.00", "0.00", ingresos="5000.00"),
        # basura del suelto real
        [".16", "616985.05", "0.00", "0.00", "", "", "", "", "", "", "", "", "", "", "", ""],
    ]
    bienes_2024 = [
        [
            "1",
            "",
            "2024",
            "C",
            "INMUEBLES EN EL PAIS",
            "Casa",
            "INGRESOS PROPIOS",
            "100.00",
            "300.00",
        ],
        [
            "2",
            "",
            "2024",
            "C",
            "DEPOSITO DE DINERO EN EL PAIS",
            "Caja",
            "INGRESOS PROPIOS",
            "",
            "200.00",
        ],
        [
            "3",
            "",
            "2024",
            "C",
            "AUTOMOTORES EN EL PAIS",
            "Auto",
            "INGRESOS PROPIOS",
            "100.00",
            "100.00",
        ],
        [
            "4",
            "",
            "2024",
            "I",
            "INMUEBLES EN EL PAIS",
            "Depto",
            "INGRESOS PROPIOS",
            "100.00",
            "700.00",
        ],
        # Detalle de una declaración que no está en el principal: huérfano.
        [
            "99",
            "",
            "2024",
            "C",
            "AUTOMOTORES EN EL PAIS",
            "Auto",
            "INGRESOS PROPIOS",
            "100.00",
            "1.00",
        ],
    ]
    deudas_2024 = [
        ["1", "", "2024", "C", "HIPOTECARIO", "BANCO", "ARGENTINA", "Hipotecas", "50.00"],
    ]
    miembros = {
        "declaraciones-juradas-2024-consolidado-al-20250218.csv": _csv(_PRINCIPAL, principal_2024),
        "declaraciones-juradas-bienes-2024-consolidado-al-20250218.csv": _csv(_BIENES, bienes_2024),
        "declaraciones-juradas-deudas-2024-consolidado-al-20250218.csv": _csv(_DEUDAS, deudas_2024),
        "declaraciones-juradas-grupo-familiar-2024-consolidado-al-20250218.csv": b"no se lee",
    }
    if not sin_2023:
        miembros["declaraciones-juradas-2023-consolidado-al-20250218.csv"] = _csv(
            _PRINCIPAL,
            [_dj(11, 2023, "ALFA", _HCDN, "90.00", "0.00", "100.00", "0.00")],
        )
    return _armar_zip(tmp_path, "oa.zip", miembros)


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


def _limpiar(engine) -> None:
    with engine.begin() as conn:
        for tabla in (dt.TABLA_DECLARACIONES, dt.TABLA_BIENES, dt.TABLA_DEUDAS):
            for sufijo in ("", "__nueva", "__previa"):
                conn.execute(text(f'DROP TABLE IF EXISTS raw."{tabla}{sufijo}"'))
        conn.execute(
            text("DELETE FROM public.raw_table_versions WHERE table_name LIKE 'cache_ddjj_%'")
        )
        conn.execute(
            text("DELETE FROM public.ingest_heartbeat WHERE resource_identity LIKE 'ddjj::%'")
        ) if conn.execute(text("SELECT to_regclass('public.ingest_heartbeat')")).scalar() else None
        conn.execute(text("DELETE FROM raw.cached_datasets WHERE table_name LIKE 'cache_ddjj_%'"))
        conn.execute(text("DELETE FROM datasets WHERE portal = :p"), {"p": dt.PORTAL})
        conn.execute(text("DELETE FROM public.ddjj_cargas"))


@pytest.fixture
def engine(monkeypatch):
    engine = _engine_or_skip()
    # Sin broker: ni marts ni embeddings salen de acá.
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


def _declaraciones(engine) -> dict[int, dict]:
    with engine.connect() as conn:
        filas = conn.execute(text(f'SELECT * FROM raw."{dt.TABLA_DECLARACIONES}"')).mappings()
        return {int(f["dj_id"]): dict(f) for f in filas}


def test_carga_completa(engine, tmp_path):
    resumen = dt.cargar_oa(engine, dt.armar_plan(_archivos(tmp_path)), permitir_menos_filas=False)

    assert resumen["declaraciones_por_anio"] == {2023: 1, 2024: 5}
    assert resumen["filas_invalidas"] == {2024: 1}
    assert resumen["detalle_huerfano"] == {"bienes": 1}
    assert resumen["bienes"] == 4 and resumen["deudas"] == 1
    assert resumen["corregidas_x10"] == 1

    d = _declaraciones(engine)
    # Sana: lo declarado es el cierre, la variación es cierre menos inicio.
    assert d[1]["poder"] == "legislativo"
    assert d[1]["bienes"] == Decimal("300.00") and d[1]["deudas"] == Decimal("50.00")
    assert d[1]["patrimonio"] == Decimal("250.00")
    assert d[1]["variacion_patrimonial"] == Decimal("150.00")
    assert d[1]["detalle_bienes_cierre"] == Decimal("300.00")
    assert d[1]["detalle_deudas_cierre"] == Decimal("50.00")
    assert not d[1]["inconsistente"] and d[1]["corregido_x10"] is None
    # ×10 contra el detalle: corregido y anotado.
    assert d[2]["bienes_cierre"] == Decimal("200.00")
    assert d[2]["corregido_x10"] == ["bienes_cierre"]
    assert not d[2]["inconsistente"]
    # No cierra con su detalle (50 veces): inconsistente, sin corregir.
    assert d[3]["bienes_cierre"] == Decimal("5000.00") and d[3]["inconsistente"]
    assert d[3]["poder"] == "sin_dato"
    # Inicial: lo declarado es el inicio, sin variación.
    assert d[4]["patrimonio"] == Decimal("500.00") and d[4]["variacion_patrimonial"] is None
    assert d[5]["ingresos_inconsistentes"] and not d[5]["inconsistente"]

    with engine.connect() as conn:
        listas = conn.execute(
            text(
                "SELECT table_name, status FROM raw.cached_datasets "
                "WHERE table_name LIKE 'cache_ddjj_%' ORDER BY 1"
            )
        ).all()
        registradas = (
            conn.execute(
                text(
                    "SELECT resource_identity FROM public.raw_table_versions "
                    "WHERE table_name LIKE 'cache_ddjj_%' ORDER BY 1"
                )
            )
            .scalars()
            .all()
        )
        titulo = conn.execute(
            text("SELECT description FROM datasets WHERE source_id = 'ddjj-declaraciones'")
        ).scalar()
        indices = (
            conn.execute(
                text(
                    "SELECT indexname FROM pg_indexes WHERE schemaname = 'raw' AND tablename = :t"
                ),
                {"t": dt.TABLA_DECLARACIONES},
            )
            .scalars()
            .all()
        )
    assert [s for _, s in listas] == ["ready", "ready", "ready"]
    assert registradas == ["ddjj::ddjj-bienes", "ddjj::ddjj-declaraciones", "ddjj::ddjj-deudas"]
    assert "Oficina Anticorrupción, años 2023–2024 (6 declaraciones)" in titulo
    assert sorted(indices) == sorted(
        f"{dt.TABLA_DECLARACIONES}_{s}" for s in ("pkey", "cuit_idx", "anio_idx")
    )


def test_segunda_carga_deja_previa_y_conserva_otras_fuentes(engine, tmp_path):
    dt.cargar_oa(engine, dt.armar_plan(_archivos(tmp_path)), permitir_menos_filas=False)
    with engine.begin() as conn:
        conn.execute(
            text(
                f'INSERT INTO raw."{dt.TABLA_DECLARACIONES}" '
                "(fuente, dj_id, jurisdiccion, poder, anio, nombre) "
                "VALUES ('caba', 1, 'caba', 'ejecutivo', 2026, 'PORTEÑO')"
            )
        )

    resumen = dt.cargar_oa(engine, dt.armar_plan(_archivos(tmp_path)), permitir_menos_filas=False)

    assert resumen["otras_fuentes_conservadas"] == 1
    with engine.connect() as conn:
        caba = conn.execute(
            text(f"SELECT nombre FROM raw.\"{dt.TABLA_DECLARACIONES}\" WHERE fuente = 'caba'")
        ).scalar()
        previa = conn.execute(
            text(f'SELECT count(*) FROM raw."{dt.TABLA_DECLARACIONES}__previa"')
        ).scalar()
        indices_previa = (
            conn.execute(
                text(
                    "SELECT indexname FROM pg_indexes WHERE schemaname = 'raw' AND tablename = :t"
                ),
                {"t": f"{dt.TABLA_DECLARACIONES}__previa"},
            )
            .scalars()
            .all()
        )
    assert caba == "PORTEÑO"
    assert previa == 7  # las 6 de la OA y la de CABA de la carga anterior
    assert f"{dt.TABLA_DECLARACIONES}__previa_pkey" in indices_previa


def test_el_guardian_no_deja_perder_un_anio(engine, tmp_path):
    dt.cargar_oa(engine, dt.armar_plan(_archivos(tmp_path)), permitir_menos_filas=False)
    otra = tmp_path / "b"
    otra.mkdir()

    with pytest.raises(dt._Rechazo, match="desaparecen los años 2023"):
        dt.cargar_oa(
            engine, dt.armar_plan(_archivos(otra, sin_2023=True)), permitir_menos_filas=False
        )

    # Nada cambió: la viva sigue con 2023 y no quedó ninguna `__nueva`.
    assert {f["anio"] for f in _declaraciones(engine).values()} == {2023, 2024}
    with engine.connect() as conn:
        assert not conn.execute(
            text("SELECT to_regclass(:q) IS NOT NULL"),
            {"q": f'raw."{dt.TABLA_DECLARACIONES}__nueva"'},
        ).scalar()


def test_al_dia_no_baja_nada(engine, tmp_path, monkeypatch):
    dt.cargar_oa(engine, dt.armar_plan(_archivos(tmp_path)), permitir_menos_filas=False)
    recursos = [dt.Recurso("r1", "https://x/oa.zip", "zip", "2026-02-02")]
    dt.anotar_carga(
        engine,
        fuente=oa.FUENTE,
        estado="escrita",
        inicio=__import__("datetime").datetime.now(__import__("datetime").UTC),
        manifiesto_=dt.manifiesto(recursos),
        resumen={},
    )
    monkeypatch.setattr(dt, "consultar_paquete", lambda client: recursos)
    monkeypatch.setattr(dt, "get_sync_engine", lambda: engine)
    bajar = MagicMock(side_effect=AssertionError("no debería bajar"))
    monkeypatch.setattr(dt, "bajar_archivos", bajar)

    assert dt.ingest_ddjj_oa.run() == {"estado": "al_dia"}
    bajar.assert_not_called()
