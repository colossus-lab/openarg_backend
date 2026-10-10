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


# ── CABA ─────────────────────────────────────────────────────────────────────

_CABA_ENCABEZADO = (
    "id_ddjj,anio_presentacion,nombre,apellido,id_cargo,cargo,total_bienes_muebles,"
    "total_bienes_inmuebles,total_acciones,total_fondos,total_bonos,total_titulos,"
    "total_dinero_efectivo,total_dinero_electronico,fecha_presentacion\r\n"
)


def _caba(tmp_path, anios: dict[int, list[str]]) -> list:
    from app.application.ddjj import caba

    archivos = []
    for anio, filas in anios.items():
        ruta = tmp_path / f"caba-{anio}.csv"
        ruta.write_text(_CABA_ENCABEZADO + "".join(f + "\r\n" for f in filas), encoding="utf-8")
        archivos.append(
            caba.ArchivoCaba(
                ruta=str(ruta),
                url=f"https://cdn.buenosaires.gob.ar/x/declaraciones-juradas-{anio}.csv",
                corte=None,
            )
        )
    largo = tmp_path / "caba-2021.csv"
    largo.write_text("informacion,tipo_de_dato,valor,presentacion,periodo\r\n", encoding="utf-8")
    archivos.append(
        caba.ArchivoCaba(
            ruta=str(largo),
            url="https://cdn.buenosaires.gob.ar/x/declaraciones-juradas-2021.csv",
            corte=None,
        )
    )
    return archivos


def _filas_caba(anio: int, base: int, n: int) -> list[str]:
    # n declaraciones de $1.000.000 y una imposible (más de 1.000 veces la mediana).
    filas = [
        f"{base + i},{anio},Nombre {i},APELLIDO,1,Director/A General,0,1000000,0,0,0,0,0,0,{anio}-03-01"
        for i in range(n)
    ]
    filas.append(
        f"{base + n},{anio},Imposible,APELLIDO,1,Controlador/A De Faltas,0,20000000000000,0,0,0,0,0,0,{anio}-03-01"
    )
    return filas


def test_caba_conserva_la_oa_y_marca_lo_imposible(engine, tmp_path):
    dt.cargar_oa(engine, dt.armar_plan(_archivos(tmp_path)), permitir_menos_filas=False)
    otra = tmp_path / "caba"
    otra.mkdir()

    resumen = dt.cargar_caba(
        engine,
        _caba(otra, {2025: _filas_caba(2025, 100, 5), 2026: _filas_caba(2026, 200, 5)}),
        permitir_menos_filas=False,
    )

    assert resumen["declaraciones_por_anio"] == {2025: 6, 2026: 6}
    assert resumen["otras_fuentes_conservadas"] == 6
    assert resumen["inconsistentes"] == 2
    assert resumen["salteados"] == [
        "declaraciones-juradas-2021.csv: formato largo (campo/valor), queda para después"
    ]
    with engine.connect() as conn:
        caba_rows = conn.execute(
            text(
                f"SELECT dj_id, bienes, patrimonio, inconsistente, poder FROM "
                f"raw.\"{dt.TABLA_DECLARACIONES}\" WHERE fuente = 'caba' ORDER BY dj_id"
            )
        ).all()
        oa_rows = conn.execute(
            text(
                f'SELECT count(*) FROM raw."{dt.TABLA_DECLARACIONES}" '
                "WHERE fuente = 'oficina_anticorrupcion'"
            )
        ).scalar()
        bienes_oa = conn.execute(text(f'SELECT count(*) FROM raw."{dt.TABLA_BIENES}"')).scalar()
        ds = conn.execute(
            text(
                "SELECT organization, description FROM datasets "
                "WHERE source_id = 'ddjj-declaraciones'"
            )
        ).one()
    assert oa_rows == 6 and bienes_oa == 4  # el detalle de la OA no se toca
    assert all(r.patrimonio is None and r.poder == "ejecutivo" for r in caba_rows)
    assert [r.dj_id for r in caba_rows if r.inconsistente] == [105, 205]
    assert ds.organization == "Ciudad de Buenos Aires y Oficina Anticorrupción"
    assert "Ciudad de Buenos Aires, años 2025–2026 (12 declaraciones)" in ds.description
    assert "sin deudas" in ds.description

    # Y la OA, cargada otra vez, conserva a CABA.
    resumen_oa = dt.cargar_oa(
        engine, dt.armar_plan(_archivos(tmp_path)), permitir_menos_filas=False
    )
    assert resumen_oa["otras_fuentes_conservadas"] == 12


def test_caba_guardian(engine, tmp_path):
    dt.cargar_caba(
        engine,
        _caba(tmp_path, {2025: _filas_caba(2025, 100, 5), 2026: _filas_caba(2026, 200, 5)}),
        permitir_menos_filas=False,
    )
    otra = tmp_path / "b"
    otra.mkdir()
    with pytest.raises(dt._Rechazo, match="desaparecen los años 2025"):
        dt.cargar_caba(
            engine, _caba(otra, {2026: _filas_caba(2026, 200, 5)}), permitir_menos_filas=False
        )


# ── inverosímiles y altos cargos (10-oct-2026) ──────────────────────────────

_PRINCIPAL_COMPLETO = [*_PRINCIPAL, "ingresos_no_alcanzados"]


def _dj_real(
    dj_id,
    nombre,
    organismo,
    cargo,
    bi,
    bc,
    *,
    tipo="Anual",
    dc="0.00",
    ingresos="0.00",
    no_alcanzados="0.00",
):
    return [
        str(dj_id),
        f"20{dj_id:09d}",
        "2024",
        tipo,
        "0",
        nombre,
        "PUBLICO",
        organismo,
        cargo,
        "202001",
        bi,
        "0.00",
        bc,
        dc,
        ingresos,
        "0.00",
        no_alcanzados,
    ]


def _bien(dj_id, periodo, tipo, descripcion, importe):
    return [
        str(dj_id),
        "",
        "2024",
        periodo,
        tipo,
        descripcion,
        "INGRESOS PROPIOS",
        "100.00",
        importe,
    ]


_CASA = "Tipo: CASA -Destino: CASA HABITACION -Localidad: Cap Fed"
_DEPTO = "Tipo: DEPARTAMENTO -Destino: CASA HABITACION -Localidad: CIUDAD DE BUENOS AIRES"


def _archivos_reales(tmp_path) -> list[oa.Archivo]:
    """Las cifras del ranking «Top 10 ddjj gobierno» de 2024, tal como las publica
    la OA, más diez declaraciones comunes ($30 M) para que la mediana sea real."""
    principal = [
        _dj_real(
            100 + i,
            f"COMUN {i}",
            "MINISTERIO DE SALUD",
            "Analista",
            "25000000.00",
            "30000000.00",
            ingresos="20000000.00",
        )
        for i in range(10)
    ]
    bienes = [
        _bien(100 + i, "C", "DEPOSITO DE DINERO EN EL PAIS", "Caja", "30000000.00")
        for i in range(10)
    ]
    bienes += [
        _bien(100 + i, "I", "DEPOSITO DE DINERO EN EL PAIS", "Caja", "25000000.00")
        for i in range(10)
    ]
    principal += [
        # Inicial: una casa en Capital de $250.000 M.
        _dj_real(
            1,
            "FLORES ALDO JAVIER",
            "FUERZA AEREA ARGENTINA",
            "Jefe Diseño Grafico",
            "250007000000.00",
            "0.00",
            tipo="Inicial",
        ),
        # Un departamento de $100.000 M al inicio y al cierre; cobra $28 M.
        _dj_real(
            2,
            "CASCABELO MARTIN GABRIEL",
            "POLICIA FEDERAL ARGENTINA",
            "SUBCOMISARIO",
            "100029068180.90",
            "100036194159.16",
            ingresos="28001707.69",
        ),
        # De $98 M a $82.597 M en un año (fondos comunes ×1000).
        _dj_real(
            3,
            "RIAL ESTEBAN JORGE",
            "SERVICIO NACIONAL DE SANIDAD",
            "COORD. REGIONAL",
            "98262134.64",
            "82597507132.08",
        ),
        # Lo mismo, con $23 M de ingresos.
        _dj_real(
            4,
            "SETTECASI TAMARA ANDREA",
            "MINISTERIO DE JUSTICIA",
            "Encargada Titular",
            "10618150.40",
            "33692540921.44",
            ingresos="23093665.61",
        ),
        # Real: $1 M de ingresos netos pero $2.711 M no alcanzados por ganancias.
        _dj_real(
            5,
            "DAZA NARBONA JOSE LUIS",
            "MINISTERIO DE ECONOMIA",
            "SECRETARIO DE POLITICA ECONOMICA",
            "18421797421.68",
            "24514308744.54",
            dc="3318730882.32",
            ingresos="1000000.00",
            no_alcanzados="2711515215.66",
        ),
        # Sin organismo: el cargo alcanza para el poder y el alto cargo.
        _dj_real(
            6,
            "CAPUTO LUIS ANDRES",
            "",
            "Ministro de Economia",
            "9000000000.00",
            "11700000000.00",
            ingresos="338600000.00",
        ),
        # Inicial: un departamento en CABA "Destino: OTROS" de $12.000 M (en su
        # baja, el mismo departamento vale $150.000).
        _dj_real(
            7,
            "FERRER MARIA LUJAN",
            "MINISTERIO DE SEGURIDAD",
            "DIRECTORA NACIONAL",
            "12000000000.00",
            "0.00",
            tipo="Inicial",
        ),
        # Un campo de $20.000 M con ingresos acordes: la tierra rural puede valer eso.
        _dj_real(
            8,
            "RURAL CON CAMPO",
            "INSTITUTO NACIONAL DE TECNOLOGIA AGROPECUARIA",
            "Investigador",
            "18000000000.00",
            "20000000000.00",
            ingresos="1000000000.00",
        ),
    ]
    bienes += [
        _bien(
            7, "I", "INMUEBLES EN EL PAIS", "Tipo: DEPARTAMENTO -Destino: OTROS", "12000000000.00"
        ),
        _bien(8, "I", "INMUEBLES EN EL PAIS", "Tipo: RURALES CON VIVIENDA", "18000000000.00"),
        _bien(8, "C", "INMUEBLES EN EL PAIS", "Tipo: RURALES CON VIVIENDA", "20000000000.00"),
        _bien(1, "I", "INMUEBLES EN EL PAIS", _CASA, "250000000000.00"),
        _bien(1, "I", "AUTOMOTORES EN EL PAIS", "Sandero", "7000000.00"),
        _bien(2, "I", "INMUEBLES EN EL PAIS", _DEPTO, "100000000000.00"),
        _bien(2, "I", "DEPOSITO DE DINERO EN EL PAIS", "Caja", "29068180.90"),
        _bien(2, "C", "INMUEBLES EN EL PAIS", _DEPTO, "100000000000.00"),
        _bien(2, "C", "DEPOSITO DE DINERO EN EL PAIS", "Caja", "36194159.16"),
        _bien(3, "I", "DEPOSITO DE DINERO EN EL PAIS", "Caja", "98262134.64"),
        _bien(3, "C", "ACCIONES -FONDOS COMUNES -DE INVERSION", "Galileo", "82502481974.90"),
        _bien(
            3, "C", "INMUEBLES EN EL PAIS", "Tipo: CASA -Destino: CASA HABITACION", "95025157.18"
        ),
        _bien(4, "I", "DEPOSITO DE DINERO EN EL PAIS", "Caja", "10618150.40"),
        _bien(4, "C", "ACCIONES -FONDOS COMUNES -DE INVERSION", "Pionero", "33670175822.67"),
        _bien(4, "C", "AUTOMOTORES EN EL PAIS", "Sandero", "22365098.77"),
        _bien(5, "I", "TITULOS Y ACCIONES EN EL EXTERIOR", "Cartera", "18421797421.68"),
        _bien(
            5,
            "C",
            "INMUEBLES EN EL EXTERIOR",
            "Tipo: DEPARTAMENTO -Localidad: NEW YORK",
            "9600000000.00",
        ),
        _bien(5, "C", "DEPOSITOS DE DINERO EN EL EXTERIOR", "Cuenta", "14914308744.54"),
        _bien(6, "I", "DEPOSITOS DE DINERO EN EL EXTERIOR", "Cuenta", "9000000000.00"),
        _bien(6, "C", "DEPOSITOS DE DINERO EN EL EXTERIOR", "Cuenta", "11700000000.00"),
    ]
    miembros = {
        "declaraciones-juradas-2024-consolidado-al-20250218.csv": _csv(
            _PRINCIPAL_COMPLETO, principal
        ),
        "declaraciones-juradas-bienes-2024-consolidado-al-20250218.csv": _csv(_BIENES, bienes),
        "declaraciones-juradas-deudas-2024-consolidado-al-20250218.csv": _csv(
            _DEUDAS,
            [["5", "", "2024", "C", "PRESTAMO", "BANCO", "ARGENTINA", "Otras", "3318730882.32"]],
        ),
    }
    return _armar_zip(tmp_path, "oa-reales.zip", miembros)


def test_marca_los_montos_inverosimiles_y_los_altos_cargos(engine, tmp_path):
    resumen = dt.cargar_oa(
        engine, dt.armar_plan(_archivos_reales(tmp_path)), permitir_menos_filas=False
    )
    d = _declaraciones(engine)

    # El error está dentro del detalle: el total cierra y H005 no lo ve.
    assert not any(f["inconsistente"] for f in d.values())
    assert d[1]["inverosimil"] == ["inmueble"]  # inicial: sólo el inmueble
    assert d[2]["inverosimil"] == ["inmueble", "ingresos"]
    assert d[3]["inverosimil"] == ["salto"]  # sin ingresos: la regla no aplica
    assert d[4]["inverosimil"] == ["salto", "ingresos"]
    # Daza y Caputo son reales: su patrimonio es pocas veces sus ingresos (el
    # mayor de los declarados, no sólo los netos).
    assert d[5]["inverosimil"] is None and d[6]["inverosimil"] is None
    # Cualquier inmueble en el país, no sólo la casa-habitación…
    assert d[7]["inverosimil"] == ["inmueble"]
    # …salvo los rurales.
    assert d[8]["inverosimil"] is None
    assert all(d[100 + i]["inverosimil"] is None for i in range(10))
    assert resumen["inverosimiles_por_regla"] == {"inmueble": 3, "salto": 2, "ingresos": 2}

    # Altos cargos: el secretario, el ministro sin organismo y la directora
    # nacional, no el resto.
    assert d[5]["alto_cargo"] and d[6]["alto_cargo"] and d[7]["alto_cargo"]
    assert d[6]["poder"] == "ejecutivo"
    assert not any(d[k]["alto_cargo"] for k in (1, 2, 3, 4, 100))
