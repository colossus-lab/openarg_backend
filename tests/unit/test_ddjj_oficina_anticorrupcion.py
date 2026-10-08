"""Lectura de las DDJJ de la Oficina Anticorrupción: montos, filas y elección del corte.

Los casos salen de los archivos reales del 08-oct-2026 (ver el docstring de
`application/ddjj/oficina_anticorrupcion.py`):

- El suelto de 2024 trae los montos con guion decimal y repite filas en
  formato con punto.
- El corte 20251222 tiene ~25 % de los totales multiplicados por 10.
- El detalle de bienes de 2016-2017 viene sin importe.
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import date
from decimal import Decimal

import pytest

from app.application.ddjj import oficina_anticorrupcion as oa
from app.infrastructure.celery.tasks import ddjj_tasks as dt

_ENCABEZADO = (
    "dj_id,cuit,anio,tipo_declaracion_jurada_id,tipo_declaracion_jurada_descripcion,"
    "rectificativa,funcionario_apellido_nombre,sector,organismo,actividad_principal_ambito,"
    "cargo,desde,goza_de_licencia,fecha_inicio_licencia,horas_dedicacion,proveedor_contratista,"
    "total_bienes_inicio,deudas_inicio,total_bienes_final,total_deudas_final,"
    "diferencia_valuacion,ingresos_neto_gastos,ingresos_no_alcanzados,bienes_por_herencia,"
    "importes_deducidos,gastos_no_deducibles,gastos_personales,"
    "ingresos_trabajos_alquileres_rentas,bienes_heredados"
).split(",")


def _principal(filas: list[dict[str, str]]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_ENCABEZADO, lineterminator="\r\n")
    w.writeheader()
    for f in filas:
        w.writerow({k: f.get(k, "") for k in _ENCABEZADO})
    return "﻿" + buf.getvalue()


def _fila(dj_id: int, total: str, **extra: str) -> dict[str, str]:
    return {
        "dj_id": str(dj_id),
        "cuit": f"20{dj_id:09d}",
        "anio": "2024",
        "tipo_declaracion_jurada_descripcion": "Anual",
        "rectificativa": "0",
        "funcionario_apellido_nombre": f"PERSONA {dj_id}",
        "organismo": "HONORABLE CAMARA DE DIPUTADOS DE LA NACION",
        "total_bienes_final": total,
        **extra,
    }


# ── montos ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("texto", "esperado"),
    [
        ("35278884-41", Decimal("35278884.41")),  # guion decimal, el original
        ("-00", Decimal("0.00")),
        ("-0", Decimal("0.0")),
        ("-47", Decimal("0.47")),  # en el formato con guion, 0,47 — no -47
        ("-15036-00", Decimal("-15036.00")),  # negativo con guion
        ("--47", Decimal("-0.47")),
        ("35278884.41", Decimal("35278884.41")),  # formato con punto
        (".27", Decimal("0.27")),
        (".150360.00", Decimal("-150360.00")),  # el signo menos convertido en punto
        ("..47", Decimal("-0.47")),
        ("1234", Decimal("1234")),
        ("", None),
        ("   ", None),
        (None, None),
        ("---", None),
        ("abc", None),
        ("1.2.3", None),
    ],
)
def test_parse_monto(texto, esperado):
    assert oa.parse_monto(texto) == esperado


# ── nombres de archivo ───────────────────────────────────────────────────────


def test_clasificar_los_cuatro_tipos():
    assert oa.clasificar("declaraciones-juradas-2024-consolidado-al-20251222.csv") == (
        "principal",
        2024,
        date(2025, 12, 22),
    )
    assert oa.clasificar("x/declaraciones-juradas-bienes-2016-consolidado-al-20190524.csv") == (
        "bienes",
        2016,
        date(2019, 5, 24),
    )
    assert oa.clasificar("declaraciones-juradas-deudas-2023-consolidado-al-20250218.csv")[0] == (
        "deudas"
    )
    assert oa.clasificar("declaraciones-juradas-grupo-familiar-2022-consolidado-al-20231027.csv")[
        0
    ] == ("grupo-familiar")


def test_clasificar_ignora_lo_que_no_es_consolidado():
    assert oa.clasificar("declaraciones-juradas-2017-altas-y-bajas-al-20170831.csv") is None
    assert oa.clasificar("declaraciones-juradas-2024.zip") is None
    assert oa.clasificar("declaraciones-juradas-2024-consolidado-al-20251399.csv") is None


# ── poder ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("organismo", "cargo", "poder"),
    [
        ("HONORABLE CAMARA DE DIPUTADOS DE LA NACION", None, "legislativo"),
        ("HONORABLE SENADO DE LA NACION", None, "legislativo"),
        ("CORTE SUPREMA DE JUSTICIA DE LA NACION", None, "judicial"),
        ("PODER JUDICIAL CONSEJO DE LA MAGISTRATURA", None, "judicial"),
        ("PODER JUDICIAL MINISTERIO PUBLICO", None, "ministerio_publico"),
        ("MINISTERIO PUBLICO FISCAL DE LA NACION", None, "ministerio_publico"),
        ("ADMINISTRACION FEDERAL DE INGRESOS PUBLICOS", None, "ejecutivo"),
        ("UNIVERSIDAD DE BUENOS AIRES", "Rector", "ejecutivo"),
        # Arjol: organismo vacío, el cargo alcanza.
        ("", "Diputado Nacional", "legislativo"),
        (None, "Senadora Nacional", "legislativo"),
        ("", "Juez de Cámara", "judicial"),
        # El cargo manda cuando nombra una banca: el organismo es otra actividad.
        (
            "CULTIVATE LA BUENA VIDA S.A.",
            "DIPUTADO NACIONAL POR LA PROVINCIA DE CORDOBA",
            "legislativo",
        ),
        ("MUNICIPALIDAD DE MERLO", "diputada nacional", "legislativo"),
        (None, "DIUTADA NACIONAL", "legislativo"),
        (None, "dipurado nacional", "legislativo"),
        # "FISCAL" con organismo no alcanza: puede ser un asesor fiscal de ARCA.
        ("ADMINISTRACION FEDERAL DE INGRESOS PUBLICOS", "Asesor Fiscal", "ejecutivo"),
        ("", "Fiscal General", "ministerio_publico"),
        # Un candidato no ocupa la banca: manda el organismo.
        ("MUNICIPALIDAD DE MERLO", "CANDIDATA A DIPUTADA NACIONAL", "ejecutivo"),
        ("", "CANDIDATO A DIPUTADO NACIONAL", "sin_dato"),
        ("MINISTERIO DE JUSTICIA", "Ministro de Justicia y Derechos Humanos", "ejecutivo"),
        ("", "Director de Compras", "sin_dato"),
        (None, None, "sin_dato"),
    ],
)
def test_poder_de(organismo, cargo, poder):
    assert oa.poder_de(organismo, cargo) == poder


# ── filas ────────────────────────────────────────────────────────────────────

_SUELTO = oa.Archivo(
    tipo="principal",
    anio=2024,
    corte=date(2025, 12, 22),
    nombre="declaraciones-juradas-2024-consolidado-al-20251222.csv",
    origen="/no/importa.csv",
    url="https://datos.jus.gob.ar/x.csv",
)


def test_fila_declaracion_con_guion_decimal():
    fila = oa.fila_declaracion(
        _fila(
            796612,
            "326035240-00",
            total_bienes_inicio="123241800-82",
            deudas_inicio="-00",
            total_deudas_final="-00",
            ingresos_neto_gastos="54066653-64",
            gastos_personales="31473671-39",
            desde="201212",
            organismo="",
            cargo="Diputado Nacional",
        ),
        _SUELTO,
    )
    d = dict(zip(oa.COLUMNAS_DECLARACION, fila, strict=True))
    assert d["fuente"] == "oficina_anticorrupcion"
    assert d["dj_id"] == 796612
    assert d["poder"] == "legislativo"
    assert d["organismo"] is None
    assert d["bienes_cierre"] == Decimal("326035240.00")  # no el ×10 del ZIP
    assert d["bienes_inicio"] == Decimal("123241800.82")
    assert d["deudas_inicio"] == Decimal("0.00")
    assert d["ingresos_netos"] == Decimal("54066653.64")
    assert d["en_funciones_desde"] == date(2012, 12, 1)
    assert d["tipo"] == "Anual"
    assert d["corte"] == date(2025, 12, 22)


@pytest.mark.parametrize(
    "crudo",
    [
        # La fila basura del suelto de 2024: dj_id ".16" y montos en las columnas de texto.
        {"dj_id": ".16", "cuit": "616985.05", "tipo_declaracion_jurada_descripcion": "0.00"},
        {"dj_id": "123", "cuit": "2033"},  # CUIT que no es de 11 cifras
        {"dj_id": "", "cuit": "20123456789"},
    ],
)
def test_fila_declaracion_descarta_basura(crudo):
    assert oa.fila_declaracion(crudo, _SUELTO) is None


def test_sector_numerico_es_basura():
    fila = oa.fila_declaracion(_fila(1, "10.00", sector="0"), _SUELTO)
    assert dict(zip(oa.COLUMNAS_DECLARACION, fila, strict=True))["sector"] is None


def test_fila_bien_y_deuda():
    bien = oa.fila_bien(
        {
            "dj_id": "685256",
            "periodo_inicio_cierre": "C",
            "bien_tipo": "AUTOMOTORES EN EL PAIS",
            "bien_descripcion": "PEUGOT 208",
            "bien_origen_fondos": "INGRESOS PROPIOS",
            "bien_titularidad": "100.00",
            "bien_importe": "4779200.00",
        }
    )
    assert bien == (
        685256,
        "cierre",
        "AUTOMOTORES EN EL PAIS",
        "PEUGOT 208",
        "INGRESOS PROPIOS",
        Decimal("100.00"),
        Decimal("4779200.00"),
    )
    deuda = oa.fila_deuda(
        {
            "dj_id": "685832",
            "periodo_inicio_cierre": "I",
            "deuda_tipo": "COMUN",
            "deuda_descripcion": "BANCO -CUIT/CUIL/CDI: 30709447846",
            "deuda_radicacion_localizacion": "ARGENTINA",
            "deuda_clasificacion": "Otras deudas en el pais al inicio",
            "deuda_importe": "3554270.60",
        }
    )
    assert deuda[1] == "inicio" and deuda[-1] == Decimal("3554270.60")


def test_detalle_vacio_se_descarta():
    # Como en el detalle 2017: dj_id con todo lo demás vacío.
    assert oa.fila_bien({"dj_id": "200920", "periodo_inicio_cierre": ""}) is None
    assert oa.fila_bien({"dj_id": "1", "periodo_inicio_cierre": "C"}) is None


def test_tiene_importe():
    con = ["dj_id", "periodo_inicio_cierre", "bien_tipo", "bien_descripcion", "bien_importe"]
    # 2016-2017: sin bien_tipo, y la última columna es la titularidad.
    sin = ["dj_id", "periodo_inicio_cierre", "bien_descripcion", "bien_titularidad", "bien_importe"]
    assert oa.tiene_importe(con, "bienes")
    assert not oa.tiene_importe(sin, "bienes")
    assert oa.tiene_importe(["deuda_importe"], "deudas")


# ── elección del corte ───────────────────────────────────────────────────────


def _zip(tmp_path, nombre_zip: str, miembros: dict[str, str]) -> str:
    ruta = tmp_path / nombre_zip
    with zipfile.ZipFile(ruta, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for nombre, contenido in miembros.items():
            z.writestr(nombre, contenido.encode("utf-8"))
    return str(ruta)


def _archivos(ruta: str) -> list[oa.Archivo]:
    return oa.archivos_del_zip(ruta, f"https://x/{ruta}")[0]


def test_abrir_lee_encabezados_con_bom_y_espacios(tmp_path):
    ruta = _zip(
        tmp_path,
        "a.zip",
        {
            "declaraciones-juradas-bienes-2024-consolidado-al-20251222.csv": (
                "﻿dj_id, bien_tipo, bien_importe\r\n1,AUTO,10.00\r\n"
            )
        },
    )
    (archivo,) = _archivos(ruta)
    with oa.abrir(archivo) as lector:
        assert lector.fieldnames == ["dj_id", "bien_tipo", "bien_importe"]
        assert next(lector)["bien_importe"] == "10.00"


def test_elige_el_corte_anterior_cuando_el_nuevo_multiplica_por_10(tmp_path):
    sano = [_fila(i, f"{1000 + i}.00") for i in range(1, 101)]
    # El corte malo: la cuarta parte con el total ×10.
    malo = [_fila(i, f"{(1000 + i) * (10 if i % 4 == 0 else 1)}.00") for i in range(1, 101)]
    ruta24 = _zip(
        tmp_path,
        "2024.zip",
        {"declaraciones-juradas-2023-consolidado-al-20251222.csv": _principal(malo)},
    )
    ruta23 = _zip(
        tmp_path,
        "2023.zip",
        {"declaraciones-juradas-2023-consolidado-al-20250218.csv": _principal(sano)},
    )
    grupos = oa.agrupar(_archivos(ruta24) + _archivos(ruta23))
    eleccion = oa.elegir_principal(grupos[("principal", 2023)])
    assert eleccion.archivo.corte == date(2025, 2, 18)
    ((descartado, motivo),) = eleccion.descartados
    assert descartado.corte == date(2025, 12, 22)
    assert "multiplicados por 10 en el 25%" in motivo


def test_el_suelto_gana_a_su_copia_del_zip(tmp_path):
    # El suelto tiene el formato original; el ZIP, el mismo corte con ×10.
    suelto = tmp_path / "declaraciones-juradas-2024-consolidado-al-20251222.csv"
    suelto.write_text(
        _principal([_fila(i, f"{1000 + i}-00") for i in range(1, 41)]), encoding="utf-8"
    )
    ruta = _zip(
        tmp_path,
        "2024.zip",
        {
            "declaraciones-juradas-2024-consolidado-al-20251222.csv": _principal(
                [_fila(i, f"{(1000 + i) * 10}.00") for i in range(1, 41)]
            )
        },
    )
    candidatos = _archivos(ruta) + [oa.archivo_suelto(str(suelto), f"https://x/{suelto.name}")]
    grupos = oa.agrupar(candidatos)
    assert grupos[("principal", 2024)][0].suelto
    eleccion = oa.elegir_principal(grupos[("principal", 2024)])
    assert eleccion.archivo.suelto and not eleccion.descartados


def test_descarta_un_corte_parcial_y_uno_vacio(tmp_path):
    completo = [_fila(i, "10.00") for i in range(1, 101)]
    ruta = _zip(
        tmp_path,
        "z.zip",
        {
            "declaraciones-juradas-2018-consolidado-al-20250218.csv": "",
            "declaraciones-juradas-2018-consolidado-al-20231027.csv": _principal(completo[:20]),
            "declaraciones-juradas-2018-consolidado-al-20220429.csv": _principal(completo),
        },
    )
    eleccion = oa.elegir_principal(oa.agrupar(_archivos(ruta))[("principal", 2018)])
    assert eleccion.archivo.corte == date(2022, 4, 29)
    motivos = [m for _, m in eleccion.descartados]
    assert motivos[0] == "vacío"
    assert motivos[1].startswith("parcial: 20 declaraciones contra 100")


def test_el_ultimo_corte_con_filas_no_tiene_contra_quien_compararse():
    a = oa.Archivo("principal", 2020, date(2025, 1, 1), "a", "a", "a", "a")
    b = oa.Archivo("principal", 2020, date(2024, 1, 1), "b", "b", "b", "b")
    resumenes = {
        a: oa.ResumenPrincipal(2, {1: Decimal(100), 2: Decimal(200)}),
        b: oa.ResumenPrincipal(2, {1: Decimal(10), 2: Decimal(20)}),
    }
    # b es la referencia de a: a queda descartado y b, sin referencia, se acepta.
    eleccion = oa.elegir_principal([a, b], resumir=resumenes.__getitem__)
    assert eleccion.archivo is b
    # Solo, el corte malo se acepta: lo corrige la carga contra el detalle.
    assert oa.elegir_principal([a], resumir=resumenes.__getitem__).archivo is a
    vacio = {a: oa.ResumenPrincipal(0, {})}
    assert oa.elegir_principal([a], resumir=vacio.__getitem__) is None


def test_elegir_detalle_prefiere_el_corte_del_principal_y_exige_importe():
    def archivo(corte: date) -> oa.Archivo:
        return oa.Archivo("bienes", 2016, corte, str(corte), "o", "u", "m")

    nuevo, mismo, viejo = (
        archivo(date(2025, 12, 22)),
        archivo(date(2025, 2, 18)),
        archivo(date(2019, 5, 24)),
    )
    columnas = {
        nuevo: ["bien_tipo", "bien_importe"],
        mismo: ["bien_tipo", "bien_importe"],
        viejo: ["bien_descripcion", "bien_importe"],
    }
    eleccion = oa.elegir_detalle([nuevo, mismo, viejo], date(2025, 2, 18), columnas.__getitem__)
    assert eleccion.archivo is mismo
    assert [m for _, m in eleccion.descartados] == ["sin importe"]
    assert oa.elegir_detalle([viejo], date(2025, 2, 18), columnas.__getitem__) is None


def test_zip_saltea_compresion_no_soportada(tmp_path, monkeypatch):
    ruta = _zip(
        tmp_path,
        "2019.zip",
        {
            "declaraciones-juradas-2015-consolidado-al-20190524.csv": "a",
            "declaraciones-juradas-2015-consolidado-al-20191115.csv": "b",
            "LEAME.txt": "c",
        },
    )
    original = zipfile.ZipFile.infolist

    def infolist(self):
        infos = original(self)
        for info in infos:
            if "20191115" in info.filename:
                info.compress_type = 9  # Deflate64, como en el ZIP de 2019
        return infos

    monkeypatch.setattr(zipfile.ZipFile, "infolist", infolist)
    archivos, salteados = oa.archivos_del_zip(ruta, "u")
    assert [a.corte for a in archivos] == [date(2019, 5, 24)]
    assert salteados == ["declaraciones-juradas-2015-consolidado-al-20191115.csv: compresión 9"]


# ── guardián ─────────────────────────────────────────────────────────────────


def _rechazo(**kw):
    base = {
        "nuevas": {2023: 100, 2024: 100},
        "vivas": {2023: 100, 2024: 100},
        "detalle_nuevo": {"b": 1000},
        "detalle_vivo": {"b": 1000},
        "permitir_menos_filas": False,
    }
    base.update(kw)
    return dt.motivo_para_rechazar(**base)


def test_guardian_deja_pasar_lo_igual_y_un_anio_nuevo():
    assert _rechazo() is None
    assert _rechazo(nuevas={2023: 100, 2024: 100, 2025: 50}) is None
    assert _rechazo(vivas={}, detalle_vivo={"b": None}) is None  # primera carga


def test_guardian_frena_un_anio_que_desaparece_o_cae():
    assert _rechazo(nuevas={2024: 100}) == "desaparecen los años 2023"
    assert "2023: 80 contra 100" in _rechazo(nuevas={2023: 80, 2024: 100})
    assert _rechazo(detalle_nuevo={"b": 800}) == "b cae de 1000 a 800 filas"
    assert _rechazo(nuevas={}) == "no quedó ninguna declaración"


def test_guardian_se_puede_saltear_a_mano():
    assert _rechazo(nuevas={2024: 1}, permitir_menos_filas=True) is None


def test_manifiesto_no_depende_del_orden():
    a = dt.Recurso("2", "u2", "zip", "t2")
    b = dt.Recurso("1", "u1", "csv", "t1")
    assert dt.manifiesto([a, b]) == dt.manifiesto([b, a])
