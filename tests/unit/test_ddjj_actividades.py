"""DDJJ de actividades anteriores y posteriores (application/ddjj/actividades.py).

Las filas copian el formato de los CSV reales del corte 20260930 (08-oct-2026):
el encabezado del documento cambia entre archivos y una fila trae varias
actividades a la vez.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import ToolContext, ToolInputError
from app.application.answers.tools.conectores import DeclaracionesJuradas
from app.application.ddjj import actividades as act
from app.application.ddjj import organismos as org
from app.infrastructure.celery.tasks import ddjj_tasks as dt

SI = "Sin información"

_ANT = [
    "documento_FODYW_fecha_hora_creacion", "documento_FODYW_nro_documento", "tramite_tipo",
    "apellidos_nombres", "cuit_cuil", "cargo_actual", "cargo_descripcion", "cargo_jurisdiccion",
    "cargo_fecha_inicio", "profesion_ocupacion_descripcion",
    "relacion_dependencia_empleador", "relacion_dependencia_empleador_cuit",
    "relacion_dependencia_sector", "relacion_dependencia_sector_otro",
    "relacion_dependencia_fecha_inicio", "relacion_dependencia_continuidad",
    "relacion_dependencia_fecha_cese", "ultimo_puesto_ocupado", "ultimo_puesto_ocupado_descripcion",
    "categoria_ocupacional_trabajo_independiente", "trabajo_independiente_razon_social",
    "trabajo_independiente_sector", "trabajo_independiente_sector_otro",
    "trabajo_independiente_fecha_inicio", "trabajo_independiente_continuidad",
    "trabajo_independiente_fecha_cese", "funcion_publica_categoria_ocupacional",
    "funcion_publica_organismo", "funcion_publica_sector", "funcion_publica_sector_otro",
    "funcion_publica_fecha_inicio", "funcion_publica_continuidad", "funcion_publica_fecha_cese",
    "actividad_ad_honorem_descripcion", "actividad_ad_honorem_entidad",
    "actividad_ad_honorem_entidad_cuit", "actividad_ad_honorem_sector",
    "actividad_ad_honorem_sector_otro", "actividad_ad_honorem_fecha_inicio",
    "actividad_ad_honorem_continuidad", "actividad_ad_honorem_fecha_cese",
]  # fmt: skip


def _csv(columnas: list[str], filas: list[dict[str, str]]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=columnas, lineterminator="\r\n")
    w.writeheader()
    for f in filas:
        w.writerow({c: f.get(c, SI) for c in columnas})
    return "﻿" + buf.getvalue()


def _fila_ant(**kw: str) -> dict[str, str]:
    base = {
        "documento_FODYW_fecha_hora_creacion": "2026-09-29 10:20:00",
        "documento_FODYW_nro_documento": "DOCPE-2026-1-APN-CPI#OA",
        "tramite_tipo": "GENE00588 - Inscripción de actividades laborales anteriores a la función pública",
        "apellidos_nombres": "Perez  Marcial Eduardo",
        "cuit_cuil": "20219362472",
        "cargo_actual": "Subsecretario/a",
        "cargo_jurisdiccion": "Ministerio de Economía",
        "cargo_fecha_inicio": "2026-08-27",
        "relacion_dependencia_empleador": "Techint S.A.",
        "relacion_dependencia_empleador_cuit": "30-54669501-4",
        "relacion_dependencia_sector": "Minero, petrolero y energético",
        "relacion_dependencia_fecha_inicio": "2010-01-24",
        "relacion_dependencia_continuidad": "No",
        "relacion_dependencia_fecha_cese": "2026-08-20",
        "ultimo_puesto_ocupado_descripcion": "Gerente de Finanzas",
        "actividad_ad_honorem_entidad": "CAMARA DE COMERCIO DEL MERCOSUR",
        "actividad_ad_honorem_entidad_cuit": "30719446457",
        "actividad_ad_honorem_sector": "Otro sector no clasificado en las opciones anteriores",
        "actividad_ad_honorem_sector_otro": "CAMARA DE COMERCIO",
        "actividad_ad_honorem_descripcion": "Comisión Fiscalizadora",
        "actividad_ad_honorem_fecha_inicio": "2025-10-17",
        "actividad_ad_honorem_continuidad": "Si",
    }
    base.update(kw)
    return base


def _filas(momento: str, texto: str) -> list[dict[str, Any]]:
    return [
        dict(zip(act.COLUMNAS, f, strict=True))
        for f in act.filas_actividades(act.Archivo(momento, texto, "https://x/a.csv"))
    ]


def test_una_fila_se_separa_en_sus_actividades():
    filas = _filas(act.ANTERIOR, _csv(_ANT, [_fila_ant()]))
    assert [f["tipo"] for f in filas] == ["relacion_dependencia", "ad_honorem"]
    empleo, honorem = filas
    assert empleo["nombre"] == "PEREZ MARCIAL EDUARDO"
    assert empleo["documento"] == "DOCPE-2026-1-APN-CPI#OA"
    assert empleo["fecha_documento"] == date(2026, 9, 29)
    assert empleo["tramite"] == "inscripcion"
    assert empleo["cargo"] == "Subsecretario/a" and empleo["organismo"] == "Ministerio de Economía"
    assert empleo["cargo_desde"] == date(2026, 8, 27) and empleo["cargo_hasta"] is None
    assert empleo["entidad"] == "Techint S.A." and empleo["entidad_cuit"] == "30546695014"
    assert empleo["puesto"] == "Gerente de Finanzas"
    assert (empleo["desde"], empleo["hasta"], empleo["continua"]) == (
        date(2010, 1, 24),
        date(2026, 8, 20),
        False,
    )
    # "Otro sector…" se reemplaza por el texto que escribió la persona.
    assert honorem["sector"] == "CAMARA DE COMERCIO" and honorem["continua"] is True


def test_el_cargo_otro_toma_la_descripcion_y_las_actualizaciones_se_marcan():
    fila = _fila_ant(
        cargo_actual="Otro",
        cargo_descripcion="Prefecto de Zona",
        tramite_tipo="GENE00589 - Actualización de actividades laborales anteriores",
    )
    [empleo, _] = _filas(act.ANTERIOR, _csv(_ANT, [fila]))
    assert empleo["cargo"] == "Prefecto de Zona" and empleo["tramite"] == "actualizacion"


def test_sin_cuit_o_sin_documento_se_descarta_y_sin_entidad_no_hay_actividad():
    filas = _filas(
        act.ANTERIOR,
        _csv(
            _ANT,
            [
                _fila_ant(cuit_cuil="123"),
                _fila_ant(documento_FODYW_nro_documento=""),
                _fila_ant(relacion_dependencia_empleador=SI, actividad_ad_honorem_entidad=""),
            ],
        ),
    )
    assert filas == []


def test_posteriores_con_su_propio_encabezado():
    columnas = [
        "documento_FOJWP_fecha_hora_creacion", "documento_FOJWP_nro_documento", "tramite_tipo",
        "apellidos_nombres", "cuit_cuil", "cargo_cese", "cargo_descripcion", "cargo_jurisdiccion",
        "cargo_fecha_cese", "relacion_dependencia_empleador", "relacion_dependencia_empleador_cuit",
        "relacion_dependencia_sector", "relacion_dependencia_fecha_inicio", "puesto_actual",
        "puesto_actual_descripcion",
    ]  # fmt: skip
    texto = _csv(
        columnas,
        [
            {
                "documento_FOJWP_fecha_hora_creacion": "2026-09-25 16:48:00",
                "documento_FOJWP_nro_documento": "DOCPE-2026-9-APN-CPI#OA",
                "tramite_tipo": "GENE00590 - Inscripción de actividades laborales al egreso",
                "apellidos_nombres": "Goñi Pablo Enrique",
                "cuit_cuil": "20232141272",
                "cargo_cese": "Otro",
                "cargo_descripcion": "Prefecto de Zona Alto Uruguay",
                "cargo_jurisdiccion": "Prefectura Naval Argentina",
                "cargo_fecha_cese": "2025-12-15",
                "relacion_dependencia_empleador": "Tecplata S.A.",
                "relacion_dependencia_empleador_cuit": "30710586051",
                "relacion_dependencia_sector": "Transporte, almacenamiento, logística y correo",
                "relacion_dependencia_fecha_inicio": "2026-09-01",
                "puesto_actual_descripcion": "Jefe Seguridad Patrimonial",
            }
        ],
    )
    [empleo] = _filas(act.POSTERIOR, texto)
    assert empleo["momento"] == "posterior" and empleo["tramite"] == "egreso"
    assert empleo["cargo"] == "Prefecto de Zona Alto Uruguay"
    assert empleo["cargo_hasta"] == date(2025, 12, 15) and empleo["cargo_desde"] is None
    assert empleo["entidad"] == "Tecplata S.A." and empleo["puesto"] == "Jefe Seguridad Patrimonial"


def test_las_actualizaciones_repetidas_se_quedan_con_la_mas_nueva():
    vieja = _fila_ant(
        documento_FODYW_fecha_hora_creacion="2024-01-01 10:00:00",
        documento_FODYW_nro_documento="DOC-VIEJO",
        actividad_ad_honorem_entidad="",
    )
    nueva = _fila_ant(
        documento_FODYW_fecha_hora_creacion="2025-06-01 10:00:00",
        documento_FODYW_nro_documento="DOC-NUEVO",
        actividad_ad_honorem_entidad="",
    )
    crudas = list(act.filas_actividades(act.Archivo(act.ANTERIOR, _csv(_ANT, [vieja, nueva]), "u")))
    [unica] = dt.deduplicar_actividades(crudas)
    assert dict(zip(act.COLUMNAS, unica, strict=True))["documento"] == "DOC-NUEVO"


def test_momento_del_recurso():
    assert act.momento_de("ddjj-actividades-anteriores-funcion-publica-20260930.csv") == "anterior"
    assert (
        act.momento_de("ddjj-actividades-posteriores-funcion-publica-20260930.csv") == "posterior"
    )
    assert (
        act.momento_de("ddjj-actividades-anteriores-posteriores-funcion-publica-2026.zip") is None
    )


def test_ultimos_csv_elige_el_corte_mas_nuevo_de_cada_momento():
    base = "https://datos.jus.gob.ar/x/ddjj-actividades-{}-funcion-publica-{}.csv"
    recursos = [
        dt.Recurso("1", base.format("anteriores", "20260630"), "csv", ""),
        dt.Recurso("2", base.format("anteriores", "20260930"), "csv", ""),
        dt.Recurso("3", base.format("posteriores", "20260930"), "csv", ""),
        dt.Recurso(
            "4",
            "https://x/ddjj-actividades-anteriores-posteriores-funcion-publica-2026.zip",
            "zip",
            "",
        ),
    ]
    elegidos = dt._ultimos_csv(recursos)
    assert elegidos["anterior"].id == "2" and elegidos["posterior"].id == "3"


@pytest.mark.parametrize(
    ("texto", "esperado"),
    [
        ("AFIP", {(r"\mAFIP\M",), (r"\mARCA\M",)}),
        ("conicet", {(r"\mCONICET",)}),
        ("Techint", {(r"\mTECHINT",)}),
        ("banco galicia", {(r"\mBANCO", r"\mGALICIA")}),
    ],
)
def test_alternativas_para_el_empleador(texto, esperado):
    alternativas = set(org.alternativas_texto_libre(texto))
    assert esperado <= alternativas


# ── la herramienta ─────────────────────────────────────────


def _ctx(ddjj: Any) -> ToolContext:
    deps = MagicMock()
    deps.ddjj = ddjj
    return ToolContext(deps, EngineRequest("q", "u"))


async def test_herramienta_actividades_pasa_los_filtros():
    from app.domain.entities.connectors.data_result import DataResult

    pedidos: list[dict] = []

    class Falso:
        async def actividades(self, **kw: Any) -> DataResult:
            pedidos.append(kw)
            return DataResult(
                source="ddjj:actividades",
                portal_name="p",
                portal_url="",
                dataset_title="t",
                format="json",
                records=[{"nombre": "X", "entidad": "Techint S.A."}],
                metadata={},
            )

    out = await DeclaracionesJuradas().run(
        {"accion": "actividades", "entidad": "Techint", "momento": "anterior"}, _ctx(Falso())
    )
    assert json.loads(out.content)["filas"][0]["entidad"] == "Techint S.A."
    await DeclaracionesJuradas().run(
        {"accion": "actividades", "organismo": "Ministerio de Economía", "momento": "posterior"},
        _ctx(Falso()),
    )
    assert pedidos == [
        {
            "persona": None,
            "entidad": "Techint",
            "momento": "anterior",
            "organismo": None,
            "cargo": None,
        },
        {
            "persona": None,
            "entidad": None,
            "momento": "posterior",
            "organismo": "Ministerio de Economía",
            "cargo": None,
        },
    ]
    with pytest.raises(ToolInputError, match="hace falta `nombre`, `entidad` u `organismo`"):
        await DeclaracionesJuradas().run({"accion": "actividades"}, _ctx(Falso()))
    with pytest.raises(ToolInputError, match="anterior o posterior"):
        await DeclaracionesJuradas().run(
            {"accion": "actividades", "nombre": "x", "momento": "durante"}, _ctx(Falso())
        )


def test_la_descripcion_no_deja_calificar_los_pases():
    description = DeclaracionesJuradas.spec.description
    assert "no lo llames conflicto de interés ni puerta giratoria" in description
    assert "no sugieras que hubo una irregularidad" in description
    assert "`actividades`" in description
    # «Los que dejaron Economía» es el organismo del cargo, no el empleador.
    assert "usá `organismo` con `momento` posterior" in description
