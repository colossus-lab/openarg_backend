"""Cómo se busca un organismo en las DDJJ (application/ddjj/organismos.py).

Los nombres de los patrones son los de las tablas reales (staging, 08-oct-2026).
"""

from __future__ import annotations

import re

import pytest

from app.application.ddjj import organismos as org


def _coincide(nombre: str, pedido: str) -> bool:
    """Lo que hace el SQL del adaptador, en Python: LIKE sobre los patrones o todas
    las regex (con \\m y \\M de Postgres pasados a \\b)."""
    columna = re.sub(r"[^A-Z0-9 ]+", " ", org.normalizar(nombre))
    cond = org.condicion(pedido)
    if cond.patrones:
        return any(re.fullmatch(re.escape(p).replace("%", ".*"), columna) for p in cond.patrones)
    return bool(cond.regex) and all(
        re.search(r.replace(r"\m", r"\b").replace(r"\M", r"\b"), columna) for r in cond.regex
    )


@pytest.mark.parametrize(
    "pedido", ["ARCA", "arca", "AFIP", "la AFIP", "Agencia de Recaudación y Control Aduanero"]
)
def test_arca_y_afip_traen_los_dos_nombres(pedido):
    assert _coincide("AGENCIA DE RECAUDACION Y CONTROL ADUANERO", pedido)
    assert _coincide("ADMINISTRACION FEDERAL DE INGRESOS PUBLICOS", pedido)
    assert _coincide("ADMINISTRACION NACIONAL DE ADUANAS", pedido)


@pytest.mark.parametrize(
    "nombre",
    [
        "UNIVERSIDAD NACIONAL DE CATAMARCA",
        "ARCANGEL SAN GABRIEL S A",
        "ABARCA LUIS ALBERTO",
        "AGENCIA DE RECAUDACION DE LA PROVINCIA DE BUENOS A",
        "AGENCIA SANTACRUCEÑA DE INGRESOS PUBLICOS",
    ],
)
def test_arca_no_trae_lo_que_solo_contiene_las_letras(nombre):
    assert not _coincide(nombre, "ARCA")


@pytest.mark.parametrize(
    ("pedido", "nombre"),
    [
        ("ANSES", "ADMINISTRACION NACIONAL DE LA SEGURIDAD SOCIAL ANS"),
        ("PAMI", "INSTITUTO NACIONAL DE SERVICIOS SOCIALES PARA JUBI"),
        ("SENASA", "SERVICIO NACIONAL DE SANIDAD Y CALIDAD AGROALIMENT"),
        ("CONICET", "CONSEJO NACIONAL DE INVESTIGACIONES CIENTIFICAS Y"),
        ("Diputados", "HONORABLE CAMARA DE DIPUTADOS DE LA NACION"),
        ("HCDN", "HONORABLE CAMARA DE DIPUTADOS DE LA NACION"),
        ("Senado", "HONORABLE SENADO DE LA NACION"),
        ("Prefectura", "PREFECTURA NAVAL ARGENTINA O. P."),
        ("ACUMAR", "(ACUMAR ) AUTORIDAD DE CUENCA MATANZA RIACHUELO"),
        ("Banco Central", "BANCO CENTRAL DE LA REPUBLICA ARGENTINA"),
        ("jefatura de gabinete", "VICEJEFATURA DE GABINETE DEL INTERIOR O. P."),
    ],
)
def test_siglas_y_alias(pedido, nombre):
    assert _coincide(nombre, pedido)


def test_anses_no_trae_al_ministerio_de_trabajo():
    assert not _coincide("MINISTERIO DE TRABAJO -EMPLEO Y SEGURIDAD SOCIAL", "ANSES")
    assert not _coincide("UNIVERSIDAD NACIONAL DE LA MATANZA", "ACUMAR")
    assert not _coincide("DIRECCION DE VIALIDAD PCIA DE BS AS", "Vialidad")


def test_lo_que_no_esta_en_el_mapa_va_por_palabras_enteras():
    # Cinco letras o más: comienzo de palabra (plurales y nombres cortados).
    assert _coincide("MINISTERIO DE TRABAJO -EMPLEO Y SEGURIDAD SOCIAL", "ministerio de trabajo")
    assert _coincide("UNIVERSIDAD NACIONAL DE CORDOBA", "universidad córdoba")
    assert _coincide("UNIVERSIDAD NACIONAL DEL NORDESTE UNNE", "universidad nacional del nordeste")
    # Menos de cinco: palabra entera.
    assert _coincide("UNIVERSIDAD NACIONAL DEL NORDESTE UNNE", "UNNE")
    assert not _coincide("UNIVERSIDAD NACIONAL DEL NORDESTE UNNE", "UNN")
    assert _coincide("GENDARMERIA NACIONAL", "gendarmes") is False


def test_vacio_no_filtra_nada():
    assert org.condicion("") == org.Condicion()
    assert org.condicion("   ") == org.Condicion()


def test_cada_alias_apunta_a_patrones():
    assert all(org.ORGANISMOS.values())
    assert org.ORGANISMOS["AFIP"] is org.ORGANISMOS["ARCA"]
