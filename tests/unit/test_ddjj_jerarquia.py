"""Altos cargos: quién entra cuando se pregunta por «el gobierno».

Los casos son cargos y organismos tal como vienen en el CSV de la OA de 2024 y
en el de la Ciudad de 2025 (ver el docstring de `jerarquia`).
"""

from __future__ import annotations

import pytest

from app.application.ddjj import jerarquia as j


@pytest.mark.parametrize(
    ("cargo", "organismo"),
    [
        # cúpula, con o sin organismo
        ("Presidente de la Nación", None),
        ("JEFE DE GABINETE DE MINISTROS", "JEFATURA DE GABINETE DE MINISTROS"),
        ("VICEJEFE DE GABINETE DEL INTERIOR DE LA NACION", "VICEJEFATURA DE GABINETE DEL I"),
        ("Ministro de Economia", None),
        ("MINISTRA DE SEGURIDAD NACIONAL", "MINISTERIO DE SEGURIDAD"),
        ("Ministro de Relac. Ext. Com. Int. y Culto", None),
        ("Ministro Desregulación y Transformación del Estado", "MINISTERIO DE DESREGULACION"),
        # secretarios y subsecretarios de la administración central
        ("SECRETARIO DE POLITICA ECONOMICA", "MINISTERIO DE ECONOMIA"),
        ("Secretario de Innovación -Ciencia y Tecnologia", "JEFATURA DE GABINETE DE MINISTROS"),
        ("SUBSECRETARIA DE INGRESOS PÚBLICOS", "MINISTERIO DE ECONOMIA"),
        ("SUBSECRETARIA PLANEAMIENTO NORMATIVO", "SECRETARIA GENERAL DE LA PRESIDENCIA DE"),
        ("Secretario de Economia del Conocimiento", None),
        # titulares de organismos y empresas del Estado
        ("Presidente", "INSTITUTO NACIONAL DE ASUNTOS INDIGENAS"),
        ("VICEPRESIDENTE", "COMISION NACIONAL DE VALORES"),
        ("ADMINISTRADOR", "INSTITUTO NACIONAL DE TECNOLOGIA AGROPECUARIA"),
        ("DIRECTOR EJECUTIVO", "ENTIDAD BINACIONAL YACYRETA"),
        ("PRESIDENTE DE VENG S.A.", "VENG SA"),
        # alcance amplio: directores nacionales y embajadores
        ("DIRECTORA NACIONAL DE INVERSIONES MINERAS", "MINISTERIO DE ECONOMIA"),
        ("director nacional", "SECRETARIA DE TRANSPORTE"),
        ("EMBAJADOR EN LA REPUBLICA DE AUSTRIA", "MINISTERIO DE RELACIONES EXTERIORES Y CU"),
        ("Embajador Extraordinario y Plenipotenciario", None),
    ],
)
def test_alto_cargo_del_ejecutivo(cargo, organismo):
    assert j.es_alto_cargo(cargo, organismo, "ejecutivo")


@pytest.mark.parametrize(
    ("cargo", "organismo"),
    [
        # el caso que lo originó
        ("Jefe Diseño Grafico", "FUERZA AEREA ARGENTINA"),
        ("SUBCOMISARIO", "POLICIA FEDERAL ARGENTINA"),
        ("INSPECTOR", "AGENCIA DE RECAUDACION Y CONTROL ADUANERO"),
        # universidades
        ("SECRETARIA DE POSGRADO", "UNIVERSIDAD NACIONAL DE CORDOBA"),
        ("SUBSECRETARIA DE GESTION ACADEMICA DE POSGRADO", "UNIVERSIDAD DE BUENOS AIRES"),
        ("PRESIDENTE", "UNIVERSIDAD NACIONAL DE SAN LUIS"),
        ("Secretario de Facultades Nº resolución 1773", None),
        # rangos diplomáticos y asesores
        ("MINISTRO PLENIPOTENCIARIO", "MINISTERIO DE RELACIONES EXTERIORES"),
        ("MINISTRO EN A EMBAJADA ARGENTINA EN VENEZUELA", "MINISTERIO DE RELACIONES EXTER"),
        ("Ministro de Segunda (EFRAN)", None),
        ("Secretario de Primera/Consul en Honduras", "MINISTERIO DE RELACIONES EXTERIORES Y CU"),
        ("SECRETARIO DE EMBAJADA ARGENT EN INDIA", "MINISTERIO DE RELACIONES EXTERIORES"),
        ("ASESOR MINISTRO DE ECONOMIA DE LA NACION", "MINISTERIO DE ECONOMIA"),
        # registros automotores, dependencias, comisiones, fuerzas, municipios
        ("INTERVENTOR DEL REG 01092 Y 25080 SAN NICOLAS", "MINISTERIO DE JUSTICIA"),
        ("INTERVENTOR FEDERAL", "CONVENIO MARCO MJYDH ACARA AUTOMOTOR LEY"),
        ("INTERVENTOR DPTO ADM CONTABLE Y FINANCIERA", "ADMINISTRACION NACIONAL DE LABORATORIOS"),
        ("ADMINISTRADOR DE ADUANA", "AGENCIA DE RECAUDACION Y CONTROL ADUANERO"),
        ("Administrador de Sistema ARCA", "AGENCIA DE RECAUDACION Y CONTROL ADUANERO"),
        ("PRESIDENTE CRE", "CONTADURIA GENERAL DEL EJERCITO"),
        ("Presidente de la comision receptora de efectos", None),
        ("SECRETARIO AYUDANTE Y JEFE DE GABINETE PERSONAL", "FUERZA AEREA ARGENTINA"),
        ("JEFE DE GABINETE", "MUNICIPALIDAD DE ROSARIO"),
        # cargos sin jerarquía
        ("SECRETARIO ADMINISTRATIVO", None),
        ("SECRETARIA", None),
        ("CANDIDATO A DIPUTADO NACIONAL", None),
        (None, "MINISTERIO DE ECONOMIA"),
    ],
)
def test_no_es_alto_cargo(cargo, organismo):
    assert not j.es_alto_cargo(cargo, organismo, "ejecutivo")


def test_el_organismo_de_un_privado_es_otra_actividad():
    """Con sector privado, el organismo es otra actividad: un "presidente" no es
    titular de un organismo del Estado. El cargo de la cúpula manda igual."""
    assert not j.es_alto_cargo("presidente", "FAMAR FUEGUINA SA", "ejecutivo", sector="PRIVADO")
    assert j.es_alto_cargo(
        "MINISTRA RELACIONES EXTERIORES Y CULTO", "BANCO ROELA SA", "ejecutivo", sector="PRIVADO"
    )


@pytest.mark.parametrize(
    ("cargo", "poder", "alto"),
    [
        ("DIPUTADO NACIONAL", "legislativo", True),
        ("Diputada de la Nación", "legislativo", True),
        ("DIUTADA NACIONAL", "legislativo", True),
        ("Senador Nacional", "legislativo", True),
        ("ASESOR DEL DIPUTADO", "legislativo", False),
        ("JEFE DE DEPARTAMENTO", "legislativo", False),
        ("Juez de Cámara", "judicial", True),
        ("MINISTRO DE LA CORTE SUPREMA DE JUSTICIA", "judicial", True),
        ("SECRETARIO DE JUZGADO", "judicial", False),
        ("Procurador General de la Nación", "ministerio_publico", True),
        ("Fiscal", "ministerio_publico", False),
    ],
)
def test_otros_poderes(cargo, poder, alto):
    assert j.es_alto_cargo(cargo, None, poder) is alto


@pytest.mark.parametrize(
    ("cargo", "alto"),
    [
        ("MINISTRO/A", True),
        ("SECRETARIO/A O EQUIVALENTE", True),
        ("SUBSECRETARIO/A O EQUIVALENTE", True),
        ("DIRECTOR/A GENERAL O EQUIVALENTE", True),
        ("PRESIDENTE/A O MAXIMA AUTORIDAD", True),
        ("JEFE DE GOBIERNO", True),
        ("VICEJEFE DE GOBIERNO", True),
        ("GERENTE OPERATIVO/A", False),
        ("CONTROLADOR/A DE FALTAS", False),
        ("INTEGRANTE DE COMISION EVALUADORA", False),
        ("ASESOR/A BAJO EL REGIMEN", False),
        ("DIRECTOR/A MEDICO/A", False),
    ],
)
def test_ciudad(cargo, alto):
    assert j.es_alto_cargo(cargo, None, "ejecutivo", ciudad=True) is alto


@pytest.mark.parametrize(
    ("cargo", "autoridad"),
    [
        ("Presidente de la Nación", True),
        ("Ministro de Economia", True),
        ("SUBSECRETARIA DE RELACIONES DEL TRABAJO", True),
        ("DIRECTOR NACIONAL DE TRANSPORTE E INFRAESTRUCTURA", True),
        ("Embajador Extraordinario y Plenipotenciario", True),
        ("SECRETARIA ACADEMICA", False),
        ("Ministro de Segunda (EFRAN)", False),
        ("MINISTRO DE LA CORTE SUPREMA", False),
        ("Asesora Ministro de Justicia de la Nación", False),
        ("Director de Compras", False),
        (None, False),
    ],
)
def test_autoridad_del_ejecutivo_sin_organismo(cargo, autoridad):
    assert j.es_autoridad_ejecutivo(cargo) is autoridad


def test_normalizar():
    assert j.normalizar("Directora Nacional de Hábitat/Vivienda") == (
        "DIRECTORA NACIONAL DE HABITAT VIVIENDA"
    )
    assert j.normalizar("SECRETARIO/A O EQUIVALENTE") == "SECRETARIO O EQUIVALENTE"
    assert j.normalizar(None) == ""
