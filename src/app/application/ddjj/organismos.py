"""Cómo se busca un organismo en las DDJJ: siglas, nombres viejos y palabras enteras.

El filtro de organismo buscaba el texto en cualquier parte del nombre. Medido en
staging el 08-oct-2026:

- **"ARCA" traía cualquier cosa.** Encontraba "UNIVERSIDAD NACIONAL DE CATAMARCA",
  "ARCANGEL SAN GABRIEL S A" o "ABARCA LUIS ALBERTO", y sólo 113 de 2024 eran de
  verdad del organismo.
- **ARCA figura con dos nombres.** La Oficina Anticorrupción publica ARCA como
  "AGENCIA DE RECAUDACION Y CONTROL ADUANERO" (10.304 declaraciones en 2024,
  reescrito hasta 2017) y AFIP como "ADMINISTRACION FEDERAL DE INGRESOS PUBLICOS"
  (856 en 2024). Pedir cualquiera de los dos tiene que traer los dos.
- **Los nombres vienen cortados a 50 caracteres** ("INSTITUTO NACIONAL DE SERVICIOS
  SOCIALES PARA JUBI"), así que nadie los escribe completos.

Por eso, primero se busca el texto en `ORGANISMOS`: siglas, alias y nombres
viejos, cada uno con los patrones LIKE de sus nombres oficiales tal como vienen
en los datos (verificados contra las tablas). Si no está, cada palabra tiene que
aparecer entera, o como comienzo de una palabra si tiene 5 letras o más (para
plurales y nombres cortados).
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

# nombre canónico → (alias, patrones LIKE sobre el organismo normalizado)
_DEFINICIONES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "ARCA": (
        (
            "AFIP",
            "AGENCIA DE RECAUDACION Y CONTROL ADUANERO",
            "ADMINISTRACION FEDERAL DE INGRESOS PUBLICOS",
            "DGI",
            "ADUANA",
        ),
        (
            "AGENCIA DE RECAUDACION Y CONTROL ADUANERO%",
            "ADMINISTRACION FEDERAL DE INGRESOS PUBLICOS%",
            "ADMINISTRACION NACIONAL DE ADUANAS%",
        ),
    ),
    "ANSES": (
        ("ADMINISTRACION NACIONAL DE LA SEGURIDAD SOCIAL",),
        ("ADMINISTRACION NACIONAL DE LA SEGURIDAD SOCIAL%",),
    ),
    "PAMI": (
        ("INSSJP", "INSTITUTO NACIONAL DE SERVICIOS SOCIALES PARA JUBILADOS Y PENSIONADOS"),
        ("INSTITUTO NACIONAL DE SERVICIOS SOCIALES PARA JUB%",),
    ),
    "INTA": ((), ("INSTITUTO NACIONAL DE TECNOLOGIA AGROPECUARIA%",)),
    "INTI": ((), ("INSTITUTO NACIONAL DE TECNOLOGIA INDUSTRIAL%",)),
    "SENASA": ((), ("SERVICIO NACIONAL DE SANIDAD Y CALIDAD AGROALIM%",)),
    "CONICET": ((), ("CONSEJO NACIONAL DE INVESTIGACIONES CIENTIFICAS%",)),
    "BCRA": (("BANCO CENTRAL",), ("BANCO CENTRAL DE LA REPUBLICA ARGENTINA%",)),
    "BANCO NACION": (("BNA", "BANCO DE LA NACION"), ("BANCO DE LA NACION ARGENTINA%",)),
    "UBA": ((), ("UNIVERSIDAD DE BUENOS AIRES%",)),
    "UTN": ((), ("UNIVERSIDAD TECNOLOGICA NACIONAL%",)),
    "CNRT": ((), ("COMISION NACIONAL DE REGULACION DEL TRANSPORTE%",)),
    "SRT": ((), ("SUPERINTENDENCIA DE RIESGOS DEL TRABAJO%",)),
    "ENACOM": ((), ("ENTE NACIONAL DE COMUNICACIONES%",)),
    "ANMAT": ((), ("ADMINISTRACION NACIONAL DE MEDICAMENTOS%",)),
    "POLICIA FEDERAL": (("PFA",), ("POLICIA FEDERAL ARGENTINA%",)),
    "PSA": (("POLICIA DE SEGURIDAD AEROPORTUARIA",), ("POLICIA DE SEGURIDAD AEROPORTUARIA%",)),
    "GENDARMERIA": (("GNA", "GENDARMERIA NACIONAL"), ("GENDARMERIA NACIONAL%",)),
    "PREFECTURA": (("PNA", "PREFECTURA NAVAL"), ("PREFECTURA NAVAL ARGENTINA%",)),
    "SERVICIO PENITENCIARIO FEDERAL": (
        ("SPF",),
        ("DIRECCION NACIONAL DEL SERVICIO PENITENCIARIO FED%",),
    ),
    "VIALIDAD NACIONAL": (
        ("DNV", "VIALIDAD", "DIRECCION NACIONAL DE VIALIDAD"),
        ("DIRECCION NACIONAL DE VIALIDAD%",),
    ),
    "CORTE SUPREMA": (("CSJN",), ("CORTE SUPREMA DE JUSTICIA DE LA NACION%",)),
    "DIPUTADOS": (
        (
            "HCDN",
            "CAMARA DE DIPUTADOS",
            "CAMARA DE DIPUTADOS DE LA NACION",
            "DIPUTADOS DE LA NACION",
        ),
        ("HONORABLE CAMARA DE DIPUTADOS DE LA NACION%",),
    ),
    "SENADO": (("SENADO DE LA NACION", "HONORABLE SENADO"), ("HONORABLE SENADO DE LA NACION%",)),
    "JEFATURA DE GABINETE": (
        ("JGM", "JEFATURA DE GABINETE DE MINISTROS"),
        (
            "JEFATURA DE GABINETE DE MINISTROS%",
            "MINISTERIO DE JEFATURA DE GABINETE DE MINISTROS%",
            "VICEJEFATURA DE GABINETE%",
        ),
    ),
    "ENRE": ((), ("ENTE NACIONAL REGULADOR DE LA ELECTRICIDAD%",)),
    "ACUMAR": ((), ("%AUTORIDAD DE CUENCA MATANZA RIACHUELO%",)),
    "AYSA": ((), ("AGUA Y SANEAMIENTOS ARGENTINOS%",)),
    "ARSAT": ((), ("EMPRESA ARGENTINA DE SOLUCIONES SATELITALES%",)),
    "CNV": (("COMISION NACIONAL DE VALORES",), ("COMISION NACIONAL DE VALORES",)),
    "SSN": (("SUPERINTENDENCIA DE SEGUROS",), ("SUPERINTENDENCIA DE SEGUROS DE LA NACION%",)),
    "SUPERINTENDENCIA DE SERVICIOS DE SALUD": (
        ("SSSALUD",),
        ("SUPERINTENDENCIA DE SERVICIOS DE SALUD%",),
    ),
    "MPF": (("MINISTERIO PUBLICO FISCAL",), ("MINISTERIO PUBLICO FISCAL DE LA NACION%",)),
    "AGN": (("AUDITORIA GENERAL DE LA NACION",), ("AUDITORIA GENERAL DE LA NACION%",)),
    "SIGEN": (("SINDICATURA GENERAL DE LA NACION",), ("SINDICATURA GENERAL DE LA NACION%",)),
    "OFICINA ANTICORRUPCION": (("OA",), ("OFICINA ANTICORRUPCION%",)),
    "INCAA": ((), ("INSTITUTO NACIONAL DE CINE Y ARTES%",)),
    "ANAC": ((), ("ADMINISTRACION NACIONAL DE AVIACION CIVIL%",)),
    "EANA": ((), ("EMPRESA ARGENTINA DE NAVEGACION AEREA%",)),
    "CONAE": ((), ("COMISION NACIONAL DE ACTIVIDADES ESPACIALES%",)),
    "CNEA": ((), ("COMISION NACIONAL DE ENERGIA ATOMICA%",)),
    "INAES": ((), ("INSTITUTO NACIONAL DE ASOCIATIVISMO%",)),
    "ENOHSA": ((), ("ENTE NACIONAL DE OBRAS HIDRICAS%",)),
}

_VACIAS = frozenset({"DE", "DEL", "LA", "LAS", "EL", "LOS", "Y", "E", "EN", "A", "AL"})


def normalizar(texto: str) -> str:
    sin_tildes = unicodedata.normalize("NFD", texto or "")
    limpio = "".join(c for c in sin_tildes if unicodedata.category(c) != "Mn").upper()
    return " ".join(re.sub(r"[^A-Z0-9 ]+", " ", limpio).split())


def _clave(texto: str) -> str:
    """Sin artículos al principio: "la ANSES" y "ANSES" son lo mismo."""
    palabras = normalizar(texto).split()
    while palabras and palabras[0] in {"LA", "EL", "LOS", "LAS"}:
        palabras = palabras[1:]
    return " ".join(palabras)


ORGANISMOS: dict[str, tuple[str, ...]] = {}
for _canonico, (_alias, _patrones) in _DEFINICIONES.items():
    for _nombre in (_canonico, *_alias):
        ORGANISMOS[_clave(_nombre)] = _patrones


@dataclass(frozen=True)
class Condicion:
    """Patrones LIKE (alguno tiene que coincidir) o expresiones regulares (todas)."""

    patrones: tuple[str, ...] = ()
    regex: tuple[str, ...] = ()


def condicion(texto: str) -> Condicion:
    """Cómo filtrar por el organismo que pidió el agente, sobre el nombre normalizado
    (mayúsculas, sin tildes)."""
    clave = _clave(texto)
    if not clave:
        return Condicion()
    if clave in ORGANISMOS:
        return Condicion(patrones=ORGANISMOS[clave])
    palabras = [p for p in clave.split() if p not in _VACIAS] or clave.split()
    regex = []
    for palabra in palabras[:6]:
        # Postgres: \m es comienzo de palabra, \M fin de palabra. Las palabras ya
        # son sólo letras y números, no hay nada que escapar.
        regex.append(rf"\m{palabra}" if len(palabra) >= 5 else rf"\m{palabra}\M")
    return Condicion(regex=tuple(regex))
