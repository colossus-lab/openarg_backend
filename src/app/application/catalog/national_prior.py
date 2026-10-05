"""Prioridad chica a las fuentes nacionales cuando la consulta no nombra un lugar.

La búsqueda ordenaba sólo por coseno, y el catálogo tiene muchos datasets
provinciales casi idénticos entre sí que copan el top. Medido el 04-oct en
prod: "deuda pública nacional" traía Mendoza 2017, 2015 y 2016 (0,649 y 0,644)
por encima de "Títulos Públicos de Deuda" nacional (0,644); "tarifas de
electricidad", cinco veces la Tarifa Social de Mendoza; "empleados públicos
nacionales", el listado de agentes de Córdoba antes que los puestos de trabajo
de la APN.

La señal es barata a propósito: el portal. ``table_catalog.geographic_scope``
está vacío en las 3.696 filas, así que no hay otra. Un dataset de un portal
nacional puede ser de una provincia (datos.gob.ar publica series de Tucumán) y
al revés; por eso el peso es chico, del orden de las diferencias de puntaje
que separan a esos empates. Si la consulta nombra una provincia, una ciudad o
una jurisdicción ("en Córdoba", "municipio de", "CABA"), no hay prior: el
usuario pidió algo local. "Por provincia" no cuenta como lugar: pide el dato
nacional abierto por provincia.

Calibrado en prod (sólo lectura, 05-oct) sobre las 14 consultas de la
auditoría y 20 más, mirando el top 5 con 0; 0,02; 0,03 y 0,04:

- 0,02 ya pone primero lo nacional en "deuda pública nacional" (Títulos
  Públicos), "matrícula escolar por provincia" (anuarios educativos),
  "vacunación covid" y "jubilación mínima", sin tocar las consultas cuyo top
  ya era nacional;
- 0,03 y 0,04 empiezan a subir ruido nacional por encima de lo provincial
  relacionado ("coparticipación": "Personal", "Servicios REFEFO" sobre el Fondo
  Federal de Córdoba);
- "cantidad de empleados públicos nacionales" necesita 0,04 para que "Puestos
  de trabajo en la APN" (0,629) pase al listado de agentes de Córdoba (0,663).
  Por eso, cuando la consulta dice "nacional" o "nación", el prior es el
  doble: el usuario lo pidió explícitamente. "Argentina" y "país" no cuentan:
  el agente se los agrega a casi cualquier búsqueda ("desempleo Argentina") y
  con eso el 0,04, que la calibración marcó como ruido sin pedido explícito,
  pasaba a ser el caso común.

Los nombres de lugar que también son palabras comunes ("gastos corrientes",
"resistencia antimicrobiana", "posadas turísticas", "misiones diplomáticas",
el río Paraná) cuentan sólo con una preposición adelante ("en Corrientes",
"provincia de Misiones"). "Provincia de residencia", "ciudad de origen" o
"por provincia de destino" no nombran un lugar, y "partido de los
trabajadores" tampoco.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Callable

from app.domain.ports.search.vector_search import SearchResult

# Cuánto se suma al puntaje de un dataset de portal nacional (ver la
# calibración en el docstring), y cuánto si la consulta pide lo nacional.
NATIONAL_PRIOR = 0.02
EXPLICIT_NATIONAL_PRIOR = 0.04

NATIONAL_PORTALS = frozenset(
    {
        "datos_gob_ar",
        "indec",
        "series_tiempo",
        "bcra",
        "presupuesto_abierto",
        "energia",
        "produccion",
        "magyp",
        "justicia",
        "salud",
        "transporte",
        "diputados",
        "senado",
        "mininterior",
        "cultura",
        "pami",
        "arsat",
        "desarrollo_social",
        "turismo",
        "ssn",
        "georef",
        "mapa_estado",
        "gobernaciones",
        "curated",
    }
)

_PLACES = (
    # Provincias.
    "buenos aires",
    "catamarca",
    "chaco",
    "chubut",
    "cordoba",
    "entre rios",
    "formosa",
    "jujuy",
    "la pampa",
    "la rioja",
    "mendoza",
    "neuquen",
    "rio negro",
    "salta",
    "san juan",
    "san luis",
    "santa cruz",
    "santa fe",
    "santiago del estero",
    "tierra del fuego",
    "tucuman",
    # La Ciudad y el conurbano.
    "caba",
    "capital federal",
    "ciudad autonoma",
    "porteno",
    "portena",
    "portenos",
    "portenas",
    "bonaerense",
    "bonaerenses",
    "pba",
    "conurbano",
    "amba",
    "gba",
    # Ciudades con portal o pedidas seguido.
    "rosario",
    "mar del plata",
    "bahia blanca",
    "ushuaia",
    "rawson",
    "viedma",
    "pinamar",
    "godoy cruz",
    "rio cuarto",
    "villa maria",
    "comodoro rivadavia",
    "bariloche",
    "tandil",
    "san rafael",
)
_PLACE_RE = re.compile(r"\b(" + "|".join(re.escape(p) for p in _PLACES) + r")\b")
# Lugares que también son palabras comunes: sólo con una preposición adelante.
# "La Plata" sí suelta, salvo en "Río de la Plata".
_AMBIGUOUS_PLACES = ("corrientes", "misiones", "parana", "resistencia", "posadas")
_AMBIGUOUS_PLACE_RE = re.compile(
    r"\b(en|de|del|desde|hasta|para)\s+(" + "|".join(_AMBIGUOUS_PLACES) + r")\b"
    r"|(?<!rio de )\bla plata\b"
)
# "provincia de X", "municipio de X"...: nombra un lugar aunque no esté en la
# lista. "por provincia" no: pide el dato nacional abierto por provincia; y
# tampoco "por/cada provincia de residencia", "ciudad de origen" o un partido
# político ("partido de los trabajadores").
_JURISDICTION_RE = re.compile(
    r"(?<!\bpor )(?<!\bcada )"
    r"\b(provincia|municipio|municipalidad|partido|localidad|departamento|ciudad|comuna)"
    r"\s+de\s+"
    r"(?!(?:origen|residencia|nacimiento|destino|procedencia|radicacion|los|las)\b)\w"
)


_EXPLICIT_NATIONAL_RE = re.compile(r"\b(nacional|nacionales|nacion)\b")


def _plain(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text or "")
    return "".join(c for c in decomposed if not unicodedata.combining(c)).lower()


def names_a_place(query: str) -> bool:
    """Si la consulta pide algo de una provincia, ciudad o jurisdicción."""
    plain = " ".join(_plain(query).split())
    return bool(
        _PLACE_RE.search(plain)
        or _AMBIGUOUS_PLACE_RE.search(plain)
        or _JURISDICTION_RE.search(plain)
    )


def asks_for_national(query: str) -> bool:
    """Si la consulta dice explícitamente que quiere el dato nacional."""
    return bool(_EXPLICIT_NATIONAL_RE.search(_plain(query)))


def national_prior(query: str, weight: float | None = None) -> Callable[[SearchResult], float]:
    """El ajuste de puntaje para ``collapse_hits`` según la consulta.

    ``weight`` fija el peso (para calibrar); por defecto, ``NATIONAL_PRIOR`` o
    ``EXPLICIT_NATIONAL_PRIOR`` si la consulta pide lo nacional.
    """
    if names_a_place(query):
        return lambda _hit: 0.0
    if weight is None:
        weight = EXPLICIT_NATIONAL_PRIOR if asks_for_national(query) else NATIONAL_PRIOR
    w = weight
    return lambda hit: w if (hit.portal or "") in NATIONAL_PORTALS else 0.0
