from __future__ import annotations

import logging
import math
import re
import unicodedata
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector

logger = logging.getLogger(__name__)

BASE_URL = "https://apis.datos.gob.ar/series/api"


def source_url(series_ids: list[str]) -> str:
    """El link de «Fuentes»: los datos en la API oficial, en CSV y en orden.

    Antes iba a ``datos.gob.ar/series/api/series/?ids=…``, que redirige a la
    página de la serie en datos.gob.ar. Esa página se cae seguido (Bad gateway en
    prod el 07-oct a la noche) aunque la API responda. El CSV sale de la misma
    API que dio las cifras, con ``;`` y coma decimal para que lo abra bien un
    Excel en castellano.

    Son los valores de la serie, sin representación: ``fetch`` pide la
    variación aparte (``representation_mode``) y los ids llegan sin sufijo.
    ``last=5000`` trae la serie entera hasta 5000 filas y, si es más larga, las
    últimas 5000 en orden cronológico. **No sirve con una representación**
    (``id:percent_change…`` o ``representation_mode``): combinada con ``last``
    la API descarta los períodos más recientes, igual que con ``sort=desc``
    (ver ``fetch``). Por eso los ids que tengan sufijo se mandan sin él.
    """
    ids = ",".join(sid.split(":", 1)[0] for sid in series_ids)
    return f"{BASE_URL}/series/?ids={ids}&format=csv&last=5000&sep=%3B&decimal=%2C"


_IPC_NACIONAL = "148.3_INIVELNAL_DICI_M_26"


def _ipc_region(sid: str, api_description: str, region: str, cubre: str, places: list[str]) -> dict:
    """El IPC de una región del INDEC, de la misma familia que el nacional (base dic-2016)."""
    return {
        "ids": [sid],
        "description": (
            f"IPC {region} del INDEC, nivel general, base dic-2016=100 (mensual, desde 2016). "
            "Con representacion=percent_change da la inflación mensual de la región. Cubre "
            f"{cubre}. El INDEC no publica el IPC por provincia ni por ciudad: para un lugar de "
            "la región, esta es la serie más cercana; dala aclarando que es la de la región. El "
            f"IPC nacional es {_IPC_NACIONAL}."
        ),
        "expected_description": {sid: api_description},
        "keywords": [],
        "places": places,
        "default_collapse": "month",
        "default_representation": "percent_change",
    }


# Catálogo curado de series de la API de Series de Tiempo.
#
# - ``ids``: los ids que se piden juntos.
# - ``description``: lo que ve el agente en ``buscar_series`` ("verificadas").
#   Tiene que describir la serie que de verdad es, no la que se quería: en
#   2026-02 se cargó ``11.3_AGCS_2004_M_41`` como "actividad industrial" y
#   era el EMAE de comercio (verificado contra la API el 04-oct).
# - ``expected_description``: por id, la descripción que da la API
#   (``metadata=full``, ``field.description``). La fija un test contra datos
#   grabados de la API: cambiar un id tiene que ser deliberado.
# - ``keywords``: se comparan sin acentos y por palabra completa
#   (``match_catalog``), nunca como subcadena: "emi" encontraba "emisiones".
# - ``places``: lugares que, junto a una palabra del IPC (``_IPC_WORDS``) en
#   cualquier parte del texto, hacen coincidir la entrada: «inflación Misiones
#   agosto 2026» es el IPC del Noreste. Se comparan sin acentos y sin
#   singularizar («misión» no es Misiones); los que también son palabras
#   comunes van con «en» o «de» delante, o pegados a «inflación» o «IPC»
#   («inflación Salta»), nunca sueltos («precios corrientes», «¿por qué salta
#   la inflación?»). ``_NOT_PLACES`` saca los que pegados son un verbo.
# - ``generic``: la entrada amplia de un tema (el IPC nacional). Se descarta si
#   las palabras con que coincidió están todas dentro de las de otras entradas
#   («inflación mayorista» contiene «inflación»): el 06-oct «inflación
#   mayorista», «IPC GBA» e «inflación Misiones» daban como verificada la
#   serie del IPC nacional, y la del IPIM o la de la región no aparecía. Una
#   frase que también es suya no la cubre: «costo de vida» es el IPC y también
#   la canasta básica, y no es más específica.
# - ``discontinued``: la fuente dejó de actualizar la serie.
# - ``default_collapse`` / ``default_representation``: sólo los usa el
#   pipeline viejo (``pipeline/connectors/series.py``).
SERIES_CATALOG: dict[str, dict] = {
    "presupuesto": {
        "ids": ["451.3_GPNGPN_0_0_3_30"],
        "description": (
            "Gasto público nacional consolidado en millones de pesos (anual, 1980-2023). "
            "Serie discontinuada: la fuente no la actualiza desde 2023. Para el presupuesto "
            "vigente buscá tablas con buscar_datos."
        ),
        "expected_description": {"451.3_GPNGPN_0_0_3_30": "Gasto público nacional"},
        "keywords": [
            "gasto publico",
            "gasto publico nacional",
            "gasto publico consolidado",
        ],
        "discontinued": True,
    },
    "inflacion": {
        "ids": ["148.3_INIVELNAL_DICI_M_26"],
        "description": "IPC Nacional Nivel General (índice base dic-2016=100). Usar con representation=percent_change para variación % mensual.",
        "expected_description": {
            "148.3_INIVELNAL_DICI_M_26": "IPC. Nivel General Nacional. Base dic 2016. Mensual."
        },
        "keywords": [
            "inflacion",
            "ipc",
            "precios",
            "indice de precios",
            "costo de vida",
            # Con una región en el mismo texto, el nacional se pidió por nombre.
            "inflacion nacional",
            "ipc nacional",
        ],
        "generic": True,
        "default_collapse": "month",
        "default_representation": "percent_change",
    },
    "ipim": {
        "ids": ["448.1_NIVEL_GENERAL_0_0_13_46"],
        "description": (
            "IPIM: Índice de Precios Internos al por Mayor del INDEC, nivel general, base "
            "dic-2015=100 (mensual, desde 2015). Es la inflación mayorista, no el IPC: con "
            "representacion=percent_change da la variación % mensual."
        ),
        "expected_description": {"448.1_NIVEL_GENERAL_0_0_13_46": "IPIM Nivel general"},
        "keywords": [
            "ipim",
            "inflacion mayorista",
            "inflacion al por mayor",
            "precios mayoristas",
            "precios al por mayor",
            "indice de precios mayoristas",
            "indice de precios al por mayor",
            "indice de precios internos al por mayor",
        ],
        "default_collapse": "month",
        "default_representation": "percent_change",
    },
    # Los ids se verificaron contra la metadata de la API el 06-oct: agosto de
    # 2026 da 1,75 % en el Noreste (el nacional, 1,66 %).
    "ipc_gba": _ipc_region(
        "148.3_INIVELGBA_DICI_M_21",
        "IPC. Nivel General. GBA. Base dic 2016. Mensual.",
        "del Gran Buenos Aires (GBA)",
        "la Ciudad de Buenos Aires y los 24 partidos del conurbano",
        ["gba", "gran buenos aires", "conurbano"],
    ),
    "ipc_pampeana": _ipc_region(
        "148.3_INIVELANA_DICI_M_26",
        "IPC. Nivel General Región pampeana. Base dic 2016. Mensual.",
        "de la región Pampeana",
        "Córdoba, Entre Ríos, La Pampa, Santa Fe y la provincia de Buenos Aires fuera del GBA",
        ["pampeana", "cordoba", "entre rios", "la pampa", "santa fe"],
    ),
    "ipc_noreste": _ipc_region(
        "148.3_INIVELNEA_DICI_M_21",
        "IPC. Nivel General Región noreste. Base dic 2016. Mensual.",
        "de la región Noreste (NEA)",
        "Chaco, Corrientes, Formosa y Misiones",
        [
            "nea",
            "noreste",
            "nordeste",
            "misiones",
            "chaco",
            "formosa",
            "en corrientes",
            "de corrientes",
            "inflacion corrientes",
            "ipc corrientes",
        ],
    ),
    "ipc_noroeste": _ipc_region(
        "148.3_INIVELNOA_DICI_M_21",
        "IPC. Nivel General Región noroeste. Base dic 2016. Mensual.",
        "de la región Noroeste (NOA)",
        "Catamarca, Jujuy, La Rioja, Salta, Santiago del Estero y Tucumán",
        [
            "noa",
            "noroeste",
            "catamarca",
            "jujuy",
            "la rioja",
            "santiago del estero",
            "tucuman",
            "en salta",
            "de salta",
            "inflacion salta",
            "ipc salta",
        ],
    ),
    "ipc_cuyo": _ipc_region(
        "148.3_INIVELUYO_DICI_M_22",
        "IPC. Nivel General Cuyo. Base dic 2016. Mensual.",
        "de la región Cuyo",
        "Mendoza, San Juan y San Luis",
        ["cuyo", "mendoza", "san juan", "san luis"],
    ),
    "ipc_patagonia": _ipc_region(
        "148.3_INIVELNIA_DICI_M_27",
        "IPC. Nivel General Patagonia. Base dic 2016. Mensual.",
        "de la región Patagonia",
        "Chubut, Neuquén, Río Negro, Santa Cruz y Tierra del Fuego",
        ["patagonia", "chubut", "neuquen", "rio negro", "santa cruz", "tierra del fuego"],
    ),
    "tipo_cambio": {
        "ids": ["92.2_TIPO_CAMBIION_0_0_21_24"],
        "description": (
            "Tipo de cambio de valuación del BCRA, pesos por dólar (diario, desde 2003; los "
            "fines de semana repiten el último dato hábil)"
        ),
        "expected_description": {
            "92.2_TIPO_CAMBIION_0_0_21_24": "Tipo de cambio de valuación (peso por dólar)"
        },
        "keywords": ["dolar", "tipo de cambio", "divisa", "cotizacion"],
        "default_collapse": "month",
    },
    "ipc_regional": {
        "ids": [
            "148.3_INIVELNAL_DICI_M_26",
            "103.1_I2N_2016_M_19",
            "148.3_INIVELNOA_DICI_M_21",
            "145.3_INGCUYUYO_DICI_M_11",
        ],
        "description": (
            "IPC Regional: Nacional, GBA, NOA, y Cuyo (mensual). Las seis regiones del INDEC "
            "(también Pampeana, Noreste y Patagonia) se buscan por su nombre: «IPC Noreste»."
        ),
        "expected_description": {
            "148.3_INIVELNAL_DICI_M_26": "IPC. Nivel General Nacional. Base dic 2016. Mensual.",
            "103.1_I2N_2016_M_19": "IPC-GBA. Nivel General. Base abr 2016. Mensual",
            "148.3_INIVELNOA_DICI_M_21": "IPC. Nivel General Región noroeste. Base dic 2016. Mensual.",
            "145.3_INGCUYUYO_DICI_M_11": "IPC. Nivel General Cuyo. Base dic 2016. Mensual.",
        },
        "keywords": ["ipc regional", "precios regionales", "inflacion regional"],
        "default_collapse": "month",
    },
    # La diaria va ANTES que la mensual: `find_catalog_match` (pipeline viejo)
    # se queda con la primera, y la mensual está parada en la fuente meses
    # atrás (174.1 llega a 2026-04; la diaria, a 2026-08-31).
    "reservas_diarias": {
        "ids": ["92.2_RESERVAS_IRES_0_0_32_40"],
        "description": (
            "Reservas internacionales del BCRA, saldo diario en millones de dólares (desde "
            "2003). Es la que llega más lejos: para el saldo a fin de cada mes, "
            "frecuencia=month con agregacion=end_of_period."
        ),
        "expected_description": {
            "92.2_RESERVAS_IRES_0_0_32_40": "Reservas internacionales del BCRA, en millones de dólares"
        },
        "keywords": [
            "reservas",
            "reservas internacionales",
            "bcra reservas",
            "reservas bcra",
            "dolares bcra",
            "reservas del banco central",
        ],
    },
    "reservas": {
        "ids": ["174.1_RRVAS_IDOS_0_0_36"],
        "description": (
            "Reservas internacionales del BCRA, saldo mensual en millones de dólares (desde "
            "1940). La fuente la actualiza con meses de atraso: para el dato más reciente usá "
            "la diaria 92.2_RESERVAS_IRES_0_0_32_40."
        ),
        "expected_description": {"174.1_RRVAS_IDOS_0_0_36": "Reservas Internacionales BCRA Saldos"},
        "keywords": [
            "reservas",
            "reservas internacionales",
            "bcra reservas",
            "reservas bcra",
            "dolares bcra",
            "reservas del banco central",
        ],
        "default_collapse": "month",
    },
    "base_monetaria": {
        "ids": ["331.1_SALDO_BASERIA__15"],
        "description": "Base monetaria — saldo en millones de pesos (mensual)",
        "expected_description": {"331.1_SALDO_BASERIA__15": "Saldo de la Base Monetaria"},
        "keywords": [
            "base monetaria",
            "emision monetaria",
            "emision de pesos",
            "dinero en circulacion",
            "masa monetaria",
            "agregados monetarios",
        ],
        "default_collapse": "month",
    },
    "leliq_pases": {
        "ids": ["331.1_PASES_REDELIQ_M_MONE_0_24_24"],
        "description": (
            "Pases y redescuentos: LELIQ, como factor de explicación de la variación de la base "
            "monetaria, en millones de pesos (mensual; vale 0 desde que se eliminaron las "
            "LELIQ). No es la tasa de política monetaria ni el stock de LELIQ."
        ),
        "expected_description": {
            "331.1_PASES_REDELIQ_M_MONE_0_24_24": "Pases y Redescuentos: Leliq"
        },
        "keywords": [
            "leliq",
            "pases",
            "letras de liquidez",
            "pases pasivos",
        ],
        "default_collapse": "month",
    },
    "emae": {
        "ids": ["143.3_NO_PR_2004_A_21"],
        "description": "EMAE — Estimador Mensual de Actividad Económica, índice base 2004 (mensual, desde 2004)",
        "expected_description": {"143.3_NO_PR_2004_A_21": "EMAE. Base 2004"},
        "keywords": [
            "emae",
            "actividad economica",
            "pbi mensual",
            "crecimiento economico",
            "recesion",
            "producto bruto",
        ],
        "default_collapse": "month",
    },
    "desempleo": {
        "ids": ["45.2_ECTDT_0_T_33"],
        "description": (
            "Tasa de desempleo total (trimestral, desde 2003). series_tiempo la devuelve en %: "
            "7.9 es 7,9 % (la API la da como fracción y se escala)."
        ),
        "expected_description": {"45.2_ECTDT_0_T_33": "Tasa de desempleo total. En porcentaje."},
        "keywords": [
            "desempleo",
            "desocupacion",
            "tasa de desempleo",
            "tasa de desocupacion",
            "mercado laboral",
        ],
    },
    "salarios": {
        "ids": ["149.1_TL_INDIIOS_OCTU_0_21"],
        "description": "Índice de Salarios nivel general, base oct-2016=100 (mensual)",
        "expected_description": {"149.1_TL_INDIIOS_OCTU_0_21": "Índice de Salarios"},
        "keywords": [
            "salarios",
            "sueldos",
            "indice de salarios",
            "remuneraciones",
            "salario real",
            "paritarias",
        ],
        "default_collapse": "month",
    },
    "canasta_basica": {
        "ids": ["150.1_LA_POBREZA_0_D_13"],
        "description": "Canasta Básica Total (CBT) / Línea de pobreza por adulto equivalente en pesos (mensual, desde 2016)",
        "expected_description": {
            "150.1_LA_POBREZA_0_D_13": "Línea de pobreza desde 2016. Pesos corrientes."
        },
        "keywords": [
            "canasta basica",
            "canasta basica total",
            "cbt",
            "linea de pobreza",
            "costo de vida",
        ],
        "default_collapse": "month",
    },
    "canasta_alimentaria": {
        "ids": ["150.1_LA_INDICIA_0_D_16"],
        "description": "Canasta Básica Alimentaria (CBA) / Línea de indigencia por adulto equivalente en pesos (mensual, desde 2016)",
        "expected_description": {
            "150.1_LA_INDICIA_0_D_16": "Línea de indigencia desde 2016. Pesos corrientes."
        },
        "keywords": [
            "canasta alimentaria",
            "canasta basica alimentaria",
            "cba",
            "linea de indigencia",
            "alimentos basicos",
        ],
        "default_collapse": "month",
    },
    "exportaciones": {
        "ids": ["74.3_IET_0_M_16"],
        "description": "Exportaciones totales en millones de dólares (mensual, desde 1992)",
        "expected_description": {
            "74.3_IET_0_M_16": "Exportaciones totales. En millones de dólares."
        },
        "keywords": ["exportaciones", "expo", "ventas externas", "comercio exterior"],
        "default_collapse": "month",
    },
    "importaciones": {
        "ids": ["74.3_IIT_0_M_25"],
        "description": "Importaciones totales en millones de dólares (mensual, desde 1992)",
        "expected_description": {
            "74.3_IIT_0_M_25": "Importaciones totales. En millones de dólares."
        },
        "keywords": ["importaciones", "impo", "compras externas"],
        "default_collapse": "month",
    },
    "balanza_comercial": {
        "ids": ["74.3_IET_0_M_16", "74.3_IIT_0_M_25"],
        "description": "Balanza comercial: exportaciones e importaciones totales en millones de dólares (mensual)",
        "expected_description": {
            "74.3_IET_0_M_16": "Exportaciones totales. En millones de dólares.",
            "74.3_IIT_0_M_25": "Importaciones totales. En millones de dólares.",
        },
        "keywords": [
            "balanza comercial",
            "saldo comercial",
            "comercio exterior",
            "intercambio comercial",
        ],
        "default_collapse": "month",
    },
    "actividad_industrial": {
        "ids": ["453.1_SERIE_ORIGNAL_0_0_14_46"],
        "description": (
            "Índice de Producción Industrial manufacturero (IPI) del INDEC, nivel general, "
            "serie original (mensual, desde 2016)"
        ),
        "expected_description": {
            "453.1_SERIE_ORIGNAL_0_0_14_46": "IPI Nivel General Serie Original"
        },
        "keywords": [
            "industria",
            "industria manufacturera",
            "produccion industrial",
            "actividad industrial",
            "manufactura",
            "ipi",
            "emi",
            "fabrica",
        ],
        "default_collapse": "month",
    },
    "emae_comercio": {
        "ids": ["11.3_AGCS_2004_M_41"],
        "description": (
            "EMAE: comercio mayorista, minorista y reparaciones, índice base 2004=100 "
            "(mensual, desde 2004)"
        ),
        "expected_description": {
            "11.3_AGCS_2004_M_41": "EMAE. Comercio mayorista y minorista y reparaciones"
        },
        "keywords": [
            "comercio mayorista",
            "comercio minorista",
            "actividad comercial",
            "emae comercio",
        ],
        "default_collapse": "month",
    },
    # Prueba de calidad del 07-oct (nueva_10): con «EPH» en el texto, la
    # /search de la API nunca trae la trimestral del total; casi siempre, sólo
    # la anual 42.1_EPAT_0_A_27 (hasta 2025). El modelo leyó esa anual y una
    # copia guardada de la trimestral que terminaba en el 1.er trimestre de
    # 2026, y lo dio como el último dato (el 05-oct, igual). La anual promedia
    # los cuatro trimestres (2025: 48,38). Va al final: el pipeline viejo toma
    # la primera entrada.
    # «tasa de actividad» coincide también con «tasa de actividad en Córdoba»
    # (revisión de #173): la descripción dice que es el total nacional, porque
    # la /search trae primero la de Gran Córdoba y quedaba debajo de esta.
    # Sacar la entrada cuando aparece un lugar o un grupo lo decide Lucho.
    "actividad": {
        "ids": ["43.2_ECTAT_0_T_33"],
        "description": (
            "Tasa de actividad total de la EPH continua del INDEC (total de aglomerados "
            "urbanos), trimestral desde 2003: la población económicamente activa como % de la "
            "población. series_tiempo la devuelve en %: 45.6 es 45,6 % (la API la da como "
            "fracción y se escala). Su último trimestre es el último dato de la EPH; la anual "
            "42.1_EPAT_0_A_27 promedia los trimestres y llega sólo al último año completo. Es "
            "el total nacional: no es la de una provincia, región o aglomerado, ni la de un "
            "grupo (mujeres, varones, una edad); para esos casos no sirve: usá la de 'series' "
            "que mida eso."
        ),
        "expected_description": {"43.2_ECTAT_0_T_33": "Tasa de actividad total. En porcentaje."},
        "keywords": ["tasa de actividad"],
    },
    # nueva_11 (07-oct): para «producto bruto interno» la única verificada era
    # el EMAE y la primera de la /search, 166.2_PPIB_0_0_3, el PIB a precios
    # corrientes, parado en el 4.º trimestre de 2025 y desactualizado según la
    # fuente; las corridas del 05, 06 y 07-oct la leyeron primero. Cada
    # trimestre de la 4.2 está en valor anualizado: el promedio de 2004 da
    # 485.115, el PIB de ese año, y el de 2025 (739.728,7) es la anual
    # 9.1_PP2_2004_A_16. Sumados, el nivel da cuatro veces el PIB (la
    # variación es la misma). Al final, por el pipeline viejo: «producto
    # bruto» sigue siendo primero del EMAE.
    # «pbi» y «pib» sueltos coinciden también con «PIB de Brasil», «PBI per
    # cápita» o «gasto en educación como % del PBI» (revisión de #173): la
    # descripción dice que es el total nacional y para qué no sirve. Sacar la
    # entrada en esos casos lo decide Lucho.
    "pbi": {
        "ids": ["4.2_OGP_2004_T_17"],
        "description": (
            "PIB a precios constantes de 2004 del INDEC (Oferta y Demanda Globales), en "
            "millones de pesos de 2004, trimestral desde 2004: el PBI real, cuya variación es "
            "el crecimiento de la economía. Cada trimestre viene en valor anualizado: el PIB de "
            "un año es el promedio de sus trimestres, no la suma. El crecimiento de cada año: "
            "frecuencia=year con representacion=percent_change. La interanual de cada "
            "trimestre: representacion=percent_change_a_year_ago. No es el EMAE (mensual) ni "
            "166.2_PPIB_0_0_3, que es el PIB a precios corrientes. Es el total nacional de la "
            "Argentina: no es per cápita, ni el de una provincia o región (el PBG), ni el de "
            "otro país, ni un cociente como el «% del PBI»; para esos casos no sirve: usá la "
            "de 'series' que mida eso."
        ),
        "expected_description": {
            "4.2_OGP_2004_T_17": (
                "PIB a precios de comprador, en millones de pesos de 2004 y Trimestral."
            )
        },
        "keywords": ["pbi", "pib", "producto bruto interno", "producto interno bruto"],
    },
}


def _strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


_WORD_RE = re.compile(r"[a-z0-9]+")


def _stem(word: str) -> str:
    """Un singular aproximado, igual para las dos puntas de la comparación.

    Alcanza para que «exportación» encuentre «exportaciones» y «dólar»
    encuentre «dólares», sin diccionario.
    """
    if len(word) > 4 and word.endswith(("ones", "res", "les", "des", "nes")):
        return word[:-2]
    if len(word) > 3 and word.endswith("s"):
        return word[:-1]
    return word


def _words(text: str) -> list[str]:
    return _WORD_RE.findall(_strip_accents(text.lower()))


def _tokens(text: str) -> list[str]:
    return [_stem(w) for w in _words(text)]


def _positions(words: list[str], phrase: tuple[str, ...]) -> set[int]:
    """Las posiciones de ``words`` que ocupa ``phrase``, en todas sus apariciones."""
    n = len(phrase)
    found: set[int] = set()
    for i in range(len(words) - n + 1):
        if n and tuple(words[i : i + n]) == phrase:
            found.update(range(i, i + n))
    return found


# Palabras clave tokenizadas una vez, en el orden del catálogo.
_CATALOG_NORMALIZED: list[tuple[tuple[str, ...], str, dict]] = [
    (tuple(_tokens(kw)), key, entry)
    for key, entry in SERIES_CATALOG.items()
    for kw in entry["keywords"]
]

# Cómo se nombra al IPC junto a un lugar (``places``). Sin «costo de vida»,
# que también es la canasta básica.
_IPC_WORDS: list[tuple[str, ...]] = [
    tuple(_tokens(w)) for w in ("inflacion", "ipc", "precios", "indice de precios")
]
_PLACES_NORMALIZED: list[tuple[tuple[str, ...], str]] = [
    (tuple(_words(place)), key)
    for key, entry in SERIES_CATALOG.items()
    for place in entry.get("places", [])
]
# Un lugar pegado a la palabra del IPC que en realidad es un verbo.
_NOT_PLACES: list[tuple[str, ...]] = [
    tuple(_words(text)) for text in ("la inflacion salta", "el ipc salta")
]
# Las frases de una entrada ``generic``: en otra entrada no la cubren.
_GENERIC_PHRASES: set[tuple[str, ...]] = {
    phrase for phrase, _key, entry in _CATALOG_NORMALIZED if entry.get("generic")
}


def match_catalog(query: str) -> list[dict]:
    """Las entradas del catálogo con alguna palabra clave entera en el texto.

    Sin acentos y por palabra completa: «inflación» encuentra la inflación,
    pero «emisiones» ya no encuentra la base monetaria ni «cambio climático»
    el tipo de cambio. Una entrada con ``places`` coincide con una palabra del
    IPC y uno de sus lugares en cualquier parte del texto, y la ``generic``
    se descarta si las palabras con que coincidió están todas en frases de
    otras entradas que no son suyas: «inflación mayorista» es el IPIM y no
    también el IPC nacional, y «costo de vida» es los dos, el IPC y la
    canasta. En el orden del catálogo, sin repetir.
    """
    raw = _words(query)
    words = [_stem(w) for w in raw]
    covered: dict[str, set[int]] = {}
    # Lo que cada entrada cubre con frases que la genérica no tiene: «costo de
    # vida» es del IPC y de la canasta, y no descarta al IPC.
    specific: dict[str, set[int]] = {}
    for phrase, key, _entry in _CATALOG_NORMALIZED:
        if at := _positions(words, phrase):
            covered.setdefault(key, set()).update(at)
            if phrase not in _GENERIC_PHRASES:
                specific.setdefault(key, set()).update(at)
    ipc_at = set().union(*(_positions(words, phrase) for phrase in _IPC_WORDS))
    if ipc_at:
        not_places = set().union(*(_positions(raw, phrase) for phrase in _NOT_PLACES))
        for place, key in _PLACES_NORMALIZED:
            if (at := _positions(raw, place)) and not at <= not_places:
                covered.setdefault(key, set()).update(at | ipc_at)
                specific.setdefault(key, set()).update(at | ipc_at)
    for key in [k for k in covered if SERIES_CATALOG[k].get("generic")]:
        others = set().union(*(at for k, at in specific.items() if k != key))
        if covered[key] <= others:
            del covered[key]
    return [entry for key, entry in SERIES_CATALOG.items() if key in covered]


def find_catalog_match(query: str) -> dict | None:
    """La primera entrada del catálogo que corresponde al texto (pipeline viejo)."""
    matches = match_catalog(query)
    return matches[0] if matches else None


def catalog_mismatches(api_descriptions: Mapping[str, str | None]) -> list[str]:
    """Ids del catálogo cuya descripción en la API no es la esperada.

    ``api_descriptions`` va de id a ``field.description`` (``metadata=full``).
    Lo usa el test contra datos grabados; sirve igual para un chequeo en vivo.
    """
    problems: list[str] = []
    for key, entry in SERIES_CATALOG.items():
        expected = entry.get("expected_description") or {}
        for sid in entry["ids"]:
            if sid not in expected:
                problems.append(f"{key}: {sid} no tiene expected_description")
                continue
            if sid not in api_descriptions:
                problems.append(f"{key}: {sid} no está en la API")
                continue
            actual = api_descriptions[sid]
            if actual != expected[sid]:
                problems.append(f"{key}: {sid} es «{actual}», no «{expected[sid]}»")
    return problems


# ── pedidos a /series ──────────────────────────────────────

# La documentación dice que `limit` llega a 1000; la API en vivo acepta hasta
# 5000 (con 10000 responde 400). Las páginas nunca bajan de 500 filas: con una
# representación, `count` cuenta las observaciones ANTES de transformar y el
# offset `start` se aplica DESPUÉS (la interanual mensual pierde las primeras
# 12), así que una página chica pedida en `count − página` puede caer más allá
# de la última fila transformada y volver vacía.
_PAGE_MIN = 500
_PAGE_MAX = 5000

# Con `desde` y una representación, la API calcula la variación DENTRO de la
# ventana pedida: la interanual con start_date=2026-07-01 vuelve vacía y la
# mensual pierde el primer mes. Se pide desde 13 meses antes (cubre la
# interanual, la mensual y la acumulada en el año de cualquier frecuencia) y
# se recorta acá.
_LOOKBACK_MONTHS = 13

# La API tiene `percent_change_since_beginning_of_year`, pero la calcula contra
# ENERO del mismo año, no contra el cierre del anterior: para agosto de 2026 da
# 17,90 % y la acumulada que publica el INDEC (agosto contra diciembre de 2025)
# es 21,30 %. Se pierde la variación de enero. Se calcula acá sobre los
# valores (medido el 04-oct; en 2025 la de la API daba 16,90 % y el INDEC
# publicó 19,5 %).
YEAR_TO_DATE = "percent_change_since_beginning_of_year"
_YEAR_TO_DATE_UNITS = "Variación porcentual acumulada en el año (contra el cierre del año anterior)"

# Series cuyas unidades dicen «Porcentaje» pero que la API da como fracción
# (desempleo 0,079 = 7,9 %). Se escalan ×100 en modo valor y en `change`
# (diferencia en puntos porcentuales). La detección es por familia de series
# (`_is_fraction_percent`); esta lista, verificada contra la API el 05-oct
# (descripción «… En porcentaje.», máximo histórico 0,204), sólo asegura el
# desempleo cuando la metadata no trae el rango de la serie. `is_percentage`
# no sirve para detectarlas: 174.1_T_INTERUS_0_0_43 también lo trae en True y
# ya viene ×100.
FRACTION_PERCENT_IDS = frozenset(
    {
        "45.2_ECTDT_0_T_33",  # desempleo, total nacional
        "45.2_ECTDTG_0_T_37",  # GBA
        "45.2_ECTDTNO_0_T_42",  # NOA
        "45.2_ECTDTNE_0_T_42",  # NEA
        "45.2_ECTDTCU_0_T_38",  # Cuyo
        "45.2_ECTDTRP_0_T_49",  # Pampeana
        "45.2_ECTDTP_0_T_43",  # Patagonia
    }
)

# Una tasa dada como fracción no pasa de 1,5 (150 %); un porcentaje ya
# multiplicado por 100 pasa, al menos alguna vez en su historia.
_FRACTION_MAX = 1.5

# Familias de series que la API da como fracción con unidades «Porcentaje…»,
# verificadas una por una contra la metadata de la API el 06-oct: la EPH 42.x
# a 48.x (actividad, empleo, desempleo, subocupación), la pobreza y la
# indigencia 60.x a 64.x («Porcentaje de hogares» y «de población») y las
# tasas por aglomerado de 1974-2003, 341.1 a 344.1. Afuera no se escala
# aunque todo el rango quepa en ±1,5: la ciencia y técnica en % del PIB
# 451.2_GPC_CIENCIPIB_0_0_23_49 va de 0,18 a 0,32 y ya está en % (0,27 % del
# PIB en 2023), y la tasa overnight de Japón 131.1_OIRJT_0_0_34, de −0,1 a
# 0,75, también. Escaladas, salían 26,74 y 75,0 rotuladas como %.
_FRACTION_FAMILIES = re.compile(r"(?:4[2-8]|6[0-4])\.\d+_|34[1-4]\.1_")

_FREQUENCY_NAMES = {
    "day": "diaria",
    "week": "semanal",
    "month": "mensual",
    "quarter": "trimestral",
    "semester": "semestral",
    "year": "anual",
}
_ISO_FREQUENCY_NAMES = {
    "R/P1D": "diaria",
    "R/P1W": "semanal",
    "R/P1M": "mensual",
    "R/P3M": "trimestral",
    "R/P6M": "semestral",
    "R/P1Y": "anual",
}
# De la más fina a la más gruesa.
_FREQUENCY_ORDER = {name: i for i, name in enumerate(_FREQUENCY_NAMES.values())}

_DATE_RE = re.compile(r"(\d{4})(?:-(\d{1,2}))?(?:-(\d{1,2}))?")


def iso_date(text: str | None) -> str | None:
    """'2020', '2020-03' o '2020-03-15' → fecha ISO completa; None si no es fecha."""
    if not text:
        return None
    match = _DATE_RE.fullmatch(str(text).strip())
    if not match:
        return None
    year, month, day = int(match.group(1)), int(match.group(2) or 1), int(match.group(3) or 1)
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _months_back(iso: str, months: int) -> str:
    year, month = int(iso[:4]), int(iso[5:7])
    total = year * 12 + (month - 1) - months
    return date(total // 12, total % 12 + 1, 1).isoformat()


_PERIOD_MONTHS = {"mensual": 1, "trimestral": 3, "semestral": 6, "anual": 12}
# Días que puede haber entre el último dato y `hasta` sin que la serie haya
# terminado antes: en una diaria, un fin de semana largo (el mismo margen que
# `_infer_frequency` en data_age); en una semanal, lo que falta para la
# semana siguiente.
_END_SLACK_DAYS = {"diaria": 4, "semanal": 6}


def _reaches_end(last: str, end: str, frequency: str | None) -> bool:
    """¿El período del último dato llega a `end`? Si llega, la ventana pudo cortar la serie.

    La API devuelve las filas fechadas hasta `end_date`, y cada período va
    fechado por su primer día: si el siguiente empieza antes de `end`, la API
    lo habría traído, así que la serie termina en `last`. Sin frecuencia
    conocida no se sabe y se supone que sí.
    """
    months = _PERIOD_MONTHS.get(frequency or "")
    try:
        if months:
            # El primer día del período siguiente.
            return _months_back(last, -months) > end
        slack = _END_SLACK_DAYS.get(frequency or "")
        if slack is None:
            return True
        return (date.fromisoformat(end) - date.fromisoformat(last)).days <= slack
    except ValueError:
        return True


def _period_end(text: str | None) -> str | None:
    """El último día del período que nombra un `hasta`, como lo lee la API.

    `end_date=2025` trae todo 2025 y `end_date=2025-06` llega al 30 de junio
    (medido el 06-oct).
    """
    match = _DATE_RE.fullmatch(str(text or "").strip())
    if not match:
        return None
    year = int(match.group(1))
    if match.group(2) is None:
        return f"{year}-12-31"
    if match.group(3) is not None:
        return iso_date(text)
    try:
        following = date.fromisoformat(_months_back(f"{year:04d}-{int(match.group(2)):02d}-01", -1))
    except ValueError:
        return None
    return date.fromordinal(following.toordinal() - 1).isoformat()


# Pobreza e indigencia de la EPH continua (63.2 y 64.2, las 78 series medidas
# el 06-oct): la API fecha cada semestre por el día siguiente a su fin, un
# semestre más tarde que la fuente y que su propia metadata. El CSV de la
# fuente (64.2) fecha el 1er semestre de 2024, 52,9 %, en `2024-01-01`; la API
# lo devuelve en `2024-07-01`. La metadata coincide con la fuente:
# time_index_end 2026-01-01 con last_value 0,231 (Gran Rosario, 1er semestre
# de 2026), y la API trae ese 0,231 en la fila `2026-07-01`. No es una
# metadata atrasada. Leídas por su primer día, todas las filas quedaban
# corridas un semestre; con `hasta` no quedaba ni la última para notarlo
# (pobreza de 2025 daba 2S-2024 y 1S-2025 rotulados 2025-S1 y 2025-S2). Las
# semestrales viejas (61.1, 62.1: time_index_end 2003-05-01, fila
# `2003-01-01`) están fechadas por el inicio: no se tocan.
_SEMESTER_ISO = "R/P6M"


def _semester_start(iso: str) -> str:
    return f"{iso[:4]}-{'01' if int(iso[5:7]) <= 6 else '07'}-01"


def _fields_by_id(raw: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for m in (raw.get("meta") or [])[1:]:
        field = m.get("field") if isinstance(m, Mapping) else None
        if isinstance(field, Mapping) and field.get("id"):
            out[str(field["id"])] = field
    return out


def _native_semesters(raw: Mapping[str, Any], series_ids: list[str]) -> bool:
    """¿Vino en semestres y alguna de las series pedidas es semestral en la fuente?

    Las demás pueden ser de otra frecuencia (el desempleo trimestral, que la
    API promedia a semestres): cada columna se confirma aparte.
    """
    meta = raw.get("meta") or []
    if not meta or not isinstance(meta[0], Mapping) or meta[0].get("frequency") != "semester":
        return False
    fields = _fields_by_id(raw)
    return any((fields.get(sid) or {}).get("frequency") == _SEMESTER_ISO for sid in series_ids)


def _dated_one_semester_late(raw: Mapping[str, Any], series_ids: list[str]) -> set[int]:
    """Las columnas (posición en `series_ids`) que la API fechó un semestre después que su metadata.

    Serie por serie, sobre la serie entera y en valores: la última fila con
    dato de esa columna tiene que caer un semestre después de su
    time_index_end Y tener su last_value. No pasan una metadata atrasada de
    verdad (su last_value sería el del semestre anterior), una semestral
    fechada por el inicio (la 64.1 de 2001-2003) ni las de otra frecuencia:
    quedan como vienen. Se exigía que TODAS lo confirmaran, y la 64.2 pedida
    junto con la 64.1 o con el desempleo trimestral salía corrida (revisión
    de #166).
    """
    if not _native_semesters(raw, series_ids):
        return set()
    fields = _fields_by_id(raw)
    data = raw.get("data") or []
    late: set[int] = set()
    for idx, sid in enumerate(series_ids):
        field = fields.get(sid) or {}
        if field.get("frequency") != _SEMESTER_ISO:
            continue
        end = iso_date(str(field.get("time_index_end") or "")[:10])
        last_value = _as_float(field.get("last_value"))
        last = next(
            (row for row in reversed(data) if idx + 1 < len(row) and row[idx + 1] is not None),
            None,
        )
        if end is None or last_value is None or last is None:
            continue
        value = _as_float(last[idx + 1])
        if str(last[0])[:10] != _months_back(_semester_start(end), -6):
            continue
        if value is None or not math.isclose(value, last_value, rel_tol=1e-9, abs_tol=1e-12):
            continue
        late.add(idx)
    return late


def _back_one_semester(rows: list[list[Any]], late: set[int]) -> list[list[Any]]:
    """Retrocede un semestre los valores de las columnas `late` y realinea las filas.

    Las demás columnas quedan en su fecha: en la fila de un semestre van los
    valores de ESE semestre de todas las series. Un semestre sin ningún dato
    queda si la API lo mandó para todas las columnas (la 64.2 no tiene 2007
    a 2016 y la API manda esas filas vacías); uno que queda vacío sólo por
    el corrimiento (el último, con la pobreza junto al desempleo) no.
    """
    width = max((len(r) for r in rows), default=1) - 1
    by_date: dict[str, list[Any]] = {}
    seen: dict[str, set[int]] = {}
    for row in rows:
        fecha = str(row[0])[:10]
        for i in range(width):
            target = _months_back(fecha, 6) if i in late else fecha
            seen.setdefault(target, set()).add(i)
            value = row[i + 1] if i + 1 < len(row) else None
            if value is not None:
                by_date.setdefault(target, [None] * width)[i] = value
    for target, columns in seen.items():
        if len(columns) == width:
            by_date.setdefault(target, [None] * width)
    return [[fecha, *values] for fecha, values in sorted(by_date.items())]


def _missing_series(exc: Exception) -> list[str]:
    """Los ids que la API dice que no existen (400 «Serie inexistente: …»).

    Nombra sólo el primero aunque falten varios (medido el 06-oct).
    """
    response = getattr(exc, "response", None)
    if not isinstance(response, httpx.Response) or response.status_code != 400:
        return []
    try:
        body = response.json()
    except ValueError:
        return []
    failed = body.get("failed_series") if isinstance(body, dict) else None
    return [str(s) for s in failed] if isinstance(failed, list) else []


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    return None


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_fraction_percent(sid: str, field: Mapping[str, Any], values: list[float]) -> bool:
    """¿Unidades «Porcentaje» con los valores como fracción (0,489 = 48,9 %)?

    Por familia y no por lista: con la lista sólo se escalaba el desempleo, y
    actividad, empleo, subocupación y pobreza llegaban como 0,489 «Porcentaje».

    - El id es de una familia verificada como fracción (``_FRACTION_FAMILIES``).
      Las unidades y el rango solos no alcanzan: el gasto en % del PIB y la
      tasa de Japón dicen «Porcentaje…», caben en ±1,5 y ya están en %.
    - Las unidades empiezan con «Porcentaje» («de hogares», «de población»),
      salvo las que aclaran «(0-100)», como la BADLAR.
    - Control de magnitud: el rango de TODA la serie (``min_value`` y
      ``max_value`` de la metadata, que no cambian con la ventana, la
      representación ni el collapse; medido el 06-oct) y los valores traídos
      caben en ±1,5. La tasa de plazo fijo en dólares 174.1_T_INTERUS_0_0_43
      dice «Porcentaje», vale 1,04 en 2026-04 y ya viene en %: en 2001 llegó a
      13,75. Sin el rango no se escala.

    Los ids de FRACTION_PERCENT_IDS no necesitan las unidades ni el rango,
    pero tampoco se escalan si sus valores ya están en %.
    """
    if any(abs(v) > _FRACTION_MAX for v in values):
        return False
    low, high = _as_float(field.get("min_value")), _as_float(field.get("max_value"))
    if low is not None and high is not None and max(abs(low), abs(high)) > _FRACTION_MAX:
        return False
    if sid in FRACTION_PERCENT_IDS:
        return True
    units = _strip_accents(str(field.get("units") or "")).strip().lower()
    return (
        low is not None
        and high is not None
        and _FRACTION_FAMILIES.match(sid) is not None
        and units.startswith("porcentaje")
        and "0-100" not in units
    )


def _year_to_date(rows: list[list[Any]]) -> list[list[Any]]:
    """Variación acumulada en el año, como fracción: valor / último del año anterior − 1.

    Las filas sin dato del año anterior (el primer año de lo traído) quedan
    afuera, como hace la API con las primeras filas de cualquier variación.
    """
    width = max((len(r) for r in rows), default=1) - 1
    last_by_year: list[dict[int, float]] = [{} for _ in range(width)]
    out: list[list[Any]] = []
    for row in rows:
        year = int(str(row[0])[:4])
        new_row: list[Any] = [row[0]]
        for i in range(width):
            value = row[i + 1] if i + 1 < len(row) else None
            base = last_by_year[i].get(year - 1)
            new_row.append(value / base - 1 if value is not None and base else None)
            if value is not None:
                last_by_year[i][year] = value
        if any(v is not None for v in new_row[1:]):
            out.append(new_row)
    return out


class SeriesTiempoAdapter(ISeriesTiempoConnector):
    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self._http = http_client

    async def search(self, query: str, limit: int = 10) -> list[dict]:
        try:
            resp = await self._http.get(
                f"{BASE_URL}/search/",
                params={"q": query, "limit": limit},
            )
            resp.raise_for_status()
            data = resp.json()
            if not data.get("data"):
                return []
            return [
                {
                    "id": item["field"]["id"],
                    "title": item["field"].get("title") or item["field"].get("description", ""),
                    "description": item["field"].get("description", ""),
                    "units": item["field"].get("units", ""),
                    "frequency": item["field"].get("frequency", ""),
                    "time_index_start": item["field"].get("time_index_start", ""),
                    "time_index_end": item["field"].get("time_index_end", ""),
                    "dataset_title": item["dataset"].get("title", ""),
                    "source": item["dataset"].get("source", ""),
                }
                for item in data["data"]
            ]
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
                details={"query": query[:100], "reason": str(exc)},
            ) from exc

    async def _get_series(self, params: dict[str, str]) -> dict[str, Any]:
        resp = await self._http.get(f"{BASE_URL}/series/", params=params)
        resp.raise_for_status()
        return resp.json() or {}

    async def fetch(
        self,
        series_ids: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        collapse: str | None = None,
        representation: str | None = None,
        limit: int = 1000,
        collapse_aggregation: str | None = None,
    ) -> DataResult | None:
        """Las observaciones MÁS RECIENTES del rango pedido, en orden ascendente.

        La API ordena ascendente y corta en `limit`: en una serie de más
        observaciones que la página, el primer pedido trae las más viejas
        (reservas 174.1 terminaba en 2023-04, el tipo de cambio diario en
        2005-09). Si `count` dice que hay más, se vuelve a pedir la cola con
        `start = count − página`, en el MISMO orden ascendente. Nunca
        `sort=desc` ni `last`: combinados con una representación, la API
        descarta los períodos más recientes (la mensual pierde el último mes y
        la interanual los últimos 12).
        """
        try:
            page = min(max(limit, _PAGE_MIN), _PAGE_MAX)
            start_iso = iso_date(start_date)
            end_iso = iso_date(end_date)
            query_start = start_date
            trim_before: str | None = None
            if representation and start_iso:
                query_start = _months_back(start_iso, _LOOKBACK_MONTHS)
                trim_before = start_iso
            local_ytd = representation == YEAR_TO_DATE

            params: dict[str, str] = {
                "ids": ",".join(series_ids),
                "format": "json",
                "limit": str(page),
                "metadata": "full",
            }
            if query_start:
                params["start_date"] = query_start
            if end_date:
                params["end_date"] = end_date
            if representation and not local_ytd:
                params["representation_mode"] = representation
            if collapse:
                params["collapse"] = collapse
                if collapse_aggregation:
                    params["collapse_aggregation"] = collapse_aggregation

            raw = await self._get_series(params)
            data = raw.get("data") or []
            total = _as_int(raw.get("count"))
            tail_start = 0
            if total is not None and len(data) >= page and total > len(data):
                tail_start = max(0, total - page)
                raw = await self._get_series({**params, "start": str(tail_start)})
                data = raw.get("data") or []
                if not data and tail_start > 0:
                    # La representación se comió más filas que la página:
                    # se retrocede una página más (ver _PAGE_MIN).
                    tail_start = max(0, tail_start - page)
                    raw = await self._get_series({**params, "start": str(tail_start)})
                    data = raw.get("data") or []

            # Pobreza e indigencia semestrales (ver _dated_one_semester_late):
            # la serie entera son unas 50 filas. Se confirma el corrimiento
            # serie por serie con la metadata, se fechan los semestres de las
            # confirmadas por su primer día, como la fuente, y la ventana
            # pedida se aplica sobre esas fechas. Las demás columnas (otra
            # semestral fechada por el inicio, el desempleo trimestral que la
            # API promedia a semestres) quedan en su fecha.
            if _native_semesters(raw, series_ids):
                plain = {
                    k: v
                    for k, v in params.items()
                    if k not in ("start_date", "end_date", "representation_mode")
                }
                whole = raw if plain == params else await self._get_series(plain)
                late = _dated_one_semester_late(whole, series_ids)
                if late:
                    if "representation_mode" in params:
                        whole = await self._get_series(
                            {**plain, "representation_mode": params["representation_mode"]}
                        )
                    window_start = iso_date(query_start)
                    window_end = _period_end(end_date)
                    raw = whole
                    data = [
                        row
                        for row in _back_one_semester(whole.get("data") or [], late)
                        if (window_start is None or row[0] >= window_start)
                        and (window_end is None or row[0] <= window_end)
                    ]
                    total = len(data)
                    tail_start = 0

            if not data:
                return None

            # Se perdió el principio de lo pedido sólo si la cola arranca
            # después de `desde` (o si no había `desde`: falta el comienzo
            # de la serie).
            truncated = tail_start > 0 and (start_iso is None or str(data[0][0])[:10] > start_iso)
            if local_ytd:
                data = _year_to_date(data)
            if trim_before:
                data = [row for row in data if str(row[0])[:10] >= trim_before]
            if not data:
                return None

            # La última fila con dato de cada serie: el fin de la fuente nunca
            # queda antes de un dato traído (H065). En la pobreza 64.2 no era
            # una metadata atrasada sino las filas corridas un semestre (ver
            # _dated_one_semester_late); ya fechadas como la fuente, coinciden.
            last_by_id: dict[str, str] = {}
            for idx, sid in enumerate(series_ids):
                for row in reversed(data):
                    if idx + 1 < len(row) and row[idx + 1] is not None:
                        last_by_id[sid] = str(row[0])[:10]
                        break

            # Labels a partir de la metadata: meta[0] es el eje de tiempo
            # (con la frecuencia de la respuesta), meta[1..N] las series.
            meta_list = raw.get("meta", [])
            axis = meta_list[0] if meta_list else {}
            id_to_label: dict[str, str] = {}
            fields: dict[str, Mapping[str, Any]] = {}
            field_descriptions: list[str] = []
            field_units = ""
            representation_units = ""
            dataset_title = ""
            organism = ""
            per_series: list[dict[str, Any]] = []
            for m in meta_list[1:]:
                field = m.get("field", {})
                ds = m.get("dataset", {})
                sid = field.get("id", "")
                label = field.get("description") or field.get("title") or sid
                if sid:
                    id_to_label[sid] = label
                    fields[sid] = field
                if field.get("description"):
                    field_descriptions.append(field["description"])
                if not field_units and field.get("units"):
                    field_units = field["units"]
                if not representation_units and field.get("representation_mode_units"):
                    representation_units = field["representation_mode_units"]
                if not dataset_title:
                    dataset_title = ds.get("title", "")
                if not organism and ds.get("source"):
                    organism = ds["source"]
                if sid:
                    # El fin de la serie en la fuente nunca es anterior a un
                    # dato que la API ya devolvió. Si sale de ahí y no de la
                    # metadata, y el período de ese dato llega al `hasta`, se
                    # marca: puede ser el fin de lo pedido y no el de la serie
                    # (el IPC sin time_index_end pedido para 2019 «terminaba»
                    # en diciembre de 2019, y el aviso de atraso lo daba por
                    # atrasado). Sin `hasta`, o si la serie termina antes, es
                    # el fin real: marcado, apagaba el «Dato atrasado» de una
                    # serie parada de verdad (revisión de ola 3).
                    source_end = field.get("time_index_end")
                    observed = last_by_id.get(sid)
                    inferred = False
                    if observed and (not source_end or observed > str(source_end)[:10]):
                        source_end = observed
                        inferred = end_iso is not None and _reaches_end(
                            observed,
                            end_iso,
                            _FREQUENCY_NAMES.get(str(axis.get("frequency", "")))
                            or _ISO_FREQUENCY_NAMES.get(field.get("frequency", "")),
                        )
                    per_series.append(
                        {
                            "id": sid,
                            "titulo": label,
                            "fecha_fin_fuente": source_end,
                            "fecha_fin_fuente_inferida": inferred,
                            "actualizada_en_fuente": _as_bool(field.get("is_updated")),
                            "dias_sin_datos": _as_int(field.get("days_without_data")),
                            "unidades": field.get("units"),
                            "frecuencia": _ISO_FREQUENCY_NAMES.get(field.get("frequency", "")),
                            "organismo": ds.get("source"),
                        }
                    )

            if not dataset_title:
                dataset_title = ", ".join(series_ids)

            # Toda representación percent_* llega como fracción (0,3354 =
            # 33,54 %). Antes sólo se escalaba percent_change y la interanual
            # le llegaba al modelo como 0,3354 con unidades «Índice».
            is_percent = (representation or "").startswith("percent_change")
            # Desempleo, actividad, pobreza…: «Porcentaje» como fracción (ver
            # _is_fraction_percent). Con una representación percent_* ya
            # entran por is_percent; acá no se escalan dos veces.
            scaled_fractions: set[str] = set()
            if representation in (None, "value", "change"):
                for idx, sid in enumerate(series_ids):
                    column = [
                        float(row[idx + 1])
                        for row in data
                        if idx + 1 < len(row) and isinstance(row[idx + 1], int | float)
                    ]
                    if _is_fraction_percent(sid, fields.get(sid, {}), column):
                        scaled_fractions.add(sid)
            records = []
            for row in data:
                record: dict = {"fecha": row[0]}
                for idx, sid in enumerate(series_ids):
                    val = row[idx + 1] if idx + 1 < len(row) else None
                    if val is not None and (is_percent or sid in scaled_fractions):
                        # Se multiplica por 100 para que el modelo, los
                        # gráficos y la UI reciban "33.54" y no "0.3354"; la
                        # escala va en metadata.unit / unidad / value_scale.
                        val = round(val * 100, 2)
                    label = id_to_label.get(sid, sid)
                    record[label] = val
                records.append(record)

            if len(records) > limit:
                records = records[-limit:]
                truncated = True

            last_observation = next(
                (
                    str(r["fecha"])[:10]
                    for r in reversed(records)
                    if any(v is not None for k, v in r.items() if k != "fecha")
                ),
                str(records[-1]["fecha"])[:10],
            )
            source_ends = [s["fecha_fin_fuente"] for s in per_series if s["fecha_fin_fuente"]]
            oldest_end = min(source_ends) if source_ends else None
            updated_flags = [s["actualizada_en_fuente"] for s in per_series]
            if any(flag is False for flag in updated_flags):
                updated: bool | None = False
            elif updated_flags and all(flag is True for flag in updated_flags):
                updated = True
            else:
                updated = None
            frequency = _FREQUENCY_NAMES.get(str(axis.get("frequency", ""))) or next(
                (s["frecuencia"] for s in per_series if s["frecuencia"]), None
            )

            units = field_units
            if local_ytd:
                units = _YEAR_TO_DATE_UNITS
            elif representation and representation_units:
                units = representation_units
            all_percent = is_percent or (
                bool(scaled_fractions) and len(scaled_fractions) == len(set(series_ids))
            )
            suffix = "en puntos porcentuales" if representation == "change" else "en %"
            if all_percent:
                units = f"{units} ({suffix})" if units else "%"
            for entry in per_series:
                if representation:
                    # Cada serie, en las unidades de lo que se pidió: IPC +
                    # salarios en percent_change decían «Índice» en
                    # `por_serie` con los valores en %.
                    rep_units = (
                        _YEAR_TO_DATE_UNITS
                        if local_ytd
                        else fields.get(entry["id"], {}).get("representation_mode_units")
                    )
                    if rep_units:
                        entry["unidades"] = rep_units
                    if is_percent:
                        entry["unidades"] = (
                            f"{entry['unidades']} (en %)" if entry["unidades"] else "%"
                        )
                if entry["id"] in scaled_fractions:
                    entry["unidades"] = (
                        f"{entry['unidades'] or 'Porcentaje'} "
                        f"({suffix}; la API la da como fracción)"
                    )
                    # Con escalas mixtas, lo que ve el modelo dice cuál es cuál.
                    entry["escalada_a_porcentaje"] = True

            # Sin `collapse`, series de distinta frecuencia van al eje de la
            # más gruesa y la API PROMEDIA las más finas sin decirlo: reservas
            # diaria + mensual daban 49.700,26 en 2026-08 (el promedio de
            # agosto) y el saldo al 31 era 48.259. Se marca cuáles.
            averaged = False
            axis_rank = _FREQUENCY_ORDER.get(_FREQUENCY_NAMES.get(str(axis.get("frequency")), ""))
            if not collapse and axis_rank is not None:
                for entry in per_series:
                    native_rank = _FREQUENCY_ORDER.get(entry["frecuencia"] or "")
                    if native_rank is not None and native_rank < axis_rank:
                        entry["promediada_por_api"] = True
                        averaged = True

            metadata: dict[str, Any] = {
                "total_records": len(records),
                "fetched_at": datetime.now(UTC).isoformat(),
                "description": "; ".join(field_descriptions),
                "units": units,
                # Contrato de frescura (lo leen el aviso de atraso y la
                # verificación de cifras). Con varias series, la fecha de la
                # fuente es la de la más atrasada.
                "ultima_observacion": last_observation,
                "frecuencia": frequency,
                "fecha_fin_fuente": oldest_end,
                "fecha_fin_fuente_inferida": any(
                    s["fecha_fin_fuente_inferida"]
                    for s in per_series
                    if oldest_end and s["fecha_fin_fuente"] == oldest_end
                ),
                "actualizada_en_fuente": updated,
                "total_fuente": total,
                "truncada": truncated,
                "oficial": True,
                "series": per_series,
            }
            if organism:
                metadata["organismo"] = organism
            if representation:
                metadata["representation"] = representation
            if collapse and collapse_aggregation:
                metadata["agregacion"] = collapse_aggregation
            if averaged:
                metadata["agregada_por_api"] = "promedio"
            if all_percent:
                # Contrato explícito: los valores ya están en puntos
                # porcentuales (15.2 es 15,2 %).
                metadata["unit"] = "percent"
                metadata["value_scale"] = "percentage_points"
                metadata["unidad"] = "porcentaje"

            return DataResult(
                source="series_tiempo",
                portal_name="API de Series de Tiempo",
                portal_url=source_url(series_ids),
                dataset_title=dataset_title,
                format="time_series",
                records=records,
                metadata=metadata,
            )
        except ConnectorError:
            raise
        except Exception as exc:
            # `str(exc)` de un HTTPStatusError trae la URL y el código, pero
            # no el cuerpo — y el cuerpo es donde la API explica qué
            # parámetro rechazó (p. ej. "Intervalo de collapse inválido …
            # Pruebe con un intervalo mayor"). Sin esto, un 400 recuperable
            # y una serie caída se ven idénticos en los logs.
            detalle = str(exc)
            cuerpo = getattr(getattr(exc, "response", None), "text", None)
            if cuerpo:
                detalle = f"{detalle} | respuesta: {cuerpo[:300]}"
            logger.warning(
                "Series fetch falló para %s (collapse=%s, representation=%s): %s",
                series_ids,
                collapse,
                representation,
                detalle,
            )
            details: dict[str, Any] = {"series_ids": series_ids, "reason": detalle}
            # Un id que no existe no es una fuente caída: el agente lo leía
            # como «La fuente no respondió» y se iba a una copia vieja
            # (nueva_08 del 06-oct, con el id armado 64.2_GR_0_0_12).
            missing = _missing_series(exc)
            if missing:
                details["series_inexistentes"] = missing
            raise ConnectorError(
                error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
                details=details,
            ) from exc
