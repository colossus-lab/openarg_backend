"""Una API de Series de Tiempo falsa que se porta como la real.

Las mañas que imita se midieron contra apis.datos.gob.ar el 04-oct-2026:

- orden ascendente; ``limit`` (por defecto 100, máximo 5000: más da 400) y
  ``start`` como offset;
- ``count`` son las observaciones de la ventana [start_date, end_date]
  ANTES de transformar;
- las representaciones se calculan DENTRO de la ventana y descartan las
  primeras filas sin dato (la mensual pierde 1, la interanual mensual 12);
- ``start`` se aplica DESPUÉS de transformar (IPC interanual con start=107 y
  count=117 vuelve vacío: sólo hay 105 filas transformadas);
- ``sort=desc`` con una representación descarta los períodos más recientes.
  El adaptador no tiene que mandarlo nunca: los tests lo verifican.

``collapse`` (medido el 05-oct): agrega TODA la serie con
``collapse_aggregation`` (por defecto el promedio), deja afuera los períodos
incompletos (exportaciones con ``collapse=year`` llegan hasta 2025; el IPC,
que arranca en 2016-12, empieza en 2017) y fecha cada período por su primer
día; después filtra por esa fecha contra [start_date, end_date]: con
start_date=2023-06-01 no aparece 2023, y con end_date=2025-06-30 aparece
2025 entero. ``count`` cuenta las filas YA agregadas, y una frecuencia más
fina que la de la serie da 400 ("Intervalo de collapse inválido…"). Sólo se
simula desde series mensuales o más gruesas; de una diaria se agrega sin
descartar nada.

Medido el 06-oct:

- lo de los períodos incompletos vale para avg, sum y end_of_period con
  ``collapse`` quarter o year. Con max o min el período en curso y el
  primero incompleto quedan (exportaciones con year+max traen 2026-01-01, de
  enero a agosto; el IPC trae 2016-01-01, sólo diciembre). Y
  ``collapse=semester`` desde una mensual agrupa desde el primer mes de la
  serie y no recorta el semestre en curso (``_monthly_to_semesters``);
- ``min_value`` y ``max_value`` de la metadata son el rango de TODA la serie:
  no cambian con la ventana, la representación ni el ``collapse``;
- la pobreza y la indigencia de la EPH continua (63.2, 64.2) vienen
  fechadas un semestre tarde: el CSV de la fuente y la metadata de la API
  (``time_index_end`` 2026-01-01, ``last_value`` 0,231 en Gran Rosario)
  fechan el 1er semestre de 2026 en 2026-01-01 y la API trae ese valor en la
  fila 2026-07-01. No es una metadata atrasada: ``last_value`` es el de la
  última fila (``serie(time_index_end=…)``). Una atrasada de verdad se
  simula con ``last_value`` del período de ``time_index_end``. Pedida junto
  con otras, cada serie conserva su fecha: la 64.1 de 2001-2003 va por el
  inicio del semestre y el desempleo trimestral promediado a semestres
  también (2025-01-01 es el promedio del 1er y 2° trimestre de 2025), y la
  pobreza sigue corrida en la misma fila;
- un id que no existe da 400 con ``failed_series`` (sólo el primero que
  falta): ``{"errors": [{"error": "Serie inexistente: X"}],
  "failed_series": ["X"]}``;
- un ``end_date`` parcial llega al fin de su período (``2025`` al 31 de
  diciembre, ``2025-06`` al 30 de junio);
- sin ``collapse``, series de distinta frecuencia van al eje de la más gruesa
  y la API PROMEDIA las más finas (reservas diaria + mensual: 2026-08 da
  49.700,26, el promedio de agosto; el saldo al 31 es 48.259). La metadata
  de cada serie sigue diciendo su frecuencia propia.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import httpx

from app.infrastructure.adapters.connectors.series_tiempo_adapter import SeriesTiempoAdapter

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "series_tiempo_api"

IPC_ID = "148.3_INIVELNAL_DICI_M_26"
IPIM_ID = "448.1_NIVEL_GENERAL_0_0_13_46"
IPC_NORESTE_ID = "148.3_INIVELNEA_DICI_M_21"
RESERVAS_ID = "174.1_RRVAS_IDOS_0_0_36"
TIPO_CAMBIO_ID = "92.2_TIPO_CAMBIION_0_0_21_24"
EXPO_ID = "74.3_IET_0_M_16"
IMPO_ID = "74.3_IIT_0_M_25"
DESEMPLEO_ID = "45.2_ECTDT_0_T_33"
ACTIVIDAD_ID = "43.2_ECTAT_0_T_33"
EMPLEO_ID = "42.3_EPH_PUNTUATAL_0_M_24"
SUBOCUPACION_ID = "46.2_ECTST_0_T_36"
HOGARES_POBRES_ID = "63.2_HOGARES_PONUA_0_0_41_84"
POBREZA_ID = "64.2_POBLACION_NUA_0_0_34_74"
DESOCUPACION_AGLOMERADO_ID = "343.1_CHUBUT_RALEW__21"
PLAZO_FIJO_USD_ID = "174.1_T_INTERUS_0_0_43"
SALARIOS_ID = "149.1_TL_INDIIOS_OCTU_0_21"
RESERVAS_DIARIAS_ID = "92.2_RESERVAS_IRES_0_0_32_40"
GASTO_PIB_TOTAL_ID = "451.2_GPC_PIBPIB_0_0_7_85"
GASTO_PIB_EDUCACION_ID = "451.2_GPC_EDUCACPIB_0_0_24_72"
GASTO_PIB_UNIVERSIDAD_ID = "451.2_GPC_EDUCACPIB_0_0_31_23"
GASTO_PIB_CIENCIA_ID = "451.2_GPC_CIENCIPIB_0_0_23_49"
TASA_JAPON_ID = "131.1_OIRJT_0_0_34"

_AXIS_FREQUENCY = {
    "R/P1D": "day",
    "R/P1M": "month",
    "R/P3M": "quarter",
    "R/P6M": "semester",
    "R/P1Y": "year",
}
_PERIODS_PER_YEAR = {"R/P1D": 365, "R/P1M": 12, "R/P3M": 4, "R/P6M": 2, "R/P1Y": 1}
# Meses por observación: 0 para la diaria.
_SOURCE_MONTHS = {"R/P1D": 0, "R/P1M": 1, "R/P3M": 3, "R/P6M": 6, "R/P1Y": 12}
_COLLAPSE_MONTHS = {"month": 1, "quarter": 3, "semester": 6, "year": 12}
_COLLAPSE_PER_YEAR = {"month": 12, "quarter": 4, "semester": 2, "year": 1}
_AGGREGATE = {
    "avg": lambda v: sum(v) / len(v),
    "sum": sum,
    "end_of_period": lambda v: v[-1],
    "max": max,
    "min": min,
}
_REPRESENTATION_UNITS = {
    "value": None,
    "change": "Variación respecto del período anterior",
    "percent_change": "Variación porcentual período anterior",
    "percent_change_a_year_ago": "Variación porcentual interanual",
    "percent_change_since_beginning_of_year": "Variación porcentual acumulada anual",
}


def serie(
    sid: str,
    data: list[tuple[str, float]],
    *,
    description: str,
    units: str = "Índice",
    frequency: str = "R/P1M",
    is_updated: bool = True,
    dataset: str = "Dataset de prueba",
    source: str = "Instituto Nacional de Estadística y Censos (INDEC)",
    time_index_end: str | None = None,
    value_range: tuple[float, float] | None = None,
    with_range: bool = True,
    last_value: float | None = None,
) -> dict[str, Any]:
    """Una serie de la API falsa.

    ``value_range`` es el (mínimo, máximo) de toda la serie en la fuente; por
    defecto, el de ``data``. ``time_index_end`` es el fin según la metadata.
    ``last_value``, como en la API, es el último valor con dato (texto); se
    puede fijar otro para simular una metadata atrasada.
    """
    with_value = [v for _, v in data if v is not None]
    field: dict[str, Any] = {
        "id": sid,
        "description": description,
        "units": units,
        "frequency": frequency,
        "time_index_start": data[0][0],
        "time_index_end": time_index_end or data[-1][0],
        "time_index_size": str(len(data)),
        "is_updated": "True" if is_updated else "False",
    }
    if last_value is not None or with_value:
        field["last_value"] = str(last_value if last_value is not None else with_value[-1])
    if with_range:
        values = [v for _, v in data if v is not None]
        low, high = value_range or (min(values), max(values))
        # Como la API: texto, y de TODA la serie.
        field["min_value"] = str(low)
        field["max_value"] = str(high)
    return {
        "id": sid,
        "data": data,
        "field": field,
        "dataset": {"title": dataset, "source": source},
    }


def _grabada(nombre: str, sid: str) -> dict[str, Any]:
    raw = json.loads((FIXTURES / nombre).read_text(encoding="utf-8"))
    field = raw["field"]
    recorded_range = None
    if field.get("min_value") is not None and field.get("max_value") is not None:
        recorded_range = (float(field["min_value"]), float(field["max_value"]))
    return serie(
        sid,
        [(f, v) for f, v in raw["data"]],
        description=field["description"],
        units=field["units"],
        frequency=field.get("frequency", "R/P1M"),
        is_updated=field.get("is_updated", "True") == "True",
        dataset=raw["dataset"]["title"],
        source=raw["dataset"].get("source") or "Instituto Nacional de Estadística y Censos (INDEC)",
        value_range=recorded_range,
    )


def ipc_real() -> dict[str, Any]:
    """El IPC nacional tal como lo devolvió la API el 04-oct (117 meses)."""
    return _grabada("ipc_148_3.json", IPC_ID)


def ipim_real() -> dict[str, Any]:
    """El IPIM nivel general (448.1), grabado de la API el 06-oct: agosto de 2026 da 2,14 %."""
    return _grabada("ipim_448_1.json", IPIM_ID)


def ipc_noreste_real() -> dict[str, Any]:
    """El IPC del Noreste (148.3), grabado de la API el 06-oct: agosto de 2026 da 1,75 %."""
    return _grabada("ipc_noreste_148_3.json", IPC_NORESTE_ID)


def exportaciones_reales() -> dict[str, Any]:
    """Exportaciones totales (74.3) de 2023-01 a 2026-08, grabadas de la API el 05-oct.

    Suma 2024: 79.703,2; suma 2025: 87.111,2 (+9,29 %). Diciembre contra
    diciembre: 7.049,0 → 7.482,4 (+6,15 %).
    """
    return _grabada("expo_74_3.json", EXPO_ID)


def importaciones_reales() -> dict[str, Any]:
    """Importaciones totales (74.3) de 2023-01 a 2026-08, grabadas de la API el 06-oct.

    Suma 2024: 60.775,6 (saldo 2024 con las exportaciones: 18.927,6); suma
    2025: 75.791,1.
    """
    return _grabada("impo_74_3.json", IMPO_ID)


def desempleo() -> dict[str, Any]:
    """La 45.2 desde 2025 (API, 05-oct): trimestral, «Porcentaje» y valores como fracción."""
    return serie(
        DESEMPLEO_ID,
        [
            ("2025-01-01", 0.079),
            ("2025-04-01", 0.0757623890941418),
            ("2025-07-01", 0.066),
            ("2025-10-01", 0.075),
            ("2026-01-01", 0.078),
            ("2026-04-01", 0.079),
        ],
        description="Tasa de desempleo total. En porcentaje.",
        units="Porcentaje",
        frequency="R/P3M",
        dataset="EPH. Tasas de actividad, empleo y desempleo",
    )


# Tasas que la API publica con unidades «Porcentaje…» y valores como fracción,
# tal como las devolvió el 06-oct (y el rango de toda la serie, de la metadata).
_TASAS: dict[str, dict[str, Any]] = {
    ACTIVIDAD_ID: {
        "description": "Tasa de actividad total. En porcentaje.",
        "units": "Porcentaje",
        "frequency": "R/P3M",
        "dataset": "Principales variables ocupacionales. EPH continua. Actividad",
        "value_range": (0.384, 0.489),
        "data": [
            ("2025-01-01", 0.482),
            ("2025-04-01", 0.481294662128481),
            ("2025-07-01", 0.486),
            ("2025-10-01", 0.486),
            ("2026-01-01", 0.486),
            ("2026-04-01", 0.489),
        ],
    },
    EMPLEO_ID: {
        "description": "Tasa de empleo total. En porcentaje.",
        "units": "Porcentaje",
        "frequency": "R/P3M",
        "dataset": "Principales variables ocupacionales. EPH puntual y continua.",
        "value_range": (0.334, 0.4579999999999999),
        "data": [
            ("2025-01-01", 0.444),
            ("2025-04-01", 0.44483062866737),
            ("2025-07-01", 0.4539999999999999),
            ("2025-10-01", 0.45),
            ("2026-01-01", 0.4479999999999999),
            ("2026-04-01", 0.45),
        ],
    },
    SUBOCUPACION_ID: {
        "description": "Tasa de subocupación total.",
        "units": "Porcentaje",
        "frequency": "R/P3M",
        "dataset": "Principales variables ocupacionales. EPH continua. Subocupación",
        "value_range": (0.074, 0.178),
        "data": [
            ("2025-01-01", 0.1),
            ("2025-04-01", 0.115868526102053),
            ("2025-07-01", 0.109),
            ("2025-10-01", 0.113),
            ("2026-01-01", 0.111),
            ("2026-04-01", 0.115),
        ],
    },
    # Las dos de pobreza, con las filas como las fecha la API: un semestre
    # tarde. time_index_end dice 2026-01-01 (el 1er semestre de 2026, como la
    # fuente) y la API trae ese valor en la fila 2026-07-01.
    HOGARES_POBRES_ID: {
        "description": (
            "Hogares con ingresos debajo de línea de pobreza (%) desde 2003. Rawson - Trelew. "
            "EPH continua."
        ),
        "units": "Porcentaje de hogares",
        "frequency": "R/P6M",
        "dataset": "Hogares con ingresos por debajo de la línea de pobreza. EPH puntual y continua.",
        "value_range": (0.0, 0.434),
        "time_index_end": "2026-01-01",
        "data": [
            ("2024-07-01", 0.434),
            ("2025-01-01", 0.281),
            ("2025-07-01", 0.218),
            ("2026-01-01", 0.254),
            ("2026-07-01", 0.2689999999999999),
        ],
    },
    POBREZA_ID: {
        "description": (
            "Población con ingresos debajo de línea de pobreza (%) desde 2003. TOTAL. EPH continua."
        ),
        "units": "Porcentaje de población",
        "frequency": "R/P6M",
        "dataset": (
            "Población con ingresos por debajo de la línea de pobreza. EPH puntual y continua."
        ),
        "value_range": (0.257, 0.54),
        "time_index_end": "2026-01-01",
        "data": [
            ("2024-07-01", 0.529),
            ("2025-01-01", 0.381),
            ("2025-07-01", 0.316),
            ("2026-01-01", 0.282),
            ("2026-07-01", 0.3229999999999999),
        ],
    },
    # Desocupación por aglomerado 1974-2003: la serie entera, discontinuada.
    DESOCUPACION_AGLOMERADO_ID: {
        "description": "Chubut Rawson - Trelew",
        "units": "Porcentaje de población",
        "frequency": "R/P6M",
        "dataset": "Tasa de Desocupación por aglomerado 1974-2003",
        "value_range": (0.166, 0.173),
        "data": [("2002-07-01", 0.173), ("2003-01-01", 0.166)],
    },
}


def tasa(sid: str) -> dict[str, Any]:
    """Una tasa de la EPH o de pobreza: «Porcentaje…» con los valores como fracción."""
    spec = _TASAS[sid]
    return serie(
        sid,
        spec["data"],
        description=spec["description"],
        units=spec["units"],
        frequency=spec["frequency"],
        dataset=spec["dataset"],
        value_range=spec["value_range"],
        time_index_end=spec.get("time_index_end"),
    )


def pobreza_con_metadata_atrasada() -> dict[str, Any]:
    """La 64.2 total con una metadata atrasada DE VERDAD (no medida en la API real).

    time_index_end 2026-01-01 con el last_value de ESA fila (0,282): la fila
    2026-07-01 se publicó después de la metadata. Sirve para el fin de la
    fuente que nunca queda antes del último dato (H065).
    """
    s = tasa(POBREZA_ID)
    s["field"]["last_value"] = "0.282"
    return s


POBREZA_GRAN_ROSARIO_ID = "64.2_POBLACION_NUA_0_0_41_1"


def _pobreza_gran_rosario_grabada() -> dict[str, Any]:
    return json.loads((FIXTURES / "pobreza_64_2_gran_rosario.json").read_text(encoding="utf-8"))


def pobreza_gran_rosario_real() -> dict[str, Any]:
    """Pobreza de Gran Rosario (64.2), la serie entera como la devolvió la API el 06-oct.

    Las filas vienen un semestre tarde: 0,231 (1er semestre de 2026) en
    2026-07-01, con time_index_end 2026-01-01 y last_value 0,231.
    """
    raw = _pobreza_gran_rosario_grabada()
    field = raw["field"]
    return serie(
        POBREZA_GRAN_ROSARIO_ID,
        [(f, v) for f, v in raw["data"]],
        description=field["description"],
        units=field["units"],
        frequency=field["frequency"],
        dataset=raw["dataset"]["title"],
        source=raw["dataset"]["source"],
        time_index_end=field["time_index_end"],
        value_range=(float(field["min_value"]), float(field["max_value"])),
        last_value=float(field["last_value"]),
    )


def pobreza_gran_rosario_fuente() -> list[tuple[str, float]]:
    """La misma serie en el CSV de la fuente (SSPM, EPH del INDEC), grabado el 06-oct.

    Fecha cada semestre por su primer día: 2026-01-01 es el 1er semestre de
    2026 (0,231) y 2024-01-01 el 1er semestre de 2024.
    """
    return [(f, v) for f, v in _pobreza_gran_rosario_grabada()["fuente_csv"]["data"]]


POBREZA_GRAN_ROSARIO_PUNTUAL_ID = "64.1_GR_0_0_12"


def pobreza_gran_rosario_puntual() -> dict[str, Any]:
    """Pobreza de Gran Rosario de la EPH puntual (64.1, 2001-2003), API del 06-oct.

    Semestral fechada por el inicio: la última fila es 2003-01-01 (61 %, la
    onda de mayo de 2003) con time_index_end 2003-05-01 y last_value 0,61.
    En nueva_08 el modelo mezcló su id con el de la 64.2.
    """
    return serie(
        POBREZA_GRAN_ROSARIO_PUNTUAL_ID,
        [
            ("2001-01-01", 0.358),
            ("2001-07-01", 0.412),
            ("2002-01-01", 0.562),
            ("2002-07-01", 0.609),
            ("2003-01-01", 0.61),
        ],
        description=(
            "Población con ingresos debajo de línea de pobreza (%) de 2001 a 2003. Gran Rosario. "
            "EPH puntual."
        ),
        units="Porcentaje de población",
        frequency="R/P6M",
        is_updated=False,
        dataset="Población con ingresos por debajo de la línea de pobreza. EPH puntual y continua.",
        time_index_end="2003-05-01",
    )


DESEMPLEO_GRAN_ROSARIO_ID = "45.2_ECTDTGR_0_T_46"


def desempleo_gran_rosario() -> dict[str, Any]:
    """Desempleo de Gran Rosario (45.2, trimestral) desde 2023, API del 06-oct.

    Junto con la pobreza, la API lo lleva a semestres promediando y lo fecha
    por el inicio: 2025-01-01 = (0,071 + 0,0771) / 2 = 7,4 % y 2026-01-01 =
    (0,082 + 0,115) / 2 = 9,85 %.
    """
    return serie(
        DESEMPLEO_GRAN_ROSARIO_ID,
        [
            ("2023-01-01", 0.0791686257560138),
            ("2023-04-01", 0.053),
            ("2023-07-01", 0.0525881997580512),
            ("2023-10-01", 0.047),
            ("2024-01-01", 0.0559999999999999),
            ("2024-04-01", 0.0723736257343959),
            ("2024-07-01", 0.0579999999999999),
            ("2024-10-01", 0.06),
            ("2025-01-01", 0.071),
            ("2025-04-01", 0.0770504140543383),
            ("2025-07-01", 0.089),
            ("2025-10-01", 0.065),
            ("2026-01-01", 0.0819999999999999),
            ("2026-04-01", 0.115),
        ],
        description="Tasa de desempleo total Gran Rosario. En porcentaje.",
        units="Porcentaje",
        frequency="R/P3M",
        dataset="Principales variables ocupacionales. EPH continua. Desempleo",
        value_range=(0.043, 0.229),
    )


def plazo_fijo_usd() -> dict[str, Any]:
    """174.1_T_INTERUS (API, 06-oct): «Porcentaje», YA en %.

    Desde 2026-03 todos los valores son menores que 1,5 (1,28 y 1,04 %), pero
    la serie llegó a 13,75 en 2001-08: el rango de la metadata lo dice.
    """
    return serie(
        PLAZO_FIJO_USD_ID,
        [
            ("2025-11-01", 1.7902765000788732),
            ("2025-12-01", 1.85401829627672),
            ("2026-01-01", 1.9505627598068456),
            ("2026-02-01", 1.6404223291653264),
            ("2026-03-01", 1.2844929451772111),
            ("2026-04-01", 1.0429558292507333),
        ],
        description=(
            "Tasa de interés Por depósitos a plazo fijo de 30 a 59 días De moneda extranjera"
        ),
        units="Porcentaje",
        is_updated=False,
        dataset="Series históricas de estadísticas monetarias",
        source="Banco Central de la República Argentina (BCRA)",
        value_range=(0.2320475981526836, 13.75208473618025),
    )


# Gasto Público Consolidado (451.x, API, 06-oct): «Porcentaje del PIB», YA en
# %, de 2019 a 2023. El total es 41,87 % del PIB en 2023 y la ciencia y
# técnica 0,27 %: todo el rango de esta (0,18 a 0,32) cabe en ±1,5, pero no
# es una fracción.
_GASTO_PIB: dict[str, dict[str, Any]] = {
    GASTO_PIB_TOTAL_ID: {
        "description": "Gasto público consolidado en porcentaje del PIB",
        "value_range": (25.92035600792439, 47.35332155887762),
        "values": [
            43.463394679430536,
            47.35332155887762,
            42.80888347010397,
            42.25644868970256,
            41.86654045227007,
        ],
    },
    GASTO_PIB_EDUCACION_ID: {
        "description": "Gasto público consolidado en Educación básica en porcentaje del PIB",
        "value_range": (1.334115662126785, 4.148832742362272),
        "values": [
            3.434737477473392,
            3.667549273366692,
            3.260382154733194,
            3.2523612326731075,
            3.4592789759436595,
        ],
    },
    GASTO_PIB_UNIVERSIDAD_ID: {
        "description": (
            "Gasto público consolidado en Educación superior y universitaria en porcentaje del PIB"
        ),
        "value_range": (0.3458402383115652, 1.2958420233838006),
        "values": [
            1.0807226125281253,
            1.1778968432375083,
            1.0578644409730298,
            1.0492658592301691,
            1.1274565845376978,
        ],
    },
    GASTO_PIB_CIENCIA_ID: {
        "description": "Gasto público consolidado en Ciencia y técnica en porcentaje del PIB",
        "value_range": (0.1825126369920748, 0.3201046710985293),
        "values": [
            0.1996238189347094,
            0.2044682970676959,
            0.2203159894286404,
            0.2460896516367026,
            0.2673829881871986,
        ],
    },
}


def gasto_pib(sid: str) -> dict[str, Any]:
    """Una serie de Gasto Público Consolidado en % del PIB, anual de 2019 a 2023."""
    spec = _GASTO_PIB[sid]
    return serie(
        sid,
        [(f"{2019 + i}-01-01", v) for i, v in enumerate(spec["values"])],
        description=spec["description"],
        units="Porcentaje del PIB",
        frequency="R/P1Y",
        is_updated=False,
        dataset="Gasto Público Consolidado",
        source="Secretaría de Política Económica, Ministerio de Economía",
        value_range=spec["value_range"],
    )


def tasa_japon() -> dict[str, Any]:
    """131.1_OIRJT (API, 06-oct): la tasa overnight de Japón, «Porcentaje» y YA en %.

    0,75 es 0,75 %: en toda su historia fue de −0,1 a 0,75.
    """
    return serie(
        TASA_JAPON_ID,
        [
            ("2025-09-01", 0.5),
            ("2025-10-01", 0.5),
            ("2025-11-01", 0.5),
            ("2025-12-01", 0.75),
            ("2026-01-01", 0.75),
            ("2026-02-01", 0.75),
            ("2026-03-01", 0.75),
            ("2026-04-01", 0.75),
            ("2026-05-01", 0.75),
        ],
        description="Overnight Interest Rate - Japón - Tasa",
        units="Porcentaje",
        is_updated=False,
        dataset="Principales Tasas de Interés de Referencia",
        source="Bancos Centrales",
        value_range=(-0.1, 0.75),
    )


def salarios() -> dict[str, Any]:
    """Índice de salarios 149.1 (API, 06-oct): mensual, de 2025-10 a 2026-07."""
    return serie(
        SALARIOS_ID,
        [
            ("2025-10-01", 7705.88),
            ("2025-11-01", 7843.14),
            ("2025-12-01", 7969.6),
            ("2026-01-01", 8171.66),
            ("2026-02-01", 8370.04),
            ("2026-03-01", 8654.99),
            ("2026-04-01", 8978.1),
            ("2026-05-01", 9177.4),
            ("2026-06-01", 9442.5),
            ("2026-07-01", 9708.64),
        ],
        description="Índice de Salarios",
        units="Índice",
        dataset="Índice de salarios. Base octubre 2016",
        value_range=(100.0, 9708.64),
    )


def reservas_diarias() -> dict[str, Any]:
    """Reservas 92.2, saldo diario de 2026-06-01 a 2026-08-31, grabado de la API el 06-oct.

    Al 31-08: 48.259. Promedio de agosto: 49.700,26.
    """
    return _grabada("reservas_diarias_92_2.json", RESERVAS_DIARIAS_ID)


def reservas_mensuales() -> dict[str, Any]:
    """Como la 174.1: 1036 meses de 1940-01 a 2026-04, parada en la fuente."""
    data = []
    for i in range(1036):
        year, month = divmod(1940 * 12 + i, 12)
        data.append((date(year, month + 1, 1).isoformat(), 100.0 + i))
    return serie(
        RESERVAS_ID,
        data,
        description="Reservas Internacionales BCRA Saldos",
        units="Millones de dólares",
        is_updated=False,
        dataset="Series históricas de estadísticas monetarias",
        source="Banco Central de la República Argentina (BCRA)",
    )


def diaria(n: int = 2345, end: date = date(2026, 8, 31), sid: str = TIPO_CAMBIO_ID) -> dict:
    """Una diaria de `n` días que termina en `end`; el valor de cada día es su índice."""
    first = end - timedelta(days=n - 1)
    data = [((first + timedelta(days=i)).isoformat(), float(i + 1)) for i in range(n)]
    return serie(
        sid,
        data,
        description="Tipo de cambio de valuación (peso por dólar)",
        units="Pesos argentinos por dólar",
        frequency="R/P1D",
        is_updated=False,
        dataset="Reservas internacionales y pasivos del BCRA",
        source="Banco Central de la República Argentina (BCRA)",
    )


def _transform(window: list[tuple[str, float]], mode: str, per_year: int) -> list[tuple[str, Any]]:
    values = [v for _, v in window]
    out: list[tuple[str, Any]] = []
    for i, (fecha, value) in enumerate(window):
        if mode == "value":
            out.append((fecha, value))
            continue
        if mode in ("change", "percent_change") and i >= 1:
            base = values[i - 1]
        elif mode == "percent_change_a_year_ago" and i >= per_year:
            base = values[i - per_year]
        elif mode == "percent_change_since_beginning_of_year":
            # Como la API real: contra el PRIMER dato del año dentro de la
            # ventana (enero), no contra el cierre del año anterior.
            year = fecha[:4]
            base = next(v for f, v in window if f[:4] == year)
        else:
            continue
        if value is None or base is None:
            # Como la API: sin dato (la pobreza 64.2 no tiene 2007 a 2016),
            # la variación tampoco.
            out.append((fecha, None))
        elif mode == "change":
            out.append((fecha, value - base))
        else:
            out.append((fecha, value / base - 1))
    return out


def _bound(text: str | None, *, end: bool) -> str | None:
    """Un `start_date`/`end_date` parcial como lo lee la API (medido el 06-oct).

    `2025` va del 1° de enero al 31 de diciembre y `2025-06` hasta el 30 de
    junio; una fecha completa queda como está.
    """
    if not text or len(text) >= 10:
        return text
    parts = [int(p) for p in text.split("-")]
    if not end:
        return date(parts[0], parts[1] if len(parts) > 1 else 1, 1).isoformat()
    if len(parts) == 1:
        return f"{parts[0]}-12-31"
    year, month = divmod(parts[0] * 12 + parts[1], 12)
    return (date(year, month + 1, 1) - timedelta(days=1)).isoformat()


def _period_start(fecha: str, months: int) -> str:
    year, month = int(fecha[:4]), int(fecha[5:7])
    return date(year, (month - 1) // months * months + 1, 1).isoformat()


def _collapse(
    window: list[tuple[str, float]], source_months: int, target_months: int, how: str
) -> list[tuple[str, float]]:
    if source_months == 1 and target_months == 6:
        return _monthly_to_semesters(window, how)
    groups: dict[str, list[float]] = {}
    for fecha, value in window:
        groups.setdefault(_period_start(fecha, target_months), []).append(value)
    expected = target_months // source_months if source_months else None
    return [
        (period, _AGGREGATE[how](values))
        for period, values in groups.items()
        # Como la API: los períodos incompletos (el año en curso, el primero
        # si la serie arranca a mitad de año) quedan afuera. Con max y min no:
        # la API los calcula al consultar, sin ese recorte.
        if expected is None or how in ("max", "min") or len(values) >= expected
    ]


def _monthly_to_semesters(window: list[tuple[str, float]], how: str) -> list[tuple[str, float]]:
    """De una mensual a semestres, como la API (medido el 06-oct).

    Agrupa de a seis meses desde el primer mes de la serie. Si ese mes no es
    enero, corre la fecha de cada grupo `mes − 1` meses para atrás, descarta
    el primer grupo y, si la fecha del siguiente no cae en el mes en que
    arranca la serie, también ese (``index_transform`` y
    ``handle_month_semester`` de series-tiempo-ar-api). El semestre sin
    terminar queda: en exportaciones, 2026-07-01 es julio más agosto
    (17.736,44); en el IPC, que arranca en 2016-12, 2025-07-01 es el promedio
    de junio a agosto de 2026.
    """
    first_year, first_month = int(window[0][0][:4]), int(window[0][0][5:7])
    groups: dict[int, list[float]] = {}
    for fecha, value in window:
        months = (int(fecha[:4]) - first_year) * 12 + int(fecha[5:7]) - first_month
        groups.setdefault(months // 6, []).append(value)
    offset = first_month - 1
    out = []
    for index, values in sorted(groups.items()):
        year, month = divmod(first_year * 12 + first_month - 1 + index * 6 - offset, 12)
        out.append((date(year, month + 1, 1).isoformat(), _AGGREGATE[how](values)))
    if offset:
        out = out[1:]
    if out and int(out[0][0][5:7]) != first_month:
        out = out[1:]
    return out


class FakeSeriesApi:
    def __init__(self, *series: dict[str, Any]) -> None:
        self.series = {s["id"]: s for s in series}
        self.requests: list[dict[str, str]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        params = {k: v[-1] for k, v in parse_qs(request.url.query.decode()).items()}
        self.requests.append(params)
        if request.url.path.rstrip("/").endswith("/search"):
            return httpx.Response(200, json={"data": [], "count": 0})
        limit = int(params.get("limit", 100))
        if limit > 5000:
            return httpx.Response(
                400, json={"errors": ["Parámetro limit por encima del límite permitido (5000)"]}
            )
        start = int(params.get("start", 0))
        start_date = _bound(params.get("start_date"), end=False)
        end_date = _bound(params.get("end_date"), end=True)
        mode = params.get("representation_mode", "value")
        collapse = params.get("collapse")
        how = params.get("collapse_aggregation", "avg")
        ids = params["ids"].split(",")
        missing = [i for i in ids if i not in self.series]
        if missing:
            # Como la API: nombra sólo el primero que falta.
            return httpx.Response(
                400,
                json={
                    "errors": [{"error": f"Serie inexistente: {missing[0]}"}],
                    "failed_series": [missing[0]],
                },
            )
        chosen = [self.series[i] for i in ids]
        # Sin `collapse` y con frecuencias distintas, todas van a la más
        # gruesa con promedio.
        natives = {s["field"]["frequency"] for s in chosen}
        implicit = (
            max(natives, key=lambda f: _SOURCE_MONTHS[f])
            if not collapse and len(natives) > 1
            else None
        )
        windows = []
        for s in chosen:
            window = list(s["data"])
            if implicit and s["field"]["frequency"] != implicit:
                window = _collapse(
                    window,
                    _SOURCE_MONTHS[s["field"]["frequency"]],
                    _SOURCE_MONTHS[implicit],
                    "avg",
                )
            if collapse:
                source = _SOURCE_MONTHS[s["field"]["frequency"]]
                target = _COLLAPSE_MONTHS[collapse]
                if target < source:
                    return httpx.Response(
                        400,
                        json={
                            "errors": [
                                {
                                    "error": "Intervalo de collapse inválido para la(s) serie(s) "
                                    f"seleccionadas: {collapse}. Pruebe con un intervalo mayor"
                                }
                            ]
                        },
                    )
                window = _collapse(window, source, target, how)
            # La ventana se aplica después de agregar, sobre la fecha de cada
            # período, y antes de la representación.
            windows.append(
                [
                    (f, v)
                    for f, v in window
                    if (not start_date or f >= start_date) and (not end_date or f <= end_date)
                ]
            )
        # Eje de tiempo común: la unión de las fechas, como la API. `count`
        # cuenta las filas ya agregadas.
        dates = sorted({f for window in windows for f, _ in window})
        count = len(dates)
        columns = []
        for s, window in zip(chosen, windows, strict=True):
            per_year = (
                _COLLAPSE_PER_YEAR[collapse]
                if collapse
                else _PERIODS_PER_YEAR[implicit or s["field"]["frequency"]]
            )
            columns.append(dict(_transform(window, mode, per_year)))
        rows = [
            [f, *[col.get(f) for col in columns]]
            for f in dates
            if any(col.get(f) is not None for col in columns)
        ]
        if params.get("sort") == "desc":
            rows = rows[::-1]
        page = rows[start : start + limit]
        axis: dict[str, Any] = {
            "frequency": collapse or _AXIS_FREQUENCY[implicit or chosen[0]["field"]["frequency"]]
        }
        if page:
            axis.update(start_date=page[0][0], end_date=page[-1][0])
        meta: list[dict[str, Any]] = [axis]
        for s in chosen:
            field = dict(s["field"])
            field["representation_mode"] = mode
            field["representation_mode_units"] = _REPRESENTATION_UNITS[mode] or field["units"]
            field["is_percentage"] = mode.startswith("percent")
            meta.append({"field": field, "dataset": s["dataset"]})
        return httpx.Response(200, json={"data": page, "count": count, "meta": meta})

    def adapter(self) -> SeriesTiempoAdapter:
        return SeriesTiempoAdapter(httpx.AsyncClient(transport=httpx.MockTransport(self.handler)))

    def series_requests(self) -> list[dict[str, str]]:
        return [r for r in self.requests if "ids" in r]
