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

- ``min_value`` y ``max_value`` de la metadata son el rango de TODA la serie:
  no cambian con la ventana, la representación ni el ``collapse``;
- ``time_index_end`` puede estar atrasado: la pobreza 64.2 dice 2026-01-01 y
  ya trae la fila 2026-07-01 (``serie(time_index_end=…)``);
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
RESERVAS_ID = "174.1_RRVAS_IDOS_0_0_36"
TIPO_CAMBIO_ID = "92.2_TIPO_CAMBIION_0_0_21_24"
EXPO_ID = "74.3_IET_0_M_16"
DESEMPLEO_ID = "45.2_ECTDT_0_T_33"
ACTIVIDAD_ID = "43.2_ECTAT_0_T_33"
EMPLEO_ID = "42.3_EPH_PUNTUATAL_0_M_24"
SUBOCUPACION_ID = "46.2_ECTST_0_T_36"
HOGARES_POBRES_ID = "63.2_HOGARES_PONUA_0_0_41_84"
POBREZA_ID = "64.2_POBLACION_NUA_0_0_34_74"
PLAZO_FIJO_USD_ID = "174.1_T_INTERUS_0_0_43"
SALARIOS_ID = "149.1_TL_INDIIOS_OCTU_0_21"
RESERVAS_DIARIAS_ID = "92.2_RESERVAS_IRES_0_0_32_40"

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
) -> dict[str, Any]:
    """Una serie de la API falsa.

    ``value_range`` es el (mínimo, máximo) de toda la serie en la fuente; por
    defecto, el de ``data``. ``time_index_end`` simula una metadata atrasada.
    """
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


def exportaciones_reales() -> dict[str, Any]:
    """Exportaciones totales (74.3) de 2023-01 a 2026-08, grabadas de la API el 05-oct.

    Suma 2024: 79.703,2; suma 2025: 87.111,2 (+9,29 %). Diciembre contra
    diciembre: 7.049,0 → 7.482,4 (+6,15 %).
    """
    return _grabada("expo_74_3.json", EXPO_ID)


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
    # Las dos de pobreza tienen la metadata atrasada: time_index_end dice
    # 2026-01-01 y la API ya trae 2026-07-01 (el 1er semestre de 2026).
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
        elif mode == "change" and i >= 1:
            out.append((fecha, value - values[i - 1]))
        elif mode == "percent_change" and i >= 1:
            out.append((fecha, value / values[i - 1] - 1))
        elif mode == "percent_change_a_year_ago" and i >= per_year:
            out.append((fecha, value / values[i - per_year] - 1))
        elif mode == "percent_change_since_beginning_of_year":
            # Como la API real: contra el PRIMER dato del año dentro de la
            # ventana (enero), no contra el cierre del año anterior.
            year = fecha[:4]
            first = next(v for f, v in window if f[:4] == year)
            out.append((fecha, value / first - 1))
    return out


def _period_start(fecha: str, months: int) -> str:
    year, month = int(fecha[:4]), int(fecha[5:7])
    return date(year, (month - 1) // months * months + 1, 1).isoformat()


def _collapse(
    window: list[tuple[str, float]], source_months: int, target_months: int, how: str
) -> list[tuple[str, float]]:
    groups: dict[str, list[float]] = {}
    for fecha, value in window:
        groups.setdefault(_period_start(fecha, target_months), []).append(value)
    expected = target_months // source_months if source_months else None
    return [
        (period, _AGGREGATE[how](values))
        for period, values in groups.items()
        # Como la API: los períodos incompletos (el año en curso, el primero
        # si la serie arranca a mitad de año) quedan afuera.
        if expected is None or len(values) >= expected
    ]


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
        start_date, end_date = params.get("start_date"), params.get("end_date")
        mode = params.get("representation_mode", "value")
        collapse = params.get("collapse")
        how = params.get("collapse_aggregation", "avg")
        ids = params["ids"].split(",")
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
