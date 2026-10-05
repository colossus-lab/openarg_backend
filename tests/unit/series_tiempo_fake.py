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

``collapse`` no se simula: los tests que lo usan sólo miran los parámetros.
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

_AXIS_FREQUENCY = {"R/P1D": "day", "R/P1M": "month", "R/P3M": "quarter", "R/P1Y": "year"}
_PERIODS_PER_YEAR = {"R/P1D": 365, "R/P1M": 12, "R/P3M": 4, "R/P1Y": 1}
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
) -> dict[str, Any]:
    return {
        "id": sid,
        "data": data,
        "field": {
            "id": sid,
            "description": description,
            "units": units,
            "frequency": frequency,
            "time_index_start": data[0][0],
            "time_index_end": data[-1][0],
            "time_index_size": str(len(data)),
            "is_updated": "True" if is_updated else "False",
        },
        "dataset": {"title": dataset, "source": source},
    }


def ipc_real() -> dict[str, Any]:
    """El IPC nacional tal como lo devolvió la API el 04-oct (117 meses)."""
    raw = json.loads((FIXTURES / "ipc_148_3.json").read_text(encoding="utf-8"))
    return serie(
        IPC_ID,
        [(f, v) for f, v in raw["data"]],
        description=raw["field"]["description"],
        units=raw["field"]["units"],
        dataset=raw["dataset"]["title"],
    )


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
        ids = params["ids"].split(",")
        chosen = [self.series[i] for i in ids]
        # Eje de tiempo común: la unión de las fechas, como la API.
        dates = sorted({f for s in chosen for f, _ in s["data"]})
        dates = [
            f
            for f in dates
            if (not start_date or f >= start_date) and (not end_date or f <= end_date)
        ]
        count = len(dates)
        columns = []
        for s in chosen:
            by_date = dict(s["data"])
            window = [(f, by_date[f]) for f in dates if f in by_date]
            per_year = _PERIODS_PER_YEAR[s["field"]["frequency"]]
            columns.append(dict(_transform(window, mode, per_year)))
        rows = [
            [f, *[col.get(f) for col in columns]]
            for f in dates
            if any(col.get(f) is not None for col in columns)
        ]
        if params.get("sort") == "desc":
            rows = rows[::-1]
        page = rows[start : start + limit]
        axis: dict[str, Any] = {"frequency": _AXIS_FREQUENCY[chosen[0]["field"]["frequency"]]}
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
