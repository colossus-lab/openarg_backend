"""Un doble de la API pública del BCRA para ``httpx.MockTransport``.

Copia lo que se midió contra la API real el 04-oct (sin token):

- ``/estadisticas/v4.0/monetarias``: catálogo paginado (``count`` total,
  ``limit`` hasta 3000, ``offset``).
- ``/estadisticas/v4.0/monetarias/{id}``: ``results[0].detalle`` de la más
  nueva a la más vieja; ``desde``/``hasta`` ISO; ``limit`` > 3000 da 400;
  ``desde`` futura da 400; con ``offset`` pasado el final, ``count`` vuelve 0.
- ``/estadisticascambiarias/v1.0/Cotizaciones``: la fecha va en la cabecera;
  ``fechaDesde`` da 400 ("El parámetro fechaDesde no es válido"); ``?fecha=``
  de un sábado vuelve ``{"fecha": null, "detalle": []}``.
- ``/estadisticascambiarias/v1.0/Cotizaciones/{moneda}``: historia paginada
  con ``fechadesde``/``fechahasta``; ``limit`` entre 10 y 1000.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import httpx

HOY = date(2026, 10, 4)  # domingo


def _habiles(desde: date, hasta: date) -> list[date]:
    out = []
    d = desde
    while d <= hasta:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _serie_reservas() -> list[tuple[str, float]]:
    dias = _habiles(date(2020, 1, 2), date(2026, 9, 30))
    serie = [(d.isoformat(), float(40000 + i)) for i, d in enumerate(dias)]
    serie[-2] = (serie[-2][0], 47482.0)
    serie[-1] = ("2026-09-30", 46092.0)
    return serie


def _serie_diaria(hasta: date, ultimo: float, n: int = 400) -> list[tuple[str, float]]:
    dias = _habiles(hasta - timedelta(days=n), hasta)
    serie = [(d.isoformat(), round(ultimo - (len(dias) - i) * 0.5, 4)) for i, d in enumerate(dias)]
    serie[-1] = (hasta.isoformat(), ultimo)
    return serie


def _serie_uva() -> list[tuple[str, float]]:
    # La UVA se publica por adelantado: hay valores de días que no llegaron.
    d, out, v = date(2026, 6, 1), [], 2000.0
    while d <= date(2026, 10, 15):
        out.append((d.isoformat(), round(v, 2)))
        d += timedelta(days=1)
        v += 1.0
    return out


SERIES: dict[int, list[tuple[str, float]]] = {
    1: _serie_reservas(),
    4: _serie_diaria(date(2026, 10, 2), 1543.18),
    5: _serie_diaria(date(2026, 10, 2), 1523.0868),
    7: _serie_diaria(date(2026, 10, 1), 23.1875),
    15: _serie_diaria(date(2026, 9, 30), 46909419.0),
    31: _serie_uva(),
}

CATALOGO: list[dict[str, Any]] = [
    {
        "idVariable": 1,
        "descripcion": "Reservas internacionales",
        "categoria": "Principales Variables",
        "periodicidad": "D",
        "unidadExpresion": "En millones de USD",
        "ultFechaInformada": "2026-09-30",
        "ultValorInformado": 46092.0,
    },
    {
        "idVariable": 4,
        "descripcion": "Tipo de cambio minorista (promedio vendedor)",
        "categoria": "Principales Variables",
        "periodicidad": "D",
        "unidadExpresion": "Pesos argentinos por dólar estadounidense",
        "ultFechaInformada": "2026-10-02",
        "ultValorInformado": 1543.18,
    },
    {
        "idVariable": 5,
        "descripcion": "Tipo de cambio mayorista de referencia",
        "categoria": "Principales Variables",
        "periodicidad": "D",
        "unidadExpresion": "Pesos argentinos por dólar estadounidense",
        "ultFechaInformada": "2026-10-02",
        "ultValorInformado": 1523.0868,
    },
    {
        "idVariable": 7,
        "descripcion": "Tasa de interés BADLAR de bancos privados",
        "categoria": "Principales Variables",
        "periodicidad": "D",
        "unidadExpresion": "En porcentaje nominal anual",
        "ultFechaInformada": "2026-10-01",
        "ultValorInformado": 23.1875,
    },
    {
        "idVariable": 15,
        "descripcion": "Base monetaria",
        "categoria": "Principales Variables",
        "periodicidad": "D",
        "unidadExpresion": "En millones de ARS",
        "ultFechaInformada": "2026-09-30",
        "ultValorInformado": 46909419.0,
    },
    {
        "idVariable": 31,
        "descripcion": "Unidad de valor adquisitivo (base 31.3.16=14.05)",
        "categoria": "Principales Variables",
        "periodicidad": "D",
        "unidadExpresion": "En ARS",
        "ultFechaInformada": "2026-10-15",
        "ultValorInformado": 2137.0,
    },
] + [
    # Relleno para que el catálogo ocupe más de una página, como el real (1.610).
    {"idVariable": 5000 + i, "descripcion": f"Relleno {i}", "periodicidad": "M"}
    for i in range(20)
]

COTIZACIONES_DIA = {
    "2026-10-02": [
        {
            "codigoMoneda": "EUR",
            "descripcion": "EURO",
            "tipoPase": 1.17,
            "tipoCotizacion": 1710.76,
        },
        {
            "codigoMoneda": "REF",
            "descripcion": "DOLAR REFERENCIA COM 3500",
            "tipoPase": 0.0,
            "tipoCotizacion": 1523.0868,
        },
        {
            "codigoMoneda": "USD",
            "descripcion": "DOLAR E.E.U.U.",
            "tipoPase": 0.0,
            "tipoCotizacion": 1520.0,
        },
    ],
    "2026-10-01": [
        {
            "codigoMoneda": "USD",
            "descripcion": "DOLAR E.E.U.U.",
            "tipoPase": 0.0,
            "tipoCotizacion": 1524.5,
        },
    ],
}
USD_HISTORIA = [
    (d.isoformat(), 1400.0 + i * 0.25) for i, d in enumerate(_habiles(date(2025, 1, 2), HOY))
]


def _bad(msg: str) -> httpx.Response:
    return httpx.Response(400, json={"status": 400, "errorMessages": [msg]})


def _iso(value: str | None) -> date | None:
    if value is None:
        return None
    return date.fromisoformat(value)


class FakeBCRA:
    """Handler para ``httpx.MockTransport`` que registra cada pedido."""

    def __init__(self, *, fail_ids: set[int] | None = None, catalog_down: bool = False) -> None:
        self.requests: list[httpx.Request] = []
        self.fail_ids = fail_ids or set()
        self.catalog_down = catalog_down

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=self.transport())

    def paths(self) -> list[str]:
        return [r.url.path for r in self.requests]

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        q = dict(request.url.params)
        if path == "/estadisticas/v4.0/monetarias":
            if self.catalog_down:
                return httpx.Response(503, json={"status": 503})
            return self._page(CATALOGO, q)
        if path.startswith("/estadisticas/v4.0/monetarias/"):
            return self._variable(int(path.rsplit("/", 1)[1]), q)
        if path == "/estadisticascambiarias/v1.0/Cotizaciones":
            return self._cotizaciones(q)
        if path.startswith("/estadisticascambiarias/v1.0/Cotizaciones/"):
            return self._historia(path.rsplit("/", 1)[1], q)
        return httpx.Response(404, json={"status": 404})

    def _page(self, rows: list[Any], q: dict[str, str]) -> httpx.Response:
        limit = int(q.get("limit", 1000))
        offset = int(q.get("offset", 0))
        if limit > 3000:
            return _bad("El límite no puede superar los 3000 registros.")
        page = rows[offset : offset + limit]
        count = len(rows) if offset < len(rows) else 0
        meta = {"resultset": {"count": count, "offset": offset, "limit": limit}}
        return httpx.Response(200, json={"status": 200, "metadata": meta, "results": page})

    def _variable(self, id_variable: int, q: dict[str, str]) -> httpx.Response:
        if id_variable in self.fail_ids:
            return httpx.Response(500, json={"status": 500})
        if id_variable not in SERIES:
            return _bad("IdVariable invalida.")
        try:
            desde, hasta = _iso(q.get("desde")), _iso(q.get("hasta"))
        except ValueError:
            return _bad("Parámetro erróneo: Validar formato fecha desde.")
        if desde and desde > HOY:
            return _bad("Parámetro erróneo: La fecha desde no puede mayor a la actual.")
        if desde and hasta and desde > hasta:
            return _bad("Fecha desde no debe ser mayor a la fecha hasta.")
        rows = [
            {"fecha": f, "valor": v}
            for f, v in reversed(SERIES[id_variable])
            if (not desde or f >= desde.isoformat()) and (not hasta or f <= hasta.isoformat())
        ]
        limit = int(q.get("limit", 1000))
        offset = int(q.get("offset", 0))
        if limit > 3000:
            return _bad("El límite no puede superar los 3000 registros.")
        page = rows[offset : offset + limit]
        count = len(rows) if offset < len(rows) else 0
        return httpx.Response(
            200,
            json={
                "status": 200,
                "metadata": {"resultset": {"count": count, "offset": offset, "limit": limit}},
                "results": [{"idVariable": id_variable, "detalle": page}],
            },
        )

    def _cotizaciones(self, q: dict[str, str]) -> httpx.Response:
        if "fechaDesde" in q or "fechaHasta" in q:
            return _bad("El parámetro fechaDesde no es válido")
        fecha = q.get("fecha") or "2026-10-02"
        detalle = COTIZACIONES_DIA.get(fecha, [])
        return httpx.Response(
            200,
            json={
                "status": 200,
                "results": {"fecha": fecha if detalle else None, "detalle": detalle},
            },
        )

    def _historia(self, moneda: str, q: dict[str, str]) -> httpx.Response:
        limit = int(q.get("limit", 1000))
        if not 10 <= limit <= 1000:
            return _bad("Parámetro erróneo: El limite debe estar entre 10 y el 1000.")
        offset = int(q.get("offset", 0))
        desde, hasta = q.get("fechadesde", ""), q.get("fechahasta", "")
        days = [
            {
                "fecha": f,
                "detalle": [
                    {
                        "codigoMoneda": moneda,
                        "descripcion": "DOLAR E.E.U.U.",
                        "tipoPase": 0.0,
                        "tipoCotizacion": v,
                    }
                ],
            }
            for f, v in reversed(USD_HISTORIA)
            if desde <= f <= hasta
        ]
        page = days[offset : offset + limit]
        count = len(days) if offset < len(days) else 0
        return httpx.Response(
            200,
            json={
                "status": 200,
                "metadata": {"resultset": {"count": count, "offset": offset, "limit": limit}},
                "results": page,
            },
        )
