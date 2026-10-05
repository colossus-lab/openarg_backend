"""Los valores esperados de la batería, calculados desde la fuente oficial.

Hasta octubre de 2026 sólo 2 de 53 casos tenían ``expected_values``, los dos
fijos a mano: los datos macro cambian todos los meses y nadie los mantenía.
Por eso el juez LLM aprobaba un «≈14 %» cuando la acumulada era 14,58 %, y
``series_004`` pasaba con "35.001 en abril de 2023".

Un caso declara **cómo** se calcula su cifra y la batería la calcula en el
momento de correr, contra la misma fuente que debería usar el motor:

- API de Series de Tiempo (``apis.datos.gob.ar``), siempre sobre el **valor
  de la serie** y nunca con ``representation_mode``: con ``sort=desc`` la API
  descarta las últimas observaciones de una transformación (la interanual
  pierde 12 meses, la mensual 1; verificado el 04-oct-2026). Las variaciones
  se calculan acá, sobre el índice.
- API v4 del BCRA (``api.bcra.gob.ar/estadisticas/v4.0``, sin token; la v3
  responde 410).

Cada oráculo devuelve una ``Resolucion``: las cifras aceptadas (alternativas),
el período del dato, la frecuencia, desde qué período un dato todavía cuenta
como vigente, y las cifras que delatan un error conocido (sumar tasas en vez
de componerlas, promediar en vez de sumar). ``resolve_entry`` materializa lo
que declara un caso en ``expected_values`` / ``forbidden_values`` comunes, que
``quality_checks.assess`` ya sabe chequear, más ``expected_period``.

La resolución se hace **una vez por corrida** y queda congelada en el
reporte: ``--rescore`` vuelve a puntuar con lo que valía el día de la
corrida, no con el dato del mes siguiente. Si un oráculo falla, el caso
queda "no evaluable" (nunca aprobado).

Campos del dataset (``golden_dataset.json``):

``expected_values_from``
    ``[{oracle, args, tolerance?, rel_tolerance?, unit_patterns?, scales?, label?,
    forbidden_tolerance?, forbidden_rel_tolerance?}]``. ``scales: [1, 1e6]``
    cuando el dato viene en millones. ``any_of: [{oracle,
    args}, …]`` en lugar de ``oracle``/``args`` acepta la cifra de cualquiera
    de los oráculos (IPI o EMAE industria).
``forbidden_values_from``
    Mismo formato: cifras que no pueden aparecer (la serie mal etiquetada).
``expected_period_from``
    ``{oracle, args, max_atraso_dias?}``: el último período disponible en la
    fuente. Ver ``quality_checks.check_fecha_del_dato``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

SERIES_URL = "https://apis.datos.gob.ar/series/api/series/"
BCRA_URL = "https://api.bcra.gob.ar/estadisticas/v4.0/monetarias/"

# (url, params) → JSON. Se inyecta para probar los oráculos con respuestas
# grabadas, sin red.
Fetch = Callable[[str, dict[str, str]], dict[str, Any]]

_FREQUENCIES = {
    "R/P1D": "diaria",
    "R/P1W": "semanal",
    "R/P1M": "mensual",
    "R/P3M": "trimestral",
    "R/P6M": "semestral",
    "R/P1Y": "anual",
}

# Cuánto puede tener un dato antes de que la fuente misma esté atrasada, por
# frecuencia, contado desde el **fin** del período y hasta justo antes de la
# publicación siguiente. El IPI de julio (fin 31-jul) sigue siendo el último
# hasta principios de octubre (~70 días); la EPH del 2° trimestre (fin 30-jun)
# hasta mediados de diciembre (~170); la pobreza del 1er semestre hasta fines
# de marzo (~275); un año completo hasta que cierra el siguiente (~420).
MAX_ATRASO_DIAS = {
    "diaria": 7,
    "semanal": 14,
    "mensual": 80,
    "trimestral": 180,
    "semestral": 280,
    "anual": 430,
}
_MESES_POR_PERIODO = {"mensual": 1, "trimestral": 3, "semestral": 6, "anual": 12}


def period_end(start: date, frecuencia: str) -> date:
    """El último día del período que empieza en ``start``."""
    months = _MESES_POR_PERIODO.get(frecuencia)
    if not months:
        return start
    return _shift_month(start.replace(day=1), months) - timedelta(days=1)


class OracleError(Exception):
    """La fuente no respondió o no tiene lo que el caso pide."""


@dataclass(frozen=True)
class Resolucion:
    valores: list[float]
    periodo: str
    frecuencia: str
    aceptable_desde: str
    prohibidos: list[float] = field(default_factory=list)
    detalle: str = ""


# ── HTTP ───────────────────────────────────────────────────


def http_fetch(timeout: float = 20.0, retries: int = 2) -> Fetch:
    """El ``Fetch`` real: httpx con reintentos cortos."""
    import httpx

    def fetch(url: str, params: dict[str, str]) -> dict[str, Any]:
        last: Exception | None = None
        for _ in range(retries + 1):
            try:
                resp = httpx.get(url, params=params, timeout=timeout)
                resp.raise_for_status()
                data: dict[str, Any] = resp.json()
                return data
            except Exception as exc:  # noqa: BLE001 — se reintenta y se informa abajo
                last = exc
        raise OracleError(f"{url}: {type(last).__name__}: {last}"[:300])

    return fetch


# ── fechas ─────────────────────────────────────────────────


def _parse_date(text: str) -> date:
    return date.fromisoformat(str(text)[:10])


def _months_apart(a: date, b: date) -> int:
    return (b.year - a.year) * 12 + (b.month - a.month)


def _shift_month(d: date, delta: int) -> date:
    total = d.year * 12 + (d.month - 1) + delta
    return date(total // 12, total % 12 + 1, 1)


def business_days_before(today: date, n: int) -> date:
    """El día hábil (lunes a viernes) ``n`` días hábiles antes de ``today``.

    Sin feriados: si el BCRA no publicó por un feriado, ``bcra_ultimo``
    igual acepta su último dato (ver ``aceptable_desde``).
    """
    d = today
    left = n
    while left > 0:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            left -= 1
    return d


# ── API de Series de Tiempo ────────────────────────────────


def _serie(
    fetch: Fetch,
    hoy: date,
    serie: str,
    *,
    last: int,
) -> tuple[list[tuple[date, float]], str]:
    """Las últimas ``last`` observaciones (ascendentes, sin nulos) y la frecuencia.

    ``sort=desc`` sin ``representation_mode``: los valores crudos de la serie
    son correctos en cualquier orden. Se descartan las observaciones
    posteriores a ``hoy`` (re-puntuar un reporte viejo).
    """
    raw = fetch(
        SERIES_URL,
        {
            "ids": serie,
            "sort": "desc",
            "limit": str(last),
            "format": "json",
            "metadata": "full",
        },
    )
    rows = raw.get("data") or []
    if not rows:
        raise OracleError(f"la serie {serie} no devolvió datos")
    meta = (raw.get("meta") or [{}, {}])[1:] or [{}]
    freq_code = str(((meta[0] or {}).get("field") or {}).get("frequency") or "")
    if not freq_code:
        freq_code = str((raw.get("meta") or [{}])[0].get("frequency") or "")
    frecuencia = _FREQUENCIES.get(
        freq_code, {"month": "mensual", "day": "diaria"}.get(freq_code, "")
    )
    obs = sorted(
        (_parse_date(r[0]), float(r[1]))
        for r in rows
        if r and len(r) > 1 and r[1] is not None and _parse_date(r[0]) <= hoy
    )
    if not obs:
        raise OracleError(f"la serie {serie} no tiene observaciones hasta {hoy}")
    return obs, frecuencia or "mensual"


def _aceptable_desde(periodo: date, frecuencia: str, hoy: date, args: dict[str, Any]) -> date:
    if frecuencia == "diaria":
        n = int(args.get("dias_habiles", 5))
        return min(periodo, business_days_before(hoy, n))
    if args.get("acepta_periodo_anterior") and frecuencia == "mensual":
        return _shift_month(periodo, -1)
    return periodo


def serie_ultimo(fetch: Fetch, hoy: date, **args: Any) -> Resolucion:
    obs, freq = _serie(fetch, hoy, str(args["serie"]), last=5)
    fecha, valor = obs[-1]
    return Resolucion(
        valores=[valor],
        periodo=fecha.isoformat(),
        frecuencia=freq,
        aceptable_desde=_aceptable_desde(fecha, freq, hoy, args).isoformat(),
        detalle=f"{args['serie']} {fecha.isoformat()} = {valor:g}",
    )


def _pct(a: float, b: float) -> float:
    return (a / b - 1.0) * 100.0


def serie_variacion_mensual(fetch: Fetch, hoy: date, **args: Any) -> Resolucion:
    """Variación % del último mes contra el anterior, sobre el índice."""
    obs, freq = _serie(fetch, hoy, str(args["serie"]), last=3)
    if len(obs) < 2:
        raise OracleError("hacen falta dos observaciones")
    (f0, v0), (f1, v1) = obs[-2], obs[-1]
    if _months_apart(f0, f1) != 1:
        raise OracleError(f"{f0} y {f1} no son meses consecutivos")
    valor = _pct(v1, v0)
    return Resolucion(
        valores=[valor],
        periodo=f1.isoformat(),
        frecuencia=freq,
        aceptable_desde=_aceptable_desde(f1, freq, hoy, args).isoformat(),
        detalle=f"{v1:g} / {v0:g} − 1 = {valor:.4f} % ({f1:%Y-%m})",
    )


def serie_variacion_interanual(fetch: Fetch, hoy: date, **args: Any) -> Resolucion:
    """``índice[t] / índice[t−12] − 1``, con t y t−12 a doce meses justos."""
    obs, freq = _serie(fetch, hoy, str(args["serie"]), last=15)
    ft, vt = obs[-1]
    target = _shift_month(ft, -12)
    base = next(((f, v) for f, v in obs if f == target), None)
    if base is None:
        raise OracleError(f"falta la observación de {target:%Y-%m} para la interanual")
    valor = _pct(vt, base[1])
    return Resolucion(
        valores=[valor],
        periodo=ft.isoformat(),
        frecuencia=freq,
        aceptable_desde=_aceptable_desde(ft, freq, hoy, args).isoformat(),
        detalle=f"{vt:g} / {base[1]:g} − 1 = {valor:.4f} % ({ft:%Y-%m} vs {target:%Y-%m})",
    )


def serie_acumulada_anio(fetch: Fetch, hoy: date, **args: Any) -> Resolucion:
    """Variación acumulada en el año: ``índice[t] / índice[diciembre anterior] − 1``.

    Es la definición del INDEC ("acumulada en el año, respecto de diciembre
    del año anterior"). La API de Series ofrece
    ``percent_change_since_beginning_of_year``, pero mide desde **enero**: el
    04-oct-2026 daba 17,90 % para enero-agosto, contra 21,30 % del INDEC.
    Esa cifra queda prohibida: es la que sale de usar la representación de la
    API como si fuera la acumulada.
    """
    obs, freq = _serie(fetch, hoy, str(args["serie"]), last=15)
    ft, vt = obs[-1]
    target = date(ft.year - 1, 12, 1)
    base = next(((f, v) for f, v in obs if f == target), None)
    if base is None:
        raise OracleError(f"falta diciembre de {ft.year - 1}")
    valor = _pct(vt, base[1])
    enero = next((v for f, v in obs if f == date(ft.year, 1, 1)), None)
    desde_enero = _pct(vt, enero) if enero is not None and ft.month > 1 else None
    return Resolucion(
        valores=[valor],
        periodo=ft.isoformat(),
        frecuencia=freq,
        aceptable_desde=_aceptable_desde(ft, freq, hoy, args).isoformat(),
        prohibidos=[desde_enero] if desde_enero is not None else [],
        detalle=(
            f"{vt:g} / {base[1]:g} − 1 = {valor:.4f} % (dic-{ft.year - 1} a {ft:%m/%Y})"
            + (f"; desde enero sería {desde_enero:.4f} %" if desde_enero is not None else "")
        ),
    )


def serie_acumulada_tramo(fetch: Fetch, hoy: date, **args: Any) -> Resolucion:
    """Acumulada de ``desde`` a ``hasta`` (meses ``AAAA-MM``), compuesta.

    La cifra prohibida es la **suma** de las tasas mensuales: es el error que
    el juez LLM no veía (marzo-agosto 2026: 14,58 % compuesta, 13,77 % sumada).
    """
    desde = _parse_date(f"{args['desde']}-01")
    hasta = _parse_date(f"{args['hasta']}-01")
    base = _shift_month(desde, -1)
    span = _months_apart(base, hasta)
    if span < 1:
        raise OracleError("el tramo tiene que cubrir al menos un mes")
    # Lo suficiente para llegar desde hoy hasta el mes base.
    last = max(_months_apart(base, hoy.replace(day=1)) + 3, span + 3)
    obs, freq = _serie(fetch, hoy, str(args["serie"]), last=min(last, 1000))
    by_month = {f.replace(day=1): v for f, v in obs}
    months = [_shift_month(base, i) for i in range(span + 1)]
    missing = [m for m in months if m not in by_month]
    if missing:
        raise OracleError(f"faltan {', '.join(f'{m:%Y-%m}' for m in missing[:3])}")
    compuesta = _pct(by_month[hasta], by_month[base])
    suma = sum(_pct(by_month[months[i]], by_month[months[i - 1]]) for i in range(1, len(months)))
    return Resolucion(
        valores=[compuesta],
        periodo=hasta.isoformat(),
        frecuencia=freq,
        aceptable_desde=hasta.isoformat(),
        prohibidos=[suma] if abs(suma - compuesta) > 0.2 else [],
        detalle=f"{desde:%Y-%m}..{hasta:%Y-%m}: compuesta {compuesta:.4f} %, suma de tasas {suma:.4f} %",
    )


def serie_suma_anual(fetch: Fetch, hoy: date, **args: Any) -> Resolucion:
    """Suma de los 12 meses del último año completo (o de ``anio``).

    La cifra prohibida es el **promedio** mensual: es lo que devuelve
    ``collapse=year`` de la API (exportaciones 2025: 7.259 en vez de 87.111).
    Como toda cuenta mal hecha, sólo falla si la respuesta no da también la
    suma: "unos 7.259 millones por mes" al lado del total está bien.
    """
    obs, freq = _serie(fetch, hoy, str(args["serie"]), last=40)
    by_year: dict[int, list[float]] = {}
    for f, v in obs:
        by_year.setdefault(f.year, []).append(v)
    wanted = args.get("anio", "ultimo_completo")
    if wanted == "ultimo_completo":
        full = [y for y, vs in by_year.items() if len(vs) == 12]
        if not full:
            raise OracleError("no hay un año completo")
        year = max(full)
    else:
        year = int(wanted)
        if len(by_year.get(year, [])) != 12:
            raise OracleError(f"{year} no tiene los 12 meses")
    values = by_year[year]
    total = sum(values)
    return Resolucion(
        valores=[total],
        periodo=date(year, 1, 1).isoformat(),
        frecuencia="anual",
        aceptable_desde=date(year, 1, 1).isoformat(),
        prohibidos=[total / 12],
        detalle=f"{args['serie']} {year}: suma {total:.2f}, promedio {total / 12:.2f}",
    )


# ── API v4 del BCRA ────────────────────────────────────────


def bcra_ultimo(fetch: Fetch, hoy: date, **args: Any) -> Resolucion:
    """El último dato de una variable del BCRA, y los de los últimos días hábiles.

    Se acepta cualquier valor publicado desde ``dias_habiles`` días hábiles
    antes de ``hoy`` (o el último, si el BCRA está más atrasado que eso).
    Variables: 1 reservas, 4 dólar minorista promedio vendedor, 5 mayorista
    Comunicación A 3500, 15 base monetaria.
    """
    variable = int(args["variable"])
    raw = fetch(
        f"{BCRA_URL}{variable}",
        {"limit": "30", "hasta": hoy.isoformat()},
    )
    results = raw.get("results") or []
    detalle = (results[0] or {}).get("detalle") if results else None
    if not detalle:
        raise OracleError(f"la variable {variable} del BCRA no devolvió datos")
    obs = sorted(
        (_parse_date(d["fecha"]), float(d["valor"]))
        for d in detalle
        if d.get("valor") is not None and _parse_date(d["fecha"]) <= hoy
    )
    if not obs:
        raise OracleError(f"la variable {variable} no tiene datos hasta {hoy}")
    ultima, valor = obs[-1]
    n = int(args.get("dias_habiles", 5))
    desde = min(ultima, business_days_before(hoy, n))
    aceptados = [v for f, v in obs if f >= desde]
    return Resolucion(
        valores=aceptados,
        periodo=ultima.isoformat(),
        frecuencia="diaria",
        aceptable_desde=desde.isoformat(),
        detalle=f"BCRA var. {variable}: {ultima.isoformat()} = {valor:g} (desde {desde})",
    )


ORACLES: dict[str, Callable[..., Resolucion]] = {
    "serie_ultimo": serie_ultimo,
    "serie_variacion_mensual": serie_variacion_mensual,
    "serie_variacion_interanual": serie_variacion_interanual,
    "serie_acumulada_anio": serie_acumulada_anio,
    "serie_acumulada_tramo": serie_acumulada_tramo,
    "serie_suma_anual": serie_suma_anual,
    "bcra_ultimo": bcra_ultimo,
}


# ── lo que declara un caso ─────────────────────────────────


def spec_problems(spec: Any, *, allow_any_of: bool = True) -> list[str]:
    """Qué está mal en una referencia a un oráculo (para ``validate_dataset``)."""
    if not isinstance(spec, dict):
        return [f"tiene que ser un objeto: {spec!r}"]
    if allow_any_of and "any_of" in spec:
        options = spec["any_of"]
        if not isinstance(options, list) or not options:
            return ["any_of tiene que ser una lista no vacía"]
        return [p for o in options for p in spec_problems(o, allow_any_of=False)]
    problems: list[str] = []
    if spec.get("oracle") not in ORACLES:
        problems.append(f"oráculo desconocido {spec.get('oracle')!r}")
    if not isinstance(spec.get("args", {}), dict):
        problems.append("args tiene que ser un objeto")
    return problems


class _Resolver:
    """Resuelve referencias con caché: dos casos que piden lo mismo, un pedido."""

    def __init__(self, fetch: Fetch, hoy: date) -> None:
        self._fetch = fetch
        self.hoy = hoy
        self._cache: dict[str, Resolucion | OracleError] = {}

    def one(self, ref: dict[str, Any]) -> Resolucion:
        key = json.dumps({"o": ref.get("oracle"), "a": ref.get("args") or {}}, sort_keys=True)
        if key not in self._cache:
            try:
                fn = ORACLES[str(ref["oracle"])]
                self._cache[key] = fn(self._fetch, self.hoy, **(ref.get("args") or {}))
            except OracleError as exc:
                self._cache[key] = exc
            except Exception as exc:  # noqa: BLE001 — un oráculo roto deja el caso no evaluable
                self._cache[key] = OracleError(f"{type(exc).__name__}: {exc}"[:200])
        hit = self._cache[key]
        if isinstance(hit, OracleError):
            raise hit
        return hit

    def spec(self, spec: dict[str, Any]) -> Resolucion:
        """Una referencia, o la unión de las alternativas de ``any_of``."""
        if "any_of" not in spec:
            return self.one(spec)
        parts = [self.one(o) for o in spec["any_of"]]
        latest = max(parts, key=lambda r: r.periodo)
        return Resolucion(
            valores=[v for r in parts for v in r.valores],
            periodo=latest.periodo,
            frecuencia=latest.frecuencia,
            aceptable_desde=min(r.aceptable_desde for r in parts),
            prohibidos=[v for r in parts for v in r.prohibidos],
            detalle=" | ".join(r.detalle for r in parts),
        )


def _label(spec: dict[str, Any]) -> str:
    if spec.get("label"):
        return str(spec["label"])
    if "any_of" in spec:
        return " o ".join(str(o.get("oracle")) for o in spec["any_of"])
    return str(spec.get("oracle"))


_VALUE_KEYS = ("tolerance", "rel_tolerance", "unit_patterns", "scales")


def resolve_entry(entry: dict[str, Any], resolver: _Resolver) -> dict[str, Any]:
    """Lo que un caso declara con oráculos, materializado para ``assess``.

    Devuelve ``{expected_values, forbidden_values, expected_period, errores,
    detalle}``: los dos primeros con el formato de siempre (``value`` puede
    ser una lista de alternativas), así ``assess`` no cambia de contrato.
    """
    out: dict[str, Any] = {
        "expected_values": [],
        "forbidden_values": [],
        "expected_period": None,
        "errores": [],
        "detalle": [],
    }
    for spec in entry.get("expected_values_from") or []:
        label = _label(spec)
        try:
            res = resolver.spec(spec)
        except OracleError as exc:
            out["errores"].append(f"{label}: {exc}")
            continue
        out["expected_values"].append(
            {
                "value": res.valores,
                **{k: spec[k] for k in _VALUE_KEYS if k in spec},
                "label": label,
                "periodo": res.periodo,
            }
        )
        out["detalle"].append(f"{label}: {res.detalle}")
        for bad in res.prohibidos:
            # Falla sólo si la respuesta da la cuenta mal hecha EN LUGAR de la
            # correcta: "87.111 millones en 2025; unos 7.259 por mes" está bien
            # (ver ``quality_checks._excused_by``).
            out["forbidden_values"].append(
                {
                    "value": bad,
                    "tolerance": spec.get("forbidden_tolerance", 0.0),
                    "rel_tolerance": spec.get("forbidden_rel_tolerance", 0.0),
                    **({"scales": spec["scales"]} if "scales" in spec else {}),
                    "label": f"{label} (cuenta mal hecha)",
                    "salvo_si_aparece": label,
                }
            )
    for spec in entry.get("forbidden_values_from") or []:
        label = _label(spec)
        try:
            res = resolver.spec(spec)
        except OracleError as exc:
            out["errores"].append(f"{label}: {exc}")
            continue
        out["forbidden_values"].append(
            {
                "value": res.valores,
                **{k: spec[k] for k in _VALUE_KEYS if k in spec},
                "label": label,
            }
        )
        out["detalle"].append(f"prohibido {label}: {res.detalle}")
    spec = entry.get("expected_period_from")
    if spec:
        label = _label(spec)
        try:
            res = resolver.spec(spec)
        except OracleError as exc:
            out["errores"].append(f"período ({label}): {exc}")
        else:
            fin = period_end(_parse_date(res.periodo), res.frecuencia)
            limite = int(spec.get("max_atraso_dias") or MAX_ATRASO_DIAS.get(res.frecuencia, 80))
            out["expected_period"] = {
                "periodo": res.periodo,
                "frecuencia": res.frecuencia,
                "aceptable_desde": res.aceptable_desde,
                "atrasado_en_fuente": (resolver.hoy - fin).days > limite,
                "hoy": resolver.hoy.isoformat(),
                "label": label,
            }
    return out


def resolve_dataset(
    entries: list[dict[str, Any]],
    fetch: Fetch | None = None,
    hoy: date | None = None,
) -> dict[str, Any]:
    """Resuelve todos los casos con oráculos. Es lo que se congela en el reporte."""
    hoy = hoy or date.today()
    resolver = _Resolver(fetch or http_fetch(), hoy)
    casos = {
        e["id"]: resolve_entry(e, resolver)
        for e in entries
        if e.get("expected_values_from")
        or e.get("forbidden_values_from")
        or e.get("expected_period_from")
    }
    return {"hoy": hoy.isoformat(), "casos": casos}


def resolucion_dict(res: Resolucion) -> dict[str, Any]:
    return asdict(res)
