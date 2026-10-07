"""Los conectores en vivo, llamados directo por sus puertos.

No pasan por los ejecutores de pasos del pipeline viejo (``pipeline/connectors``),
que traen sus propios desvíos: personal legislativo elegía mal el método y
BCRA no filtraba la moneda en algunas respuestas. Acá el agente elige la
acción y los parámetros a la vista.
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

from app.application.answers.engine import ClarificationEvent
from app.application.answers.tools.base import (
    MAX_ROWS_FOR_MODEL,
    ToolContext,
    ToolInputError,
    ToolOutcome,
    count,
    int_arg,
    quoted,
    result_for_model,
    str_arg,
    to_json,
)
from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.ports.llm.agent_llm import AgentTool
from app.infrastructure.adapters.connectors.series_tiempo_adapter import (
    iso_date,
    match_catalog,
)

_FREQUENCIES = ("day", "week", "month", "quarter", "semester", "year")
_REPRESENTATIONS = (
    "value",
    "change",
    "percent_change",
    "percent_change_a_year_ago",
    "percent_change_since_beginning_of_year",
)
# Cómo agrega la API con `collapse`. Sin esto promedia: exportaciones 2025 con
# frecuencia=year daba 7.259 (el promedio mensual) en vez de 87.111.
_AGGREGATIONS = ("avg", "sum", "end_of_period", "max", "min")
_CASAS = (
    "oficial",
    "blue",
    "bolsa",
    "contadoconliqui",
    "cripto",
    "mayorista",
    "solidario",
    "tarjeta",
)


def _months_between(a: str, b: str) -> int | None:
    try:
        ya, ma = int(a[:4]), int(a[5:7])
        yb, mb = int(b[:4]), int(b[5:7])
    except (ValueError, IndexError):
        return None
    return (yb - ya) * 12 + (mb - ma)


def _shift_months(year: int, month: int, delta: int) -> tuple[int, int]:
    total = year * 12 + (month - 1) + delta
    return total // 12, total % 12 + 1


def _period_label(fecha: str, step: int | None, *, dated_by_end: bool = False) -> str | None:
    """La fecha de una observación como período: "2024-S1", "2026-T2", "2025".

    La API no fecha todas las series igual. El PBI trimestral fecha cada
    período por su primer día (el 2° trimestre de 2026 es `2026-04-01`), pero
    las series semestrales de pobreza del INDEC lo fechan por el día siguiente
    a su fin (el 1er semestre de 2024, 52,9 %, es `2024-07-01`). Con
    ``dated_by_end`` la fecha cierra el período anterior.
    """
    if not step or len(fecha) < 7:
        return None
    try:
        year, month = int(fecha[:4]), int(fecha[5:7])
    except ValueError:
        return None
    if dated_by_end:
        year, month = _shift_months(year, month, -step)
    if step == 12:
        return str(year)
    if step == 6:
        return f"{year}-S{1 if month <= 6 else 2}"
    if step == 3:
        return f"{year}-T{(month - 1) // 3 + 1}"
    return None


def _is_dated_by_end(last_fecha: str, step: int, today: date) -> bool:
    """¿La serie fecha cada período por su final?

    Si leer la última fecha como inicio del período da un período que todavía
    no terminó, la serie no puede estar fechada así: ningún organismo publica
    un semestre en curso. Medido en staging el 02-oct: la tasa de pobreza
    termina en `2026-07-01`, que leído como inicio sería el 2° semestre de
    2026, y el modelo lo presentó así.
    """
    try:
        year, month = int(last_fecha[:4]), int(last_fecha[5:7])
    except (ValueError, IndexError):
        return False
    end_year, end_month = _shift_months(year, month, step)
    # El período termina el día anterior al primer día de (end_year, end_month).
    return date(end_year, end_month, 1) > today


def _short(text: Any, limit: int = 80) -> str:
    clean = " ".join(str(text or "").split())
    return clean if len(clean) <= limit else clean[: limit - 1].rstrip() + "…"


def _freshness_for_model(meta: dict[str, Any]) -> dict[str, Any]:
    """Hasta cuándo llega el dato y si la fuente lo da por actualizado.

    Sale del contrato de metadatos de los conectores; sin esto el modelo no
    tenía cómo saber que reservas 174.1 está parada en abril en la fuente y
    la presentaba como "actual".

    Con varias series va el estado de cada una (``metadata["series"]``): el
    agregado junta la fecha de la más atrasada con el «desactualizada» de
    cualquiera, y el aviso de tipo de cambio + IPC decía «su último dato es
    del 2026-08-01» (el IPC, que está al día) por el tipo de cambio, que
    llega al 31-08.

    Con varias series va también la escala de cada una: desempleo (en %) y
    salarios (índice) en una misma llamada llevaban una sola unidad,
    «Porcentaje». Y si la API promedió una serie más fina para alinearla con
    las otras (diaria con mensual), se avisa.
    """
    out: dict[str, Any] = {}
    if meta.get("ultima_observacion"):
        out["ultima_observacion"] = meta["ultima_observacion"]
    if meta.get("frecuencia"):
        out["frecuencia"] = meta["frecuencia"]
    series = [s for s in (meta.get("series") or []) if isinstance(s, dict)]
    if len(series) > 1:
        out["por_serie"] = [
            {
                "serie": _short(s.get("titulo") or s.get("id")),
                "unidades": s.get("unidades"),
                "la_fuente_llega_hasta": s.get("fecha_fin_fuente"),
                "actualizada_en_fuente": s.get("actualizada_en_fuente"),
            }
            for s in series
        ]
    else:
        if meta.get("fecha_fin_fuente"):
            out["la_fuente_llega_hasta"] = meta["fecha_fin_fuente"]
        if meta.get("actualizada_en_fuente") is not None:
            out["actualizada_en_fuente"] = meta["actualizada_en_fuente"]
    scaled = [s for s in series if s.get("escalada_a_porcentaje")]
    if meta.get("unidad") == "porcentaje":
        out["escala"] = "Los valores ya están en %: 33.54 es 33,54 %."
    elif 0 < len(scaled) < len(series):
        en_pct = " y ".join(f"«{_short(s.get('titulo') or s.get('id'))}»" for s in scaled)
        resto = "; ".join(
            f"«{_short(s.get('titulo') or s.get('id'))}» va en sus unidades "
            f"({s.get('unidades') or 'sin unidades'})"
            for s in series
            if not s.get("escalada_a_porcentaje")
        )
        out["escala"] = (
            f"Las series no vienen en la misma escala: {en_pct} "
            f"{'están' if len(scaled) > 1 else 'está'} en % (7.9 es 7,9 %); {resto}. "
            "Leé cada columna con sus `unidades` de `por_serie`."
        )
    averaged = [s for s in series if s.get("promediada_por_api")]
    if averaged:
        nombres = " y ".join(
            f"«{_short(s.get('titulo') or s.get('id'))}» ({s.get('frecuencia') or 'más fina'})"
            for s in averaged
        )
        out["aviso_agregacion"] = (
            "Se pidieron juntas series de distinta frecuencia y la API las llevó a frecuencia "
            f"{meta.get('frecuencia') or 'más gruesa'} PROMEDIANDO {nombres}: cada valor es el "
            "promedio del período, no su último dato (en un saldo, como las reservas, no es el "
            "saldo a fin de período). Si eso importa, pedila sola, o con `frecuencia` y "
            "`agregacion=end_of_period` (saldos) o `agregacion=sum` (flujos)."
        )
    stale = [s for s in series if s.get("actualizada_en_fuente") is False]
    if len(series) > 1 and stale:
        detalle = " y ".join(
            f"«{_short(s.get('titulo') or s.get('id'))}» (su último dato es del "
            f"{s.get('fecha_fin_fuente') or 'sin fecha'})"
            for s in stale
        )
        plural = len(stale) > 1
        out["aviso"] = (
            f"De las series pedidas, la fuente marca como desactualizada{'s' if plural else ''} "
            f"{detalle}: no {'las' if plural else 'la'} presentes como el dato actual; decí de "
            f"qué fecha es{' cada una' if plural else ''}."
        )
    elif meta.get("actualizada_en_fuente") is False:
        hasta = meta.get("fecha_fin_fuente") or meta.get("ultima_observacion")
        out["aviso"] = (
            f"La fuente marca esta serie como desactualizada (su último dato es del {hasta}): "
            "no la presentes como el dato actual; decí de qué fecha es."
        )
    return out


def _tail_for_model(result: DataResult, last: int, *, today: date | None = None) -> dict[str, Any]:
    """Una serie como la ve el modelo: las últimas observaciones, no las primeras."""
    records = result.records or []
    meta = result.metadata or {}
    shown = records[-last:]
    if len(records) >= 2 and "fecha" in records[-1] and "fecha" in records[-2]:
        step = _months_between(str(records[-2]["fecha"]), str(records[-1]["fecha"]))
        if step in (3, 6, 12):
            by_end = _is_dated_by_end(str(records[-1]["fecha"]), step, today or date.today())
            shown = [
                {"periodo": _period_label(str(r.get("fecha", "")), step, dated_by_end=by_end), **r}
                for r in shown
            ]
    payload = result_for_model(result)
    payload["filas"] = shown
    payload.pop("nota", None)
    total = meta.get("total_fuente")
    if meta.get("truncada") and isinstance(total, int) and total > len(shown):
        # El total es el de la fuente (`count` de la API), no el de las filas
        # traídas: "las últimas 3 de 1000" escondía que la serie tenía 1036
        # y que esas 1000 eran las más viejas.
        payload["filas_totales"] = total
        payload["nota"] = (
            f"Se muestran las últimas {len(shown)} de un total de {total} observaciones "
            "de la serie (las más recientes del rango pedido)."
        )
    elif len(records) > len(shown):
        payload["nota"] = f"Se muestran las últimas {len(shown)} de {len(records)} observaciones."
    payload.update(_freshness_for_model(meta))
    return payload


# ── variación entre dos períodos, calculada en código ──────

# Cuántos meses cubre una observación según la frecuencia de la serie.
_STEP_MONTHS = {"mensual": 1, "trimestral": 3, "semestral": 6, "anual": 12}


def _period_bounds(text: str) -> tuple[str, str] | None:
    """'2026' → (2026-01-01, 2026-12-31); '2026-02' → el mes; una fecha → ese día."""
    raw = text.strip()
    start = iso_date(raw)
    if start is None:
        return None
    parts = raw.split("-")
    year, month = int(start[:4]), int(start[5:7])
    if len(parts) == 1:
        return start, f"{year}-12-31"
    if len(parts) == 2:
        nxt_year, nxt_month = _shift_months(year, month, 1)
        last_day = date.fromordinal(date(nxt_year, nxt_month, 1).toordinal() - 1)
        return start, last_day.isoformat()
    return start, start


def _is_year(text: str) -> bool:
    return len(text.strip().split("-")) == 1


def _observation_end(fecha: str, step_months: int | None) -> date | None:
    """El último día que cubre la observación fechada en `fecha`.

    Una diaria o semanal (sin `step_months`) cubre una semana: tolera un fin
    de semana largo sin dato.
    """
    try:
        obs = date.fromisoformat(fecha[:10])
    except ValueError:
        return None
    if step_months:
        year, month = _shift_months(obs.year, obs.month, step_months)
        return date.fromordinal(date(year, month, 1).toordinal() - 1)
    return date.fromordinal(obs.toordinal() + 7)


def _covers(fecha: str, step_months: int | None, period_start: str) -> bool:
    """¿La observación fechada en `fecha` cubre algún día del período pedido?

    Una trimestral fechada `2026-04-01` cubre abril a junio: sirve para
    "junio de 2026". El IPC de agosto no sirve para septiembre: si la serie
    todavía no publicó el período, no se usa el anterior en silencio.
    """
    obs_end = _observation_end(fecha, step_months)
    try:
        return obs_end is not None and obs_end >= date.fromisoformat(period_start)
    except ValueError:
        return False


def _leaves_period_open(fecha: str, step_months: int | None, period_end: str) -> bool:
    """¿El período pedido sigue después de la observación usada?

    `hasta=2026` en el IPC usa agosto de 2026: el año todavía no terminó.
    """
    obs_end = _observation_end(fecha, step_months)
    try:
        return obs_end is not None and obs_end < date.fromisoformat(period_end)
    except ValueError:
        return False


def _pick(
    records: list[dict[str, Any]], column: str, bounds: tuple[str, str], step: int | None
) -> tuple[str, float] | None:
    """La última observación no nula de `column` dentro del período pedido."""
    period_start, period_end = bounds
    for row in reversed(records):
        fecha = str(row.get("fecha", ""))[:10]
        value = row.get(column)
        if not fecha or fecha > period_end or value is None:
            continue
        if not _covers(fecha, step, period_start):
            return None
        try:
            return fecha, float(value)
        except (TypeError, ValueError):
            return None
    return None


def _value_columns(result: DataResult) -> list[str]:
    first = (result.records or [{}])[0]
    return [k for k in first if k not in ("fecha", "periodo")]


def _pct(ratio: float) -> float:
    return round(ratio * 100, 2)


# ── series de tiempo ───────────────────────────────────────


class BuscarSeries:
    status = "Buscando series de tiempo..."

    def describe(self, args: dict[str, Any]) -> str:
        return f"Buscando series oficiales de {quoted(args.get('texto'))}"

    spec = AgentTool(
        name="buscar_series",
        description=(
            "Busca series de tiempo oficiales en la API de Series de Tiempo de datos.gob.ar "
            "(INDEC, BCRA, Ministerio de Economía…). Es la fuente preferida para indicadores "
            "macro: inflación (IPC), PBI, EMAE, desempleo, salarios, reservas, base monetaria, "
            "tipo de cambio oficial, exportaciones, canastas. Devuelve id, título, unidades, "
            "frecuencia y hasta cuándo llega cada serie según el catálogo de la API "
            "(`hasta_segun_catalogo`): ese metadato puede estar atrasado, así que sirve para "
            "descartar series paradas hace años, no para decir cuál es el último dato (eso lo "
            "dice series_tiempo). Después pedí los datos con series_tiempo. Elegí la serie que "
            "mide exactamente lo pedido y, entre dos que miden "
            "lo mismo, la que llega más lejos: el EMAE no es el PBI, y la línea de pobreza "
            "(valor de la canasta) no es la tasa de pobreza."
        ),
        input_schema={
            "type": "object",
            "properties": {"texto": {"type": "string", "description": "El indicador buscado."}},
            "required": ["texto"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        texto = str_arg(args, "texto", required=True, max_len=200) or ""
        # Sin acentos y por palabra completa: «inflación» no encontraba nada y
        # «emisiones» traía la base monetaria (por la subcadena "emi").
        curated: list[dict[str, Any]] = []
        for e in match_catalog(texto):
            item: dict[str, Any] = {"ids": e["ids"], "descripcion": e["description"]}
            if e.get("discontinued"):
                item["discontinuada"] = True
            curated.append(item)
        try:
            found = await ctx.deps.series.search(texto, limit=8)
        except ConnectorError:
            found = []
        series = []
        for s in found:
            item = {
                "id": s.get("id"),
                "titulo": s.get("title"),
                "descripcion": (s.get("description") or "")[:200],
                "unidades": s.get("units"),
                "frecuencia": s.get("frequency"),
                "dataset": s.get("dataset_title"),
                "fuente": s.get("source"),
            }
            # Hasta cuándo llega cada serie según el catálogo: entre dos que
            # miden lo mismo, la que está al día. No es el último dato: la
            # pobreza 64.2 dice 2026-01-01 y ya publicó 2026-07-01, y con
            # «hasta» el modelo lo tomaba como el fin de la serie.
            if s.get("time_index_end"):
                item["hasta_segun_catalogo"] = s["time_index_end"]
            series.append(item)
        if not curated and not series:
            return ToolOutcome(
                to_json({"series": [], "nota": "No hay series con ese nombre."}),
                summary="No encontró series oficiales con ese nombre",
            )
        return ToolOutcome(
            to_json({"verificadas": curated, "series": series}),
            summary=f"Encontró {count(len(curated) + len(series), 'serie', 'series')}",
        )


class SeriesTiempo:
    status = "Consultando series de tiempo..."

    def describe(self, args: dict[str, Any]) -> str:
        if isinstance(args.get("variacion"), dict):
            return "Calculando la variación de la serie"
        return "Leyendo la serie de tiempo"

    spec = AgentTool(
        name="series_tiempo",
        description=(
            "Trae los valores de una o más series de tiempo por id (de buscar_series): las "
            "observaciones más recientes del rango pedido, en orden cronológico, con la fecha "
            "de la última observación y hasta cuándo llega la serie en la fuente. "
            "`representacion`: `percent_change` es la variación % respecto del período anterior "
            "(p. ej. inflación mensual a partir del IPC), `percent_change_a_year_ago` la "
            "interanual y `percent_change_since_beginning_of_year` la acumulada en el año (contra "
            "el último dato del año anterior, como la publica el INDEC); los porcentajes ya "
            "vienen multiplicados por 100 (33.54 es 33,54 %). `frecuencia` agrega "
            "a una frecuencia más gruesa que la de la serie (no más fina) y por defecto "
            "PROMEDIA: para el total de un flujo (exportaciones, importaciones, recaudación, "
            "gasto) pedí `agregacion=sum`; para un saldo (reservas, base monetaria), "
            "`end_of_period`. `variacion` calcula en código la variación entre dos períodos "
            "sobre los valores (valor de `hasta` / valor de `desde` − 1) y, con `deflactar_con` "
            "(el id de un índice de precios, p. ej. el IPC 148.3_INIVELNAL_DICI_M_26), también "
            "la real: usala para acumuladas de varios meses, variaciones punta a punta y "
            "comparaciones entre años. `desde` es el período base: la inflación acumulada de "
            "marzo a agosto es desde=AAAA-02, hasta=AAAA-08. Con años (AAAA) y sin `frecuencia` "
            "compara la última observación de cada año (diciembre contra diciembre: lo que va "
            "para precios y saldos); para el total anual de un flujo pedila con frecuencia=year "
            "y agregacion=sum (la API deja afuera el año en curso). No sumes ni compongas tasas "
            "vos."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ids": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 5},
                "desde": {"type": "string", "description": "AAAA-MM-DD"},
                "hasta": {"type": "string", "description": "AAAA-MM-DD"},
                "frecuencia": {"type": "string", "enum": list(_FREQUENCIES)},
                "agregacion": {
                    "type": "string",
                    "enum": list(_AGGREGATIONS),
                    "description": (
                        "Cómo agrega `frecuencia`: avg (promedio, por defecto), sum, "
                        "end_of_period, max o min."
                    ),
                },
                "representacion": {"type": "string", "enum": list(_REPRESENTATIONS)},
                "ultimos": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 120,
                    "description": "Cuántas observaciones finales mostrar (por defecto 24).",
                },
                "variacion": {
                    "type": "object",
                    "description": "Variación entre dos períodos, calculada sobre los valores.",
                    "properties": {
                        "desde": {
                            "type": "string",
                            "description": "Período base: AAAA, AAAA-MM o AAAA-MM-DD.",
                        },
                        "hasta": {
                            "type": "string",
                            "description": "Período final: AAAA, AAAA-MM o AAAA-MM-DD.",
                        },
                        "deflactar_con": {
                            "type": "string",
                            "description": "Id de un índice de precios para la variación real.",
                        },
                    },
                    "required": ["desde", "hasta"],
                },
            },
            "required": ["ids"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        ids = args.get("ids")
        if not isinstance(ids, list) or not ids or len(ids) > 5:
            raise ToolInputError("`ids` es una lista de 1 a 5 ids de series.")
        ids = [str(i).strip()[:80] for i in ids]
        frequency = str_arg(args, "frecuencia", max_len=10)
        representation = str_arg(args, "representacion", max_len=40)
        aggregation = str_arg(args, "agregacion", max_len=15)
        if frequency and frequency not in _FREQUENCIES:
            raise ToolInputError(f"`frecuencia` es una de {', '.join(_FREQUENCIES)}.")
        if representation and representation not in _REPRESENTATIONS:
            raise ToolInputError(f"`representacion` es una de {', '.join(_REPRESENTATIONS)}.")
        if aggregation and aggregation not in _AGGREGATIONS:
            raise ToolInputError(f"`agregacion` es una de {', '.join(_AGGREGATIONS)}.")
        if args.get("variacion") is not None:
            # La variación se compone sobre los valores: una `representacion`
            # pedida junto con ella se ignora y se dice, en vez de devolver un
            # error que le cuesta una vuelta al modelo (pasó en staging con la
            # acumulada marzo–agosto).
            ignored = bool(representation and representation != "value")
            return await self._variation(
                ids, args["variacion"], frequency, aggregation, ctx, ignored_representation=ignored
            )
        last = int_arg(args, "ultimos", 24, 1, 120)
        kwargs: dict[str, Any] = {
            "start_date": str_arg(args, "desde", max_len=10),
            "end_date": str_arg(args, "hasta", max_len=10),
            "collapse": frequency,
            "representation": None if representation == "value" else representation,
        }
        if frequency and aggregation:
            kwargs["collapse_aggregation"] = aggregation
        result = await _fetch_series(ctx, ids, kwargs)
        if result is None or not result.records:
            return ToolOutcome(to_json({"filas": [], "nota": "La serie no devolvió datos."}))
        payload = _tail_for_model(result, last)
        dropped = (result.metadata or {}).get(_DROPPED_FREQUENCY)
        if dropped:
            payload["nota"] = " ".join(
                n for n in (payload.get("nota"), _dropped_frequency_note(dropped)) if n
            )
        complete = _complete_periods_note(result.metadata or {}, frequency)
        if complete:
            payload["periodos"] = complete
        return ToolOutcome(to_json(payload), results=[result])

    async def _variation(
        self,
        ids: list[str],
        spec: Any,
        frequency: str | None,
        aggregation: str | None,
        ctx: ToolContext,
        *,
        ignored_representation: bool = False,
    ) -> ToolOutcome:
        """valor[hasta] / valor[desde] − 1, sobre los valores y no sobre tasas.

        El modelo sumaba tasas mensuales de cabeza (acumulada marzo–agosto
        «≈14 %» contra 14,58 %; la suma da 13,77). Acá se compone con los
        valores de la serie y vuelve como un resultado citable.
        """
        if not isinstance(spec, dict):
            raise ToolInputError("`variacion` es un objeto {desde, hasta, deflactar_con}.")
        desde = str_arg(spec, "desde", required=True, max_len=10) or ""
        hasta = str_arg(spec, "hasta", required=True, max_len=10) or ""
        base_bounds, end_bounds = _period_bounds(desde), _period_bounds(hasta)
        if base_bounds is None or end_bounds is None:
            raise ToolInputError("`desde` y `hasta` son AAAA, AAAA-MM o AAAA-MM-DD.")
        if base_bounds[1] >= end_bounds[0]:
            raise ToolInputError("`desde` tiene que ser un período anterior a `hasta`.")
        deflator = str_arg(spec, "deflactar_con", max_len=80)
        # Lo que el modelo tiene que saber del cálculo, en `nota`.
        notes: list[str] = []
        if ignored_representation:
            notes.append(
                "La variación se calcula sobre los valores de la serie: se ignoró `representacion`."
            )

        base_kwargs: dict[str, Any] = {"collapse": frequency, "representation": None}
        if frequency and aggregation:
            base_kwargs["collapse_aggregation"] = aggregation
        # Desde 13 meses antes del período base: una anual o una trimestral
        # fechan la observación al principio del período.
        window_start = _shift_iso_months(base_bounds[0], -13)
        result = await _fetch_series(
            ctx, ids, {**base_kwargs, "start_date": window_start, "end_date": end_bounds[1]}
        )
        if result is None or not result.records:
            return ToolOutcome(to_json({"filas": [], "nota": "La serie no devolvió datos."}))
        meta = result.metadata or {}
        dropped = meta.get(_DROPPED_FREQUENCY)
        if dropped:
            notes.append(_dropped_frequency_note(dropped))
            base_kwargs = {"collapse": None, "representation": None}
        applied_frequency = None if dropped else frequency
        step = _STEP_MONTHS.get(str(meta.get("frecuencia")))
        base_rows = result.records
        if meta.get("truncada"):
            # Un rango largo de una diaria no entra en una página: el período
            # base se pide aparte.
            head = await _fetch_series(
                ctx, ids, {**base_kwargs, "start_date": window_start, "end_date": base_bounds[1]}
            )
            base_rows = head.records if head is not None and head.records else []

        deflator_points: tuple[tuple[str, float], tuple[str, float]] | None = None
        deflator_label = ""
        if deflator:
            # El deflactor se agrega con promedio: el nivel de precios de un
            # año es el promedio del índice, no su suma.
            defl = await _fetch_series(
                ctx,
                [deflator],
                {
                    "collapse": applied_frequency,
                    "representation": None,
                    "start_date": window_start,
                    "end_date": end_bounds[1],
                },
            )
            if defl is None or not defl.records:
                raise ToolInputError(f"El índice `{deflator}` no devolvió datos.")
            if (defl.metadata or {}).get(_DROPPED_FREQUENCY):
                notes.append(
                    f"El índice `{deflator}` no admite `frecuencia={applied_frequency}`: se "
                    "usó en su frecuencia original."
                )
            defl_step = _STEP_MONTHS.get(str((defl.metadata or {}).get("frecuencia")))
            deflator_label = _value_columns(defl)[0]
            d0 = _pick(defl.records, deflator_label, base_bounds, defl_step)
            d1 = _pick(defl.records, deflator_label, end_bounds, defl_step)
            if d0 is None or d1 is None or d0[1] == 0:
                raise ToolInputError(
                    f"El índice `{deflator}` no tiene dato para {desde} y {hasta} "
                    f"(llega hasta {(defl.metadata or {}).get('ultima_observacion')})."
                )
            deflator_points = (d0, d1)

        rows: list[dict[str, Any]] = []
        missing: list[str] = []
        for column in _value_columns(result):
            p0 = _pick(base_rows, column, base_bounds, step)
            p1 = _pick(result.records, column, end_bounds, step)
            if p0 is None or p1 is None or p0[1] == 0:
                missing.append(column)
                continue
            ratio = p1[1] / p0[1] - 1
            row: dict[str, Any] = {
                "serie": column,
                "desde": p0[0],
                "valor_desde": p0[1],
                "hasta": p1[0],
                "valor_hasta": p1[1],
                "variacion_pct": _pct(ratio),
            }
            if deflator_points is not None:
                (_, i0), (_, i1) = deflator_points
                inflation = i1 / i0 - 1
                row["deflactor"] = deflator_label
                row["inflacion_pct"] = _pct(inflation)
                row["variacion_real_pct"] = _pct((1 + ratio) / (1 + inflation) - 1)
            rows.append(row)
        if not rows:
            raise ToolInputError(
                f"La serie no tiene dato para {desde} y {hasta} "
                f"(llega hasta {meta.get('ultima_observacion')})."
                + (
                    " Con `frecuencia`, la API deja afuera el período que todavía no terminó "
                    "(con `year`, el año en curso)."
                    if applied_frequency
                    else ""
                )
            )
        if _is_year(desde) and _is_year(hasta) and not applied_frequency:
            if meta.get("frecuencia") != "anual":
                # Exportaciones 2024 → 2025 daba +6,15 % (diciembre contra
                # diciembre) y el total anual creció 9,29 %.
                notes.append(
                    "Con años y sin `frecuencia` se comparó la última observación de cada año "
                    f"({rows[0]['desde']} contra {rows[0]['hasta']}): vale para precios, índices "
                    "y saldos, no para el total del año. Para comparar el total anual de un flujo "
                    "(exportaciones, importaciones, recaudación, gasto) volvé a pedirla con "
                    "frecuencia=year y agregacion=sum."
                )
        open_end = [
            r["hasta"] for r in rows if _leaves_period_open(r["hasta"], step, end_bounds[1])
        ]
        if open_end:
            notes.append(
                f"El período final ({hasta}) no está completo en la serie: se usó su último "
                f"dato, del {max(open_end)}. Decilo en la respuesta."
            )

        computed = DataResult(
            source="series_tiempo",
            portal_name=result.portal_name,
            portal_url=result.portal_url,
            dataset_title=f"Variación entre {rows[0]['desde']} y {rows[0]['hasta']}: "
            f"{result.dataset_title}",
            format="json",
            records=rows,
            metadata={
                "total_records": len(rows),
                "description": meta.get("description", ""),
                "units": f"variación en %; valores en {meta.get('units') or 'unidades de la serie'}",
                "calculo": (
                    "valor_hasta / valor_desde − 1, calculado por OpenArg sobre los valores "
                    "publicados"
                    + (
                        "; real = (1 + nominal) / (1 + inflación) − 1"
                        if deflator_points is not None
                        else ""
                    )
                ),
                "ultima_observacion": max(r["hasta"] for r in rows),
                "frecuencia": meta.get("frecuencia"),
                "fecha_fin_fuente": meta.get("fecha_fin_fuente"),
                "fecha_fin_fuente_inferida": meta.get("fecha_fin_fuente_inferida", False),
                "actualizada_en_fuente": meta.get("actualizada_en_fuente"),
                "total_fuente": None,
                "truncada": False,
                # Sin `unidad`: valor_desde y valor_hasta están en las unidades
                # de la serie; sólo estas columnas son porcentajes.
                "columnas_porcentaje": [
                    c
                    for c in ("variacion_pct", "inflacion_pct", "variacion_real_pct")
                    if c in rows[0]
                ],
                "oficial": True,
                "series": meta.get("series", []),
            },
        )
        if notes:
            computed.metadata["advertencias"] = notes
        payload = result_for_model(computed, calculo=computed.metadata["calculo"])
        if missing:
            payload["sin_dato"] = missing
        if notes:
            payload["nota"] = " ".join(notes)
        complete = _complete_periods_note(meta, applied_frequency)
        if complete:
            payload["periodos"] = complete
        payload.update(_freshness_for_model(computed.metadata))
        return ToolOutcome(
            to_json(payload),
            results=[computed],
            summary=f"Calculó la variación de {quoted(result.dataset_title)}",
        )


# Lo que dice la API (en el cuerpo del 400) cuando la frecuencia pedida es
# más fina que la de la serie: "Intervalo de collapse inválido para la(s)
# serie(s) seleccionadas: month. Pruebe con un intervalo mayor".
_INVALID_COLLAPSE = "Intervalo de collapse"
# Marca en la metadata de un resultado que vino sin la frecuencia pedida.
_DROPPED_FREQUENCY = "frecuencia_descartada"


def _dropped_frequency_note(frequency: str) -> str:
    return (
        f"La serie no admite `frecuencia={frequency}` (es más fina que la suya): vino en su "
        "frecuencia original, sin agregar."
    )


_COLLAPSE_NOUNS = {
    "month": (1, "meses", "mes"),
    "quarter": (3, "trimestres", "trimestre"),
    "semester": (6, "semestres", "semestre"),
    "year": (12, "años", "año"),
}
_NATIVE_MONTHS = {"mensual": 1, "trimestral": 3, "semestral": 6, "anual": 12}


def _complete_periods_note(meta: dict[str, Any], frequency: str | None) -> str | None:
    """Que la API agrega sólo períodos completos, dicho para que el modelo no lo verifique.

    Medido el 05-oct: con `collapse` la API deja afuera el período en curso
    (exportaciones con year llegan a 2025) y el primero si la serie arranca
    a mitad de período (el IPC, desde 2016-12, empieza en 2017). Sin decirlo,
    después de «exportaciones 2025, year+sum» el modelo pedía los 12 meses
    para comprobar que el año estaba entero: una vuelta más. Sólo desde
    series mensuales o más gruesas (de una diaria no está medido).
    """
    nouns = _COLLAPSE_NOUNS.get(frequency or "")
    if nouns is None or meta.get(_DROPPED_FREQUENCY):
        return None
    target, plural, singular = nouns
    natives = [
        _NATIVE_MONTHS.get(str(s.get("frecuencia")))
        for s in meta.get("series") or []
        if isinstance(s, dict)
    ]
    if not natives or any(n is None or n >= target for n in natives):
        return None
    return f"Cada fila es un {singular} completo: la API no agrega {plural} sin terminar."


async def _fetch_series(
    ctx: ToolContext, ids: list[str], kwargs: dict[str, Any]
) -> DataResult | None:
    try:
        return await ctx.deps.series.fetch(series_ids=ids, **kwargs)
    except ConnectorError as exc:
        # Se reintenta sin frecuencia SÓLO con el 400 de frecuencia inválida
        # (p. ej. mensual sobre una trimestral). Un timeout o un 5xx no: con
        # frecuencia=year y agregacion=sum, el reintento devolvía la mensual
        # y la variación de exportaciones salía de diciembre contra
        # diciembre (6,15 %) en vez del total anual (9,29 %), sin aviso.
        collapse = kwargs.get("collapse")
        if not collapse or _INVALID_COLLAPSE not in str(exc.details.get("reason", "")):
            raise
        retry = {k: v for k, v in kwargs.items() if k != "collapse_aggregation"}
        result = await ctx.deps.series.fetch(series_ids=ids, **{**retry, "collapse": None})
        if result is not None:
            result.metadata = {**(result.metadata or {}), _DROPPED_FREQUENCY: collapse}
        return result


def _shift_iso_months(iso: str, delta: int) -> str:
    year, month = _shift_months(int(iso[:4]), int(iso[5:7]), delta)
    return date(year, month, 1).isoformat()


# ── cotizaciones ───────────────────────────────────────────


class Cotizaciones:
    status = "Consultando cotizaciones..."

    def describe(self, args: dict[str, Any]) -> str:
        if args.get("indicador") == "riesgo_pais":
            return "Consultando el riesgo país"
        casa = str(args.get("casa") or "").strip()
        return f"Consultando la cotización del dólar {casa}".rstrip()

    spec = AgentTool(
        name="cotizaciones",
        description=(
            "Cotizaciones del dólar de agregadores no oficiales (DolarApi/ArgentinaDatos): "
            "blue, bolsa/MEP, contado con liqui, cripto, tarjeta, y la pizarra del Banco "
            "Nación (casa 'oficial'); su 'mayorista' no es la referencia A 3500. También el "
            "riesgo país. Para el dólar oficial usá variables_bcra (minorista y mayorista de "
            "referencia del BCRA); si sumás la pizarra del Banco Nación, nombrala así y "
            "aclarala como no oficial. `solo_actual=true` trae el último valor; si no, la "
            "historia."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "indicador": {"type": "string", "enum": ["dolar", "riesgo_pais"]},
                "casa": {"type": "string", "enum": list(_CASAS)},
                "solo_actual": {"type": "boolean"},
            },
            "required": ["indicador"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        indicador = str_arg(args, "indicador", required=True, max_len=20)
        current = bool(args.get("solo_actual", True))
        conn = ctx.deps.arg_datos
        if indicador == "dolar":
            casa = str_arg(args, "casa", max_len=20)
            if casa and casa not in _CASAS:
                raise ToolInputError(f"`casa` es una de {', '.join(_CASAS)}.")
            result = await conn.fetch_dolar(casa=casa, ultimo=current)
        elif indicador == "riesgo_pais":
            result = await conn.fetch_riesgo_pais(ultimo=current)
        else:
            raise ToolInputError("`indicador` es 'dolar' o 'riesgo_pais'.")
        if result is None or not result.records:
            return ToolOutcome(to_json({"filas": [], "nota": "Sin datos."}))
        return ToolOutcome(to_json(_tail_for_model(result, 30)), results=[result])


# ── declaraciones juradas ──────────────────────────────────


class DeclaracionesJuradas:
    status = "Consultando declaraciones juradas..."

    def describe(self, args: dict[str, Any]) -> str:
        if args.get("accion") == "buscar" and args.get("nombre"):
            return f"Buscando la declaración jurada de {quoted(args.get('nombre'))}"
        if args.get("accion") == "ranking":
            return "Armando el ranking de declaraciones juradas"
        return self.status

    spec = AgentTool(
        name="declaraciones_juradas",
        description=(
            "Declaraciones juradas patrimoniales de diputados nacionales (Oficina "
            "Anticorrupción). `buscar` por nombre, `ranking` por patrimonio, ingresos o bienes, "
            "`estadisticas` para totales generales (incluye cuántos tienen patrimonio negativo). "
            "No califiques ninguna variación, patrimonio ni ingreso como sospechoso o llamativo "
            "ni lo atribuyas a nada: describí cifras con nombre y año. "
            "Una fila con `inconsistente: true` (total de bienes) o `ingresos_inconsistentes: "
            "true` es un registro del dataset con cifras que no cierran con la propia DDJJ, "
            "probable error de carga no verificado (el motivo está en la fila), y esa cifra no es "
            "comparable. `inconsistente` la saca de rankings y estadísticas; "
            "`ingresos_inconsistentes`, sólo del ranking por ingresos: sus bienes cierran y sigue "
            "en los demás rankings y en las estadísticas (`excluidas_por_inconsistencia` dice "
            "cuántas se excluyeron). No uses esa cifra, no le calcules variación, nunca la "
            "presentes como enriquecimiento y no nombres a la persona excluida salvo que pregunten "
            "por ella; si preguntan, decí que en el dataset esa cifra de su DDJJ no cierra con el "
            "resto de la declaración, sin atribuírselo a la persona."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "accion": {"type": "string", "enum": ["buscar", "ranking", "estadisticas"]},
                "nombre": {"type": "string"},
                "ordenar_por": {"type": "string", "enum": ["patrimonio", "ingresos", "bienes"]},
                "orden": {"type": "string", "enum": ["desc", "asc"]},
                "cantidad": {"type": "integer", "minimum": 1, "maximum": 50},
            },
            "required": ["accion"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        ddjj = ctx.deps.ddjj
        accion = str_arg(args, "accion", required=True, max_len=20)
        if accion == "buscar":
            nombre = str_arg(args, "nombre", required=True, max_len=120) or ""
            result = await asyncio.to_thread(ddjj.search, nombre, 10)
        elif accion == "ranking":
            result = await asyncio.to_thread(
                ddjj.ranking,
                str_arg(args, "ordenar_por", max_len=20) or "patrimonio",
                int_arg(args, "cantidad", 10, 1, 50),
                str_arg(args, "orden", max_len=4) or "desc",
            )
        elif accion == "estadisticas":
            result = await asyncio.to_thread(ddjj.stats)
        else:
            raise ToolInputError("`accion` es buscar, ranking o estadisticas.")
        if result is None or not result.records:
            return ToolOutcome(to_json({"filas": [], "nota": "Sin resultados."}))
        extra: dict[str, Any] = {}
        if excluidas := (result.metadata or {}).get("excluidas_por_inconsistencia"):
            extra["excluidas_por_inconsistencia"] = excluidas
        return ToolOutcome(to_json(result_for_model(result, **extra)), results=[result])


# ── sesiones del Congreso ──────────────────────────────────


class Sesiones:
    status = "Buscando en las sesiones del Congreso..."

    def describe(self, args: dict[str, Any]) -> str:
        return f"Buscando {quoted(args.get('texto'))} en las sesiones del Congreso"

    spec = AgentTool(
        name="sesiones",
        description=(
            "Busca fragmentos de las versiones taquigráficas de las sesiones de la Cámara de "
            "Diputados: qué se dijo sobre un tema, opcionalmente de un orador o un período "
            "(año legislativo). Trae los fragmentos más parecidos, con un tope: no sirve para "
            "contar cuántas veces se habló de algo."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "texto": {"type": "string"},
                "orador": {"type": "string"},
                "periodo": {"type": "integer", "minimum": 1983, "maximum": 2100},
            },
            "required": ["texto"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        periodo = args.get("periodo")
        result = await ctx.deps.sesiones.search(
            str_arg(args, "texto", required=True, max_len=300) or "",
            periodo=int(periodo) if periodo else None,
            orador=str_arg(args, "orador", max_len=120),
            limit=12,
        )
        if result is None or not result.records:
            return ToolOutcome(to_json({"filas": [], "nota": "Sin fragmentos sobre eso."}))
        meta = result.metadata or {}
        extra: dict[str, Any] = {}
        if meta.get("tope_alcanzado"):
            # El tope no es un total: con `filas_totales: 12` el agente dijo
            # "12 fragmentos registrados" y había 51 (nueva_06, 06-oct).
            n = len(result.records)
            tope = meta.get("tope_busqueda") or n
            extra["filas_totales"] = f"al menos {n} (tope de la búsqueda: {tope})"
            extra["aviso"] = (
                f"La búsqueda trae como máximo {tope} fragmentos, los más parecidos, así que "
                "estos no son todos: no digas cuántos fragmentos, intervenciones u oradores "
                "hubo sobre el tema, porque no se contaron. Si hace falta una cantidad, decí "
                f"«al menos {n}»."
            )
        return ToolOutcome(to_json(result_for_model(result, **extra)), results=[result])


# ── personal legislativo ───────────────────────────────────


class PersonalLegislativo:
    status = "Consultando la nómina de personal..."
    spec = AgentTool(
        name="personal_legislativo",
        description=(
            "Nómina de personal de la Cámara de Diputados. `por_legislador`: empleados de un "
            "diputado; `cantidad`: cuántos tiene; `cambios`: altas y bajas recientes; `buscar`: "
            "por nombre de empleado o texto; `estadisticas`: totales."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "accion": {
                    "type": "string",
                    "enum": ["por_legislador", "cantidad", "cambios", "buscar", "estadisticas"],
                },
                "texto": {
                    "type": "string",
                    "description": "Nombre del legislador o texto a buscar.",
                },
            },
            "required": ["accion"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        staff = ctx.deps.staff
        accion = str_arg(args, "accion", required=True, max_len=20)
        texto = str_arg(args, "texto", max_len=120)
        if accion in ("por_legislador", "cantidad", "buscar") and not texto:
            raise ToolInputError("Falta `texto`.")
        if accion == "por_legislador":
            result = await staff.get_by_legislator(texto or "", limit=50)
        elif accion == "cantidad":
            result = await staff.count_by_legislator(texto or "")
        elif accion == "cambios":
            result = await staff.get_changes(texto, limit=20)
        elif accion == "buscar":
            result = await staff.search(texto or "", limit=20)
        elif accion == "estadisticas":
            result = await staff.stats()
        else:
            raise ToolInputError("Acción desconocida.")
        if result is None or not result.records:
            return ToolOutcome(to_json({"filas": [], "nota": "Sin resultados."}))
        return ToolOutcome(to_json(result_for_model(result)), results=[result])


# ── ubicar un lugar ────────────────────────────────────────


class UbicarLugar:
    status = "Ubicando el lugar..."

    def describe(self, args: dict[str, Any]) -> str:
        return f"Ubicando {quoted(args.get('texto'))}"

    spec = AgentTool(
        name="ubicar_lugar",
        description=(
            "Normaliza un nombre de lugar con Georef (provincia, departamento/partido, "
            "municipio, localidad) y devuelve su nombre oficial, provincia y códigos. Sirve para "
            "filtrar tablas por el nombre correcto. No es un dato que responda la pregunta: no "
            "lo cites como fuente de una cifra."
        ),
        input_schema={
            "type": "object",
            "properties": {"texto": {"type": "string"}},
            "required": ["texto"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        result = await ctx.deps.georef.normalize_location(
            str_arg(args, "texto", required=True, max_len=120) or ""
        )
        if result is None or not result.records:
            return ToolOutcome(to_json({"filas": [], "nota": "No se encontró ese lugar."}))
        payload = result_for_model(result)
        payload["filas"] = (result.records or [])[: min(10, MAX_ROWS_FOR_MODEL)]
        # Ubicar Pinamar no responde cuántas personas viven en Pinamar: no va a
        # las fuentes ni a la evidencia de la respuesta.
        first = (result.records or [{}])[0]
        place = first.get("nombre") or ""
        return ToolOutcome(to_json(payload), summary=f"Ubicó {quoted(place)}" if place else None)


# ── pedir una aclaración ───────────────────────────────────


class PedirAclaracion:
    status = "Pidiendo una aclaración..."
    spec = AgentTool(
        name="pedir_aclaracion",
        description=(
            "Último recurso: termina el turno con una pregunta al usuario, con opciones. Usala "
            "sólo si la pregunta tiene dos lecturas que llevan a respuestas distintas (p. ej. una "
            "sigla con varios significados) y mirar los datos no lo resuelve. Si hay una lectura "
            "razonable, respondé con ella y aclarala en la respuesta. Nunca preguntes por el "
            "período (usá el más reciente), por el nivel de detalle ni por el formato: decidilo "
            "vos."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "pregunta": {"type": "string"},
                "opciones": {"type": "array", "items": {"type": "string"}, "maxItems": 5},
            },
            "required": ["pregunta"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        pregunta = str_arg(args, "pregunta", required=True, max_len=300) or ""
        opciones = [str(o)[:120] for o in (args.get("opciones") or []) if str(o).strip()][:5]
        return ToolOutcome(
            content=to_json({"ok": True}),
            clarification=ClarificationEvent(pregunta, tuple(opciones)),
        )
