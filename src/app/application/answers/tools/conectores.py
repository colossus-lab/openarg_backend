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
from app.infrastructure.adapters.connectors.series_tiempo_adapter import SERIES_CATALOG

_FREQUENCIES = ("day", "week", "month", "quarter", "semester", "year")
_REPRESENTATIONS = ("value", "change", "percent_change", "percent_change_a_year_ago")
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


def _tail_for_model(result: DataResult, last: int, *, today: date | None = None) -> dict[str, Any]:
    """Una serie como la ve el modelo: las últimas observaciones, no las primeras."""
    records = result.records or []
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
    if len(records) > len(shown):
        payload["nota"] = f"Se muestran las últimas {len(shown)} de {len(records)} observaciones."
    return payload


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
            "tipo de cambio oficial, exportaciones, canastas. Devuelve id, título, unidades y "
            "frecuencia; después pedí los datos con series_tiempo. Elegí la serie que mide "
            "exactamente lo pedido: el EMAE no es el PBI, y la línea de pobreza (valor de la "
            "canasta) no es la tasa de pobreza."
        ),
        input_schema={
            "type": "object",
            "properties": {"texto": {"type": "string", "description": "El indicador buscado."}},
            "required": ["texto"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        texto = str_arg(args, "texto", required=True, max_len=200) or ""
        lowered = texto.lower()
        curated = [
            {"ids": e["ids"], "descripcion": e["description"]}
            for e in SERIES_CATALOG.values()
            if any(kw in lowered for kw in e.get("keywords", []))
        ]
        try:
            found = await ctx.deps.series.search(texto, limit=8)
        except ConnectorError:
            found = []
        series = [
            {
                "id": s.get("id"),
                "titulo": s.get("title"),
                "descripcion": (s.get("description") or "")[:200],
                "unidades": s.get("units"),
                "frecuencia": s.get("frequency"),
                "dataset": s.get("dataset_title"),
                "fuente": s.get("source"),
            }
            for s in found
        ]
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
        return "Leyendo la serie de tiempo"

    spec = AgentTool(
        name="series_tiempo",
        description=(
            "Trae los valores de una o más series de tiempo por id (de buscar_series). "
            "`representacion=percent_change` da la variación % respecto del período anterior "
            "(p. ej. inflación mensual a partir del IPC), `percent_change_a_year_ago` la "
            "interanual. `frecuencia` agrega a una frecuencia más gruesa que la de la serie "
            "(no más fina). Devuelve las últimas observaciones."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ids": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 5},
                "desde": {"type": "string", "description": "AAAA-MM-DD"},
                "hasta": {"type": "string", "description": "AAAA-MM-DD"},
                "frecuencia": {"type": "string", "enum": list(_FREQUENCIES)},
                "representacion": {"type": "string", "enum": list(_REPRESENTATIONS)},
                "ultimos": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 120,
                    "description": "Cuántas observaciones finales mostrar (por defecto 24).",
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
        representation = str_arg(args, "representacion", max_len=30)
        if frequency and frequency not in _FREQUENCIES:
            raise ToolInputError(f"`frecuencia` es una de {', '.join(_FREQUENCIES)}.")
        if representation and representation not in _REPRESENTATIONS:
            raise ToolInputError(f"`representacion` es una de {', '.join(_REPRESENTATIONS)}.")
        last = int_arg(args, "ultimos", 24, 1, 120)
        kwargs = {
            "start_date": str_arg(args, "desde", max_len=10),
            "end_date": str_arg(args, "hasta", max_len=10),
            "collapse": frequency,
            "representation": None if representation == "value" else representation,
        }
        try:
            result = await ctx.deps.series.fetch(series_ids=ids, **kwargs)
        except ConnectorError:
            # La API da 400 cuando la frecuencia pedida es más fina que la de
            # la serie (p. ej. mensual sobre una trimestral).
            if not frequency:
                raise
            result = await ctx.deps.series.fetch(series_ids=ids, **{**kwargs, "collapse": None})
        if result is None or not result.records:
            return ToolOutcome(to_json({"filas": [], "nota": "La serie no devolvió datos."}))
        return ToolOutcome(to_json(_tail_for_model(result, last)), results=[result])


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
            "Cotizaciones del dólar (oficial, blue, bolsa/MEP, contado con liqui, cripto, "
            "mayorista, tarjeta) y riesgo país, de ArgentinaDatos/DolarApi. `solo_actual=true` "
            "trae el último valor; si no, la historia. Para el tipo de cambio oficial de largo "
            "plazo, preferí series_tiempo."
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
            "Una fila con `inconsistente: true` declara un total de bienes que no coincide con "
            "su propio detalle (`motivo_inconsistencia`): el ranking y las estadísticas ya la "
            "excluyen. No la compares, no le calcules variación y nunca la presentes como "
            "enriquecimiento ni como caso llamativo; si preguntan por esa persona, decí que el "
            "total declarado no coincide con el detalle de sus bienes."
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
            "(año legislativo)."
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
        return ToolOutcome(to_json(result_for_model(result)), results=[result])


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
