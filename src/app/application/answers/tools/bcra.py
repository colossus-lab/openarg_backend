"""Variables del BCRA: la fuente oficial y más fresca de reservas, dólar
oficial, tasas y base monetaria.

Hasta el 04-oct el agente no tenía ninguna herramienta del BCRA. Para las
reservas usaba Series de Tiempo, cuya serie curada terminaba en abril (y con
la truncación a 1.000 filas llegó a contestar "abril de 2023"), y para el
dólar oficial sólo DolarApi, que es la pizarra del Banco Nación servida por un
agregador. La API de estadísticas v4 del BCRA publica al día hábil anterior y
no pide token.

"Dólar oficial" son las dos referencias del BCRA, con fecha (decisión de
producto del 04-oct): el minorista promedio vendedor (variable 4) y el
mayorista de referencia de la Comunicación A 3500 (variable 5).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from app.application.answers.tools.base import (
    ToolContext,
    ToolInputError,
    ToolOutcome,
    count,
    int_arg,
    quoted,
    str_arg,
    to_json,
)
from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.llm.agent_llm import AgentTool

# Argentina no tiene horario de verano desde 2009. El servidor corre en UTC:
# a las 22 h del domingo su "hoy" ya es el lunes.
_AR = timezone(timedelta(hours=-3), "ART")

_FUENTE = "Banco Central de la República Argentina (BCRA)"
_MAX_VARIABLES = 5
# Observaciones que se piden cuando no hay `desde`: alcanzan para el último
# dato, para comparar con hace un par de meses y para el gráfico.
_VENTANA_RECIENTE = 60
# Valores publicados por adelantado que se le muestran al modelo (la UVA llega
# hasta el día 15 del mes siguiente: unas 40 filas).
_MAX_ADELANTADOS = 45


@dataclass(frozen=True)
class Variable:
    id: int  # idVariable de la API v4 (verificado contra el catálogo el 04-oct)
    titulo: str
    corto: str  # para los pasos que ve el usuario
    unidades: str
    # Los valores ya vienen en puntos porcentuales (23,19 = 23,19 %). Va al
    # contrato de metadatos (`unidad: "porcentaje"`) aunque el catálogo del
    # BCRA, que también lo dice, no haya respondido.
    porcentaje: bool = False


# Las variables curadas. Los ids salen del catálogo v4
# (`/estadisticas/v4.0/monetarias`), no de la v3: pueden cambiar entre
# versiones. La inflación (27 y 28) queda afuera a propósito: es una copia del
# IPC del INDEC, y una herramienta "oficial y más fresca" invitaría a citarla
# como del BCRA.
VARIABLES: dict[str, Variable] = {
    "reservas": Variable(
        1,
        "Reservas internacionales del BCRA (saldo diario)",
        "reservas internacionales",
        "millones de dólares",
    ),
    "dolar_minorista": Variable(
        4,
        "Tipo de cambio minorista, promedio vendedor (BCRA)",
        "dólar minorista",
        "pesos por dólar",
    ),
    "dolar_mayorista": Variable(
        5,
        "Tipo de cambio mayorista de referencia, Comunicación A 3500 (BCRA)",
        "dólar mayorista (A 3500)",
        "pesos por dólar",
    ),
    "badlar": Variable(
        7,
        "Tasa BADLAR de bancos privados (BCRA)",
        "tasa BADLAR",
        "% nominal anual",
        porcentaje=True,
    ),
    "tamar": Variable(
        44,
        "Tasa TAMAR de bancos privados (BCRA)",
        "tasa TAMAR",
        "% nominal anual",
        porcentaje=True,
    ),
    "tasa_plazo_fijo": Variable(
        12,
        "Tasa de depósitos a plazo fijo a 30 días, promedio de entidades (BCRA)",
        "tasa de plazo fijo",
        "% nominal anual",
        porcentaje=True,
    ),
    "base_monetaria": Variable(
        15,
        "Base monetaria (BCRA)",
        "base monetaria",
        "millones de pesos",
    ),
    "circulacion_monetaria": Variable(
        16,
        "Circulación monetaria (BCRA)",
        "circulación monetaria",
        "millones de pesos",
    ),
    "uva": Variable(
        31,
        "Unidad de Valor Adquisitivo, UVA (BCRA)",
        "UVA",
        "pesos",
    ),
    "cer": Variable(
        30,
        "Coeficiente de Estabilización de Referencia, CER (BCRA)",
        "CER",
        "índice, base 2/2/2002 = 1",
    ),
    "icl": Variable(
        40,
        "Índice para Contratos de Locación, ICL (BCRA)",
        "índice de alquileres (ICL)",
        "índice, base 30/6/2020 = 1",
    ),
    "banda_cambiaria_piso": Variable(
        1187,
        "Régimen de bandas cambiarias: límite inferior (BCRA)",
        "piso de la banda cambiaria",
        "pesos por dólar",
    ),
    "banda_cambiaria_techo": Variable(
        1188,
        "Régimen de bandas cambiarias: límite superior (BCRA)",
        "techo de la banda cambiaria",
        "pesos por dólar",
    ),
}


def hoy_ar() -> date:
    """La fecha de hoy en Argentina."""
    return datetime.now(_AR).date()


def _fecha_ar(iso: str | None) -> str:
    """ "2026-09-30" → "30/09/2026"."""
    if not iso or len(iso) < 10:
        return iso or ""
    return f"{iso[8:10]}/{iso[5:7]}/{iso[:4]}"


def _date_arg(args: dict[str, Any], name: str) -> str | None:
    value = str_arg(args, name, max_len=10)
    if value is None:
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        raise ToolInputError(f"`{name}` es una fecha AAAA-MM-DD.") from None


def _keys(args: dict[str, Any]) -> list[str]:
    raw = args.get("variables")
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list) or not raw:
        raise ToolInputError("`variables` es una lista de 1 a 5 variables.")
    keys: list[str] = []
    for item in raw:
        key = str(item).strip().lower()
        if key not in VARIABLES:
            raise ToolInputError(
                f"Variable desconocida {str(item)[:40]!r}. Son: {', '.join(VARIABLES)}."
            )
        if key not in keys:
            keys.append(key)
    if len(keys) > _MAX_VARIABLES:
        raise ToolInputError(f"Pedí hasta {_MAX_VARIABLES} variables por vez.")
    return keys


def _agrupar(records: list[dict[str, Any]], width: int) -> list[dict[str, Any]]:
    """Cierre, mínimo y máximo por período (``width`` 7 = mes, 4 = año)."""
    grupos: dict[str, list[tuple[str, float]]] = {}
    for r in records:
        fecha, valor = str(r.get("fecha", "")), r.get("valor")
        if len(fecha) >= width and isinstance(valor, int | float):
            grupos.setdefault(fecha[:width], []).append((fecha, float(valor)))
    out: list[dict[str, Any]] = []
    for periodo, obs in sorted(grupos.items()):
        valores = [v for _, v in obs]
        out.append(
            {
                "periodo": periodo,
                "cierre": obs[-1][1],
                "fecha_cierre": obs[-1][0],
                "minimo": min(valores),
                "maximo": max(valores),
            }
        )
    return out


def _resumen(records: list[dict[str, Any]], max_periodos: int) -> tuple[str, list[dict]]:
    """La historia entera en pocas líneas, calculada acá y no por el modelo.

    Con `desde` llegan cientos de observaciones diarias y el modelo ve sólo
    las últimas: para contar la evolución de un año pedía la misma serie tres
    veces con otros rangos (medido en staging el 04-oct).
    """
    mensual = _agrupar(records, 7)
    if len(mensual) <= max_periodos:
        return "resumen_mensual", mensual
    return "resumen_anual", _agrupar(records, 4)[-max_periodos:]


def _separar_adelantados(
    records: list[dict[str, Any]], today: date
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(hasta hoy, posteriores a hoy). UVA, CER e ICL se publican por adelantado."""
    hoy = today.isoformat()
    al_dia = [r for r in records if str(r.get("fecha", "")) <= hoy]
    adelantados = [r for r in records if str(r.get("fecha", "")) > hoy]
    return al_dia, adelantados


def _sin_adelantados(
    result: DataResult, al_dia: list[dict[str, Any]], adelantados: list[dict[str, Any]]
) -> None:
    """Deja en la evidencia sólo lo que ya pasó.

    El DataResult alimenta el gráfico del motor, el aviso de atraso y la
    verificación de cifras: con los valores de días que no llegaron, el gráfico
    de la UVA terminaba el 15-oct y ``ultima_observacion`` decía 15-oct un 05-oct.
    Los adelantados quedan aparte, en la metadata y en lo que ve el modelo.
    """
    meta = result.metadata
    ultima = str(al_dia[-1].get("fecha"))
    result.records = al_dia
    meta["total_records"] = len(al_dia)
    meta["last_updated"] = ultima
    meta["ultima_observacion"] = ultima
    meta["publicado_hasta"] = str(adelantados[-1].get("fecha"))
    meta["publicados_por_adelantado"] = adelantados
    if str(meta.get("fecha_fin_fuente") or "") > ultima:
        # La serie llega hasta hoy: lo posterior no es un dato que falte.
        meta["fecha_fin_fuente"] = ultima


def _payload(
    key: str,
    var: Variable,
    result: DataResult,
    shown: int,
    max_periodos: int,
    adelantados: list[dict[str, Any]],
    max_adelantados: int,
) -> dict:
    """Una variable como la ve el modelo: el último dato con su fecha, arriba."""
    records = result.records or []
    meta = result.metadata or {}
    ultimo = records[-1]
    payload: dict[str, Any] = {
        "variable": key,
        "titulo": result.dataset_title,
        "fuente": _FUENTE,
        "unidades": var.unidades,
        "frecuencia": meta.get("frecuencia") or "diaria",
        "ultimo_dato": {"fecha": ultimo.get("fecha"), "valor": ultimo.get("valor")},
        "filas_totales": len(records),
        "filas": records[-shown:],
    }
    if len(records) > shown:
        nombre, resumen = _resumen(records, max_periodos)
        payload[nombre] = resumen
    if adelantados:
        # El de hoy es el último que no es futuro; los que siguen ya los
        # publicó el BCRA y sirven si preguntan por una fecha que viene.
        if len(adelantados) > max_adelantados:
            # Los más cercanos y el último publicado.
            adelantados = adelantados[: max_adelantados - 1] + adelantados[-1:]
        payload["publicados_por_adelantado"] = adelantados
        payload["nota"] = (
            f"El BCRA ya publicó valores hasta el {_fecha_ar(adelantados[-1].get('fecha'))} "
            "(se conocen por adelantado, van en `publicados_por_adelantado`); el de hoy es "
            "`ultimo_dato`."
        )
    elif len(records) > shown:
        payload["nota"] = (
            f"Se muestran las últimas {shown} de {len(records)} observaciones; el resumen "
            "cubre todo el período leído."
        )
    return payload


class VariablesBCRA:
    status = "Consultando al BCRA..."

    def describe(self, args: dict[str, Any]) -> str:
        raw = args.get("variables")
        names = [
            VARIABLES[k].corto
            for k in (raw if isinstance(raw, list) else [raw])
            if isinstance(k, str) and k in VARIABLES
        ]
        if not names:
            return self.status
        return f"Consultando al BCRA: {', '.join(names)}"

    spec = AgentTool(
        name="variables_bcra",
        description=(
            "Variables oficiales del Banco Central (API de estadísticas del BCRA). Es la "
            "fuente oficial y la más fresca (publica el día hábil anterior) para reservas "
            "internacionales, dólar oficial, tasas de interés y base monetaria: para el valor "
            "de hoy o reciente de esos indicadores usala antes que series_tiempo o "
            "cotizaciones. Dólar oficial: pedí dolar_minorista (promedio vendedor de las "
            "entidades) y dolar_mayorista (referencia Comunicación A 3500) y da los dos, cada "
            "uno con su fecha. Reservas: saldo diario en millones de dólares. Variables: "
            "reservas, dolar_minorista, dolar_mayorista, badlar, tamar, tasa_plazo_fijo (30 "
            "días), base_monetaria, circulacion_monetaria, uva, cer, icl (índice de contratos "
            "de alquiler), banda_cambiaria_piso, banda_cambiaria_techo. Devuelve por variable "
            "el último dato con su fecha, las últimas observaciones y un resumen por mes (o por "
            "año) con cierre, mínimo y máximo; con `desde` el resumen cubre todo el período. "
            "Para inflación usá las series del INDEC (series_tiempo)."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "variables": {
                    "type": "array",
                    "items": {"type": "string", "enum": list(VARIABLES)},
                    "minItems": 1,
                    "maxItems": _MAX_VARIABLES,
                },
                "desde": {"type": "string", "description": "AAAA-MM-DD"},
                "hasta": {"type": "string", "description": "AAAA-MM-DD"},
                "ultimos": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 120,
                    "description": "Cuántas observaciones finales mostrar (por defecto 10).",
                },
            },
            "required": ["variables"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        keys = _keys(args)
        desde = _date_arg(args, "desde")
        hasta = _date_arg(args, "hasta")
        today = hoy_ar()
        if desde and desde > today.isoformat():
            raise ToolInputError("`desde` no puede ser una fecha futura.")
        if desde and hasta and desde > hasta:
            raise ToolInputError("`desde` tiene que ser anterior a `hasta`.")
        ultimos = int_arg(args, "ultimos", 10, 1, 120)
        # Que entren todas en el tope de texto de la herramienta.
        shown = min(ultimos, max(10, 200 // len(keys)))
        max_periodos = max(12, 36 // len(keys))
        max_adelantados = max(10, _MAX_ADELANTADOS // len(keys))

        bcra = ctx.deps.bcra
        fetched = await asyncio.gather(
            *(
                bcra.get_variable(
                    VARIABLES[k].id,
                    desde,
                    hasta,
                    limit=max(ultimos, _VENTANA_RECIENTE),
                    title=VARIABLES[k].titulo,
                )
                for k in keys
            ),
            return_exceptions=True,
        )

        payloads: list[dict[str, Any]] = []
        results: list[DataResult] = []
        failures = 0
        for key, got in zip(keys, fetched, strict=True):
            var = VARIABLES[key]
            if isinstance(got, BaseException):
                # Una variable que no respondió no tira abajo a las demás; un
                # error nuestro (no del BCRA) sí sube.
                if not isinstance(got, ConnectorError):
                    raise got
                failures += 1
                payloads.append({"variable": key, "error": "El BCRA no respondió."})
                continue
            if not got.records:
                payloads.append({"variable": key, "filas": [], "nota": "Sin datos en ese período."})
                continue
            meta = got.metadata
            meta["units"] = meta.get("units") or var.unidades
            meta["frecuencia"] = meta.get("frecuencia") or "diaria"
            meta["oficial"] = True
            if var.porcentaje:
                meta["unidad"] = "porcentaje"
            al_dia, adelantados = _separar_adelantados(got.records, today)
            if al_dia and adelantados:
                _sin_adelantados(got, al_dia, adelantados)
            else:
                adelantados = []
            payloads.append(
                _payload(key, var, got, shown, max_periodos, adelantados, max_adelantados)
            )
            results.append(got)

        if failures == len(keys):
            raise ConnectorError(
                error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                details={"action": "variables_bcra", "variables": keys},
            )
        return ToolOutcome(
            to_json({"variables": payloads}),
            results=results,
            summary=_summary(results, today),
        )


def _summary(results: list[DataResult], today: date) -> str:
    if not results:
        return "El BCRA no tiene datos de ese período"
    # El último dato de cada variable (sin los publicados por adelantado). Si
    # no coinciden (reservas al 30/09, dólar al 02/10), no se muestra uno solo.
    lasts = {
        max(
            (
                str(r.get("fecha"))
                for r in res.records or []
                if str(r.get("fecha", "")) <= today.isoformat()
            ),
            default="",
        )
        for res in results
    }
    latest = lasts.pop() if len(lasts) == 1 else ""
    when = f" (último dato: {_fecha_ar(latest)})" if latest else ""
    if len(results) == 1:
        return f"Leyó {quoted(results[0].dataset_title, 80)} del BCRA{when}"
    return f"Leyó {count(len(results), 'variable', 'variables')} del BCRA{when}"
