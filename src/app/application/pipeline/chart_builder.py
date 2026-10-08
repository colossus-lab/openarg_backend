"""Deterministic chart building, LLM chart extraction, and META parsing."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from app.application.consultas.fechas import es_fecha_de_atributo, es_nombre_de_fecha, fecha_iso
from app.domain.entities.connectors.data_result import DataResult

logger = logging.getLogger(__name__)


# Columnas de fecha que ordenan una serie y se grafican como línea. `indice_tiempo`
# es el nombre estándar de TODAS las series de datos.gob.ar (Series de Tiempo);
# sin él, el gráfico determinístico no se armaba y quedaba el que dibuja el
# modelo, que sólo ve una muestra de las filas (ver context_builder).
DATE_COLUMNS = frozenset({"fecha", "indice_tiempo", "periodo"})


def is_date_column(name: str) -> bool:
    """True si la columna es una fecha que ordena una serie.

    La regla vive en ``consultas.fechas``: nombre exacto, o una palabra
    "fecha"/"date" en el nombre (``PUBLICACION_FECHA``, ``start_date``), y nunca
    una fecha de carga o auditoría. Antes alcanzaba la subcadena "date", y
    ``updated_at``/``updated_ts`` (132 tablas en prod) se tomaban como la fecha
    de la serie.
    """
    return es_nombre_de_fecha(name)


def tabla_del_resultado(result: DataResult) -> str:
    """La tabla servida y el título publicado: dicen de qué evento es una fecha.

    El título genérico de NL2SQL ("Consulta SQL: <pregunta>") no cuenta: con él,
    el eje dependía de las palabras que usara el usuario.
    """
    titulo = result.dataset_title or ""
    if titulo.startswith(_GENERIC_TITLE_PREFIX):
        titulo = ""
    served = (result.metadata or {}).get("served_table")
    return " ".join(str(t) for t in (served, titulo) if t)


def es_eje_temporal(name: str, tabla: str = "") -> bool:
    """Una fecha que ordena la serie (gráfico y resumen del contexto).

    Una fecha de nacimiento, vencimiento o alta describe a alguien de la fila,
    no cuándo pasó el dato: no ordena la serie, salvo que sea el evento que
    registra la tabla (``fecha_nacimiento`` en caba__nacimientos), igual que en
    ``consultas.fechas`` (H044). A diferencia del filtro por período, no se usa
    ni como último recurso: en un registro crudo daba líneas de números de DNI
    ordenados por vencimiento. Sin esto, un ranking de DDJJ se graficaba como
    línea por ``fecha_nacimiento`` y el contexto le resumía el patrimonio "de
    primero a último" por cumpleaños.
    """
    return is_date_column(name) and not es_fecha_de_atributo(name, tabla)


def _sort_chart_rows(rows: list[dict[str, Any]], x_key: str) -> list[dict[str, Any]]:
    """Ordena el eje temporal por la fecha normalizada, no por el texto.

    Con ``str()``, "1/10/2017" quedaba antes que "1/9/2017" y "Junio de 2026"
    antes que "Marzo de 2026": el gráfico de línea salía desordenado en las
    respuestas del agente. Lo que no se reconoce como fecha va al final, en
    su orden de texto.
    """

    def key(row: dict[str, Any]) -> tuple[int, str]:
        value = row.get(x_key)
        iso = fecha_iso(value, "inicio")
        if iso is not None:
            return (0, iso)
        return (1, "" if value is None else str(value))

    return sorted(rows, key=key)


def _looks_like_mixed_quote_snapshot(
    result: DataResult,
    rows: list[dict[str, Any]],
    x_key: str,
    numeric_keys: list[str],
) -> bool:
    if x_key != "fecha":
        return False
    if not rows:
        return False
    if {key.lower() for key in numeric_keys} != {"compra", "venta"}:
        return False
    title = result.dataset_title.lower()
    if "todas las casas" not in title:
        return False
    x_values = [str(row.get(x_key, "")) for row in rows]
    return len(set(x_values)) < len(x_values)


def _ddjj_comparable(row: dict[str, Any], numeric_keys: list[str]) -> dict[str, Any] | None:
    """H005: lo que se puede graficar de una DDJJ, o ``None`` si nada.

    Sin la tarjeta, la barra seguía en ``chart_data``: el total de bienes que no
    cierra (31.251 M) quedaba 11,8 veces por encima del siguiente. Con
    ``ingresos_inconsistentes`` sólo sus ingresos no son comparables: se anulan
    y la fila sigue con sus bienes (en un gráfico de ingresos solos, sale).
    """
    if row.get("inconsistente"):
        return None
    if row.get("ingresos_inconsistentes"):
        return {**row, **{k: None for k in numeric_keys if "ingreso" in k.lower()}}
    return row


def build_deterministic_charts(
    results: list[DataResult], max_charts: int = 4
) -> list[dict[str, Any]]:
    """Build charts deterministically from structured data results."""
    charts: list[dict[str, Any]] = []
    for result in results:
        if len(charts) >= max_charts:
            break
        if not result.records or len(result.records) < 2:
            continue
        first = result.records[0]
        if not isinstance(first, dict):
            continue
        if first.get("_type") == "resource_metadata":
            continue
        is_ddjj = result.source.startswith("ddjj:")
        if is_ddjj:
            # La DDJJ que no cierra no va al gráfico, y tampoco decide sus
            # series: su variación es null y la sacaba para todas.
            first = next((r for r in result.records if not r.get("inconsistente")), first)

        keys = list(first.keys())

        # Detect temporal key
        tabla = tabla_del_resultado(result)
        time_key = None
        for k in keys:
            kl = k.lower()
            if es_eje_temporal(k, tabla) or kl in ("año", "year", "mes"):
                time_key = k
                break

        # Detect label key for categorical data (e.g., "nombre")
        label_key = None
        if not time_key:
            for k in keys:
                kl = k.lower()
                if kl in (
                    "nombre",
                    "name",
                    "titulo",
                    "title",
                    "label",
                    "categoria",
                    "category",
                ):
                    label_key = k
                    break

        x_key = time_key or label_key
        if not x_key:
            continue

        # Columns that are numeric but should never be charted
        _SKIP_NUMERIC = {
            "centroide_lat",
            "centroide_lon",
            "lat",
            "lon",
            "latitud",
            "longitud",
            "latitude",
            "longitude",
            "id",
            "provincia_id",
            "departamento_id",
            "municipio_id",
            "localidad_censal_id",
        }
        numeric_keys = [
            k
            for k in keys
            if k != x_key
            and not k.startswith("_")
            and k.lower() not in _SKIP_NUMERIC
            and isinstance(first.get(k), int | float)
            # ``isinstance(False, int)`` es True: una marca no es una serie.
            and not isinstance(first.get(k), bool)
        ]
        if not numeric_keys:
            continue

        # For categorical charts, pick the most relevant numeric column
        if label_key and not time_key:
            # Prefer patrimonio/value columns for rankings
            preferred = [
                k
                for k in numeric_keys
                if any(
                    t in k.lower()
                    for t in ("patrimonio", "total", "monto", "valor", "cantidad", "importe")
                )
            ]
            if preferred:
                numeric_keys = preferred[:1]
            else:
                numeric_keys = numeric_keys[:1]

        records = result.records
        if is_ddjj:
            comparable = (_ddjj_comparable(row, numeric_keys) for row in records)
            records = [row for row in comparable if row is not None]
        clean = [row for row in records if any(row.get(k) is not None for k in numeric_keys)]
        if len(clean) < 2:
            continue

        is_time = result.format == "time_series" or (
            time_key is not None and is_date_column(time_key)
        )
        if is_time:
            clean = _sort_chart_rows(clean, x_key)
        if _looks_like_mixed_quote_snapshot(result, clean, x_key, numeric_keys):
            logger.info(
                "Skipping misleading mixed quote line chart for dataset '%s'",
                result.dataset_title,
            )
            continue
        chart_type = "line_chart" if is_time else "bar_chart"
        title = result.dataset_title
        units = result.metadata.get("units")
        if units:
            title += f" ({units})"

        charts.append(
            {
                "type": chart_type,
                "title": title,
                "data": [
                    {x_key: row[x_key], **{k: row.get(k) for k in numeric_keys}} for row in clean
                ],
                "xKey": x_key,
                "yKeys": numeric_keys,
            }
        )
    return charts


_GENERIC_TITLE_PREFIX = "Consulta SQL:"


def adopt_llm_titles(
    det_charts: list[dict[str, Any]],
    llm_charts: list[dict[str, Any]],
    query_titles: frozenset[str] = frozenset(),
) -> list[dict[str, Any]]:
    """Ponerle a un gráfico determinístico el título que propuso el modelo.

    Los datos del gráfico determinístico son los buenos (todas las filas), pero
    en una consulta NL2SQL su título es el del resultado: "Consulta SQL: <la
    pregunta>", o el nombre del dataset entero ("Principales tasas de
    interés"), que no dice qué recorte se consultó. Si el modelo armó un gráfico
    sobre el mismo eje, su título describe mejor lo que se ve ("… dic 2025 – jun
    2026"). `query_titles` son los títulos de resultados NL2SQL. Sólo se toma
    el título: los datos no se tocan.
    """
    llm_titles = {
        c.get("xKey"): c.get("title") for c in llm_charts if c.get("xKey") and c.get("title")
    }
    for chart in det_charts:
        title = str(chart.get("title", ""))
        adoptable = title.startswith(_GENERIC_TITLE_PREFIX) or title in query_titles
        if adoptable and chart.get("xKey") in llm_titles:
            chart["title"] = llm_titles[chart["xKey"]]
    return det_charts


def extract_llm_charts(text: str) -> list[dict[str, Any]]:
    """Extract chart definitions from LLM <!--CHART:{...}--> tags."""
    charts: list[dict[str, Any]] = []
    for match in re.finditer(r"<!--CHART:(.*?)-->", text, re.DOTALL):
        try:
            chart = json.loads(match.group(1))
            if not (
                chart.get("type") and chart.get("data") and chart.get("xKey") and chart.get("yKeys")
            ):
                continue
            # Validate that data rows actually contain numeric values
            y_keys = chart["yKeys"]
            valid_rows = [
                row
                for row in chart["data"]
                if any(isinstance(row.get(k), int | float) for k in y_keys)
            ]
            if len(valid_rows) < 2:
                continue
            chart["data"] = valid_rows
            charts.append(chart)
        except (json.JSONDecodeError, KeyError):
            logger.debug("Failed to parse LLM chart tag", exc_info=True)
    return charts


def extract_meta(text: str) -> tuple[float, list[dict[str, Any]]]:
    """Parse <!--META:{...}--> tag for confidence and citations."""
    match = re.search(r"<!--META:(.*?)-->", text, re.DOTALL)
    if not match:
        return 1.0, []
    try:
        meta = json.loads(match.group(1))
        confidence = max(0.0, min(1.0, float(meta.get("confidence", 1.0))))
        citations = meta.get("citations", [])
        if not isinstance(citations, list):
            citations = []
        return confidence, citations
    except (json.JSONDecodeError, ValueError, TypeError):
        return 1.0, []
