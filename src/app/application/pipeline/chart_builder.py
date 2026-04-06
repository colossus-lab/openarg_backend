"""Deterministic chart building, LLM chart extraction, and META parsing."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from app.domain.entities.connectors.data_result import DataResult

logger = logging.getLogger(__name__)


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

        keys = list(first.keys())

        # Detect temporal key
        time_key = None
        for k in keys:
            kl = k.lower()
            if k == "fecha" or "date" in kl or kl in ("año", "year", "mes"):
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

        clean = [row for row in result.records if any(row.get(k) is not None for k in numeric_keys)]
        if len(clean) < 2:
            continue

        is_time = result.format == "time_series" or time_key == "fecha"
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
