"""Build LLM context from data results and system capabilities."""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import time
from typing import Any

from app.domain.entities.connectors.data_result import DataResult

logger = logging.getLogger(__name__)

_NO_RESULTS_CONTEXT = (
    "No se obtuvieron resultados directos en esta búsqueda. "
    "Sin embargo, TENÉS acceso en tiempo real a estos "
    "portales de datos abiertos:\n"
    "- **Portal Nacional** (datos.gob.ar): 1200+ datasets "
    "de economía, salud, educación, transporte, energía\n"
    "- **CABA** (data.buenosaires.gob.ar): movilidad, "
    "presupuesto, educación\n"
    "- **Buenos Aires Provincia** "
    "(catalogo.datos.gba.gob.ar): salud, estadísticas\n"
    "- **Córdoba, Santa Fe, Mendoza, Entre Ríos, "
    "Neuquén** y más\n"
    "- **Cámara de Diputados** (datos.hcdn.gob.ar): "
    "legisladores, proyectos, leyes\n"
    "- **Series de Tiempo**: inflación, tipo de cambio, "
    "PBI, presupuesto\n"
    "- **DDJJ**: 195 declaraciones juradas patrimoniales "
    "de diputados\n\n"
    "INSTRUCCIÓN: NO digas que 'no pudiste acceder' o "
    "'no tenés datos'. En cambio, explicale al usuario "
    "qué fuentes están disponibles y sugerí búsquedas "
    "concretas. Ofrecé 3-4 opciones temáticas."
)
_VECTOR_RESULT_NOTE = (
    "\nEste dataset está indexado en la base de datos. "
    "Listalo al usuario con su título, descripción y URL."
)
_METADATA_ONLY_NOTE = (
    "NOTA: Este dataset no tiene Datastore habilitado. Solo metadatos de los recursos."
)
_METADATA_ONLY_INSTRUCTION = "\nExplicale al usuario qué datos contiene y proporcioná el link."
_TABULAR_CHART_INSTRUCTION = (
    "\n\nIMPORTANTE: Si hay una columna temporal "
    "(año, fecha, mes), generá un gráfico de línea "
    "temporal con <!--CHART:{}--> usando TODOS los "
    "datos proporcionados."
)
_CONTEXT_TRUNCATED_SUFFIX = "\n[contexto recortado por espacio]"
# LLM-001: a non-aggregated query (SELECT * ... LIMIT n) returns a capped
# sample. The number of rows is a query artefact, not a real total.
_SAMPLE_RESULT_NOTE = (
    "⚠ DATOS DE MUESTRA: las filas de abajo son una muestra acotada por un LIMIT, "
    "NO el universo completo. La cantidad de filas es un artefacto de la consulta — "
    "NUNCA la reportes como un conteo o total (no digas 'hubo N registros de X'). "
    "Si la pregunta pedía un conteo o cuántos, este resultado NO lo responde: "
    "decilo con honestidad y ofrecé reformular para obtener el número exacto."
)
# LLM-002: SUM/AVG over a tasa/índice/porcentaje column inflates a
# meaningless figure (rates are not additive across granularities).
_NONADDITIVE_WARNING_NOTE = (
    "⚠ MÉTRICA NO ADITIVA: la consulta sumó o promedió una columna de "
    "tasa/índice/porcentaje. Esos valores no son aditivos — el número resultante "
    "puede estar inflado y carecer de sentido. NO lo presentes como dato firme: "
    "aclará la limitación o respondé con los valores absolutos (cantidades)."
)


@functools.cache
def _display_key(key: str) -> str:
    """Convert raw field names into analyst-friendly labels once."""
    return key.replace("_", " ")


@functools.cache
def _display_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
    """Render a dataset schema once per unique column signature."""
    return tuple(_display_key(column) for column in columns)


@functools.cache
def _display_columns_text(columns: tuple[str, ...]) -> str:
    """Render the display-schema line once per unique column signature."""
    return ", ".join(_display_columns(columns))


@functools.cache
def _display_key_map(columns: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    """Build a stable display-key mapping once per unique column signature."""
    return tuple((column, _display_key(column)) for column in columns)


@functools.cache
def build_capabilities_block() -> str:
    """Build a concise list of system capabilities from the taxonomy."""
    try:
        from app.infrastructure.adapters.connectors.dataset_index import TAXONOMY

        lines = ["CAPACIDADES DEL SISTEMA — OpenArg tiene datos sobre:"]
        for cat in TAXONOMY.values():
            children = cat.get("children", {})
            child_labels = [c["label"] for c in children.values()]
            lines.append(f"• {cat['label']}: {', '.join(child_labels)}")
        lines.append("• Georeferenciación: provincias, departamentos, municipios y localidades")
        return "\n".join(lines)
    except Exception:
        logger.debug("Failed to build capabilities from taxonomy", exc_info=True)
        return (
            "CAPACIDADES DEL SISTEMA — OpenArg tiene datos sobre:\n"
            "• Economía (inflación, dólar, empleo, actividad económica)\n"
            "• Gobierno (presupuesto, autoridades, gobernadores, DDJJ)\n"
            "• Congreso (legisladores, sesiones, personal, comisiones)\n"
            "• Datos sociales (educación, salud, seguridad, género)\n"
            "• Infraestructura (transporte, energía, telecomunicaciones)\n"
            "• Georeferenciación (provincias, municipios, localidades)"
        )


# The no-data reply must only suggest questions the system can actually
# answer, so it needs the list of marts that are alive right now. TTL-cached
# in-process: deflections are frequent enough that hitting mart_definitions
# on every one would be wasteful, and 5 minutes of staleness is harmless.
_LIVE_MARTS_TTL_S = 300.0
_live_marts_cache: tuple[float, str] = (0.0, "")


async def build_live_marts_block(sandbox: Any) -> str:
    """List live marts (``last_row_count > 0``) for the no-data reply.

    Returns an empty string when the sandbox engine is unavailable or the
    query fails — the caller renders the reply without the block.
    """
    global _live_marts_cache
    ts, cached = _live_marts_cache
    if cached and time.monotonic() - ts < _LIVE_MARTS_TTL_S:
        return cached

    try:
        from sqlalchemy import text

        def _query() -> list[tuple[str, str | None, str | None]]:
            eng = getattr(sandbox, "_engine", None) or (
                sandbox._get_engine() if hasattr(sandbox, "_get_engine") else None
            )
            if eng is None:
                return []
            with eng.connect() as conn:
                rs = conn.execute(
                    text(
                        "SELECT mart_view_name, domain, description "
                        "FROM mart_definitions "
                        "WHERE COALESCE(last_row_count, 0) > 0 "
                        "ORDER BY domain NULLS LAST, mart_view_name"
                    )
                ).fetchall()
            return [(r[0], r[1], r[2]) for r in rs]

        rows = await asyncio.to_thread(_query)
    except Exception:
        logger.debug("build_live_marts_block failed", exc_info=True)
        return ""

    if not rows:
        return ""

    lines = ["DATASETS VIVOS (los únicos temas con datos consultables ahora mismo):"]
    for view_name, domain, description in rows:
        label = view_name.replace("_", " ")
        detail = (description or "").strip().splitlines()[0] if description else ""
        suffix = f" — {detail}" if detail else ""
        prefix = f"[{domain}] " if domain else ""
        lines.append(f"• {prefix}{label}{suffix}")

    block = "\n".join(lines)
    _live_marts_cache = (time.monotonic(), block)
    return block


def build_data_context(results: list[DataResult]) -> str:
    """Format data results into a text block for the LLM analyst."""
    if not results:
        return _NO_RESULTS_CONTEXT

    max_per_result = 20_000
    max_results = 10
    max_total = 80_000

    # Warn analyst about truncated results so it can inform the user
    total_available = len(results)
    parts: list[str] = []
    current_len = 0

    def append_part(text: str) -> None:
        nonlocal current_len
        separator_len = 2 if parts else 0
        parts.append(text)
        current_len += separator_len + len(text)

    if total_available > max_results:
        note = (
            f"⚠ NOTA INTERNA (no mencionar al usuario): Se recopilaron {total_available} fuentes "
            f"pero solo se incluyen las {max_results} más relevantes. "
            f"Respondé con los datos disponibles sin mencionar truncación ni límites."
        )
        append_part(note)

    for i, result in enumerate(results[:max_results]):
        metadata = result.metadata
        dataset_title = result.dataset_title
        portal_name = result.portal_name
        portal_url = result.portal_url
        source = result.source
        description = metadata.get("description")
        valid_records = [r for r in result.records if isinstance(r, dict)]
        if not valid_records and result.records:
            continue

        # Vector search results: no records but have metadata
        is_vector_result = not valid_records and source.startswith("pgvector:")
        is_metadata_only = valid_records and valid_records[0].get("_type") == "resource_metadata"

        # Build each part using list+join instead of += (avoids O(n²) string concat)
        lines: list[str] = []

        if is_vector_result:
            lines.append(f"--- Dataset {i + 1}: {dataset_title} ---")
            lines.append(f"Portal: {portal_name}")
            lines.append(f"URL: {portal_url}")
            if description:
                lines.append(f"Descripción: {description}")
            if metadata.get("columns"):
                lines.append(f"Columnas: {metadata['columns']}")
            if metadata.get("score"):
                lines.append(f"Relevancia: {metadata['score']}")
            lines.append(_VECTOR_RESULT_NOTE)
        elif is_metadata_only:
            preview = valid_records[:20]
            records_text = json.dumps(
                preview, ensure_ascii=False, separators=(",", ":"), default=str
            )
            lines.append(f"--- Dataset {i + 1}: {dataset_title} ---")
            lines.append(f"Fuente: {portal_name} ({source})")
            lines.append(f"URL: {portal_url}")
            lines.append(_METADATA_ONLY_NOTE)
            if description:
                lines.append(f"Descripción: {description}")
            lines.append(f"Recursos disponibles:\n{records_text}")
            lines.append(_METADATA_ONLY_INSTRUCTION)
        else:
            columns = tuple(valid_records[0].keys()) if valid_records else ()
            display_columns_text = _display_columns_text(columns)
            total_rows = len(valid_records)

            if total_rows > 50:
                records_to_send = valid_records[:25] + valid_records[-25:]
            else:
                records_to_send = valid_records

            # Pre-compute key mapping once, reuse for all records
            if records_to_send:
                key_map = dict(_display_key_map(columns))
                display_records = [
                    {key_map.get(k, k): v for k, v in rec.items()} for rec in records_to_send
                ]
            else:
                display_records = []
            records_text = json.dumps(
                display_records, ensure_ascii=False, separators=(",", ":"), default=str
            )

            lines.append(f"--- Dataset {i + 1}: {dataset_title} ---")
            lines.append(f"Fuente: {portal_name} ({source})")
            lines.append(f"URL: {portal_url}")
            lines.append(f"Formato: {result.format}")
            lines.append(f"Total de registros: {metadata.get('total_records', total_rows)}")
            if metadata.get("result_kind") == "sample":
                lines.append(_SAMPLE_RESULT_NOTE)
            if metadata.get("nonadditive_warning"):
                lines.append(_NONADDITIVE_WARNING_NOTE)
            lines.append(f"Columnas: {display_columns_text}")
            if description:
                lines.append(f"Descripción: {description}")
            if metadata.get("table_descriptions"):
                lines.append("Tablas consultadas:")
                for td in metadata["table_descriptions"]:
                    lines.append(f"  - {td}")
            lines.append(
                f"Datos ({len(records_to_send)} registros):\n"
                f"{records_text}"
                f"{_TABULAR_CHART_INSTRUCTION}"
            )

        part = "\n".join(lines)

        # Per-result truncation
        if len(part) > max_per_result:
            part = part[:max_per_result]

        # Budget-aware: stop adding parts when budget exhausted
        remaining = max_total - current_len - (2 if parts else 0)
        if remaining <= 0:
            break
        if len(part) > remaining:
            keep = max(0, remaining - len(_CONTEXT_TRUNCATED_SUFFIX))
            part = part[:keep] + _CONTEXT_TRUNCATED_SUFFIX
            append_part(part)
            break

        append_part(part)

    return "\n\n".join(parts)
