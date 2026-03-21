"""Build LLM context from data results and system capabilities."""

from __future__ import annotations

import json
import logging

from app.domain.entities.connectors.data_result import DataResult

logger = logging.getLogger(__name__)


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


def build_data_context(results: list[DataResult]) -> str:
    """Format data results into a text block for the LLM analyst."""
    if not results:
        return (
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

    max_per_result = 8_000
    max_results = 10
    max_total = 80_000

    # Warn analyst about truncated results so it can inform the user
    total_available = len(results)
    parts = []
    if total_available > max_results:
        parts.append(
            f"⚠ NOTA PARA EL ANALISTA: Se recopilaron {total_available} fuentes de datos "
            f"pero solo se incluyen las {max_results} más relevantes por límites de contexto. "
            f"Advertí al usuario que la respuesta se basa en un subconjunto de los datos disponibles "
            f"y que puede refinar su consulta para obtener resultados más específicos."
        )

    for i, result in enumerate(results[:max_results]):
        valid_records = [r for r in result.records if isinstance(r, dict)]
        if not valid_records and result.records:
            continue

        # Vector search results: no records but have metadata
        is_vector_result = not valid_records and result.source.startswith("pgvector:")

        is_metadata_only = valid_records and valid_records[0].get("_type") == "resource_metadata"

        if is_vector_result:
            part = (
                f"--- Dataset {i + 1}: {result.dataset_title} ---\n"
                f"Portal: {result.portal_name}\n"
                f"URL: {result.portal_url}\n"
            )
            if result.metadata.get("description"):
                part += f"Descripción: {result.metadata['description']}\n"
            if result.metadata.get("columns"):
                part += f"Columnas: {result.metadata['columns']}\n"
            if result.metadata.get("score"):
                part += f"Relevancia: {result.metadata['score']}\n"
            part += (
                "\nEste dataset está indexado en la base de datos. "
                "Listalo al usuario con su título, descripción y URL."
            )
        elif is_metadata_only:
            preview = valid_records[:20]
            records_text = json.dumps(
                preview, ensure_ascii=False, separators=(",", ":"), default=str
            )
            part = (
                f"--- Dataset {i + 1}: {result.dataset_title} ---\n"
                f"Fuente: {result.portal_name} ({result.source})\n"
                f"URL: {result.portal_url}\n"
                "NOTA: Este dataset no tiene Datastore "
                "habilitado. Solo metadatos de los recursos.\n"
            )
            if result.metadata.get("description"):
                part += f"Descripción: {result.metadata['description']}\n"
            part += (
                f"Recursos disponibles:\n{records_text}\n\n"
                "Explicale al usuario qué datos contiene "
                "y proporcioná el link."
            )
        else:
            columns = list(valid_records[0].keys()) if valid_records else []
            # Humanize column names for LLM display (replace _ with spaces)
            display_columns = [c.replace("_", " ") for c in columns]
            total_rows = len(valid_records)

            if total_rows > 50:
                records_to_send = valid_records[:25] + valid_records[-25:]
                truncation_note = f", primeros 25 + últimos 25 de {total_rows} totales"
            else:
                records_to_send = valid_records
                truncation_note = ""

            # Humanize keys in records for LLM context
            display_records = [
                {k.replace("_", " "): v for k, v in rec.items()} for rec in records_to_send
            ]
            records_text = json.dumps(
                display_records, ensure_ascii=False, separators=(",", ":"), default=str
            )

            part = (
                f"--- Dataset {i + 1}: {result.dataset_title} ---\n"
                f"Fuente: {result.portal_name} ({result.source})\n"
                f"URL: {result.portal_url}\n"
                f"Formato: {result.format}\n"
                f"Total de registros: {result.metadata.get('total_records', total_rows)}\n"
                f"Columnas: {', '.join(display_columns)}\n"
            )
            if result.metadata.get("description"):
                part += f"Descripción: {result.metadata['description']}\n"
            part += (
                f"Datos ({len(records_to_send)} registros{truncation_note}):\n"
                f"{records_text}\n\n"
                "IMPORTANTE: Si hay una columna temporal "
                "(año, fecha, mes), generá un gráfico de línea "
                "temporal con <!--CHART:{}--> usando TODOS los "
                "datos proporcionados."
            )

        # Per-result truncation: prevent a single large result
        # from consuming the entire context budget
        if len(part) > max_per_result:
            part = part[:max_per_result] + "\n... [truncado, datos parciales]\n"

        parts.append(part)

    joined = "\n\n".join(parts)
    if len(joined) > max_total:
        joined = joined[:max_total] + "\n\n[... datos truncados por límite de contexto ...]"
    return joined
