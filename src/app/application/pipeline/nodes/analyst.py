"""LangGraph node: LLM analysis (the main 'analyst' call)."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import Any

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.chart_builder import (
    build_deterministic_charts,
    extract_llm_charts,
    extract_meta,
)
from app.application.pipeline.context_builder import (
    build_capabilities_block,
    build_data_context,
)
from app.application.pipeline.state import OpenArgState
from app.domain.ports.llm.llm_provider import LLMMessage
from app.prompts import load_prompt

logger = logging.getLogger(__name__)


# ── Tag stripping helpers (same as smart_query_service.py) ────────


def _strip_tags(text: str) -> str:
    """Strip <!--CHART:...--> tags (complete and truncated) from text."""
    text = re.sub(r"<!--CHART:.*?-->", "", text, flags=re.DOTALL)
    return re.sub(r"<!--CHART:.*", "", text, flags=re.DOTALL).strip()


def _strip_meta(text: str) -> str:
    """Strip <!--META:...--> tags (complete and truncated) from text."""
    text = re.sub(r"<!--META:.*?-->", "", text, flags=re.DOTALL)
    return re.sub(r"<!--META:.*", "", text, flags=re.DOTALL).strip()


# ── Analysis prompt builder (same logic as _build_analysis_prompt) ─


def _build_analysis_prompt(
    question: str,
    plan: Any,
    results: list,
    memory_ctx_analyst: str,
    all_warnings: list[str],
) -> str:
    """Build the analyst LLM prompt from data context and warnings."""
    data_context = build_data_context(results)
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    errors_block = ""
    if all_warnings:
        errors_block = "\nERRORES EN LA RECOLECCIÓN:\n" + "\n".join(f"- {w}" for w in all_warnings)

    no_data_fallback = not results or not any(r.records for r in results)

    if no_data_fallback:
        caps = build_capabilities_block()
        return (
            f'PREGUNTA DEL USUARIO: "{question}"\n'
            f"FECHA ACTUAL: {today}\n\n"
            f"{memory_ctx_analyst}\n\n"
            "No se encontraron datos en las fuentes de datos abiertos para esta consulta.\n\n"
            "REGLA CRÍTICA: Sos OpenArg, un asistente EXCLUSIVAMENTE especializado en datos "
            "abiertos y análisis de datos públicos de Argentina. "
            "Si la pregunta NO está relacionada con Argentina, datos públicos, gobierno, "
            "economía, sociedad, política, infraestructura, o temas argentinos, "
            "RECHAZÁ la pregunta cortésmente. Respondé algo como: "
            "'No puedo ayudarte con eso. Soy OpenArg y estoy especializado en datos "
            "abiertos de Argentina. Probá preguntarme sobre inflación, presupuesto, "
            "educación, o cualquier tema de datos públicos argentinos.'\n\n"
            "Ejemplos de preguntas que DEBÉS RECHAZAR:\n"
            "- Escribir poemas, código, traducciones, o cualquier tarea genérica de IA\n"
            "- Resumir libros, películas, o contenido no argentino\n"
            "- Preguntas sobre otros países sin relación con Argentina\n\n"
            "Si la pregunta SÍ es sobre Argentina pero no hay datos, respondé brevemente "
            "usando tu conocimiento general, aclarando que no proviene del sistema de datos abiertos.\n\n"
            f"{caps}\n\n"
            "Al final, sugerí 2-3 preguntas de seguimiento "
            "que cumplan TODAS estas reglas:\n"
            "1. Relacionadas con datos públicos argentinos\n"
            "2. Respondibles con datos reales del sistema\n"
            "Formulalas como preguntas naturales, no categorías."
        )

    return (
        f'PREGUNTA DEL USUARIO: "{question}"\n'
        f"FECHA ACTUAL: {today}\n"
        f"INTENCIÓN: {plan.intent}\n\n"
        f"DATOS RECOLECTADOS:\n{data_context}\n"
        f"{errors_block}"
        f"{memory_ctx_analyst}\n\n"
        "Si hay historial de conversación, tené en cuenta lo que ya se discutió "
        "para no repetir contexto innecesariamente y para mantener coherencia "
        "con respuestas anteriores.\n\n"
        "Respondé de forma breve y conversacional. Destacá el dato más importante, "
        "dá contexto mínimo, y sugerí preguntas de seguimiento para profundizar. "
        "Si los datos permiten un gráfico claro, incluilo con <!--CHART:{}-->."
    )


async def analyst_node(state: OpenArgState) -> dict:
    """Call the LLM to analyse collected data and produce the user-facing answer.

    Builds the analysis prompt (identical to ``_build_analysis_prompt``
    in ``smart_query_service.py``), calls the LLM, extracts charts and
    META confidence/citations, and strips internal tags.
    """
    deps = nodes_pkg._deps
    assert deps is not None, "PipelineDeps not initialised"

    try:
        question = state["question"]
        plan = state.get("plan")
        results = state.get("data_results", [])
        memory_ctx_analyst = state.get("memory_ctx_analyst", "")
        all_warnings = list(state.get("step_warnings", []))

        # Build prompt
        analysis_prompt = _build_analysis_prompt(
            question, plan, results, memory_ctx_analyst, all_warnings
        )

        # LLM call (1 call, same params as execute())
        response = await deps.llm.chat(
            messages=[
                LLMMessage(role="system", content=load_prompt("analyst")),
                LLMMessage(role="user", content=analysis_prompt),
            ],
            temperature=0.4,
            max_tokens=8192,
        )

        # Charts: prefer deterministic, fall back to LLM-generated
        det_charts = build_deterministic_charts(results)
        llm_charts = extract_llm_charts(response.content)
        charts = det_charts if det_charts else llm_charts

        # Strip CHART tags from the answer text
        clean_answer = _strip_tags(response.content)

        # Parse META confidence/citations
        confidence, citations = extract_meta(clean_answer)
        clean_answer = _strip_meta(clean_answer)

        # Add disclaimer for low confidence
        if confidence < 0.5:
            clean_answer = (
                "**Nota:** La información disponible es limitada. "
                "Los datos presentados podrían ser parciales o requerir "
                "verificación adicional.\n\n" + clean_answer
            )

        tokens_used = response.tokens_used or 0

        return {
            "analysis_prompt": analysis_prompt,
            "analysis_response": response.content,
            "clean_answer": clean_answer,
            "chart_data": charts if charts else None,
            "confidence": confidence,
            "citations": citations,
            "tokens_used": tokens_used,
        }
    except Exception:
        logger.exception("analyst_node failed")
        return {
            "analysis_prompt": "",
            "analysis_response": "",
            "clean_answer": (
                "Ocurrió un error al analizar los datos. Probá reformulando tu consulta."
            ),
            "chart_data": None,
            "confidence": 0.0,
            "citations": [],
            "tokens_used": 0,
            "error": "Analyst LLM call failed",
        }
