"""LangGraph node: LLM analysis (the main 'analyst' call)."""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

from langgraph.config import get_stream_writer

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

_MAX_MAP_FEATURES = 500


def _build_map_data(results: list) -> dict[str, Any] | None:
    """Build a GeoJSON FeatureCollection from data results that contain geometry.

    Returns None if no results carry ``_geometry_geojson`` columns.
    Deterministic — no LLM involved.
    """
    features: list[dict[str, Any]] = []
    for r in results:
        for rec in r.records:
            geojson_str = rec.get("_geometry_geojson")
            if not geojson_str:
                continue
            try:
                geom = json.loads(geojson_str) if isinstance(geojson_str, str) else geojson_str
            except (json.JSONDecodeError, TypeError):
                continue
            props = {
                k: v for k, v in rec.items() if k not in ("_geometry_geojson", "_source_dataset_id")
            }
            features.append({"type": "Feature", "geometry": geom, "properties": props})
            if len(features) >= _MAX_MAP_FEATURES:
                break
        if len(features) >= _MAX_MAP_FEATURES:
            break
    if not features:
        return None
    return {"type": "FeatureCollection", "features": features}


# ── Pre-compiled regex patterns (avoid recompilation per call) ────

_RE_CHART_TAG = re.compile(r"<!--CHART:.*?-->", re.DOTALL)
_RE_CHART_TRUNC = re.compile(r"<!--CHART:.*", re.DOTALL)
_RE_META_TAG = re.compile(r"<!--META:.*?-->", re.DOTALL)
_RE_META_TRUNC = re.compile(r"<!--META:.*", re.DOTALL)
_RE_ANY_TAG = re.compile(r"<!--.*?-->", re.DOTALL)
_RE_ANY_TAG_TRUNC = re.compile(r"<!--.*", re.DOTALL)


def _strip_tags(text: str) -> str:
    """Strip <!--CHART:...--> tags (complete and truncated) from text."""
    text = _RE_CHART_TAG.sub("", text)
    return _RE_CHART_TRUNC.sub("", text).strip()


def _strip_meta(text: str) -> str:
    """Strip <!--META:...--> tags (complete and truncated) from text."""
    text = _RE_META_TAG.sub("", text)
    return _RE_META_TRUNC.sub("", text).strip()


# ── Analysis prompt builder (same logic as _build_analysis_prompt) ─


def _build_analysis_prompt(
    question: str,
    plan: Any,
    results: list,
    memory_ctx_analyst: str,
    all_warnings: list[str],
    skill_context: dict[str, str] | None = None,
) -> str:
    """Build the analyst LLM prompt from data context and warnings."""
    data_context = build_data_context(results)
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    errors_block = ""
    if all_warnings:
        errors_block = "\nERRORES EN LA RECOLECCIÓN:\n" + "\n".join(f"- {w}" for w in all_warnings)

    no_data_fallback = not results or not any(
        r.records or r.source.startswith("pgvector:") or r.source.startswith("ckan:")
        for r in results
    )

    if no_data_fallback:
        caps = build_capabilities_block()
        # Don't pass memory context in no-data mode — it causes the LLM to
        # hallucinate "we already discussed this" when no data was ever shown.
        return load_prompt(
            "analyst_no_data",
            question=question,
            today=today,
            memory_ctx_analyst="",
            caps=caps,
        )

    skill_block = ""
    if skill_context and skill_context.get("analyst"):
        skill_block = "\n\n--- FORMATO REQUERIDO POR SKILL ---\n" + skill_context["analyst"]

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
        f"{skill_block}"
    )


async def analyst_node(state: OpenArgState) -> dict:
    """Call the LLM to analyse collected data and produce the user-facing answer.

    Builds the analysis prompt (identical to ``_build_analysis_prompt``
    in ``smart_query_service.py``), calls the LLM, extracts charts and
    META confidence/citations, and strips internal tags.
    """
    writer = get_stream_writer()
    deps = nodes_pkg.get_deps()
    replan_count = state.get("replan_count", 0)

    # If this is a second-pass analyst (after replan), tell the frontend
    # to replace the previous response instead of appending to it.
    if replan_count > 0:
        writer({"type": "clear_answer"})
        writer({"type": "status", "step": "generating", "detail": "Reintentando análisis..."})
    else:
        writer({"type": "status", "step": "generating", "detail": "Generando análisis..."})

    try:
        question = state["question"]
        plan = state.get("plan")
        results = state.get("data_results", [])
        memory_ctx_analyst = state.get("memory_ctx_analyst", "")
        all_warnings = list(state.get("step_warnings", []))

        # Build prompt
        skill_context = state.get("skill_context")
        analysis_prompt = _build_analysis_prompt(
            question, plan, results, memory_ctx_analyst, all_warnings,
            skill_context=skill_context,
        )

        # LLM streaming call — emit chunks as they arrive
        messages = [
            LLMMessage(role="system", content=load_prompt("analyst")),
            LLMMessage(role="user", content=analysis_prompt),
        ]

        full_text = ""
        stream_buf = ""
        try:
            async for chunk_text in deps.llm.chat_stream(
                messages=messages,
                temperature=0.2,
                max_tokens=8192,
            ):
                full_text += chunk_text
                stream_buf += chunk_text

                # Buffer to avoid sending incomplete <!--CHART:--> or <!--META:--> tags
                if "<!--" in stream_buf and "-->" not in stream_buf:
                    continue  # Wait for tag to close

                # Strip any complete tags from the buffer before sending
                cleaned = _RE_ANY_TAG.sub("", stream_buf)
                if cleaned:
                    writer({"type": "chunk", "content": cleaned})
                stream_buf = ""

            # Flush remaining buffer
            if stream_buf:
                cleaned = _RE_ANY_TAG.sub("", stream_buf)
                cleaned = _RE_ANY_TAG_TRUNC.sub("", cleaned)
                if cleaned:
                    writer({"type": "chunk", "content": cleaned})
        except Exception:
            # Fallback to non-streaming if chat_stream fails
            logger.warning("chat_stream failed, falling back to chat()", exc_info=True)
            response = await deps.llm.chat(messages=messages, temperature=0.4, max_tokens=8192)
            full_text = response.content
            writer({"type": "chunk", "content": _strip_tags(full_text)})

        # Charts: prefer deterministic, fall back to LLM-generated
        det_charts = build_deterministic_charts(results)
        llm_charts = extract_llm_charts(full_text)
        charts = det_charts if det_charts else llm_charts

        # Maps: build GeoJSON FeatureCollection from geo results (deterministic)
        map_data = _build_map_data(results)
        # When map data is present, suppress charts (geo data doesn't chart well)
        if map_data:
            charts = None

        # Strip CHART tags from the answer text
        clean_answer = _strip_tags(full_text)

        # Parse META confidence/citations
        confidence, citations = extract_meta(clean_answer)
        clean_answer = _strip_meta(clean_answer)

        # Low confidence: just lower the confidence score, don't prepend text
        # (adding disclaimers triggers negative-phrase detection in E2E tests
        # and confuses users who see "Nota:" before every uncertain answer)

        tokens_used = 0  # Token count not available in streaming mode

        return {
            "analysis_prompt": analysis_prompt,
            "analysis_response": full_text,
            "clean_answer": clean_answer,
            "chart_data": charts if charts else None,
            "map_data": map_data,
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
            "map_data": None,
            "confidence": 0.0,
            "citations": [],
            "tokens_used": 0,
            "error": "Analyst LLM call failed",
        }
