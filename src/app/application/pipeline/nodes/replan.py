"""LangGraph node: re-plan when analyst detects insufficient data."""

from __future__ import annotations

import logging

from langgraph.config import get_stream_writer

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.connectors.sandbox import discover_catalog_hints_for_planner
from app.application.pipeline.state import RESET_LIST, OpenArgState
from app.infrastructure.adapters.connectors.query_planner import generate_plan

logger = logging.getLogger(__name__)


def _strategy_hint(strategy: str) -> str:
    """Return planner-facing hint text for the given replan strategy."""
    if strategy == "broaden":
        return (
            "ESTRATEGIA: AMPLIAR BÚSQUEDA\n"
            "El intento anterior fue muy específico. Probá con:\n"
            "- search_datasets (vector search) para descubrir tablas relevantes\n"
            "- query_sandbox con tablas más generales (cache_presupuesto_*, cache_indec_*)\n"
            "- Términos de búsqueda más amplios\n"
            "- Incluir query_series o query_argentina_datos como fuentes complementarias"
        )
    if strategy == "switch_source":
        return (
            "ESTRATEGIA: CAMBIAR FUENTE\n"
            "Los conectores del intento anterior fallaron. Usá fuentes DIFERENTES:\n"
            "- Si query_series falló → probá query_sandbox con cache_series_*\n"
            "- Si query_sandbox falló → probá search_datasets o search_ckan\n"
            "- Si un conector de API falló → probá query_sandbox con tablas cacheadas\n"
            "- NUNCA repitas los mismos conectores que ya fallaron"
        )
    if strategy == "narrow":
        return (
            "ESTRATEGIA: ENFOCAR BÚSQUEDA\n"
            "El intento anterior devolvió datos pero no los correctos. Sé más específico:\n"
            "- Usá filtros más estrictos en los parámetros\n"
            "- Buscá tablas más específicas en el catálogo\n"
            "- Reducí el scope temporal o geográfico de la consulta"
        )
    return "Probá una estrategia diferente al intento anterior."


async def replan_node(state: OpenArgState) -> dict:
    """Re-plan with strategy-aware context when coordinator detects insufficient data.

    Reads *replan_strategy* from state (set by coordinator) and builds
    strategy-specific hints for the planner. Increments *replan_count*
    and resets execution state for the next pass.
    """
    writer = get_stream_writer()
    deps = nodes_pkg.get_deps()

    replan_count = state.get("replan_count", 0) + 1
    strategy = state.get("replan_strategy", "broaden")

    writer({"type": "status", "step": "replanning", "detail": f"Replanificando búsqueda ({strategy})..."})

    try:
        preprocessed_q = state.get("preprocessed_query", state["question"])
        planner_ctx = state.get("planner_ctx", "")

        # Enrich planner context with info about what was already tried
        step_warnings = list(state.get("step_warnings", []))
        data_results = state.get("data_results", [])

        replan_context = planner_ctx
        if step_warnings:
            replan_context += "\n\nINTENTO ANTERIOR - ADVERTENCIAS:\n" + "\n".join(
                f"- {w}" for w in step_warnings
            )
        if not data_results or not any(r.records for r in data_results):
            replan_context += "\n\nEl intento anterior no devolvió datos."

        # Strategy-specific hints from coordinator
        replan_context += "\n\n" + _strategy_hint(strategy)

        # Discover catalog hints again (same logic as planner_node)
        catalog_hints = await discover_catalog_hints_for_planner(
            preprocessed_q, deps.sandbox, deps.embedding
        )

        # Generate a new plan with the enriched context
        plan = await generate_plan(
            deps.llm,
            preprocessed_q,
            memory_context=replan_context,
            catalog_hints=catalog_hints,
        )

        return {
            "replan_count": replan_count,
            "catalog_hints": catalog_hints,
            "plan": plan,
            "plan_intent": plan.intent,
            # Reset data_results and step_warnings for the new execution pass.
            # Must use RESET_LIST sentinel (not []) because the reducer is
            # _resettable_add — returning [] would be a no-op (old + [] = old).
            "data_results": RESET_LIST,
            "step_warnings": RESET_LIST,
        }
    except Exception:
        logger.exception("replan_node failed")
        return {
            "replan_count": replan_count,
            "error": "Replan failed",
        }
