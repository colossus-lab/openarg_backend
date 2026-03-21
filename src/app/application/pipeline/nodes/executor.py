"""LangGraph nodes: inject fallbacks and execute data-retrieval steps."""

from __future__ import annotations

import logging

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.classifiers import DATA_ACTIONS
from app.application.pipeline.state import OpenArgState
from app.application.pipeline.step_executor import ConnectorDeps, execute_steps
from app.domain.entities.connectors.data_result import PlanStep

logger = logging.getLogger(__name__)


async def inject_fallbacks_node(state: OpenArgState) -> dict:
    """Inject a search_datasets fallback step when the plan has no data step.

    Ensures every non-clarification query hits at least one data source
    (vector search) even if the planner omitted it.
    """
    plan = state.get("plan")
    if not plan:
        return {"plan": plan}

    try:
        question = state["question"]
        preprocessed_q = state.get("preprocessed_query", question)

        has_vector_step = any(s.action == "search_datasets" for s in plan.steps)
        has_data_step = any(s.action in DATA_ACTIONS for s in plan.steps)

        if not has_vector_step and not has_data_step:
            plan.steps.insert(
                0,
                PlanStep(
                    id="step_vector_fallback",
                    action="search_datasets",
                    description=(
                        f"Buscar datasets relevantes por similitud semántica: {question[:100]}"
                    ),
                    params={"query": preprocessed_q, "limit": 5},
                    depends_on=[],
                ),
            )
        return {"plan": plan}
    except Exception:
        logger.exception("inject_fallbacks_node failed")
        return {"plan": plan}


async def execute_steps_node(state: OpenArgState) -> dict:
    """Execute the data-retrieval steps defined in the plan.

    Steps are dispatched in parallel per dependency level through
    ``execute_steps()``, which handles retries and error isolation.
    """
    deps = nodes_pkg.get_deps()

    plan = state.get("plan")
    if not plan:
        return {
            "data_results": [],
            "step_warnings": ["No execution plan available"],
        }

    try:
        question = state["question"]

        connector_deps = ConnectorDeps(
            series=deps.series,
            arg_datos=deps.arg_datos,
            georef=deps.georef,
            ckan=deps.ckan,
            sesiones=deps.sesiones,
            ddjj=deps.ddjj,
            staff=deps.staff,
            bcra=deps.bcra,
            sandbox=deps.sandbox,
            vector_search=deps.vector_search,
            llm=deps.llm,
            embedding=deps.embedding,
            semantic_cache=deps.semantic_cache,
        )

        results, step_warnings = await execute_steps(
            plan, connector_deps, deps.metrics, nl_query=question
        )

        return {
            "data_results": results,
            "step_warnings": step_warnings,
        }
    except Exception:
        logger.exception("execute_steps_node failed")
        return {
            "data_results": [],
            "step_warnings": ["Step execution failed unexpectedly"],
        }
