"""LangGraph nodes: inject fallbacks and execute data-retrieval steps."""

from __future__ import annotations

import logging

from langgraph.config import get_stream_writer

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.classifiers import DATA_ACTIONS
from app.application.pipeline.nodes import llm_for
from app.application.pipeline.state import OpenArgState
from app.application.pipeline.step_executor import (
    _CONNECTOR_LABELS,
    ConnectorDeps,
    execute_steps,
)
from app.domain.entities.connectors.data_result import PlanStep

logger = logging.getLogger(__name__)


async def inject_fallbacks_node(state: OpenArgState) -> dict:
    """Inject a search_datasets fallback step when the plan has no data step.

    Ensures every non-clarification query hits at least one data source
    (vector search) even if the planner omitted it.

    Also enforces BUG-001 Capa 1: when the planner emitted both
    ``query_sandbox`` and ``search_ckan`` AND a curated mart confidently
    covers the question, the redundant ``search_ckan`` is dropped before
    dispatch. Verified pattern: F4 ("Palermo 2023") consistently emitted
    both steps, wall-clock dominated by the live CKAN step (30-63s); F3
    ("horario nocturno") emits only ``query_sandbox`` and runs in 22-24s.
    The LLM ignores planner.txt's anti-search_ckan rule in ~100% of cases
    that include a specific year/entity filter, so a deterministic
    pre-dispatch filter is the only reliable fix.
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

        # BUG-001 Capa 1: strip redundant search_ckan when a mart covers it.
        has_sandbox = any(s.action == "query_sandbox" for s in plan.steps)
        has_ckan = any(s.action == "search_ckan" for s in plan.steps)
        if has_sandbox and has_ckan:
            try:
                from types import SimpleNamespace

                from app.application.pipeline.step_executor import (
                    _MART_REDIRECT_THRESHOLD,
                    _top_mart_similarity,
                )

                deps = nodes_pkg.get_deps()
                probe_deps = SimpleNamespace(sandbox=deps.sandbox, embedding=deps.embedding)
                sim = await _top_mart_similarity(preprocessed_q, probe_deps)
                if sim >= _MART_REDIRECT_THRESHOLD:
                    dropped = [s for s in plan.steps if s.action == "search_ckan"]
                    plan.steps = [s for s in plan.steps if s.action != "search_ckan"]
                    logger.info(
                        "BUG-001 Capa 1: dropped %d redundant search_ckan step(s) — "
                        "mart sim=%.3f >= %.2f",
                        len(dropped),
                        sim,
                        _MART_REDIRECT_THRESHOLD,
                    )
            except Exception:
                logger.debug("BUG-001 Capa 1 strip-ckan check failed", exc_info=True)

        return {"plan": plan}
    except Exception:
        logger.exception("inject_fallbacks_node failed")
        return {"plan": plan}


async def execute_steps_node(state: OpenArgState) -> dict:
    """Execute the data-retrieval steps defined in the plan.

    Steps are dispatched in parallel per dependency level through
    ``execute_steps()``, which handles retries and error isolation.
    Per-connector status events are streamed so the frontend can show
    which data source is being queried in real time.
    """
    writer = get_stream_writer()
    writer({"type": "status", "step": "searching", "detail": "Consultando fuentes de datos..."})
    deps = nodes_pkg.get_deps()

    plan = state.get("plan")
    if not plan:
        return {
            "data_results": [],
            "step_warnings": ["No execution plan available"],
        }

    try:
        question = state["question"]

        # H4 (round v46): propagate the caller's identity into the
        # connector deps so the sandbox dispatcher can pass it to the
        # few-shot scoping query. The smart_query_v2 controller writes
        # `user_id` from the JWT-verified email; anonymous paths fall
        # through to None and the few-shot helper consults the legacy
        # bucket only.
        caller_user_id = state.get("user_id") or state.get("owner_user_id")  # type: ignore[typeddict-item]

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
            llm=llm_for(deps, state),
            embedding=deps.embedding,
            semantic_cache=deps.semantic_cache,
            user_id=caller_user_id,
        )

        def _on_step_start(step: PlanStep) -> None:
            label = _CONNECTOR_LABELS.get(step.action, step.action)
            writer(
                {
                    "type": "status",
                    "step": "searching",
                    "detail": f"Consultando {label}...",
                    "connector": step.action,
                }
            )

        results, step_warnings = await execute_steps(
            plan,
            connector_deps,
            deps.metrics,
            nl_query=question,
            on_step_start=_on_step_start,
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
