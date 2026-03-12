from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime

from app.domain.entities.connectors.data_result import ExecutionPlan, PlanStep
from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage
from app.prompts import load_prompt

logger = logging.getLogger(__name__)

# Actions recognised by _dispatch_step in SmartQueryService
_VALID_ACTIONS: frozenset[str] = frozenset({
    "search_ckan",
    "query_series",
    "query_ddjj",
    "query_argentina_datos",
    "query_georef",
    "query_sesiones",
    "query_bcra",
    "query_staff",
    "query_sandbox",
    "search_datasets",
    # Passthrough / meta-actions (return [] but are valid in a plan)
    "analyze",
    "compare",
    "synthesize",
})


def _validate_plan(plan_data: dict) -> dict:
    """Validate and sanitise the LLM-generated execution plan.

    * Removes steps whose ``action`` is not a known connector/meta-action.
    * Ensures every step has a string ``id``, a dict ``params`` and a list
      ``dependsOn`` that only references previously-declared step IDs.
    * Logs a warning for each step that is dropped or patched.
    """
    raw_steps = plan_data.get("steps", [])
    if not isinstance(raw_steps, list):
        logger.warning("Plan validation: 'steps' is not a list (%s), resetting", type(raw_steps).__name__)
        plan_data["steps"] = []
        return plan_data

    valid_steps: list[dict] = []
    valid_ids: set[str] = set()

    for idx, step in enumerate(raw_steps):
        if not isinstance(step, dict):
            logger.warning("Plan validation: step %d is not a dict, skipping", idx)
            continue

        # ── id ──
        step_id = step.get("id")
        if not step_id or not isinstance(step_id, str):
            step_id = f"step_{idx + 1}"
            step["id"] = step_id

        # ── action ──
        action = step.get("action", "")
        if action not in _VALID_ACTIONS:
            logger.warning(
                "Plan validation: step '%s' has invalid action '%s', skipping",
                step_id, action,
            )
            continue

        # ── params ──
        if not isinstance(step.get("params"), dict):
            step["params"] = {}

        # ── dependsOn (camelCase key used by the LLM) ──
        depends_key = "dependsOn" if "dependsOn" in step else "depends_on"
        depends = step.get(depends_key, [])
        if not isinstance(depends, list):
            depends = []
        cleaned_depends = [d for d in depends if isinstance(d, str) and d in valid_ids]
        if len(cleaned_depends) != len(depends):
            logger.warning(
                "Plan validation: step '%s' had invalid depends_on refs removed "
                "(before=%s, after=%s)",
                step_id, depends, cleaned_depends,
            )
        step[depends_key] = cleaned_depends

        valid_ids.add(step_id)
        valid_steps.append(step)

    if len(valid_steps) != len(raw_steps):
        logger.info(
            "Plan validation: kept %d / %d steps", len(valid_steps), len(raw_steps),
        )

    # ── Cycle detection (DFS) ──
    # Build adjacency from depends_on and detect circular dependencies.
    adj: dict[str, list[str]] = {}
    step_by_id: dict[str, dict] = {}
    for step in valid_steps:
        sid = step["id"]
        dep_key = "dependsOn" if "dependsOn" in step else "depends_on"
        adj[sid] = step.get(dep_key, [])
        step_by_id[sid] = step

    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {sid: WHITE for sid in adj}

    def _dfs_cycle(node: str) -> bool:
        """Return True if a cycle was found and broken."""
        color[node] = GRAY
        found_cycle = False
        dep_key = "dependsOn" if "dependsOn" in step_by_id[node] else "depends_on"
        for neighbour in list(adj[node]):
            if neighbour not in color:
                continue
            if color[neighbour] == GRAY:
                # Cycle detected: remove the back-edge by clearing depends_on
                logger.warning(
                    "Plan validation: circular dependency detected — "
                    "step '%s' depends on '%s' which is still in progress. "
                    "Removing depends_on for '%s'.",
                    node, neighbour, node,
                )
                step_by_id[node][dep_key] = []
                adj[node] = []
                found_cycle = True
                break
            if color[neighbour] == WHITE:
                if _dfs_cycle(neighbour):
                    found_cycle = True
        color[node] = BLACK
        return found_cycle

    for step_id in adj:
        if color[step_id] == WHITE:
            _dfs_cycle(step_id)

    plan_data["steps"] = valid_steps
    return plan_data


async def _classify_ambiguity(
    llm: ILLMProvider, question: str,
) -> dict | None:
    """Use a lightweight LLM call to classify query ambiguity.

    Returns a clarification dict if the query is too vague, None otherwise.
    """
    messages = [
        LLMMessage(
            role="system",
            content=load_prompt("ambiguity_classifier"),
        ),
        LLMMessage(
            role="user",
            content=f'Consulta: "{question}"',
        ),
    ]
    try:
        response = await llm.chat(
            messages, temperature=0.0, max_tokens=256,
        )
        text = response.content.strip()
        # Strip markdown fences if present
        md = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
        if md:
            text = md.group(1).strip()
        result = json.loads(text)
        if result.get("type") == "clarification":
            return result
    except Exception:
        logger.debug(
            "Ambiguity classifier failed, proceeding to planner",
            exc_info=True,
        )
    return None


def _resolve_routing_hints(question: str) -> str:
    """Resolve routing hints from the dataset index. Returns formatted text or empty string."""
    try:
        from app.infrastructure.adapters.connectors.dataset_index import (
            format_hints_for_prompt,
            resolve_hints,
        )

        hints = resolve_hints(question)
        if hints:
            return format_hints_for_prompt(hints)
    except Exception:
        logger.debug("Dataset index hints unavailable, continuing without hints", exc_info=True)
    return ""


async def generate_plan(
    llm: ILLMProvider, question: str, memory_context: str = ""
) -> ExecutionPlan:
    """Generate an execution plan from a user query using the LLM."""
    # Classify ambiguity with a lightweight LLM call (skip if history)
    has_history = bool(memory_context and memory_context.strip())
    if not has_history:
        clar = await _classify_ambiguity(llm, question)
        if clar:
            logger.info(
                "Ambiguity classifier triggered clarification: %s",
                question,
            )
            return ExecutionPlan(
                query=question,
                intent="clarification",
                steps=[
                    PlanStep(
                        id="clarification",
                        action="clarification",
                        description=clar.get(
                            "question",
                            "¿Podés ser más específico?",
                        ),
                        params={
                            "question": clar.get(
                                "question",
                                "¿Podés ser más específico?",
                            ),
                            "options": clar.get("options", []),
                        },
                    )
                ],
            )

    today = datetime.now(UTC).strftime("%Y-%m-%d")

    # Resolve routing hints from the semantic dataset index
    hints_text = _resolve_routing_hints(question)

    user_content = f'FECHA ACTUAL: {today}\n\nPregunta del usuario: "{question}"'
    if memory_context:
        user_content += f"\n\n{memory_context}"
    if hints_text:
        user_content += f"\n\n{hints_text}"
    user_content += (
        "\n\nSi la pregunta del usuario hace referencia a algo mencionado antes "
        "en el historial (ej: \"y eso?\", \"compará\", \"lo mismo pero...\"), "
        "usá el contexto del historial para entender a qué se refiere."
        "\n\nGenerá el plan de ejecución en JSON."
    )

    messages = [
        LLMMessage(role="system", content=load_prompt("planner")),
        LLMMessage(role="user", content=user_content),
    ]

    try:
        response = await llm.chat(messages, temperature=0.3, max_tokens=4096)
        text = response.content.strip()

        # Try to extract JSON from the response (handle markdown code blocks)
        json_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
        if json_match:
            text = json_match.group(1).strip()

        plan_data = json.loads(text)

        # Handle clarification response from planner
        if plan_data.get("type") == "clarification":
            return ExecutionPlan(
                query=question,
                intent="clarification",
                steps=[
                    PlanStep(
                        id="clarification",
                        action="clarification",
                        description=plan_data.get("question", "¿Podés ser más específico?"),
                        params={
                            "question": plan_data.get("question", "¿Podés ser más específico?"),
                            "options": plan_data.get("options", []),
                        },
                    )
                ],
            )

        plan_data = _validate_plan(plan_data)

        steps = []
        for i, s in enumerate(plan_data.get("steps", [])):
            steps.append(
                PlanStep(
                    id=s.get("id", f"step_{i + 1}"),
                    action=s.get("action", "search_ckan"),
                    description=s.get("description", ""),
                    params=s.get("params", {}),
                    depends_on=s.get("dependsOn") or s.get("depends_on", []),
                )
            )

        return ExecutionPlan(
            query=plan_data.get("query", question),
            intent=plan_data.get("intent", ""),
            steps=steps,
            suggested_visualizations=plan_data.get("suggestedVisualizations", []),
        )
    except Exception:
        logger.warning("LLM planner failed, using regex fallback", exc_info=True)
        return _fallback_plan(question)


def _fallback_plan(question: str) -> ExecutionPlan:
    """Regex-based classification fallback when LLM fails."""
    lower = question.lower()

    is_national = bool(re.search(r"\b(nacional|nivel nacional|datos\.gob|cat[aá]logo|datasets?\s+(hay|tiene|disponibles))\b", lower, re.IGNORECASE))
    is_list_all = bool(re.search(r"\b(todos|listado|cat[aá]logo|cu[aá]ntos|qu[eé]\s+(hay|tiene|datos))\b", lower, re.IGNORECASE))

    # Detect economic queries for series de tiempo fallback
    is_economic = bool(re.search(
        r"(inflaci[oó]n|ipc|d[oó]lar|tipo\s+de\s+cambio|pbi|emae|reservas|tasa|salario|desempleo"
        r"|exportaci|importaci|canasta|presupuesto|base\s+monetaria|leliq)",
        lower, re.IGNORECASE,
    ))

    # Detect DDJJ queries
    is_ddjj = bool(re.search(r"(ddjj|declaraci[oó]n\s+jurada|patrimonio\s+de\s+diputado)", lower, re.IGNORECASE))

    if is_economic:
        return ExecutionPlan(
            query=question,
            intent="Consulta económica (fallback)",
            steps=[
                PlanStep(
                    id="step_1",
                    action="query_series",
                    description=f"Buscar series de tiempo: {question}",
                    params={"query": question},
                ),
                PlanStep(
                    id="step_2",
                    action="analyze",
                    description="Analizar indicadores económicos",
                    params={"focus": question},
                    depends_on=["step_1"],
                ),
            ],
            suggested_visualizations=["line_chart"],
        )

    if is_ddjj:
        return ExecutionPlan(
            query=question,
            intent="Consulta DDJJ (fallback)",
            steps=[
                PlanStep(
                    id="step_1",
                    action="query_ddjj",
                    description=f"Buscar DDJJ: {question}",
                    params={"action": "search", "query": question},
                ),
            ],
            suggested_visualizations=["table"],
        )

    return ExecutionPlan(
        query=question,
        intent="Explorar catálogo nacional de datos abiertos" if is_national else "Búsqueda general de datos",
        steps=[
            PlanStep(
                id="step_1",
                action="search_ckan",
                description="Listar datasets del portal nacional" if is_national else f"Buscar datasets: {question}",
                params={
                    "query": "*" if is_list_all else question,
                    **({"portalId": "nacional"} if is_national else {}),
                    "rows": 20 if is_list_all else 10,
                },
            ),
            PlanStep(
                id="step_2",
                action="analyze",
                description="Analizar los resultados encontrados",
                params={"focus": question},
                depends_on=["step_1"],
            ),
        ],
        suggested_visualizations=["table"],
    )
