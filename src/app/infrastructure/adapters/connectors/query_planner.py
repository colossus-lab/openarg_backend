from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime

from app.domain.entities.connectors.data_result import ExecutionPlan, PlanStep
from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage
from app.prompts import load_prompt

logger = logging.getLogger(__name__)


async def generate_plan(
    llm: ILLMProvider, question: str, memory_context: str = ""
) -> ExecutionPlan:
    """Generate an execution plan from a user query using the LLM."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    user_content = f'FECHA ACTUAL: {today}\n\nPregunta del usuario: "{question}"'
    if memory_context:
        user_content += f"\n\n{memory_context}"
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

        steps = []
        for i, s in enumerate(plan_data.get("steps", [])):
            steps.append(
                PlanStep(
                    id=s.get("id", f"step_{i + 1}"),
                    action=s.get("action", "search_ckan"),
                    description=s.get("description", ""),
                    params=s.get("params", {}),
                    depends_on=s.get("dependsOn", []),
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
