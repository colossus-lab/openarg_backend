from __future__ import annotations

import logging

from app.domain.entities.connectors.data_result import DataResult, ExecutionPlan
from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage
from app.prompts import load_prompt

logger = logging.getLogger(__name__)


async def analyze_policy(
    llm: ILLMProvider,
    plan: ExecutionPlan,
    results: list[DataResult],
    analysis_text: str,
    memory_ctx: str,
) -> str:
    """Generate a public policy evaluation based on collected data and preliminary analysis."""
    try:
        from datetime import UTC, datetime

        today = datetime.now(UTC).strftime("%Y-%m-%d")

        data_summary = "\n".join(
            f"- {r.dataset_title} ({r.portal_name}): {len(r.records)} registros"
            for r in results if r.records
        ) or "Sin datos tabulares recolectados"

        user_content = (
            f"FECHA ACTUAL: {today}\n"
            f'CONSULTA DEL USUARIO: "{plan.query}"\n'
            f"INTENCIÓN: {plan.intent}\n\n"
            f"DATOS DISPONIBLES:\n{data_summary}\n\n"
            f"ANÁLISIS PRELIMINAR (ya se le mostró al usuario):\n{analysis_text[:1500]}\n"
        )
        if memory_ctx:
            user_content += f"\nCONTEXTO CONVERSACIONAL:\n{memory_ctx}\n"

        user_content += (
            "\nGenerá el análisis de política pública. Recordá: sé conciso, "
            "basate en los datos, y agregá perspectiva nueva que el análisis "
            "preliminar no cubrió."
        )

        response = await llm.chat(
            messages=[
                LLMMessage(role="system", content=load_prompt("policy")),
                LLMMessage(role="user", content=user_content),
            ],
            temperature=0.3,
            max_tokens=1024,
        )

        return response.content.strip()
    except Exception:
        logger.warning("Policy agent failed, returning fallback", exc_info=True)
        return (
            "## Evaluación de Política Pública\n\n"
            "No fue posible generar la evaluación de política pública en este momento. "
            "Intentá de nuevo o reformulá la consulta."
        )
