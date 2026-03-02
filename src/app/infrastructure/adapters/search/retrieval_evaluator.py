"""CRAG-style retrieval evaluator using LLM to assess retrieval quality."""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage
from app.domain.ports.search.retrieval_evaluator import (
    IRetrievalEvaluator,
    RetrievalQuality,
    RetrievalVerdict,
)

logger = logging.getLogger(__name__)

_EVAL_SYSTEM_PROMPT = """Sos un evaluador de calidad de recuperación de datos. Analizá si los resultados recuperados son relevantes para responder la pregunta del usuario.

Clasificá la calidad como:
- "correct": Los resultados contienen datos directamente relevantes para responder la pregunta.
- "ambiguous": Los resultados son parcialmente relevantes o podrían responder la pregunta de forma incompleta.
- "incorrect": Los resultados no son relevantes para la pregunta o están vacíos.

Respondé SOLO con JSON válido:
{
  "quality": "correct" | "ambiguous" | "incorrect",
  "confidence": 0.0-1.0,
  "reasoning": "breve explicación",
  "suggested_actions": ["acción1", "acción2"]
}

Acciones sugeridas posibles:
- "retry_with_broader_query": Reintentar con una consulta más amplia
- "try_alternative_connector": Probar otro conector de datos
- "fallback_to_hybrid_search": Usar búsqueda híbrida como complemento
- "add_temporal_context": Agregar contexto temporal a la búsqueda
- "none": No se necesita acción correctiva"""


class RetrievalEvaluator(IRetrievalEvaluator):
    def __init__(self, llm: ILLMProvider) -> None:
        self._llm = llm

    async def evaluate(
        self,
        question: str,
        results: list[Any],
        plan: Any,
    ) -> RetrievalVerdict:
        """Evaluate retrieval quality using LLM as judge."""
        try:
            results_summary = self._summarize_results(results)
            intent = plan.intent if hasattr(plan, "intent") else str(plan)

            user_prompt = (
                f"PREGUNTA: {question}\n"
                f"INTENCIÓN: {intent}\n"
                f"RESULTADOS RECUPERADOS:\n{results_summary}"
            )

            response = await self._llm.chat(
                messages=[
                    LLMMessage(role="system", content=_EVAL_SYSTEM_PROMPT),
                    LLMMessage(role="user", content=user_prompt),
                ],
                temperature=0.0,
                max_tokens=512,
            )

            return self._parse_verdict(response.content)
        except Exception:
            logger.debug("Retrieval evaluator failed, defaulting to CORRECT", exc_info=True)
            return RetrievalVerdict(
                quality=RetrievalQuality.CORRECT,
                confidence=0.5,
                reasoning="Evaluation failed, defaulting to correct",
            )

    @staticmethod
    def _summarize_results(results: list[Any]) -> str:
        """Summarize retrieval results into concise text for the LLM prompt."""
        if not results:
            return "Sin resultados (lista vacía)."

        parts: list[str] = []
        for i, r in enumerate(results[:5]):
            if isinstance(r, DataResult):
                record_count = len(r.records) if r.records else 0
                part = (
                    f"  {i + 1}. {r.dataset_title} (fuente: {r.source}, "
                    f"registros: {record_count})"
                )
                if r.records and record_count > 0:
                    first = r.records[0]
                    if isinstance(first, dict):
                        keys = list(first.keys())[:8]
                        part += f"\n     Columnas: {', '.join(keys)}"
                        # Include sample values for better evaluation
                        sample_vals = {k: first[k] for k in keys[:4] if first.get(k) is not None}
                        if sample_vals:
                            part += f"\n     Ejemplo: {sample_vals}"
                parts.append(part)
            else:
                parts.append(f"  {i + 1}. {str(r)[:200]}")

        return "\n".join(parts)

    @staticmethod
    def _parse_verdict(content: str) -> RetrievalVerdict:
        """Parse LLM response into a RetrievalVerdict."""
        try:
            # Handle markdown code blocks
            text = content.strip()
            if "```" in text:
                match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
                if match:
                    text = match.group(1).strip()

            data = json.loads(text)
            quality_str = data.get("quality", "correct").lower()
            quality_map = {
                "correct": RetrievalQuality.CORRECT,
                "ambiguous": RetrievalQuality.AMBIGUOUS,
                "incorrect": RetrievalQuality.INCORRECT,
            }
            quality = quality_map.get(quality_str, RetrievalQuality.CORRECT)
            confidence = max(0.0, min(1.0, float(data.get("confidence", 0.5))))
            reasoning = data.get("reasoning", "")
            actions = data.get("suggested_actions", [])
            if isinstance(actions, str):
                actions = [actions]

            return RetrievalVerdict(
                quality=quality,
                confidence=confidence,
                reasoning=reasoning,
                suggested_actions=actions,
            )
        except (json.JSONDecodeError, KeyError, ValueError):
            return RetrievalVerdict(
                quality=RetrievalQuality.CORRECT,
                confidence=0.5,
                reasoning="Failed to parse evaluation response",
            )
