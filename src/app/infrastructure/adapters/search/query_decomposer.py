"""Query decomposer — splits complex multi-part questions into sub-queries."""
from __future__ import annotations

import json
import logging
import re

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage

logger = logging.getLogger(__name__)

_DECOMPOSE_SYSTEM_PROMPT = """Analizá si esta pregunta necesita descomponerse en sub-preguntas independientes para buscar datos por separado.

Reglas:
- Si la pregunta tiene UN solo tema → retorná ["pregunta original"]
- Si la pregunta compara DOS o más temas independientes → descomponé en 2-3 sub-preguntas
- Máximo 3 sub-preguntas
- Cada sub-pregunta debe ser autocontenida (no referenciar a las otras)

Respondé SOLO con un JSON array de strings. Ejemplos:
- Input: "inflación del último mes" → ["inflación del último mes"]
- Input: "comparar inflación con dólar blue en 2025" → ["inflación en 2025", "dólar blue en 2025"]
- Input: "PBI, exportaciones e importaciones del último año" → ["PBI del último año", "exportaciones del último año", "importaciones del último año"]"""


class QueryDecomposer:
    def __init__(self, llm: ILLMProvider) -> None:
        self._llm = llm

    async def decompose(self, question: str) -> list[str]:
        """Decompose a complex question into 1-3 sub-queries.

        Skips decomposition for short queries (<=5 words).
        Falls back to [question] on any error.
        """
        if len(question.split()) <= 5:
            return [question]

        try:
            response = await self._llm.chat(
                messages=[
                    LLMMessage(role="system", content=_DECOMPOSE_SYSTEM_PROMPT),
                    LLMMessage(role="user", content=question),
                ],
                temperature=0.1,
                max_tokens=512,
            )

            text = response.content.strip()
            # Handle markdown code blocks
            if "```" in text:
                match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
                if match:
                    text = match.group(1).strip()

            sub_queries = json.loads(text)
            if not isinstance(sub_queries, list) or not sub_queries:
                return [question]

            # Cap at 3 sub-queries and ensure they're strings
            return [str(q) for q in sub_queries[:3]]
        except Exception:
            logger.debug("Query decomposition failed, using original query")
            return [question]
