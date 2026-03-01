"""Query preprocessor — uses LLM to reformulate user queries for better search relevance."""
from __future__ import annotations

import logging

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "Reformulá esta consulta para maximizar la relevancia de búsqueda en datos abiertos argentinos. "
    "Solo retorná la consulta reformulada, sin explicaciones."
)


class QueryPreprocessor:
    def __init__(self, llm: ILLMProvider) -> None:
        self._llm = llm

    async def reformulate(self, query: str) -> str:
        """Reformulate a user query for better search relevance.

        Skips short queries (3 words or fewer) since they're already concise.
        Falls back to the original query on any error.
        """
        if len(query.split()) <= 3:
            return query

        try:
            response = await self._llm.chat(
                messages=[
                    LLMMessage(role="system", content=_SYSTEM_PROMPT),
                    LLMMessage(role="user", content=query),
                ],
                temperature=0.3,
                max_tokens=256,
            )
            reformulated = response.content.strip()
            return reformulated or query
        except Exception:
            logger.debug("Query reformulation failed, using original query")
            return query
