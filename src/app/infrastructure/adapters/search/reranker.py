"""LLM-based re-ranker for search results."""
from __future__ import annotations

import json
import logging

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage
from app.domain.ports.search.vector_search import SearchResult

logger = logging.getLogger(__name__)

_RERANK_PROMPT = """Reordená estos resultados de búsqueda según su relevancia para la pregunta del usuario.
Retorná SOLO un JSON array con los índices (0-based) en orden de mayor a menor relevancia.

Ejemplo: [2, 0, 1, 3] significa que el resultado 2 es el más relevante.

Pregunta: {question}

Resultados:
{results_text}

Array de índices ordenados:"""


class LLMReranker:
    def __init__(self, llm: ILLMProvider) -> None:
        self._llm = llm

    async def rerank(
        self, question: str, results: list[SearchResult], top_k: int = 5,
    ) -> list[SearchResult]:
        """Re-rank search results using LLM. Falls back to original order on failure."""
        if len(results) <= 1:
            return results

        try:
            results_text = "\n".join(
                f"[{i}] {r.title}: {r.description[:150]}"
                for i, r in enumerate(results)
            )

            prompt = _RERANK_PROMPT.format(
                question=question, results_text=results_text,
            )

            response = await self._llm.chat(
                messages=[LLMMessage(role="user", content=prompt)],
                temperature=0.0,
                max_tokens=128,
            )

            text = response.content.strip()
            indices = json.loads(text)

            if not isinstance(indices, list):
                return results[:top_k]

            reranked: list[SearchResult] = []
            seen: set[int] = set()
            for idx in indices:
                idx = int(idx)
                if 0 <= idx < len(results) and idx not in seen:
                    reranked.append(results[idx])
                    seen.add(idx)

            # Append any results not in the reranked list
            for i, r in enumerate(results):
                if i not in seen:
                    reranked.append(r)

            return reranked[:top_k]
        except Exception:
            logger.debug("LLM reranking failed, using original order", exc_info=True)
            return results[:top_k]
