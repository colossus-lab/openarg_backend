"""Chat history persistence and few-shot NL2SQL example retrieval."""

from __future__ import annotations

import contextlib
import json
import logging
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.domain.ports.chat.chat_repository import IChatRepository
    from app.domain.ports.llm.llm_provider import IEmbeddingProvider
    from app.infrastructure.adapters.cache.semantic_cache import SemanticCache

logger = logging.getLogger(__name__)


async def load_chat_history(
    conversation_id: str,
    chat_repo: IChatRepository | None,
) -> str:
    """Load recent messages from the DB to build conversation context.

    Only called when there is a conversation_id and a chat_repo.
    Returns a formatted string or empty if no history.
    """
    if not conversation_id or not chat_repo:
        return ""
    try:
        from uuid import UUID

        messages = await chat_repo.get_messages(UUID(conversation_id), limit=7)
        if len(messages) <= 1:
            return ""
        # Skip the last message (it's the current question the frontend just saved)
        recent = messages[:-1][-6:]
        if not recent:
            return ""
        parts = ["\nHISTORIAL DE CONVERSACIÓN:"]
        for m in recent:
            label = "Usuario" if m.role == "user" else "Asistente"
            content = m.content[:300].replace("\n", " ")
            parts.append(f"  - {label}: {content}")
        parts.append(
            "INSTRUCCIÓN: Si la pregunta actual es ambigua o le falta sujeto "
            "(ej: 'a qué partido pertenece', 'cuánto gana', 'dónde queda'), "
            "resolvé la referencia usando el historial. "
            "Si la pregunta es autocontenida, ignorá el historial.\n"
        )
        return "\n".join(parts)
    except Exception:
        logger.debug("Failed to load chat history for %s", conversation_id, exc_info=True)
        return ""


async def save_history(
    session: AsyncSession,
    question: str,
    user_id: str,
    answer: str,
    sources: list[dict[str, Any]],
    tokens_used: int,
    duration_ms: int,
    plan_json: str = "",
) -> None:
    """Persist a query and its answer to the user_queries table."""
    try:
        query_id = str(uuid4())
        await session.execute(
            text(
                "INSERT INTO user_queries "
                "(id, question, user_id, status, "
                "analysis_result, sources_json, "
                "tokens_used, duration_ms, plan_json) "
                "VALUES (CAST(:id AS uuid), :question, "
                ":user_id, 'completed', :result, "
                ":sources, :tokens, :duration_ms, :plan)"
            ),
            {
                "id": query_id,
                "question": question,
                "user_id": user_id,
                "result": answer,
                "sources": json.dumps(sources, ensure_ascii=False),
                "tokens": tokens_used,
                "duration_ms": duration_ms,
                "plan": plan_json,
            },
        )
        await session.commit()
    except Exception:
        logger.warning(
            "Failed to save conversation history",
            exc_info=True,
        )
        with contextlib.suppress(Exception):
            await session.rollback()


async def save_successful_query(
    question: str,
    sql: str,
    table_name: str,
    row_count: int,
    embedding_provider: IEmbeddingProvider,
    semantic_cache: SemanticCache,
) -> None:
    """Save a successful NL2SQL query for future few-shot examples."""
    try:
        embedding = await embedding_provider.embed(question)
        emb_str = "[" + ",".join(str(v) for v in embedding) + "]"
        async with semantic_cache._session_factory() as session:
            await session.execute(
                text(
                    "INSERT INTO successful_queries (question, sql, table_name, row_count, embedding) "
                    "VALUES (:q, :s, :t, :r, CAST(:e AS vector))"
                ),
                {
                    "q": question[:500],
                    "s": sql[:2000],
                    "t": table_name,
                    "r": row_count,
                    "e": emb_str,
                },
            )
            await session.commit()
    except Exception:
        logger.debug("Failed to save successful query for few-shot", exc_info=True)


async def get_few_shot_examples(
    question: str,
    embedding_provider: IEmbeddingProvider,
    semantic_cache: SemanticCache,
    limit: int = 3,
) -> str:
    """Retrieve similar successful queries as few-shot examples for NL2SQL."""
    try:
        embedding = await embedding_provider.embed(question)
        emb_str = "[" + ",".join(str(v) for v in embedding) + "]"
        async with semantic_cache._session_factory() as session:
            result = await session.execute(
                text(
                    "SELECT question, sql, "
                    "1 - (embedding <=> CAST(:emb AS vector)) AS score "
                    "FROM successful_queries "
                    "WHERE 1 - (embedding <=> CAST(:emb AS vector)) > 0.6 "
                    "ORDER BY embedding <=> CAST(:emb AS vector) "
                    "LIMIT :lim"
                ),
                {"emb": emb_str, "lim": limit},
            )
            rows = result.fetchall()
        if not rows:
            return ""
        lines = ["Successful similar queries (use as reference):"]
        for r in rows:
            lines.append(f"\nQuestion: {r.question}\nSQL: {r.sql}")
        return "\n".join(lines)
    except Exception:
        logger.debug("Failed to retrieve few-shot examples", exc_info=True)
        return ""
