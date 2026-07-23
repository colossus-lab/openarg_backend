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
    owner_user_id: str | None = None,
) -> str:
    """Load recent messages from the DB to build conversation context.

    Only called when there is a conversation_id and a chat_repo.
    Returns a formatted string or empty if no history.

    H3: when `owner_user_id` is provided, the repo lookup is scoped to
    that owner. A foreign conversation_id returns []. The endpoint layer
    is the primary ownership gate; this is defense-in-depth so a future
    bypass at the controller layer can't read another user's history.
    """
    if not conversation_id or not chat_repo:
        return ""
    try:
        from uuid import UUID

        owner_uuid = UUID(owner_user_id) if owner_user_id else None
        messages = await chat_repo.get_messages(
            UUID(conversation_id), limit=7, user_id=owner_uuid
        )
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


async def save_query_attempt(
    *,
    question: str,
    served_table: str | None,
    row_count: int,
    success: bool,
    duration_ms: int | None,
    error_message: str | None,
    semantic_cache: SemanticCache,
) -> None:
    """Persist EVERY query attempt for analytics — successful or not.

    Separate from `save_successful_query` (which feeds few-shot) so that
    failed/empty queries can be tracked without polluting the few-shot
    example set. The table is created lazily on first call so this drops
    into a running staging without an alembic migration.

    The ``embedding`` column is intentionally left NULL: BUG-016/017
    showed that computing it (a Bedrock round-trip) inside a
    fire-and-forget background task raced request teardown and silently
    lost ~2/3 of writes. This is now awaited inline by the terminal
    nodes, so it must stay cheap — a bare INSERT, no embedding call.

    P1 (round v4.2): ~11% of rows missed query_analytics (R002/R014/C5
    in a 27-call batch). Symptom: the terminal node ran and emitted
    ``complete`` but no row landed — a transient pool/lock failure on
    the first INSERT, swallowed silently. Retry once after a brief
    backoff before giving up, and keep the DDL setup outside the retry
    loop (it's idempotent and a CREATE TABLE failure isn't worth
    retrying).
    """
    import asyncio as _asyncio

    # Lazy table create — idempotent. Errors here don't block the INSERT
    # below from being attempted (the table almost certainly already
    # exists in long-running deployments).
    try:
        async with semantic_cache._session_factory() as session:
            await session.execute(text(
                """
                CREATE TABLE IF NOT EXISTS query_analytics (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    question TEXT NOT NULL,
                    served_table TEXT,
                    mart_used BOOLEAN NOT NULL DEFAULT FALSE,
                    row_count INTEGER,
                    success BOOLEAN NOT NULL,
                    duration_ms INTEGER,
                    error_message TEXT,
                    embedding vector(1024)
                )
                """
            ))
            await session.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_query_analytics_ts ON query_analytics(ts DESC)"
            ))
            await session.commit()
    except Exception:
        logger.debug("query_analytics DDL setup failed", exc_info=True)

    served = (served_table or "").strip()
    mart_used = served.startswith("mart.")
    params = {
        "q": question[:500],
        "t": served[:200] if served else None,
        "mu": mart_used,
        "r": row_count if row_count is not None else None,
        "ok": success,
        "d": duration_ms,
        "err": (error_message or "")[:500] if error_message else None,
    }

    last_err: Exception | None = None
    for attempt in range(2):
        try:
            async with semantic_cache._session_factory() as session:
                await session.execute(
                    text(
                        "INSERT INTO query_analytics "
                        "(question, served_table, mart_used, row_count, success, "
                        " duration_ms, error_message) "
                        "VALUES (:q, :t, :mu, :r, :ok, :d, :err)"
                    ),
                    params,
                )
                await session.commit()
            return
        except Exception as exc:
            last_err = exc
            if attempt == 0:
                # Transient (pool exhaustion / lock contention) — brief
                # backoff then retry once.
                await _asyncio.sleep(0.5)
    logger.warning(
        "Failed to save query_analytics after 2 attempts: %s",
        last_err,
        exc_info=last_err,
    )


async def record_terminal_analytics(
    *,
    question: str,
    served_table: str | None,
    row_count: int,
    success: bool,
    duration_ms: int | None,
    error_message: str | None,
    semantic_cache: SemanticCache | None,
) -> None:
    """Write a ``query_analytics`` row from a terminal pipeline node.

    BUG-016/017: previously only NL2SQL/sandbox queries were logged (via
    ``save_success_node``). Cache hits, fast replies, clarification replies
    and connector/mart flows never reached ``query_analytics`` — roughly
    two thirds of calls went unrecorded. Each of the four terminal nodes
    now awaits this so every query is logged exactly once.

    Awaited inline (not fire-and-forget): the write is a single cheap
    INSERT (~20ms) so the latency cost is negligible, and awaiting
    guarantees it completes before the request scope tears down.
    """
    if semantic_cache is None:
        logger.warning("record_terminal_analytics: semantic_cache is None — skipping")
        return
    await save_query_attempt(
        question=question,
        served_table=served_table,
        row_count=row_count,
        success=success,
        duration_ms=duration_ms,
        error_message=error_message,
        semantic_cache=semantic_cache,
    )


# H4 (round v46): the 'legacy' sentinel represents historical rows that
# entered the table before per-user scoping landed. They are surfaced to
# every caller as if operator-curated; new rows always carry the actual
# caller email. A user can NEVER persist as 'legacy' because the controller
# layer (smart_query_v2_router) rejects body.user_email='legacy' shapes via
# the H3 spoof check — JWT-derived emails are real Google addresses.
_LEGACY_OWNER = "legacy"


async def save_successful_query(
    question: str,
    sql: str,
    table_name: str,
    row_count: int,
    embedding_provider: IEmbeddingProvider,
    semantic_cache: SemanticCache,
    *,
    user_id: str | None = None,
) -> None:
    """Save a successful NL2SQL query for future few-shot examples.

    H4 (round v46): scoped per user. The row is also dropped silently when
    the question trips the prompt-injection scorer — keeping poisoned
    inputs out of the few-shot pool is cheaper than trying to neutralize
    them at retrieval time.
    """
    # Lazy import to keep this module free of infrastructure deps unless
    # the few-shot path actually fires.
    from app.infrastructure.adapters.search.prompt_injection_detector import (
        is_suspicious,
    )

    suspicious, score = is_suspicious(question)
    if suspicious or score > 0.4:
        logger.info(
            "Skipping successful_queries save (suspicious score=%.2f)", score
        )
        return

    owner = (user_id or "").strip().lower() or _LEGACY_OWNER
    try:
        embedding = await embedding_provider.embed(question)
        emb_str = "[" + ",".join(str(v) for v in embedding) + "]"
        async with semantic_cache._session_factory() as session:
            await session.execute(
                text(
                    "INSERT INTO successful_queries "
                    "(question, sql, table_name, row_count, embedding, user_id) "
                    "VALUES (:q, :s, :t, :r, CAST(:e AS vector), :uid)"
                ),
                {
                    "q": question[:500],
                    "s": sql[:2000],
                    "t": table_name,
                    "r": row_count,
                    "e": emb_str,
                    "uid": owner,
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
    *,
    user_id: str | None = None,
) -> str:
    """Retrieve similar successful queries as few-shot examples for NL2SQL.

    H4 (round v46): only rows owned by the caller (or by `_LEGACY_OWNER`,
    the operator-curated historical bucket) are surfaced. Cross-tenant
    rows are filtered at the SQL layer so a malicious neighbor's poison
    can't make it into the planner's prompt.
    """
    owner = (user_id or "").strip().lower() or _LEGACY_OWNER
    try:
        embedding = await embedding_provider.embed(question)
        emb_str = "[" + ",".join(str(v) for v in embedding) + "]"
        async with semantic_cache._session_factory() as session:
            result = await session.execute(
                text(
                    "SELECT question, sql, "
                    "1 - (embedding <=> CAST(:emb AS vector)) AS score "
                    "FROM successful_queries "
                    "WHERE (user_id = :uid OR user_id = :legacy) "
                    "  AND 1 - (embedding <=> CAST(:emb AS vector)) > 0.6 "
                    "ORDER BY embedding <=> CAST(:emb AS vector) "
                    "LIMIT :lim"
                ),
                {
                    "emb": emb_str,
                    "lim": limit,
                    "uid": owner,
                    "legacy": _LEGACY_OWNER,
                },
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
