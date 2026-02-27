from __future__ import annotations

import json

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from app.infrastructure.persistence_sqla.provider import MainAsyncSession

router = APIRouter(prefix="/conversations", tags=["conversations"])


class ConversationSummary(BaseModel):
    id: str
    question: str
    status: str
    created_at: str
    preview: str | None


class ConversationDetail(BaseModel):
    id: str
    question: str
    status: str
    analysis_result: str | None
    sources: list[dict] | None
    tokens_used: int
    duration_ms: int
    created_at: str


@router.get("/", response_model=list[ConversationSummary])
@inject
async def list_conversations(
    session: FromDishka[MainAsyncSession],
    user_id: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[ConversationSummary]:
    """List past conversations for a user, ordered by created_at DESC."""
    query = """
        SELECT CAST(id AS text) AS id, question, status,
               LEFT(analysis_result, 100) AS preview,
               CAST(created_at AS text) AS created_at
        FROM user_queries
    """
    params: dict = {"limit": limit, "offset": offset}

    if user_id:
        query += " WHERE user_id = :user_id"
        params["user_id"] = user_id

    query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"

    result = await session.execute(text(query), params)
    rows = result.fetchall()

    return [
        ConversationSummary(
            id=row.id,
            question=row.question,
            status=row.status,
            created_at=row.created_at,
            preview=row.preview,
        )
        for row in rows
    ]


@router.get("/{conversation_id}", response_model=ConversationDetail)
@inject
async def get_conversation(
    conversation_id: str,
    session: FromDishka[MainAsyncSession],
) -> ConversationDetail:
    """Get full conversation detail with analysis_result, sources, tokens_used."""
    result = await session.execute(
        text("""
            SELECT CAST(id AS text) AS id, question, status, analysis_result,
                   sources_json, tokens_used, duration_ms,
                   CAST(created_at AS text) AS created_at
            FROM user_queries
            WHERE id = CAST(:id AS uuid)
        """),
        {"id": conversation_id},
    )
    row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Conversation not found")

    sources = None
    if row.sources_json:
        try:
            sources = json.loads(row.sources_json)
        except json.JSONDecodeError:
            pass

    return ConversationDetail(
        id=row.id,
        question=row.question,
        status=row.status,
        analysis_result=row.analysis_result,
        sources=sources,
        tokens_used=row.tokens_used or 0,
        duration_ms=row.duration_ms or 0,
        created_at=row.created_at,
    )


@router.delete("/{conversation_id}")
@inject
async def delete_conversation(
    conversation_id: str,
    session: FromDishka[MainAsyncSession],
) -> dict:
    """Delete a conversation."""
    result = await session.execute(
        text("""
            DELETE FROM user_queries
            WHERE id = CAST(:id AS uuid)
            RETURNING CAST(id AS text) AS id
        """),
        {"id": conversation_id},
    )
    row = result.fetchone()
    await session.commit()

    if not row:
        raise HTTPException(status_code=404, detail="Conversation not found")

    return {"status": "deleted", "id": row.id}
