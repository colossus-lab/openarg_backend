from __future__ import annotations

from uuid import UUID

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.domain.entities.chat.conversation import Conversation
from app.domain.entities.chat.message import Message
from app.domain.ports.chat.chat_repository import IChatRepository
from app.domain.ports.user.user_repository import IUserRepository

router = APIRouter(prefix="/conversations", tags=["conversations"])


# ---- Schemas ----

class ConversationCreate(BaseModel):
    user_email: str
    title: str = ""


class ConversationSummary(BaseModel):
    id: str
    title: str
    created_at: str
    updated_at: str


class MessageCreate(BaseModel):
    role: str
    content: str
    sources: list[dict] | None = None


class MessageResponse(BaseModel):
    id: str
    conversation_id: str
    role: str
    content: str
    sources: list[dict]
    created_at: str


class ConversationDetail(BaseModel):
    id: str
    title: str
    created_at: str
    updated_at: str
    messages: list[MessageResponse]


class TitleUpdate(BaseModel):
    title: str


# ---- Endpoints ----

@router.get("/", response_model=list[ConversationSummary])
@inject
async def list_conversations(
    user_repo: FromDishka[IUserRepository],
    chat_repo: FromDishka[IChatRepository],
    user_email: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[ConversationSummary]:
    """List conversations for a user by email."""
    if not user_email:
        return []

    user = await user_repo.get_by_email(user_email)
    if not user:
        return []

    conversations = await chat_repo.list_conversations(user.id, limit, offset)
    return [
        ConversationSummary(
            id=str(c.id),
            title=c.title,
            created_at=c.created_at.isoformat(),
            updated_at=c.updated_at.isoformat(),
        )
        for c in conversations
    ]


@router.post("/", response_model=ConversationDetail)
@inject
async def create_conversation(
    body: ConversationCreate,
    user_repo: FromDishka[IUserRepository],
    chat_repo: FromDishka[IChatRepository],
) -> ConversationDetail:
    """Create a new conversation."""
    user = await user_repo.get_by_email(body.user_email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found. Sync user first.")

    conversation = Conversation(user_id=user.id, title=body.title)
    saved = await chat_repo.create_conversation(conversation)

    return ConversationDetail(
        id=str(saved.id),
        title=saved.title,
        created_at=saved.created_at.isoformat(),
        updated_at=saved.updated_at.isoformat(),
        messages=[],
    )


@router.get("/{conversation_id}", response_model=ConversationDetail)
@inject
async def get_conversation(
    conversation_id: UUID,
    chat_repo: FromDishka[IChatRepository],
) -> ConversationDetail:
    """Get a conversation with all its messages."""
    conv = await chat_repo.get_conversation(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    messages = await chat_repo.get_messages(conversation_id)

    return ConversationDetail(
        id=str(conv.id),
        title=conv.title,
        created_at=conv.created_at.isoformat(),
        updated_at=conv.updated_at.isoformat(),
        messages=[
            MessageResponse(
                id=str(m.id),
                conversation_id=str(m.conversation_id),
                role=m.role,
                content=m.content,
                sources=m.sources or [],
                created_at=m.created_at.isoformat(),
            )
            for m in messages
        ],
    )


@router.delete("/{conversation_id}")
@inject
async def delete_conversation(
    conversation_id: UUID,
    chat_repo: FromDishka[IChatRepository],
) -> dict:
    """Delete a conversation and all its messages (CASCADE)."""
    deleted = await chat_repo.delete_conversation(conversation_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"status": "deleted", "id": str(conversation_id)}


@router.patch("/{conversation_id}", response_model=ConversationSummary)
@inject
async def update_conversation_title(
    conversation_id: UUID,
    body: TitleUpdate,
    chat_repo: FromDishka[IChatRepository],
) -> ConversationSummary:
    """Update conversation title."""
    conv = await chat_repo.update_conversation_title(conversation_id, body.title)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return ConversationSummary(
        id=str(conv.id),
        title=conv.title,
        created_at=conv.created_at.isoformat(),
        updated_at=conv.updated_at.isoformat(),
    )


@router.post("/{conversation_id}/messages", response_model=MessageResponse)
@inject
async def add_message(
    conversation_id: UUID,
    body: MessageCreate,
    chat_repo: FromDishka[IChatRepository],
) -> MessageResponse:
    """Add a message to a conversation."""
    conv = await chat_repo.get_conversation(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    message = Message(
        conversation_id=conversation_id,
        role=body.role,
        content=body.content,
        sources=body.sources or [],
    )
    saved = await chat_repo.add_message(message)

    return MessageResponse(
        id=str(saved.id),
        conversation_id=str(saved.conversation_id),
        role=saved.role,
        content=saved.content,
        sources=saved.sources or [],
        created_at=saved.created_at.isoformat(),
    )


@router.get("/{conversation_id}/messages", response_model=list[MessageResponse])
@inject
async def get_messages(
    conversation_id: UUID,
    chat_repo: FromDishka[IChatRepository],
    limit: int = 100,
    offset: int = 0,
) -> list[MessageResponse]:
    """Get messages for a conversation."""
    messages = await chat_repo.get_messages(conversation_id, limit, offset)
    return [
        MessageResponse(
            id=str(m.id),
            conversation_id=str(m.conversation_id),
            role=m.role,
            content=m.content,
            sources=m.sources or [],
            created_at=m.created_at.isoformat(),
        )
        for m in messages
    ]
