from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.domain.entities.chat.conversation import Conversation
from app.domain.entities.chat.message import Message
from app.domain.ports.chat.chat_repository import IChatRepository
from app.domain.ports.user.user_repository import IUserRepository
from app.presentation.http.middleware.google_jwt_middleware import get_request_user_email

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
    sources: list[dict[str, Any]] | None = None
    chart_data: list[dict[str, Any]] | None = None
    map_data: dict[str, Any] | None = None
    documents: list[dict[str, Any]] | None = None
    # Set by the frontend chat bridge when the assistant message is being
    # persisted on an error path (stream broke, WS failure, backend 5xx).
    # FR-014/FR-015 of 001-chat-bridge/001d-conversation-lifecycle — the
    # 2026-04-11 migration 0029 adds the backing `messages.errored`
    # column so the UI can render a regenerate affordance across page
    # refreshes.
    errored: bool = False


class MessageResponse(BaseModel):
    id: str
    conversation_id: str
    role: str
    content: str
    sources: list[dict[str, Any]]
    chart_data: list[dict[str, Any]] | None = None
    map_data: dict[str, Any] | None = None
    documents: list[dict[str, Any]] | None = None
    errored: bool = False
    created_at: str
    feedback: str | None = None
    feedback_comment: str | None = None


class ConversationDetail(BaseModel):
    id: str
    title: str
    created_at: str
    updated_at: str
    messages: list[MessageResponse]


class TitleUpdate(BaseModel):
    title: str


class FeedbackCreate(BaseModel):
    feedback: Literal["up", "down"]
    comment: str | None = None


# ---- Helpers ----


async def _resolve_user(user_repo: IUserRepository, email: str) -> Any:
    """Resolve user by email, raise 404 if not found."""
    user = await user_repo.get_by_email(email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found. Sync user first.")
    return user


async def _get_owned_conversation(
    chat_repo: IChatRepository,
    user_repo: IUserRepository,
    conversation_id: UUID,
    user_email: str,
) -> Any:
    """Fetch a conversation and verify it belongs to the user. Raises 404 or 403."""
    conv = await chat_repo.get_conversation(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    user = await user_repo.get_by_email(user_email)
    if not user or conv.user_id != user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    return conv


# ---- Endpoints ----


@router.get("/", response_model=list[ConversationSummary])
@inject  # type: ignore[untyped-decorator]
async def list_conversations(
    request: Request,
    user_repo: FromDishka[IUserRepository],
    chat_repo: FromDishka[IChatRepository],
    user_email: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[ConversationSummary]:
    """List conversations for the authenticated user."""
    email = get_request_user_email(request) or user_email or ""
    if not email:
        return []

    user = await user_repo.get_by_email(email)
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
@inject  # type: ignore[untyped-decorator]
async def create_conversation(
    request: Request,
    body: ConversationCreate,
    user_repo: FromDishka[IUserRepository],
    chat_repo: FromDishka[IChatRepository],
) -> ConversationDetail:
    """Create a new conversation."""
    email = get_request_user_email(request) or body.user_email
    user = await _resolve_user(user_repo, email)

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
@inject  # type: ignore[untyped-decorator]
async def get_conversation(
    conversation_id: UUID,
    request: Request,
    user_repo: FromDishka[IUserRepository],
    chat_repo: FromDishka[IChatRepository],
) -> ConversationDetail:
    """Get a conversation with all its messages."""
    conv = await _get_owned_conversation(
        chat_repo, user_repo, conversation_id, get_request_user_email(request)
    )
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
                chart_data=m.chart_data,
                map_data=m.map_data,
                documents=m.documents,
                errored=getattr(m, "errored", False),
                created_at=m.created_at.isoformat(),
                feedback=m.feedback,
                feedback_comment=m.feedback_comment,
            )
            for m in messages
        ],
    )


@router.delete("/{conversation_id}")
@inject  # type: ignore[untyped-decorator]
async def delete_conversation(
    conversation_id: UUID,
    request: Request,
    user_repo: FromDishka[IUserRepository],
    chat_repo: FromDishka[IChatRepository],
) -> dict[str, str]:
    """Delete a conversation and all its messages (CASCADE)."""
    await _get_owned_conversation(
        chat_repo, user_repo, conversation_id, get_request_user_email(request)
    )
    deleted = await chat_repo.delete_conversation(conversation_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"status": "deleted", "id": str(conversation_id)}


@router.patch("/{conversation_id}", response_model=ConversationSummary)
@inject  # type: ignore[untyped-decorator]
async def update_conversation_title(
    conversation_id: UUID,
    request: Request,
    body: TitleUpdate,
    user_repo: FromDishka[IUserRepository],
    chat_repo: FromDishka[IChatRepository],
) -> ConversationSummary:
    """Update conversation title."""
    await _get_owned_conversation(
        chat_repo, user_repo, conversation_id, get_request_user_email(request)
    )
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
@inject  # type: ignore[untyped-decorator]
async def add_message(
    conversation_id: UUID,
    request: Request,
    body: MessageCreate,
    user_repo: FromDishka[IUserRepository],
    chat_repo: FromDishka[IChatRepository],
) -> MessageResponse:
    """Add a message to a conversation."""
    await _get_owned_conversation(
        chat_repo, user_repo, conversation_id, get_request_user_email(request)
    )

    message = Message(
        conversation_id=conversation_id,
        role=body.role,
        content=body.content,
        sources=body.sources or [],
        chart_data=body.chart_data,
        map_data=body.map_data,
        documents=body.documents,
        errored=body.errored,
    )
    saved = await chat_repo.add_message(message)

    return MessageResponse(
        id=str(saved.id),
        conversation_id=str(saved.conversation_id),
        role=saved.role,
        content=saved.content,
        sources=saved.sources or [],
        chart_data=saved.chart_data,
        map_data=saved.map_data,
        documents=saved.documents,
        errored=getattr(saved, "errored", False),
        created_at=saved.created_at.isoformat(),
        feedback=saved.feedback,
        feedback_comment=saved.feedback_comment,
    )


@router.get("/{conversation_id}/messages", response_model=list[MessageResponse])
@inject  # type: ignore[untyped-decorator]
async def get_messages(
    conversation_id: UUID,
    request: Request,
    user_repo: FromDishka[IUserRepository],
    chat_repo: FromDishka[IChatRepository],
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[MessageResponse]:
    """Get messages for a conversation."""
    await _get_owned_conversation(
        chat_repo, user_repo, conversation_id, get_request_user_email(request)
    )

    messages = await chat_repo.get_messages(conversation_id, limit, offset)
    return [
        MessageResponse(
            id=str(m.id),
            conversation_id=str(m.conversation_id),
            role=m.role,
            content=m.content,
            sources=m.sources or [],
            chart_data=m.chart_data,
            map_data=m.map_data,
            documents=m.documents,
            errored=getattr(m, "errored", False),
            created_at=m.created_at.isoformat(),
            feedback=m.feedback,
            feedback_comment=m.feedback_comment,
        )
        for m in messages
    ]


@router.patch(
    "/{conversation_id}/messages/{message_id}/feedback",
    response_model=MessageResponse,
)
@inject  # type: ignore[untyped-decorator]
async def submit_feedback(
    conversation_id: UUID,
    message_id: UUID,
    request: Request,
    body: FeedbackCreate,
    user_repo: FromDishka[IUserRepository],
    chat_repo: FromDishka[IChatRepository],
) -> MessageResponse:
    """Submit thumbs up/down feedback on an assistant message."""
    await _get_owned_conversation(
        chat_repo, user_repo, conversation_id, get_request_user_email(request)
    )

    # Verify the message belongs to this conversation
    messages = await chat_repo.get_messages(conversation_id)
    msg = next((m for m in messages if m.id == message_id), None)
    if not msg:
        raise HTTPException(status_code=404, detail="Message not found in this conversation")

    updated = await chat_repo.update_message_feedback(
        message_id,
        body.feedback,
        body.comment,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Message not found")

    return MessageResponse(
        id=str(updated.id),
        conversation_id=str(updated.conversation_id),
        role=updated.role,
        content=updated.content,
        sources=updated.sources or [],
        chart_data=updated.chart_data,
        map_data=updated.map_data,
        documents=updated.documents,
        errored=getattr(updated, "errored", False),
        created_at=updated.created_at.isoformat(),
        feedback=updated.feedback,
        feedback_comment=updated.feedback_comment,
    )
