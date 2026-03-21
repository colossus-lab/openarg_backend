from __future__ import annotations

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    String,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.domain.entities.chat.conversation import Conversation
from app.domain.entities.chat.message import Message
from app.infrastructure.persistence_sqla.registry import mapping_registry

_mapped = False


def map_chat_tables() -> None:
    global _mapped
    if _mapped:
        return

    conversations_table = Table(
        "conversations",
        mapping_registry.metadata,
        Column(
            "id",
            PG_UUID(as_uuid=True),
            primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column(
            "user_id",
            PG_UUID(as_uuid=True),
            ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        Column("title", String(1000), nullable=False, server_default=""),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column(
            "updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
        ),
    )

    messages_table = Table(
        "messages",
        mapping_registry.metadata,
        Column(
            "id",
            PG_UUID(as_uuid=True),
            primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column(
            "conversation_id",
            PG_UUID(as_uuid=True),
            ForeignKey("conversations.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        Column("role", String(20), nullable=False),
        Column("content", Text, nullable=False),
        Column("sources", JSONB, nullable=True, server_default=text("'[]'::jsonb")),
        Column("chart_data", JSONB, nullable=True),
        Column("documents", JSONB, nullable=True),
        Column("feedback", String(10), nullable=True),
        Column("feedback_comment", Text, nullable=True),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column(
            "updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
        ),
    )

    mapping_registry.map_imperatively(Conversation, conversations_table)
    mapping_registry.map_imperatively(Message, messages_table)

    _mapped = True
