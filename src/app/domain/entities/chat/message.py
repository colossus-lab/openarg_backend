from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID, uuid4

from app.domain.entities.base import BaseEntity


@dataclass
class Message(BaseEntity):
    """Mensaje individual dentro de una conversación."""

    conversation_id: UUID = field(default_factory=uuid4)
    role: str = ""  # "user" | "assistant"
    content: str = ""
    sources: list[dict] = field(default_factory=list)
    feedback: str | None = None  # "up" | "down" | None
    feedback_comment: str | None = None
