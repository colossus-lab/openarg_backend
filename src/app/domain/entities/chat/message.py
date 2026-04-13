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
    chart_data: list[dict] | None = None
    map_data: dict | None = None
    documents: list[dict] | None = None
    confidence: float | None = None
    ui_trace: dict | None = None
    feedback: str | None = None  # "up" | "down" | None
    feedback_comment: str | None = None
    # FR-015 (001d-conversation-lifecycle): true when the assistant
    # message was persisted on an error path (stream broken, WS error,
    # caught exception). Write-once — a regenerate appends a new
    # message instead of mutating this flag.
    errored: bool = False
