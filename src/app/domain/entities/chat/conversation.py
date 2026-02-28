from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID, uuid4

from app.domain.entities.base import BaseEntity


@dataclass
class Conversation(BaseEntity):
    """Conversación de chat vinculada a un usuario."""

    user_id: UUID = field(default_factory=uuid4)
    title: str = ""
