from __future__ import annotations

from abc import ABC, abstractmethod
from uuid import UUID

from app.domain.entities.chat.conversation import Conversation
from app.domain.entities.chat.message import Message


class IChatRepository(ABC):
    @abstractmethod
    async def create_conversation(self, conversation: Conversation) -> Conversation: ...

    @abstractmethod
    async def list_conversations(
        self, user_id: UUID, limit: int = 20, offset: int = 0
    ) -> list[Conversation]: ...

    @abstractmethod
    async def get_conversation(self, conversation_id: UUID) -> Conversation | None: ...

    @abstractmethod
    async def delete_conversation(self, conversation_id: UUID) -> bool: ...

    @abstractmethod
    async def update_conversation_title(
        self, conversation_id: UUID, title: str
    ) -> Conversation | None: ...

    @abstractmethod
    async def add_message(self, message: Message) -> Message: ...

    @abstractmethod
    async def get_messages(
        self, conversation_id: UUID, limit: int = 100, offset: int = 0
    ) -> list[Message]: ...
