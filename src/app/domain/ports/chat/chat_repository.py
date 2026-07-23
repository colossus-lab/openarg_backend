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
    async def get_conversation(
        self, conversation_id: UUID, user_id: UUID | None = None
    ) -> Conversation | None:
        """Return the conversation. When `user_id` is provided, the lookup
        is scoped to that owner — any conversation belonging to a different
        user returns None. Defense-in-depth against IDOR: callers that
        already verified ownership at the application layer can omit it,
        but pipeline / WS paths must pass it. See H3 in round v46.
        """
        ...

    @abstractmethod
    async def delete_conversation(
        self, conversation_id: UUID, user_id: UUID | None = None
    ) -> bool: ...

    @abstractmethod
    async def update_conversation_title(
        self, conversation_id: UUID, title: str, user_id: UUID | None = None
    ) -> Conversation | None: ...

    @abstractmethod
    async def add_message(self, message: Message) -> Message: ...

    @abstractmethod
    async def get_messages(
        self,
        conversation_id: UUID,
        limit: int = 100,
        offset: int = 0,
        user_id: UUID | None = None,
    ) -> list[Message]:
        """Return messages. When `user_id` is provided, the lookup joins
        on the parent conversation and filters by owner; a mismatched
        owner returns []. See H3 in round v46.
        """
        ...

    @abstractmethod
    async def update_message_feedback(
        self,
        message_id: UUID,
        feedback: str,
        comment: str | None = None,
    ) -> Message | None: ...
