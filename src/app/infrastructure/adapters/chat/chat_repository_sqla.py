from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select, update

from app.domain.entities.chat.conversation import Conversation
from app.domain.entities.chat.message import Message
from app.domain.ports.chat.chat_repository import IChatRepository
from app.infrastructure.persistence_sqla.provider import MainAsyncSession


class ChatRepositorySQLA(IChatRepository):
    def __init__(self, session: MainAsyncSession) -> None:
        self._session = session

    async def create_conversation(self, conversation: Conversation) -> Conversation:
        self._session.add(conversation)
        await self._session.flush()
        await self._session.commit()
        return conversation

    async def list_conversations(
        self, user_id: UUID, limit: int = 20, offset: int = 0
    ) -> list[Conversation]:
        stmt = (
            select(Conversation)
            .where(Conversation.user_id == user_id)
            .order_by(Conversation.updated_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_conversation(
        self, conversation_id: UUID, user_id: UUID | None = None
    ) -> Conversation | None:
        if user_id is None:
            return await self._session.get(Conversation, conversation_id)
        # H3 fix: owner-scoped read. A mismatched owner returns None, so
        # callers can treat the response identically to "not found" and the
        # IDOR vector closes at the SQL layer.
        stmt = select(Conversation).where(
            Conversation.id == conversation_id,
            Conversation.user_id == user_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def delete_conversation(
        self, conversation_id: UUID, user_id: UUID | None = None
    ) -> bool:
        conv = await self.get_conversation(conversation_id, user_id=user_id)
        if not conv:
            return False
        await self._session.delete(conv)
        await self._session.commit()
        return True

    async def update_conversation_title(
        self, conversation_id: UUID, title: str, user_id: UUID | None = None
    ) -> Conversation | None:
        where_clause = [Conversation.id == conversation_id]
        if user_id is not None:
            where_clause.append(Conversation.user_id == user_id)
        stmt = (
            update(Conversation)
            .where(*where_clause)
            .values(title=title, updated_at=func.now())
        )
        result = await self._session.execute(stmt)
        await self._session.commit()
        if result.rowcount == 0:
            return None
        return await self.get_conversation(conversation_id, user_id=user_id)

    async def add_message(self, message: Message) -> Message:
        self._session.add(message)
        await self._session.flush()
        await self._session.commit()
        return message

    async def get_messages(
        self,
        conversation_id: UUID,
        limit: int = 100,
        offset: int = 0,
        user_id: UUID | None = None,
    ) -> list[Message]:
        stmt = (
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.created_at.asc())
            .limit(limit)
            .offset(offset)
        )
        if user_id is not None:
            # H3 fix: owner-scoped read. Join the parent conversation and
            # filter so a foreign conv_id returns []. Cheap subquery — the
            # planner just adds an index lookup on conversations.user_id.
            from sqlalchemy import exists

            stmt = stmt.where(
                exists().where(
                    Conversation.id == conversation_id,
                    Conversation.user_id == user_id,
                )
            )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def update_message_feedback(
        self,
        message_id: UUID,
        feedback: str,
        comment: str | None = None,
    ) -> Message | None:
        stmt = (
            update(Message)
            .where(Message.id == message_id)
            .values(feedback=feedback, feedback_comment=comment, updated_at=func.now())
        )
        await self._session.execute(stmt)
        await self._session.commit()
        return await self._session.get(Message, message_id)
