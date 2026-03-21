from __future__ import annotations

from uuid import UUID

from sqlalchemy import delete, select

from app.domain.entities.chat.conversation import Conversation
from app.domain.entities.chat.message import Message
from app.domain.entities.query.query import UserQuery
from app.domain.entities.user.user import User
from app.domain.ports.user.user_repository import IUserRepository
from app.infrastructure.persistence_sqla.provider import MainAsyncSession


class UserRepositorySQLA(IUserRepository):
    def __init__(self, session: MainAsyncSession) -> None:
        self._session = session

    async def upsert_by_email(self, user: User) -> User:
        existing = await self.get_by_email(user.email)
        if existing:
            existing.name = user.name
            existing.image_url = user.image_url
            await self._session.flush()
            await self._session.commit()
            return existing
        self._session.add(user)
        await self._session.flush()
        await self._session.commit()
        return user

    async def get_by_email(self, email: str) -> User | None:
        stmt = select(User).where(User.email == email)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, user_id: UUID) -> User | None:
        return await self._session.get(User, user_id)

    async def delete_user_and_data(self, user_id: UUID) -> None:
        """Delete user and all associated data (ARCO right to erasure)."""
        # Delete messages in user's conversations
        conv_ids = (
            await self._session.execute(
                select(Conversation.id).where(Conversation.user_id == user_id)
            )
        ).scalars().all()

        if conv_ids:
            await self._session.execute(
                delete(Message).where(Message.conversation_id.in_(conv_ids))
            )
            await self._session.execute(
                delete(Conversation).where(Conversation.user_id == user_id)
            )

        # Delete user queries (user_id stored as text)
        await self._session.execute(
            delete(UserQuery).where(UserQuery.user_id == str(user_id))
        )

        # Delete the user
        await self._session.execute(
            delete(User).where(User.id == user_id)
        )

        await self._session.flush()
        await self._session.commit()

    async def export_user_data(self, user_id: UUID) -> dict:
        """Export all user data as a dictionary (data portability)."""
        user = await self.get_by_id(user_id)
        if not user:
            return {}

        # Conversations
        convs = (
            await self._session.execute(
                select(Conversation).where(Conversation.user_id == user_id)
            )
        ).scalars().all()

        conversations_data = []
        for conv in convs:
            msgs = (
                await self._session.execute(
                    select(Message).where(Message.conversation_id == conv.id)
                )
            ).scalars().all()
            conversations_data.append({
                "id": str(conv.id),
                "title": conv.title,
                "created_at": conv.created_at.isoformat() if conv.created_at else None,
                "messages": [
                    {
                        "id": str(m.id),
                        "role": m.role,
                        "content": m.content,
                        "sources": m.sources,
                        "created_at": m.created_at.isoformat() if m.created_at else None,
                    }
                    for m in msgs
                ],
            })

        # Queries
        queries = (
            await self._session.execute(
                select(UserQuery).where(UserQuery.user_id == str(user_id))
            )
        ).scalars().all()

        queries_data = [
            {
                "id": str(q.id),
                "question": q.question,
                "status": q.status,
                "analysis_result": q.analysis_result,
                "created_at": q.created_at.isoformat() if q.created_at else None,
            }
            for q in queries
        ]

        return {
            "user": {
                "id": str(user.id),
                "email": user.email,
                "name": user.name,
                "image_url": user.image_url,
                "created_at": user.created_at.isoformat() if user.created_at else None,
            },
            "conversations": conversations_data,
            "queries": queries_data,
        }
