from __future__ import annotations

from uuid import UUID

from sqlalchemy import select

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
