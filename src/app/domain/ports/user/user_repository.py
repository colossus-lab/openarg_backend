from __future__ import annotations

from abc import ABC, abstractmethod
from uuid import UUID

from app.domain.entities.user.user import User


class IUserRepository(ABC):
    @abstractmethod
    async def upsert_by_email(self, user: User) -> User: ...

    @abstractmethod
    async def get_by_email(self, email: str) -> User | None: ...

    @abstractmethod
    async def get_by_id(self, user_id: UUID) -> User | None: ...
