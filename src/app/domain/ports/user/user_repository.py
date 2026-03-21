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

    @abstractmethod
    async def update(self, user: User) -> User: ...

    @abstractmethod
    async def delete_user_conversations(self, user_id: UUID) -> None:
        """Delete all conversations and messages (but keep the user)."""

    @abstractmethod
    async def delete_user_and_data(self, user_id: UUID) -> None:
        """Delete user and all associated data (conversations, messages, queries)."""

    @abstractmethod
    async def export_user_data(self, user_id: UUID) -> dict:
        """Export all user data as a dictionary (data portability)."""
