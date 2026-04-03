"""Port (interface) for API key persistence."""

from __future__ import annotations

from abc import ABC, abstractmethod
from uuid import UUID

from app.domain.entities.api_key.api_key import ApiKey, ApiUsage


class IApiKeyRepository(ABC):
    """Abstract repository for API key operations."""

    @abstractmethod
    async def create(self, api_key: ApiKey) -> ApiKey: ...

    @abstractmethod
    async def get_by_key_hash(self, key_hash: str) -> ApiKey | None: ...

    @abstractmethod
    async def list_by_user(self, user_id: UUID) -> list[ApiKey]: ...

    @abstractmethod
    async def deactivate(self, key_id: UUID, user_id: UUID) -> bool: ...

    @abstractmethod
    async def update_last_used(self, key_id: UUID) -> None: ...

    @abstractmethod
    async def record_usage(self, usage: ApiUsage) -> None: ...

    @abstractmethod
    async def get_usage_summary(self, user_id: UUID) -> dict: ...
