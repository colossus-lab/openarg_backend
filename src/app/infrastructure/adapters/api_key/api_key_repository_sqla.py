"""SQLAlchemy adapter for API key persistence."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, select, update

from app.domain.entities.api_key.api_key import ApiKey, ApiUsage
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.infrastructure.persistence_sqla.provider import MainAsyncSession

logger = logging.getLogger(__name__)


class ApiKeyRepositorySQLA(IApiKeyRepository):
    def __init__(self, session: MainAsyncSession) -> None:
        self._session = session

    async def create(self, api_key: ApiKey) -> ApiKey:
        self._session.add(api_key)
        await self._session.flush()
        await self._session.commit()
        return api_key

    async def get_by_key_hash(self, key_hash: str) -> ApiKey | None:
        stmt = select(ApiKey).where(ApiKey.key_hash == key_hash)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_user(self, user_id: UUID) -> list[ApiKey]:
        stmt = select(ApiKey).where(ApiKey.user_id == user_id).order_by(ApiKey.created_at.desc())
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def deactivate(self, key_id: UUID, user_id: UUID) -> bool:
        stmt = (
            update(ApiKey)
            .where(ApiKey.id == key_id, ApiKey.user_id == user_id)
            .values(is_active=False, updated_at=datetime.now(UTC))
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        await self._session.commit()
        return result.rowcount > 0  # type: ignore[union-attr]

    async def update_last_used(self, key_id: UUID) -> None:
        stmt = update(ApiKey).where(ApiKey.id == key_id).values(last_used_at=datetime.now(UTC))
        await self._session.execute(stmt)
        await self._session.flush()
        await self._session.commit()

    async def record_usage(self, usage: ApiUsage) -> None:
        self._session.add(usage)
        await self._session.flush()
        await self._session.commit()

    async def get_usage_summary(self, user_id: UUID) -> dict:
        """Return usage stats for all keys owned by a user."""
        today = datetime.now(UTC).date()

        # Get all key IDs for this user
        key_ids_stmt = select(ApiKey.id).where(ApiKey.user_id == user_id)
        key_ids_result = await self._session.execute(key_ids_stmt)
        key_ids = list(key_ids_result.scalars().all())

        if not key_ids:
            return {"total_requests": 0, "total_tokens": 0, "requests_today": 0}

        # Total usage
        total_stmt = select(
            func.count().label("total_requests"),
            func.coalesce(func.sum(ApiUsage.tokens_used), 0).label("total_tokens"),
        ).where(ApiUsage.api_key_id.in_(key_ids))
        total_result = await self._session.execute(total_stmt)
        total_row = total_result.one()

        # Today's usage
        today_stmt = select(func.count().label("requests_today")).where(
            ApiUsage.api_key_id.in_(key_ids),
            func.date(ApiUsage.created_at) == today,
        )
        today_result = await self._session.execute(today_stmt)
        today_row = today_result.one()

        return {
            "total_requests": total_row.total_requests,
            "total_tokens": total_row.total_tokens,
            "requests_today": today_row.requests_today,
        }
