from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select

from app.domain.entities.dataset.dataset import Dataset
from app.domain.ports.dataset.dataset_repository import IDatasetRepository
from app.infrastructure.persistence_sqla.provider import MainAsyncSession


class DatasetRepositorySQLA(IDatasetRepository):
    def __init__(self, session: MainAsyncSession) -> None:
        self._session = session

    async def save(self, dataset: Dataset) -> Dataset:
        self._session.add(dataset)
        await self._session.flush()
        await self._session.commit()
        return dataset

    async def get_by_id(self, dataset_id: UUID) -> Dataset | None:
        return await self._session.get(Dataset, dataset_id)

    async def get_by_source_id(self, source_id: str, portal: str) -> Dataset | None:
        stmt = select(Dataset).where(
            Dataset.source_id == source_id,
            Dataset.portal == portal,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_portal(
        self, portal: str, limit: int = 100, offset: int = 0
    ) -> list[Dataset]:
        stmt = (
            select(Dataset)
            .where(Dataset.portal == portal)
            .order_by(Dataset.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def count_by_portal(self, portal: str) -> int:
        stmt = select(func.count()).select_from(Dataset).where(Dataset.portal == portal)
        result = await self._session.execute(stmt)
        return result.scalar_one()

    async def upsert(self, dataset: Dataset) -> Dataset:
        existing = await self.get_by_source_id(dataset.source_id, dataset.portal)
        if existing:
            existing.title = dataset.title
            existing.description = dataset.description
            existing.organization = dataset.organization
            existing.url = dataset.url
            existing.download_url = dataset.download_url
            existing.format = dataset.format
            existing.columns = dataset.columns
            existing.sample_rows = dataset.sample_rows
            existing.tags = dataset.tags
            existing.last_updated_at = dataset.last_updated_at
            await self._session.flush()
            await self._session.commit()
            return existing
        return await self.save(dataset)
