from __future__ import annotations

from abc import ABC, abstractmethod
from uuid import UUID

from app.domain.entities.dataset.dataset import Dataset


class IDatasetRepository(ABC):
    @abstractmethod
    async def save(self, dataset: Dataset) -> Dataset: ...

    @abstractmethod
    async def get_by_id(self, dataset_id: UUID) -> Dataset | None: ...

    @abstractmethod
    async def get_by_source_id(self, source_id: str, portal: str) -> Dataset | None: ...

    @abstractmethod
    async def list_by_portal(self, portal: str, limit: int = 100, offset: int = 0) -> list[Dataset]: ...

    @abstractmethod
    async def count_by_portal(self, portal: str) -> int: ...

    @abstractmethod
    async def upsert(self, dataset: Dataset) -> Dataset: ...
