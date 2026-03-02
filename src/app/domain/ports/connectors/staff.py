from __future__ import annotations

from abc import ABC, abstractmethod

from app.domain.entities.connectors.data_result import DataResult


class IStaffConnector(ABC):
    @abstractmethod
    async def get_by_legislator(self, name: str, limit: int = 50) -> DataResult:
        """Return staff members whose area matches *name*."""
        ...

    @abstractmethod
    async def count_by_legislator(self, name: str) -> DataResult:
        """Count staff members whose area matches *name*."""
        ...

    @abstractmethod
    async def get_changes(self, name: str | None = None, limit: int = 20) -> DataResult:
        """Return recent altas/bajas, optionally filtered by area name."""
        ...

    @abstractmethod
    async def search(self, query: str, limit: int = 20) -> DataResult:
        """Free-text search across apellido, nombre, area_desempeno."""
        ...

    @abstractmethod
    async def stats(self) -> DataResult:
        """Aggregate statistics about the latest snapshot."""
        ...
