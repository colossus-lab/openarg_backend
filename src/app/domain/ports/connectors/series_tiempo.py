from __future__ import annotations

from abc import ABC, abstractmethod

from app.domain.entities.connectors.data_result import DataResult


class ISeriesTiempoConnector(ABC):
    @abstractmethod
    async def search(self, query: str, limit: int = 10) -> list[dict]:
        """Search for available time series by keyword."""
        ...

    @abstractmethod
    async def fetch(
        self,
        series_ids: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        collapse: str | None = None,
        representation: str | None = None,
        limit: int = 1000,
    ) -> DataResult | None:
        """Fetch time series data and return as DataResult."""
        ...
