from __future__ import annotations

from abc import ABC, abstractmethod

from app.domain.entities.connectors.data_result import DataResult


class ICKANSearchConnector(ABC):
    @abstractmethod
    async def search_datasets(
        self,
        query: str,
        portal_id: str | None = None,
        rows: int = 10,
    ) -> list[DataResult]:
        """Search datasets across CKAN portals. Tries datastore, CSV, then metadata fallback."""
        ...

    @abstractmethod
    async def query_datastore(
        self,
        portal_id: str,
        resource_id: str,
        q: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Query a specific CKAN datastore resource."""
        ...
