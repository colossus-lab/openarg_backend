from __future__ import annotations

from abc import ABC, abstractmethod

from app.domain.entities.connectors.data_result import DataResult


class IGeorefConnector(ABC):
    @abstractmethod
    async def get_provincias(self, nombre: str | None = None) -> list[dict]:
        ...

    @abstractmethod
    async def get_departamentos(
        self, provincia: str | None = None, nombre: str | None = None
    ) -> list[dict]:
        ...

    @abstractmethod
    async def get_municipios(
        self, provincia: str | None = None, nombre: str | None = None
    ) -> list[dict]:
        ...

    @abstractmethod
    async def get_localidades(
        self, provincia: str | None = None, nombre: str | None = None
    ) -> list[dict]:
        ...

    @abstractmethod
    async def normalize_location(self, query: str) -> DataResult | None:
        """Normalize a geographic name, trying all entity types."""
        ...
