from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class CatalogEntry:
    source_id: str
    title: str
    description: str
    organization: str
    url: str
    download_url: str
    format: str
    tags: list[str]
    columns: list[str]
    last_updated: str


@dataclass
class DownloadedData:
    content: bytes
    format: str
    filename: str
    size_bytes: int


class IDataSource(ABC):
    """Port para cada fuente de datos pública (datos.gob.ar, CABA, etc.)."""

    @property
    @abstractmethod
    def portal_name(self) -> str: ...

    @abstractmethod
    async def fetch_catalog(
        self, limit: int = 100, offset: int = 0
    ) -> list[CatalogEntry]: ...

    @abstractmethod
    async def fetch_catalog_count(self) -> int: ...

    @abstractmethod
    async def download_dataset(self, download_url: str) -> DownloadedData: ...
