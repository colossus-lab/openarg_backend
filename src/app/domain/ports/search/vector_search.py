from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class SearchResult:
    dataset_id: str
    title: str
    description: str
    portal: str
    download_url: str
    columns: str
    score: float


class IVectorSearch(ABC):
    @abstractmethod
    async def search_datasets(
        self,
        query_embedding: list[float],
        limit: int = 10,
        portal_filter: str | None = None,
        min_similarity: float = 0.30,
    ) -> list[SearchResult]: ...

    @abstractmethod
    async def index_dataset(
        self,
        dataset_id: str,
        content: str,
        embedding: list[float],
    ) -> None: ...

    @abstractmethod
    async def search_datasets_hybrid(
        self,
        query_embedding: list[float],
        query_text: str,
        limit: int = 10,
        portal_filter: str | None = None,
        rrf_k: int = 60,
        min_score: float = 0.05,
    ) -> list[SearchResult]: ...

    @abstractmethod
    async def delete_dataset_chunks(self, dataset_id: str) -> None: ...
