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
    async def search_datasets_ann(
        self,
        query_embedding: list[float],
        limit: int = 10,
        portal_filter: str | None = None,
        min_similarity: float = 0.40,
    ) -> list[SearchResult]:
        """Como ``search_datasets``, pero por vecinos aproximados (índice HNSW).

        La búsqueda exacta filtra por similitud en el WHERE, el índice no se
        usa y recorre todos los chunks: en staging tardó de 1 a más de 60 s.
        Ésta ordena por distancia con LIMIT —lo que el índice sí resuelve— y
        tarda menos de un segundo. Por defecto, la exacta.
        """
        return await self.search_datasets(query_embedding, limit, portal_filter, min_similarity)

    async def reset(self) -> None:
        """Deja el adapter usable después de una búsqueda que falló o se canceló.

        No es abstracto: un adapter sin estado no tiene nada que hacer.
        """
        return None

    @abstractmethod
    async def search_datasets(
        self,
        query_embedding: list[float],
        limit: int = 10,
        portal_filter: str | None = None,
        min_similarity: float = 0.55,
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
