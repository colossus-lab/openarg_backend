from __future__ import annotations

from sqlalchemy import text

from app.domain.ports.search.vector_search import IVectorSearch, SearchResult
from app.infrastructure.persistence_sqla.provider import MainAsyncSession


class PgVectorSearchAdapter(IVectorSearch):
    def __init__(self, session: MainAsyncSession) -> None:
        self._session = session

    async def search_datasets(
        self,
        query_embedding: list[float],
        limit: int = 10,
        portal_filter: str | None = None,
    ) -> list[SearchResult]:
        embedding_str = "[" + ",".join(str(v) for v in query_embedding) + "]"

        portal_clause = ""
        params: dict = {"embedding": embedding_str, "limit": limit}
        if portal_filter:
            portal_clause = "AND d.portal = :portal"
            params["portal"] = portal_filter

        query = text(f"""
            SELECT
                CAST(d.id AS text) AS dataset_id,
                d.title,
                d.description,
                d.portal,
                d.download_url,
                d.columns,
                1 - (dc.embedding <=> CAST(:embedding AS vector)) AS score
            FROM dataset_chunks dc
            JOIN datasets d ON d.id = dc.dataset_id
            WHERE 1=1 {portal_clause}
            ORDER BY dc.embedding <=> CAST(:embedding AS vector)
            LIMIT :limit
        """)

        result = await self._session.execute(query, params)
        rows = result.fetchall()

        return [
            SearchResult(
                dataset_id=row.dataset_id,
                title=row.title,
                description=row.description or "",
                portal=row.portal,
                download_url=row.download_url or "",
                columns=row.columns or "",
                score=float(row.score),
            )
            for row in rows
        ]

    async def index_dataset(
        self,
        dataset_id: str,
        content: str,
        embedding: list[float],
    ) -> None:
        embedding_str = "[" + ",".join(str(v) for v in embedding) + "]"

        query = text("""
            INSERT INTO dataset_chunks (dataset_id, content, embedding)
            VALUES (:dataset_id, :content, CAST(:embedding AS vector))
        """)
        await self._session.execute(
            query,
            {"dataset_id": dataset_id, "content": content, "embedding": embedding_str},
        )
        await self._session.commit()

    async def delete_dataset_chunks(self, dataset_id: str) -> None:
        query = text("DELETE FROM dataset_chunks WHERE dataset_id = :dataset_id")
        await self._session.execute(query, {"dataset_id": dataset_id})
        await self._session.commit()
