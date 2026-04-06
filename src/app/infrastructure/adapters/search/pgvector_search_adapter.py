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
        min_similarity: float = 0.55,
    ) -> list[SearchResult]:
        embedding_str = "[" + ",".join(str(v) for v in query_embedding) + "]"

        params: dict = {"embedding": embedding_str, "limit": limit, "min_sim": min_similarity}
        base_query = (
            "WITH ranked_chunks AS ("
            " SELECT"
            "   CAST(d.id AS text) AS dataset_id,"
            "   d.title, d.description, d.portal, d.download_url, d.columns,"
            "   1 - (dc.embedding <=> CAST(:embedding AS vector)) AS score,"
            "   ROW_NUMBER() OVER ("
            "     PARTITION BY d.id"
            "     ORDER BY dc.embedding <=> CAST(:embedding AS vector)"
            "   ) AS dataset_rank"
            " FROM dataset_chunks dc"
            " JOIN datasets d ON d.id = dc.dataset_id"
            " {where_clause}"
            ")"
            " SELECT dataset_id, title, description, portal, download_url, columns, score"
            " FROM ranked_chunks"
            " WHERE dataset_rank = 1"
            " ORDER BY score DESC"
            " LIMIT :limit"
        )
        if portal_filter:
            params["portal"] = portal_filter
            query = text(
                base_query.format(
                    where_clause=(
                        "WHERE d.portal = :portal"
                        " AND 1 - (dc.embedding <=> CAST(:embedding AS vector)) >= :min_sim"
                    )
                )
            )
        else:
            query = text(
                base_query.format(
                    where_clause="WHERE 1 - (dc.embedding <=> CAST(:embedding AS vector)) >= :min_sim"
                )
            )

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

    @staticmethod
    def _sanitize_tsquery_input(query_text: str) -> str:
        """Sanitize input for websearch_to_tsquery to prevent syntax errors.

        Balances unclosed quotes and strips characters that cause parse failures.
        """
        # Balance unclosed double quotes
        if query_text.count('"') % 2 != 0:
            query_text = query_text.replace('"', "")
        return query_text.strip()

    async def search_datasets_hybrid(
        self,
        query_embedding: list[float],
        query_text: str,
        limit: int = 10,
        portal_filter: str | None = None,
        rrf_k: int = 60,
        min_score: float = 0.05,
    ) -> list[SearchResult]:
        """Hybrid search combining vector cosine similarity and BM25 full-text search with RRF fusion."""
        embedding_str = "[" + ",".join(str(v) for v in query_embedding) + "]"
        query_text = self._sanitize_tsquery_input(query_text)

        params: dict = {
            "embedding": embedding_str,
            "query_text": query_text,
            "limit": limit,
            "fetch_limit": limit * 4,  # fetch more candidates for fusion
            "rrf_k": rrf_k,
        }

        _HYBRID_BASE = (
            "WITH vector_ranked AS ("
            " SELECT CAST(d.id AS text) AS dataset_id,"
            " d.title, d.description, d.portal, d.download_url, d.columns,"
            " 1 - (dc.embedding <=> CAST(:embedding AS vector)) AS vec_score,"
            " ROW_NUMBER() OVER (ORDER BY dc.embedding <=> CAST(:embedding AS vector)) AS vec_rank"
            " FROM dataset_chunks dc"
            " JOIN datasets d ON d.id = dc.dataset_id"
            " {where_vec}"
            " ORDER BY dc.embedding <=> CAST(:embedding AS vector)"
            " LIMIT :fetch_limit"
            "), bm25_ranked AS ("
            " SELECT CAST(d.id AS text) AS dataset_id,"
            " d.title, d.description, d.portal, d.download_url, d.columns,"
            " ts_rank_cd(dc.tsv, websearch_to_tsquery('spanish', :query_text)) AS bm25_score,"
            " ROW_NUMBER() OVER ("
            "   ORDER BY ts_rank_cd(dc.tsv, websearch_to_tsquery('spanish', :query_text)) DESC"
            " ) AS bm25_rank"
            " FROM dataset_chunks dc"
            " JOIN datasets d ON d.id = dc.dataset_id"
            " WHERE dc.tsv @@ websearch_to_tsquery('spanish', :query_text)"
            " {and_portal}"
            " ORDER BY bm25_score DESC"
            " LIMIT :fetch_limit"
            "), fused AS ("
            " SELECT"
            "   COALESCE(v.dataset_id, b.dataset_id) AS dataset_id,"
            "   COALESCE(v.title, b.title) AS title,"
            "   COALESCE(v.description, b.description) AS description,"
            "   COALESCE(v.portal, b.portal) AS portal,"
            "   COALESCE(v.download_url, b.download_url) AS download_url,"
            "   COALESCE(v.columns, b.columns) AS columns,"
            "   COALESCE(1.0 / (:rrf_k + v.vec_rank), 0) +"
            "   COALESCE(1.0 / (:rrf_k + b.bm25_rank), 0) AS rrf_score"
            " FROM vector_ranked v"
            " FULL OUTER JOIN bm25_ranked b ON v.dataset_id = b.dataset_id"
            ")"
            " SELECT dataset_id, title, description, portal, download_url, columns, rrf_score AS score"
            " FROM fused ORDER BY rrf_score DESC LIMIT :limit"
        )

        if portal_filter:
            params["portal"] = portal_filter
            query = text(
                _HYBRID_BASE.format(
                    where_vec="WHERE d.portal = :portal",
                    and_portal="AND d.portal = :portal",
                )
            )
        else:
            query = text(
                _HYBRID_BASE.format(
                    where_vec="",
                    and_portal="",
                )
            )

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
            if float(row.score) >= min_score
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
