from __future__ import annotations

import re

from sqlalchemy import text

from app.domain.ports.search.vector_search import IVectorSearch, SearchResult
from app.infrastructure.persistence_sqla.provider import MainAsyncSession


class PgVectorSearchAdapter(IVectorSearch):
    _RETRIEVAL_VECTOR_ONLY = "vector_only"
    _RETRIEVAL_HYBRID_FULL = "hybrid_full"

    # Chunks fetched through the HNSW index before grouping by dataset. The
    # catalog repeats datasets (the datos.gob.ar migration regenerated ids),
    # so it takes a few hundred chunks to yield a few dozen distinct ones.
    _ANN_MIN_CANDIDATES = 200
    _ANN_CANDIDATES_PER_RESULT = 10
    # pgvector's ceiling for hnsw.ef_search.
    _ANN_MAX_CANDIDATES = 1000

    # pgvector version, read once per process: iterative index scans (needed
    # to filter by portal inside the index scan) exist from 0.8.0 on.
    _pgvector_version: tuple[int, ...] | None = None

    def __init__(self, session: MainAsyncSession) -> None:
        self._session = session

    async def reset(self) -> None:
        """Rollback the request session after a failed or cancelled query.

        A query cancelled mid-flight (an agent tool timeout) leaves the session
        in an invalid transaction, and every later query in the same request
        fails with ``PendingRollbackError`` until someone rolls it back.
        """
        try:
            await self._session.rollback()
        except Exception:  # noqa: BLE001 — best effort; the caller already failed
            pass

    async def _supports_iterative_scan(self) -> bool:
        cls = type(self)
        if cls._pgvector_version is None:
            raw = (
                await self._session.execute(
                    text("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
                )
            ).scalar()
            cls._pgvector_version = tuple(int(p) for p in re.findall(r"\d+", str(raw or "0")))
        return cls._pgvector_version >= (0, 8)

    async def search_datasets_ann(
        self,
        query_embedding: list[float],
        limit: int = 10,
        portal_filter: str | None = None,
        min_similarity: float = 0.40,
    ) -> list[SearchResult]:
        """Approximate nearest neighbours through the HNSW index.

        ``ORDER BY embedding <=> q LIMIT n`` is what the index answers; a
        similarity threshold in the WHERE clause is not, and turns the search
        into a scan of every chunk (staging, 76k chunks: 9-16 s and up to the
        60 s statement timeout for ``search_datasets``, 0.3-1.6 s here, same
        top 20). Grouping by dataset and the threshold apply afterwards.

        Two pgvector settings make the index return what the query asks for:

        - ``hnsw.ef_search`` caps how many rows one index scan returns (40 by
          default). Left at 40, ``LIMIT 200`` yields 40 chunks, and for
          "personas con discapacidad" none of the right datasets.
        - With a portal filter, ``hnsw.iterative_scan`` keeps scanning until
          enough chunks of that portal turn up. Filtering after the fetch
          instead returned nothing for small portals (caba, neuquen_legislatura)
          even with 1000 candidates. Before pgvector 0.8 there is no iterative
          scan; the exact search, which a portal filter keeps small, is used.

        Both are set with ``is_local`` and end with the transaction.
        """
        if portal_filter and not await self._supports_iterative_scan():
            return await self.search_datasets(query_embedding, limit, portal_filter, min_similarity)

        candidates = min(
            max(self._ANN_MIN_CANDIDATES, limit * self._ANN_CANDIDATES_PER_RESULT),
            self._ANN_MAX_CANDIDATES,
        )
        await self._session.execute(
            text("SELECT set_config('hnsw.ef_search', :ef, true)"), {"ef": str(candidates)}
        )
        embedding_str = "[" + ",".join(str(v) for v in query_embedding) + "]"
        params: dict = {
            "embedding": embedding_str,
            "candidates": candidates,
            "min_sim": min_similarity,
            "limit": limit,
        }
        portal_clause = ""
        if portal_filter:
            await self._session.execute(
                text("SELECT set_config('hnsw.iterative_scan', 'relaxed_order', true)")
            )
            portal_clause = (
                " WHERE dc.dataset_id IN (SELECT id FROM datasets WHERE portal = :portal)"
            )
            params["portal"] = portal_filter

        query = text(
            "WITH nn AS ("
            " SELECT dc.dataset_id, dc.embedding <=> CAST(:embedding AS vector) AS dist"
            " FROM dataset_chunks dc"
            f"{portal_clause}"
            " ORDER BY dc.embedding <=> CAST(:embedding AS vector)"
            " LIMIT :candidates"
            ")"
            " SELECT CAST(d.id AS text) AS dataset_id, d.title, d.description, d.portal,"
            "        d.download_url, d.columns, 1 - min(nn.dist) AS score"
            " FROM nn JOIN datasets d ON d.id = nn.dataset_id"
            " GROUP BY d.id"
            " HAVING 1 - min(nn.dist) >= :min_sim"
            " ORDER BY score DESC"
            " LIMIT :limit"
        )
        result = await self._session.execute(query, params)
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
            for row in result.fetchall()
        ]

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
            "WITH query_input AS ("
            " SELECT CAST(:embedding AS vector) AS embedding"
            "), ranked_chunks AS ("
            " SELECT"
            "   CAST(d.id AS text) AS dataset_id,"
            "   d.title, d.description, d.portal, d.download_url, d.columns,"
            "   1 - (dc.embedding <=> qi.embedding) AS score,"
            "   ROW_NUMBER() OVER ("
            "     PARTITION BY d.id"
            "     ORDER BY dc.embedding <=> qi.embedding"
            "   ) AS dataset_rank"
            " FROM dataset_chunks dc"
            " JOIN datasets d ON d.id = dc.dataset_id"
            " CROSS JOIN query_input qi"
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
                        " AND 1 - (dc.embedding <=> qi.embedding) >= :min_sim"
                    )
                )
            )
        else:
            query = text(
                base_query.format(
                    where_clause="WHERE 1 - (dc.embedding <=> qi.embedding) >= :min_sim"
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

    @staticmethod
    def _has_lexical_signal(query_text: str) -> bool:
        """Return whether BM25 is likely to add value for this query."""
        tokens = re.findall(r"\w+", query_text.lower())
        meaningful = [token for token in tokens if len(token) >= 3]
        return len(meaningful) >= 2

    @staticmethod
    def _meaningful_term_count(query_text: str) -> int:
        """Count lexical terms that are likely to help BM25 ranking."""
        return sum(1 for token in re.findall(r"\w+", query_text.lower()) if len(token) >= 3)

    @classmethod
    def _hybrid_fetch_limit(cls, retrieval_mode: str, limit: int) -> int:
        """Scale candidate fetch size to the chosen retrieval mode."""
        if retrieval_mode == cls._RETRIEVAL_VECTOR_ONLY:
            multiplier = 2
        else:
            multiplier = 6
        return limit * multiplier

    @classmethod
    def _retrieval_mode(cls, query_text: str) -> str:
        """Pick the retrieval strategy that best matches the query signal."""
        term_count = cls._meaningful_term_count(query_text)
        if term_count < 2:
            return cls._RETRIEVAL_VECTOR_ONLY
        return cls._RETRIEVAL_HYBRID_FULL

    @classmethod
    def _bm25_rrf_weight(cls, retrieval_mode: str) -> float:
        """Scale BM25 contribution based on retrieval mode."""
        return 1.0

    @classmethod
    def _vector_candidate_min_score(cls, retrieval_mode: str) -> float:
        """Drop clearly weak vector candidates before ranking/fusion."""
        if retrieval_mode == cls._RETRIEVAL_VECTOR_ONLY:
            return 0.20
        return 0.10

    @classmethod
    def _bm25_candidate_min_score(cls, retrieval_mode: str) -> float:
        """Drop clearly weak BM25 candidates before ranking/fusion."""
        return 0.01

    @classmethod
    def _effective_min_score(
        cls, retrieval_mode: str, requested_min_score: float, rrf_k: int
    ) -> float:
        """Keep the final threshold realistic for the selected scoring mode."""
        if retrieval_mode == cls._RETRIEVAL_VECTOR_ONLY:
            return requested_min_score

        bm25_weight = cls._bm25_rrf_weight(retrieval_mode)
        theoretical_max_rrf = (1.0 + bm25_weight) / (rrf_k + 1)
        # Leave headroom so good hybrid matches are not filtered out by an unreachable default.
        hybrid_cap = theoretical_max_rrf * 0.4
        return min(requested_min_score, hybrid_cap)

    @staticmethod
    def _retry_result_threshold(limit: int) -> int:
        """Minimum useful result count before escalating retrieval cost."""
        return min(limit, 1)

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
        initial_mode = self._retrieval_mode(query_text)

        _VECTOR_ONLY_BASE = (
            "WITH query_input AS ("
            " SELECT CAST(:embedding AS vector) AS embedding"
            " ), vector_candidates AS ("
            " SELECT CAST(d.id AS text) AS dataset_id,"
            " d.title, d.description, d.portal, d.download_url, d.columns,"
            " 1 - (dc.embedding <=> qi.embedding) AS score,"
            " ROW_NUMBER() OVER ("
            "   PARTITION BY d.id"
            "   ORDER BY dc.embedding <=> qi.embedding"
            " ) AS dataset_rank"
            " FROM dataset_chunks dc"
            " JOIN datasets d ON d.id = dc.dataset_id"
            " CROSS JOIN query_input qi"
            " {where_vec}"
            "), ranked AS ("
            " SELECT dataset_id, title, description, portal, download_url, columns, score"
            " FROM vector_candidates"
            " WHERE dataset_rank = 1"
            " ORDER BY score DESC"
            " LIMIT :fetch_limit"
            ")"
            " SELECT dataset_id, title, description, portal, download_url, columns, score"
            " FROM ranked WHERE score >= :min_score ORDER BY score DESC LIMIT :limit"
        )

        _HYBRID_BASE = (
            "WITH query_input AS ("
            " SELECT CAST(:embedding AS vector) AS embedding,"
            " websearch_to_tsquery('spanish', :query_text) AS ts_query"
            " ), vector_candidates AS ("
            " SELECT CAST(d.id AS text) AS dataset_id,"
            " d.title, d.description, d.portal, d.download_url, d.columns,"
            " 1 - (dc.embedding <=> qi.embedding) AS vec_score,"
            " ROW_NUMBER() OVER ("
            "   PARTITION BY d.id"
            "   ORDER BY dc.embedding <=> qi.embedding"
            " ) AS dataset_rank"
            " FROM dataset_chunks dc"
            " JOIN datasets d ON d.id = dc.dataset_id"
            " CROSS JOIN query_input qi"
            " {where_vec}"
            "), vector_ranked AS ("
            " SELECT dataset_id, title, description, portal, download_url, columns, vec_score,"
            " ROW_NUMBER() OVER (ORDER BY vec_score DESC) AS vec_rank"
            " FROM vector_candidates"
            " WHERE dataset_rank = 1"
            " ORDER BY vec_score DESC"
            " LIMIT :fetch_limit"
            "), bm25_candidates AS ("
            " SELECT CAST(d.id AS text) AS dataset_id,"
            " d.title, d.description, d.portal, d.download_url, d.columns,"
            " ts_rank_cd(dc.tsv, qi.ts_query) AS bm25_score,"
            " ROW_NUMBER() OVER ("
            "   PARTITION BY d.id"
            "   ORDER BY ts_rank_cd(dc.tsv, qi.ts_query) DESC"
            " ) AS dataset_rank"
            " FROM dataset_chunks dc"
            " JOIN datasets d ON d.id = dc.dataset_id"
            " CROSS JOIN query_input qi"
            " WHERE dc.tsv @@ qi.ts_query"
            " AND ts_rank_cd(dc.tsv, qi.ts_query) >= :bm25_candidate_min"
            " {and_portal}"
            "), bm25_ranked AS ("
            " SELECT dataset_id, title, description, portal, download_url, columns, bm25_score,"
            " ROW_NUMBER() OVER (ORDER BY bm25_score DESC) AS bm25_rank"
            " FROM bm25_candidates"
            " WHERE dataset_rank = 1"
            " ORDER BY bm25_score DESC"
            " LIMIT :fetch_limit"
            "), scored_candidates AS ("
            " SELECT"
            "   dataset_id, title, description, portal, download_url, columns,"
            "   1.0 / (:rrf_k + vec_rank) AS score_contrib"
            " FROM vector_ranked"
            " UNION ALL"
            " SELECT"
            "   dataset_id, title, description, portal, download_url, columns,"
            "   :bm25_weight / (:rrf_k + bm25_rank) AS score_contrib"
            " FROM bm25_ranked"
            "), fused AS ("
            " SELECT"
            "   dataset_id,"
            "   MAX(title) AS title,"
            "   MAX(description) AS description,"
            "   MAX(portal) AS portal,"
            "   MAX(download_url) AS download_url,"
            "   MAX(columns) AS columns,"
            "   SUM(score_contrib) AS rrf_score"
            " FROM scored_candidates"
            " GROUP BY dataset_id"
            ")"
            " SELECT dataset_id, title, description, portal, download_url, columns, rrf_score AS score"
            " FROM fused WHERE rrf_score >= :min_score ORDER BY rrf_score DESC LIMIT :limit"
        )

        async def _run_mode(retrieval_mode: str) -> list[SearchResult]:
            has_lexical_signal = retrieval_mode != self._RETRIEVAL_VECTOR_ONLY
            effective_min_score = self._effective_min_score(retrieval_mode, min_score, rrf_k)
            params: dict = {
                "embedding": embedding_str,
                "limit": limit,
                "fetch_limit": self._hybrid_fetch_limit(retrieval_mode, limit),
                "rrf_k": rrf_k,
                "min_score": effective_min_score,
                "vec_candidate_min": self._vector_candidate_min_score(retrieval_mode),
            }
            if has_lexical_signal:
                params["query_text"] = query_text
                params["bm25_weight"] = self._bm25_rrf_weight(retrieval_mode)
                params["bm25_candidate_min"] = self._bm25_candidate_min_score(retrieval_mode)

            if not has_lexical_signal:
                if portal_filter:
                    params["portal"] = portal_filter
                    query = text(
                        _VECTOR_ONLY_BASE.format(
                            where_vec=(
                                "WHERE d.portal = :portal"
                                " AND 1 - (dc.embedding <=> qi.embedding) >= :vec_candidate_min"
                            )
                        )
                    )
                else:
                    query = text(
                        _VECTOR_ONLY_BASE.format(
                            where_vec="WHERE 1 - (dc.embedding <=> qi.embedding) >= :vec_candidate_min"
                        )
                    )
            elif portal_filter:
                params["portal"] = portal_filter
                query = text(
                    _HYBRID_BASE.format(
                        where_vec=(
                            "WHERE d.portal = :portal"
                            " AND 1 - (dc.embedding <=> qi.embedding) >= :vec_candidate_min"
                        ),
                        and_portal="AND d.portal = :portal",
                    )
                )
            else:
                query = text(
                    _HYBRID_BASE.format(
                        where_vec="WHERE 1 - (dc.embedding <=> qi.embedding) >= :vec_candidate_min",
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
                if float(row.score) >= effective_min_score
            ]

        results = await _run_mode(initial_mode)
        if len(results) < self._retry_result_threshold(limit):
            if initial_mode == self._RETRIEVAL_VECTOR_ONLY:
                results = await _run_mode(self._RETRIEVAL_HYBRID_FULL)

        return results

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
