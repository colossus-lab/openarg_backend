from __future__ import annotations

import logging
import re
import time
from collections.abc import Sequence
from typing import Any

from sqlalchemy import Row, TextClause, text

from app.domain.ports.search.vector_search import IVectorSearch, SearchResult
from app.infrastructure.persistence_sqla.provider import MainAsyncSession

logger = logging.getLogger(__name__)


class PgVectorSearchAdapter(IVectorSearch):
    _RETRIEVAL_VECTOR_ONLY = "vector_only"
    _RETRIEVAL_HYBRID_FULL = "hybrid_full"

    # hnsw.ef_search for every ANN search, whatever the limit.
    #
    # It was max(200, limit*10), 200 for the MCP and the agent, and at 200
    # the walk got stuck in clusters of near-identical documents built from
    # one template (Córdoba municipalities' "Transparencia activa", one
    # "Presupuesto APN" per year): on 2026-10-04 prod's "salario mínimo vital
    # y móvil" topped at 0.517 with five Córdoba municipalities while the
    # exact search puts SMVM first (0.746).
    #
    # #131 raised it to 1000, and at 1000 there was no walk at all. pgvector's
    # cost estimate grows with ef_search, and past ~400 on staging (76k
    # chunks) and ~600 on prod (102k) the planner prefers a parallel seq scan
    # of dataset_chunks (EXPLAIN, 2026-10-05): the "index search" read the
    # same 350k buffers as the exact one. Its recall of 1.0 was brute force,
    # at 2.2 s median with 8 concurrent searches on staging (69-86 ms at 200)
    # and 10.1 s cold on prod.
    #
    # 300 is under both thresholds, and the walk no longer depends on them
    # (``_walk_index`` keeps it on the index). With the iterative scan the
    # width that matters is the candidates', not the frontier's: with the
    # index forced, recall@10 against the exact search over the canary's 20
    # queries on staging was 0.915 at ef_search 40 and the same at 1000
    # (2026-10-05).
    _ANN_EF_SEARCH = 300
    # Chunks fetched through the index before grouping by dataset (about 3
    # per dataset, and the MCP asks for up to 100 datasets). With the
    # iterative scan the walk goes on past ef_search until it has this many,
    # up to hnsw.max_scan_tuples (20,000 by default); without it (pgvector
    # before 0.8) one scan stops at ef_search.
    _ANN_CANDIDATES = 1000
    # A best score under this is not trusted and the exact search runs
    # instead. With Cohere v3 an unrelated dataset scores 0.50-0.57 and a
    # genuine match 0.60-0.77; the trapped walks above topped at 0.48-0.52.
    # Weak queries ("dólar oficial" 0.557) stay just above it, so the exact
    # search is the exception and not the rule.
    _ANN_WEAK_TOP_SCORE = 0.55

    # pgvector version, read once per process: iterative index scans exist
    # from 0.8.0 on.
    _pgvector_version: tuple[int, ...] | None = None

    # Portals in the catalogue, read at most once an hour per process.
    _PORTALS_TTL_S = 3600
    _portals: tuple[float, list[str]] | None = None

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

    async def known_portals(self) -> list[str]:
        """Distinct `datasets.portal` values (38 in prod), cached per process.

        Only asked when a portal-filtered search comes back empty: an unknown
        portal ("INDEC", "datos.gob.ar") empties the filter and the search
        used to answer an empty list with a 200, which the MCP showed as "no
        encontré datasets".
        """
        cls = type(self)
        now = time.monotonic()
        if cls._portals is None or now - cls._portals[0] > self._PORTALS_TTL_S:
            rows = await self._session.execute(
                text("SELECT DISTINCT portal FROM datasets WHERE portal IS NOT NULL ORDER BY 1")
            )
            cls._portals = (now, [str(r[0]) for r in rows.fetchall()])
        return list(cls._portals[1])

    async def search_datasets_ann(
        self,
        query_embedding: list[float],
        limit: int = 10,
        portal_filter: str | None = None,
        min_similarity: float = 0.40,
    ) -> list[SearchResult]:
        """Nearest datasets: the HNSW index, checked, with the exact search behind it.

        What every caller uses (``/catalogo/buscar`` for the MCP, the agent's
        ``buscar_datos``, ``/data/search``). The index answers first
        (``search_datasets_hnsw``, a walk of the graph and never a scan of
        the table); its answer is distrusted, and the exact search
        (``search_datasets_exact``, the only one that reads every chunk) runs
        instead, when it brings fewer datasets than asked for or its best
        score is weak. A trapped walk does not come back empty: it comes back
        full of the wrong neighbours with low scores, which is the signal
        checked here.

        Before pgvector 0.8 there is no iterative scan, and an index scan
        filtered by portal can come back empty for a small portal; with a
        portal filter the exact search, which the filter keeps small, runs
        directly.
        """
        if portal_filter and not await self._supports_iterative_scan():
            return await self.search_datasets_exact(
                query_embedding, limit, portal_filter, min_similarity
            )

        t0 = time.perf_counter()
        hits = await self.search_datasets_hnsw(
            query_embedding, limit, portal_filter, min_similarity
        )
        hnsw_ms = (time.perf_counter() - t0) * 1000
        reason = self._distrust_reason(hits, limit)
        if reason is None:
            logger.info(
                "search_datasets_ann: hnsw hits=%d top=%.3f ms=%.0f",
                len(hits),
                hits[0].score,
                hnsw_ms,
            )
            return hits

        t1 = time.perf_counter()
        exact = await self.search_datasets_exact(
            query_embedding, limit, portal_filter, min_similarity
        )
        logger.info(
            "search_datasets_ann: hnsw→exacta (%s) hits=%d→%d top=%.3f→%.3f hnsw_ms=%.0f exact_ms=%.0f",
            reason,
            len(hits),
            len(exact),
            hits[0].score if hits else 0.0,
            exact[0].score if exact else 0.0,
            hnsw_ms,
            (time.perf_counter() - t1) * 1000,
        )
        return exact

    @classmethod
    def _distrust_reason(cls, hits: list[SearchResult], limit: int) -> str | None:
        """Why the index's answer should not be served, or ``None`` if it can."""
        if len(hits) < limit:
            return "pocos"
        if hits[0].score < cls._ANN_WEAK_TOP_SCORE:
            return "puntaje bajo"
        return None

    async def search_datasets_hnsw(
        self,
        query_embedding: list[float],
        limit: int = 10,
        portal_filter: str | None = None,
        min_similarity: float = 0.40,
    ) -> list[SearchResult]:
        """Approximate nearest neighbours through the HNSW index, unchecked.

        ``ORDER BY embedding <=> q LIMIT n`` is what the index answers; a
        similarity threshold in the WHERE clause is not, and turns the search
        into a scan of every chunk (staging, 76k chunks: 9-16 s and up to the
        60 s statement timeout for ``search_datasets``). Grouping by dataset
        and the threshold apply afterwards.

        Two pgvector settings make the index return what the query asks for:

        - ``hnsw.ef_search`` is the size of the walk's frontier and, without
          an iterative scan, the ceiling on the rows one index scan returns
          (40 by default). See ``_ANN_EF_SEARCH``.
        - ``hnsw.iterative_scan`` keeps scanning when the first pass falls
          short. It is what brings the candidates past ef_search (1000
          chunks against a frontier of 300), and with a portal filter what
          finds the portal's chunks behind closer ones of other portals
          (filtering after the fetch returned nothing for caba and
          neuquen_legislatura even with 1000 candidates).

        Both are set with ``is_local`` and end with the transaction. The walk
        itself runs through ``_walk_index``, which keeps the planner off a
        seq scan. Public so the recall canary can compare the index alone
        against the exact search; callers that serve results use
        ``search_datasets_ann``.
        """
        await self._session.execute(
            text("SELECT set_config('hnsw.ef_search', :ef, true)"),
            {"ef": str(self._ANN_EF_SEARCH)},
        )
        if await self._supports_iterative_scan():
            await self._session.execute(
                text("SELECT set_config('hnsw.iterative_scan', 'relaxed_order', true)")
            )
        params: dict = {
            "embedding": self._literal(query_embedding),
            "candidates": self._ANN_CANDIDATES,
            "min_sim": min_similarity,
            "limit": limit,
        }
        portal_clause = ""
        if portal_filter:
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
            "), best AS ("
            " SELECT nn.dataset_id, min(nn.dist) AS dist"
            " FROM nn"
            " GROUP BY nn.dataset_id"
            " HAVING 1 - min(nn.dist) >= :min_sim"
            " ORDER BY min(nn.dist), nn.dataset_id"
            " LIMIT :limit"
            ")"
            f"{self._RESULT_SELECT}"
        )
        return self._results(await self._walk_index(query, params))

    async def _walk_index(self, query: TextClause, params: dict) -> Sequence[Row[Any]]:
        """Run the index walk with sequential scans off, then put them back.

        Reading every chunk is ``search_datasets_exact``'s job, and it runs
        only when the index's answer is distrusted; the walk must not do it
        on its own. Left to the planner it does: the choice between the
        index and a parallel seq scan moves with ef_search and with the
        table's size (1000 crossed it on staging and prod, see
        ``_ANN_EF_SEARCH``), and the recall canary then compares the exact
        search against itself. With a portal filter the planner still picks
        between the index and the btree on ``dataset_id``; only the scan of
        the whole table is off.

        ``enable_seqscan`` goes back to the value it had right after the
        fetch, before anything else runs in the transaction: the agent shares
        it across a whole turn, and the exact search has to scan. A failed or
        cancelled walk skips that, and the rollback the transaction needs
        anyway (``reset``) undoes the local setting with it.
        """
        before = (
            await self._session.execute(text("SELECT current_setting('enable_seqscan')"))
        ).scalar()
        await self._session.execute(text("SELECT set_config('enable_seqscan', 'off', true)"))
        rows = (await self._session.execute(query, params)).fetchall()
        await self._session.execute(
            text("SELECT set_config('enable_seqscan', :before, true)"),
            {"before": str(before or "on")},
        )
        return rows

    async def search_datasets_exact(
        self,
        query_embedding: list[float],
        limit: int = 10,
        portal_filter: str | None = None,
        min_similarity: float = 0.40,
    ) -> list[SearchResult]:
        """The true nearest datasets: every chunk, by its distance to the query.

        Grouping by dataset with ``min(distance)`` is a shape the HNSW index
        cannot answer, so Postgres scans the table; there is no need for
        ``SET LOCAL enable_indexscan = off``, which would stay on for the rest
        of a transaction the agent shares across a whole turn. Measured on
        2026-10-04: 0.45 s on staging (76k chunks, p95 0.73 s) and 0.71-0.77 s
        on prod (112k, parallel seq scan); cold on prod the same scan took
        10.1 s and read ~885 MB (2026-10-05), and with 8 at once on staging
        2.2 s median. That is why it runs only behind the index. Unlike
        ``search_datasets`` it keeps the threshold out of the WHERE and does
        not window over every chunk.
        """
        params: dict = {
            "embedding": self._literal(query_embedding),
            "min_sim": min_similarity,
            "limit": limit,
        }
        portal_clause = ""
        if portal_filter:
            portal_clause = (
                " WHERE dc.dataset_id IN (SELECT id FROM datasets WHERE portal = :portal)"
            )
            params["portal"] = portal_filter
        query = text(
            "WITH best AS ("
            " SELECT dc.dataset_id, min(dc.embedding <=> CAST(:embedding AS vector)) AS dist"
            " FROM dataset_chunks dc"
            f"{portal_clause}"
            " GROUP BY dc.dataset_id"
            " HAVING 1 - min(dc.embedding <=> CAST(:embedding AS vector)) >= :min_sim"
            " ORDER BY 2, dc.dataset_id"
            " LIMIT :limit"
            ")"
            f"{self._RESULT_SELECT}"
        )
        return self._results((await self._session.execute(query, params)).fetchall())

    # Shared tail of the HNSW and exact searches: only the `limit` datasets
    # that survived are joined, not every chunk the scan touched.
    _RESULT_SELECT = (
        " SELECT CAST(d.id AS text) AS dataset_id, d.title, d.description, d.portal,"
        "        d.download_url, d.columns, 1 - best.dist AS score"
        " FROM best JOIN datasets d ON d.id = best.dataset_id"
        " ORDER BY best.dist, best.dataset_id"
    )

    @staticmethod
    def _literal(query_embedding: list[float]) -> str:
        return "[" + ",".join(str(v) for v in query_embedding) + "]"

    @staticmethod
    def _results(rows: Sequence[Row[Any]]) -> list[SearchResult]:
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
