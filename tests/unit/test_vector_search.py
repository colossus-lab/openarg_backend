from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter


@pytest.fixture
def mock_session():
    session = AsyncMock()
    return session


@pytest.fixture
def adapter(mock_session):
    return PgVectorSearchAdapter(mock_session)


class TestPgVectorSearchAdapter:
    async def test_search_returns_empty_list_when_no_results(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        results = await adapter.search_datasets([0.1] * 1536, limit=5)
        assert results == []

    async def test_search_passes_embedding_and_limit(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        embedding = [0.5] * 1536
        await adapter.search_datasets(embedding, limit=3)

        call_args = mock_session.execute.call_args
        params = call_args[0][1]
        assert params["limit"] == 3
        assert "0.5" in params["embedding"]

    async def test_search_with_portal_filter(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets([0.1] * 1536, portal_filter="caba")

        call_args = mock_session.execute.call_args
        params = call_args[0][1]
        assert params["portal"] == "caba"

    async def test_search_deduplicates_by_dataset_in_sql(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets([0.1] * 1536, limit=5)

        call_args = mock_session.execute.call_args
        query = str(call_args[0][0])
        assert "query_input AS" in query
        assert "CROSS JOIN query_input qi" in query
        assert "ROW_NUMBER() OVER (" in query
        assert "PARTITION BY d.id" in query
        assert "WHERE dataset_rank = 1" in query

    async def test_search_maps_rows_to_search_results(self, adapter, mock_session):
        mock_row = MagicMock()
        mock_row.dataset_id = "abc-123"
        mock_row.title = "Test Dataset"
        mock_row.description = "Test description"
        mock_row.portal = "datos_gob_ar"
        mock_row.download_url = "https://example.com/data.csv"
        mock_row.columns = '["col1", "col2"]'
        mock_row.score = 0.85

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [mock_row]
        mock_session.execute.return_value = mock_result

        results = await adapter.search_datasets([0.1] * 1536)
        assert len(results) == 1
        assert results[0].dataset_id == "abc-123"
        assert results[0].title == "Test Dataset"
        assert results[0].score == 0.85

    async def test_delete_dataset_chunks(self, adapter, mock_session):
        await adapter.delete_dataset_chunks("abc-123")
        mock_session.execute.assert_awaited_once()
        mock_session.commit.assert_awaited_once()

    async def test_hybrid_search_deduplicates_both_rankings_by_dataset(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets_hybrid([0.1] * 1536, "inflacion cordoba", limit=5)

        call_args = mock_session.execute.call_args
        query = str(call_args[0][0])
        assert "query_input AS" in query
        assert "vector_candidates AS" in query
        assert "bm25_candidates AS" in query
        assert "scored_candidates AS" in query
        assert "UNION ALL" in query
        assert "GROUP BY dataset_id" in query
        assert "1 - (dc.embedding <=> qi.embedding) >= :vec_candidate_min" in query
        assert "ts_rank_cd(dc.tsv, qi.ts_query) >= :bm25_candidate_min" in query
        assert "WHERE rrf_score >= :min_score" in query
        assert "FULL OUTER JOIN" not in query
        assert "websearch_to_tsquery('spanish', :query_text) AS ts_query" in query
        assert "dc.tsv @@ qi.ts_query" in query
        assert query.count("PARTITION BY d.id") >= 2
        assert "WHERE dataset_rank = 1" in query

    async def test_hybrid_search_skips_bm25_when_query_lacks_lexical_signal(
        self, adapter, mock_session
    ):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets_hybrid([0.1] * 1536, "ipc", limit=5)

        assert mock_session.execute.await_count == 2
        first_query = str(mock_session.execute.await_args_list[0].args[0])
        first_params = mock_session.execute.await_args_list[0].args[1]
        assert "bm25_candidates AS" not in first_query
        assert "query_text" not in first_params
        assert "WHERE score >= :min_score" in first_query
        assert first_params["vec_candidate_min"] == 0.2
        assert first_params["fetch_limit"] == 10

    async def test_hybrid_search_keeps_bm25_for_richer_queries(self, adapter, mock_session):
        mock_row = MagicMock()
        mock_row.dataset_id = "1"
        mock_row.title = "Dataset"
        mock_row.description = ""
        mock_row.portal = "test"
        mock_row.download_url = ""
        mock_row.columns = ""
        mock_row.score = 0.1

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [mock_row]
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets_hybrid([0.1] * 1536, "inflacion cordoba", limit=5)

        call_args = mock_session.execute.await_args_list[0]
        params = call_args.args[1]
        query = str(call_args.args[0])
        assert "bm25_candidates AS" in query
        assert params["query_text"] == "inflacion cordoba"
        assert params["bm25_weight"] == 1.0
        assert params["vec_candidate_min"] == 0.1
        assert params["bm25_candidate_min"] == 0.01
        assert params["fetch_limit"] == 30
        assert mock_session.execute.await_count == 1

    async def test_hybrid_search_expands_fetch_limit_for_richer_queries(
        self, adapter, mock_session
    ):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets_hybrid(
            [0.1] * 1536,
            "inflacion cordoba empleo salarios",
            limit=5,
        )

        assert mock_session.execute.await_count == 1
        call_args = mock_session.execute.await_args_list[0]
        params = call_args.args[1]
        assert params["bm25_weight"] == 1.0
        assert params["vec_candidate_min"] == 0.1
        assert params["bm25_candidate_min"] == 0.01
        assert params["fetch_limit"] == 30

    async def test_hybrid_full_skips_vector_precheck_by_default(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets_hybrid(
            [0.1] * 1536,
            "inflacion cordoba empleo salarios",
            limit=5,
        )

        assert mock_session.execute.await_count == 1
        call_args = mock_session.execute.await_args_list[0]
        params = call_args.args[1]
        query = str(call_args.args[0])
        assert "bm25_candidates AS" in query
        assert params["query_text"] == "inflacion cordoba empleo salarios"
        assert params["bm25_weight"] == 1.0
        assert params["vec_candidate_min"] == 0.1
        assert params["bm25_candidate_min"] == 0.01
        assert params["fetch_limit"] == 30

    async def test_lexical_query_skips_vector_precheck(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets_hybrid([0.1] * 1536, "inflacion cordoba", limit=5)

        assert mock_session.execute.await_count == 1
        first_query = str(mock_session.execute.await_args_list[0].args[0])
        assert "bm25_candidates AS" in first_query

    async def test_hybrid_search_caps_min_score_to_reachable_rrf_range(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets_hybrid([0.1] * 1536, "inflacion cordoba", limit=5)

        params = mock_session.execute.await_args_list[0].args[1]
        assert params["min_score"] == pytest.approx(((1.0 + 1.0) / 61) * 0.4)

    async def test_vector_only_keeps_requested_min_score(self, adapter, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets_hybrid([0.1] * 1536, "ipc", limit=5)

        first_params = mock_session.execute.await_args_list[0].args[1]
        assert first_params["min_score"] == 0.05

    async def test_lexical_query_does_not_requery_when_results_are_sparse(
        self, adapter, mock_session
    ):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        await adapter.search_datasets_hybrid([0.1] * 1536, "inflacion cordoba", limit=5)

        assert mock_session.execute.await_count == 1
        params = mock_session.execute.await_args_list[0].args[1]
        assert params["bm25_weight"] == 1.0
        assert params["fetch_limit"] == 30

    async def test_lexical_query_returns_results_without_vector_fast_path(
        self, adapter, mock_session
    ):
        rows = []
        for idx in range(3):
            row = MagicMock()
            row.dataset_id = str(idx)
            row.title = f"Dataset {idx}"
            row.description = ""
            row.portal = "test"
            row.download_url = ""
            row.columns = ""
            row.score = 0.1
            rows.append(row)

        mock_result = MagicMock()
        mock_result.fetchall.return_value = rows
        mock_session.execute.return_value = mock_result

        results = await adapter.search_datasets_hybrid([0.1] * 1536, "inflacion cordoba", limit=5)

        assert len(results) == 3
        assert mock_session.execute.await_count == 1

    async def test_lexical_query_does_not_return_vector_fast_path_even_if_vector_scores_are_strong(
        self, adapter, mock_session
    ):
        rows = []
        for idx, score in enumerate((0.66, 0.61, 0.59, 0.57)):
            row = MagicMock()
            row.dataset_id = str(idx)
            row.title = f"Dataset {idx}"
            row.description = ""
            row.portal = "test"
            row.download_url = ""
            row.columns = ""
            row.score = score
            rows.append(row)

        mock_result = MagicMock()
        mock_result.fetchall.return_value = rows
        mock_session.execute.return_value = mock_result

        results = await adapter.search_datasets_hybrid([0.1] * 1536, "inflacion cordoba", limit=5)

        first_query = str(mock_session.execute.await_args_list[0].args[0])
        assert len(results) == 4
        assert "bm25_candidates AS" in first_query
        assert mock_session.execute.await_count == 1

    async def test_vector_only_escalates_to_hybrid_full_when_results_are_empty(
        self, adapter, mock_session
    ):
        mock_result_vector = MagicMock()
        mock_result_vector.fetchall.return_value = []
        mock_result_full = MagicMock()
        mock_result_full.fetchall.return_value = []
        mock_session.execute.side_effect = [mock_result_vector, mock_result_full]

        await adapter.search_datasets_hybrid([0.1] * 1536, "ipc", limit=5)

        assert mock_session.execute.await_count == 2
        first_params = mock_session.execute.await_args_list[0].args[1]
        second_params = mock_session.execute.await_args_list[1].args[1]
        first_query = str(mock_session.execute.await_args_list[0].args[0])
        second_query = str(mock_session.execute.await_args_list[1].args[0])
        assert "bm25_candidates AS" not in first_query
        assert "bm25_candidates AS" in second_query
        assert "query_text" not in first_params
        assert second_params["query_text"] == "ipc"
        assert second_params["bm25_weight"] == 1.0

    async def test_vector_only_does_not_escalate_when_it_already_has_results(
        self, adapter, mock_session
    ):
        row = MagicMock()
        row.dataset_id = "1"
        row.title = "Dataset"
        row.description = ""
        row.portal = "test"
        row.download_url = ""
        row.columns = ""
        row.score = 0.1

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [row]
        mock_session.execute.return_value = mock_result

        results = await adapter.search_datasets_hybrid([0.1] * 1536, "ipc", limit=5)

        assert len(results) == 1
        assert mock_session.execute.await_count == 1


def _row(i: int, score: float) -> SimpleNamespace:
    return SimpleNamespace(
        dataset_id=f"ds-{i}",
        title=f"Dataset {i}",
        description=None,
        portal="datos_gob_ar",
        download_url=None,
        columns=None,
        score=score,
    )


def _scripted_session(
    extversion: str = "0.8.1",
    rows: list | None = None,
    exact_rows: list | None = None,
) -> AsyncMock:
    """Session double that answers by statement.

    The version lookup gets ``extversion``; the HNSW search (the one with
    ``LIMIT :candidates``) gets ``rows``; the exact search (grouped over every
    chunk) gets ``exact_rows``.
    """
    session = AsyncMock()

    async def _execute(statement, params=None):
        sql = str(statement)
        result = MagicMock()
        if "pg_extension" in sql:
            result.scalar.return_value = extversion
        elif "LIMIT :candidates" in sql:
            result.fetchall.return_value = rows or []
        elif "GROUP BY dc.dataset_id" in sql:
            result.fetchall.return_value = exact_rows or []
        else:
            result.fetchall.return_value = []
        return result

    session.execute.side_effect = _execute
    return session


def _statements(session: AsyncMock) -> list[tuple[str, dict]]:
    return [
        (str(c.args[0]), c.args[1] if len(c.args) > 1 else {})
        for c in session.execute.await_args_list
    ]


def _hnsw_sql(session: AsyncMock) -> tuple[str, dict]:
    [stmt] = [(s, p) for s, p in _statements(session) if "LIMIT :candidates" in s]
    return stmt


def _exact_sqls(session: AsyncMock) -> list[tuple[str, dict]]:
    return [(s, p) for s, p in _statements(session) if "GROUP BY dc.dataset_id" in s]


# Enough strong hits for any limit used below: the index's answer is trusted.
_GOOD = [_row(i, 0.74 - i * 0.001) for i in range(40)]


class TestSearchDatasetsAnn:
    """``search_datasets_ann``: the HNSW index, walked wide enough to find what
    the exact search finds, and the exact search when its answer looks wrong."""

    @pytest.fixture(autouse=True)
    def _fresh_version_cache(self, monkeypatch):
        monkeypatch.setattr(PgVectorSearchAdapter, "_pgvector_version", None)

    async def test_orders_by_distance_with_limit_and_thresholds_afterwards(self):
        session = _scripted_session(rows=_GOOD)
        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        sql, params = _hnsw_sql(session)
        # The index answers ORDER BY distance LIMIT n ...
        assert "ORDER BY dc.embedding <=> CAST(:embedding AS vector) LIMIT :candidates" in " ".join(
            sql.split()
        )
        # ... and not a similarity predicate in the WHERE (that is a full scan).
        assert "WHERE 1 -" not in sql
        assert "HAVING 1 - min(nn.dist) >= :min_sim" in sql
        assert "GROUP BY nn.dataset_id" in sql
        assert params["min_sim"] == 0.40
        assert params["limit"] == 20
        assert _exact_sqls(session) == []

    @pytest.mark.parametrize("limit", [5, 16, 20, 40, 500])
    async def test_ef_search_is_pgvector_ceiling_whatever_the_limit(self, limit):
        """It was max(200, limit*10): 200 for the MCP (20) and the agent (16).
        At 200, prod's "salario mínimo vital y móvil" topped at 0.517 with five
        Córdoba municipalities while the exact search puts SMVM first (0.746)."""
        session = _scripted_session(rows=[_row(i, 0.7) for i in range(limit)])
        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=limit)

        stmts = _statements(session)
        assert [p["ef"] for s, p in stmts if "hnsw.ef_search" in s] == ["1000"]
        assert _hnsw_sql(session)[1]["candidates"] == 1000
        # Set inside the transaction only, and before the search runs.
        first_set = next(s for s, _ in stmts if "set_config" in s)
        assert "set_config('hnsw.ef_search', :ef, true)" in first_set

    async def test_iterative_scan_is_on_without_a_portal_too(self):
        session = _scripted_session(rows=_GOOD)
        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        sqls = [s for s, _ in _statements(session)]
        assert any("'hnsw.iterative_scan', 'relaxed_order', true" in s for s in sqls)
        assert ":portal" not in _hnsw_sql(session)[0]

    async def test_portal_filter_goes_inside_the_index_scan(self):
        """Filtering after fetching N neighbours returned nothing for small
        portals (caba, neuquen_legislatura) on staging, even with 1000."""
        session = _scripted_session(extversion="0.8.1", rows=_GOOD)
        await PgVectorSearchAdapter(session).search_datasets_ann(
            [0.1] * 8, limit=10, portal_filter="caba"
        )

        stmts = _statements(session)
        assert any("'hnsw.iterative_scan', 'relaxed_order', true" in s for s, _ in stmts)
        sql, params = _hnsw_sql(session)
        nn_cte = sql.split("), best AS (")[0]
        assert "WHERE dc.dataset_id IN (SELECT id FROM datasets WHERE portal = :portal)" in nn_cte
        assert params["portal"] == "caba"

    async def test_weak_best_score_runs_the_exact_search_and_serves_it(self):
        """A trapped walk comes back full, with the wrong neighbours: staging's
        "votaciones nominales" topped at 0.479 at ef_search=200 against 0.675."""
        trapped = [_row(100 + i, 0.479 - i * 0.001) for i in range(20)]
        exact = [_row(i, 0.675 - i * 0.001) for i in range(20)]
        session = _scripted_session(rows=trapped, exact_rows=exact)

        results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert [r.dataset_id for r in results] == [r.dataset_id for r in exact]
        [(sql, params)] = _exact_sqls(session)
        assert params["limit"] == 20 and params["min_sim"] == 0.40

    async def test_fewer_datasets_than_asked_runs_the_exact_search(self):
        session = _scripted_session(rows=_GOOD[:3], exact_rows=_GOOD[:10])

        results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=10)

        assert len(results) == 10
        assert len(_exact_sqls(session)) == 1

    async def test_trusted_answer_does_not_pay_for_the_exact_search(self):
        session = _scripted_session(rows=_GOOD[:10], exact_rows=_GOOD[:10])

        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=10)

        assert _exact_sqls(session) == []

    async def test_exact_search_is_a_shape_the_index_cannot_answer(self):
        """No `SET LOCAL enable_indexscan = off`: it would stay on for the rest
        of the transaction, which the agent shares across a whole turn."""
        session = _scripted_session()
        await PgVectorSearchAdapter(session).search_datasets_exact(
            [0.1] * 8, limit=7, portal_filter="caba", min_similarity=0.4
        )

        [(sql, params)] = _exact_sqls(session)
        flat = " ".join(sql.split())
        assert "min(dc.embedding <=> CAST(:embedding AS vector))" in flat
        assert "ORDER BY dc.embedding <=>" not in flat
        assert "WHERE 1 -" not in flat
        assert not any("enable_indexscan" in s for s, _ in _statements(session))
        assert "WHERE dc.dataset_id IN (SELECT id FROM datasets WHERE portal = :portal)" in flat
        assert (params["portal"], params["limit"], params["min_sim"]) == ("caba", 7, 0.4)

    async def test_portal_filter_goes_straight_to_the_exact_search_before_pgvector_0_8(self):
        session = _scripted_session(extversion="0.7.4")
        await PgVectorSearchAdapter(session).search_datasets_ann(
            [0.1] * 8, limit=7, portal_filter="caba", min_similarity=0.4
        )

        stmts = _statements(session)
        assert not any("iterative_scan" in s or "ef_search" in s for s, _ in stmts)
        [(sql, params)] = _exact_sqls(session)
        assert params["portal"] == "caba"
        assert params["limit"] == 7

    async def test_without_iterative_scan_and_without_portal_the_index_still_answers(self):
        session = _scripted_session(extversion="0.7.4", rows=_GOOD)
        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        sqls = [s for s, _ in _statements(session)]
        assert not any("iterative_scan" in s for s in sqls)
        assert _hnsw_sql(session)[1]["candidates"] == 1000

    async def test_pgvector_version_is_read_once_per_process(self):
        adapter_a = PgVectorSearchAdapter(_scripted_session(rows=_GOOD))
        await adapter_a.search_datasets_ann([0.1] * 8, portal_filter="caba")
        session_b = _scripted_session(rows=_GOOD)
        await PgVectorSearchAdapter(session_b).search_datasets_ann([0.1] * 8, portal_filter="caba")

        assert not any("pg_extension" in s for s, _ in _statements(session_b))

    async def test_maps_rows_to_search_results(self):
        row = SimpleNamespace(
            dataset_id="abc-123",
            title="Estudio Nacional sobre el Perfil de las Personas con Discapacidad",
            description=None,
            portal="datos_gob_ar",
            download_url=None,
            columns=None,
            score=0.674,
        )
        session = _scripted_session(rows=[row])
        results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=1)

        assert len(results) == 1
        r = results[0]
        assert (r.dataset_id, r.portal, r.score) == ("abc-123", "datos_gob_ar", 0.674)
        assert (r.description, r.download_url, r.columns) == ("", "", "")


class TestKnownPortals:
    @pytest.fixture(autouse=True)
    def _fresh_cache(self, monkeypatch):
        monkeypatch.setattr(PgVectorSearchAdapter, "_portals", None)

    async def test_reads_distinct_portals_once_per_process(self):
        session = AsyncMock()
        result = MagicMock()
        result.fetchall.return_value = [("caba",), ("datos_gob_ar",)]
        session.execute.return_value = result

        assert await PgVectorSearchAdapter(session).known_portals() == ["caba", "datos_gob_ar"]
        other = AsyncMock()
        assert await PgVectorSearchAdapter(other).known_portals() == ["caba", "datos_gob_ar"]
        other.execute.assert_not_awaited()
        assert "DISTINCT portal" in str(session.execute.await_args.args[0])
