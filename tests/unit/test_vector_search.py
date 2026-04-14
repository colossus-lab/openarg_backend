from __future__ import annotations

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

    async def test_lexical_query_does_not_requery_when_results_are_sparse(self, adapter, mock_session):
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
