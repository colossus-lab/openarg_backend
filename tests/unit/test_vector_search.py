from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import psycopg
import pytest
from sqlalchemy.exc import InternalError, OperationalError

from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter

_ADAPTER_LOGGER = "app.infrastructure.adapters.search.pgvector_search_adapter"


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


_SESSION_TIMEOUT = "1min"  # the session's own statement_timeout in the double


def _statement_timeout_error() -> OperationalError:
    """What psycopg raises, wrapped by SQLAlchemy, when statement_timeout fires."""
    return OperationalError(
        "SELECT ... GROUP BY dc.dataset_id ...",
        {},
        psycopg.errors.QueryCanceled("canceling statement due to statement timeout"),
    )


def _scripted_session(
    extversion: str = "0.8.1",
    rows: list | None = None,
    exact_rows: list | None = None,
    seqscan: str = "on",
    walk_error: Exception | None = None,
    exact_error: BaseException | None = None,
    savepoint_rollback_error: Exception | None = None,
    wide_rows: list | None = None,
) -> AsyncMock:
    """Session double that answers by statement, with Postgres' transaction rules.

    The version lookup gets ``extversion``; reading ``enable_seqscan`` gets
    ``seqscan``; the HNSW search (the one with ``LIMIT :candidates``) gets
    ``rows``, or raises ``walk_error``, and the wider walk (``candidates`` of
    ``_ANN_WIDE_CANDIDATES``) gets ``wide_rows`` if given (``rows`` if not);
    the exact search (grouped over every chunk) gets ``exact_rows``, or
    raises ``exact_error``.

    What a statement timeout does to a transaction is modelled too, since it
    is what the savepoint is for: a statement that fails leaves the
    transaction aborted, and from then on every statement fails ("current
    transaction is aborted") until a rollback. ``begin_nested`` opens a
    savepoint; rolling it back restores ``statement_timeout`` as it was when
    it opened and clears the aborted state (``savepoint_rollback_error``
    makes that rollback fail instead); releasing it (``commit``) keeps what
    was set inside, as ``SET LOCAL`` does in Postgres. ``session.state``
    exposes the timeout and the aborted flag; ``session.events`` the
    savepoint operations, in order with the statements' kinds.
    """
    session = AsyncMock()
    state = {"statement_timeout": _SESSION_TIMEOUT, "aborted": False}
    savepoints: list[dict] = []
    events: list[str] = []

    async def _execute(statement, params=None):
        sql = str(statement)
        events.append(_kind(sql))
        if state["aborted"]:
            raise InternalError(
                sql,
                params,
                psycopg.errors.InFailedSqlTransaction("current transaction is aborted"),
            )
        result = MagicMock()
        if "pg_extension" in sql:
            result.scalar.return_value = extversion
        elif "current_setting('enable_seqscan')" in sql:
            result.scalar.return_value = seqscan
        elif "current_setting('statement_timeout')" in sql:
            result.scalar.return_value = state["statement_timeout"]
        elif "set_config('statement_timeout'" in sql:
            state["statement_timeout"] = next(iter(params.values()))
        elif "LIMIT :candidates" in sql:
            if walk_error is not None:
                state["aborted"] = True
                raise walk_error
            wide = params["candidates"] == PgVectorSearchAdapter._ANN_WIDE_CANDIDATES
            result.fetchall.return_value = (
                wide_rows if wide and wide_rows is not None else rows
            ) or []
        elif "GROUP BY dc.dataset_id" in sql:
            if exact_error is not None:
                state["aborted"] = True
                raise exact_error
            result.fetchall.return_value = exact_rows or []
        else:
            result.fetchall.return_value = []
        return result

    async def _begin_nested():
        savepoints.append(dict(state))
        events.append("savepoint")
        savepoint = MagicMock()

        async def _release():
            if state["aborted"]:
                raise InternalError("RELEASE SAVEPOINT", {}, Exception("transaction is aborted"))
            savepoints.pop()
            events.append("release")

        async def _rollback_to():
            events.append("rollback to savepoint")
            if savepoint_rollback_error is not None:
                raise savepoint_rollback_error
            state.update(savepoints.pop())

        savepoint.commit = AsyncMock(side_effect=_release)
        savepoint.rollback = AsyncMock(side_effect=_rollback_to)
        return savepoint

    session.execute.side_effect = _execute
    session.begin_nested = AsyncMock(side_effect=_begin_nested)
    session.state = state
    session.events = events
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


def _kind(sql: str) -> str:
    """A short name for each statement the adapter sends, to check their order."""
    if "current_setting('enable_seqscan')" in sql:
        return "lee seqscan"
    if "set_config('enable_seqscan', 'off', true)" in sql:
        return "seqscan off"
    if "set_config('enable_seqscan', :before, true)" in sql:
        return "seqscan como estaba"
    if "current_setting('statement_timeout')" in sql:
        return "lee timeout"
    if "set_config('statement_timeout', :ms, true)" in sql:
        return "timeout tope"
    if "set_config('statement_timeout', :before, true)" in sql:
        return "timeout como estaba"
    if "LIMIT :candidates" in sql:
        return "recorrido"
    if "GROUP BY dc.dataset_id" in sql:
        return "exacta"
    if "hnsw.ef_search" in sql:
        return "ef_search"
    if "hnsw.iterative_scan" in sql:
        return "iterative"
    if "pg_extension" in sql:
        return "versión"
    return sql


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

    @pytest.mark.parametrize("limit", [5, 16, 20, 40, 100, 500])
    async def test_ef_search_is_300_and_candidates_400_whatever_the_limit(self, limit):
        """#131 set ef_search to 1000, and at 1000 the planner left the index
        for a parallel seq scan (past ~400 on staging, ~600 on prod): brute
        force, 2.2 s median with 8 at once on staging. The review asked for
        300 or less. Candidates went to 1000 with #176, and on prod
        (2026-10-08, gold set) that doubled the p95 against 300-500 with the
        same hit@3: 400, old main's value for the agent, past ef_search so
        the iterative scan still walks beyond the frontier."""
        session = _scripted_session(rows=[_row(i, 0.7) for i in range(limit)])
        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=limit)

        stmts = _statements(session)
        assert [p["ef"] for s, p in stmts if "hnsw.ef_search" in s] == ["300"]
        assert _hnsw_sql(session)[1]["candidates"] == 400
        # Set inside the transaction only, and before the search runs.
        first_set = next(s for s, _ in stmts if "set_config" in s)
        assert "set_config('hnsw.ef_search', :ef, true)" in first_set

    def test_ef_search_stays_under_the_planner_threshold(self):
        """Even with the walk forced onto the index, a frontier of 1000 is the
        cost #131 paid for nothing: the recall measured the same at 40."""
        assert PgVectorSearchAdapter._ANN_EF_SEARCH <= 300
        assert PgVectorSearchAdapter._ANN_CANDIDATES > PgVectorSearchAdapter._ANN_EF_SEARCH

    async def test_iterative_scan_is_on_without_a_portal_too(self):
        """Without a portal it is what brings candidates 301 to 400."""
        session = _scripted_session(rows=_GOOD)
        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        sqls = [s for s, _ in _statements(session)]
        assert any("'hnsw.iterative_scan', 'relaxed_order', true" in s for s in sqls)
        assert ":portal" not in _hnsw_sql(session)[0]

    async def test_the_walk_runs_with_seqscan_off_and_puts_it_back(self):
        """The planner must not trade the walk for a scan of every chunk
        (H091), and the rest of the transaction must not inherit that."""
        session = _scripted_session(rows=_GOOD)
        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        kinds = [_kind(s) for s, _ in _statements(session)]
        assert kinds == [
            "ef_search",
            "versión",
            "iterative",
            "lee seqscan",
            "seqscan off",
            "recorrido",
            "seqscan como estaba",
        ]
        restore = [p for s, p in _statements(session) if _kind(s) == "seqscan como estaba"]
        assert restore == [{"before": "on"}]

    async def test_seqscan_goes_back_to_what_it_was_not_to_on(self):
        """A caller that had turned it off for its own transaction keeps it off."""
        session = _scripted_session(rows=_GOOD, seqscan="off")
        await PgVectorSearchAdapter(session).search_datasets_hnsw([0.1] * 8, limit=20)

        restore = [p for s, p in _statements(session) if _kind(s) == "seqscan como estaba"]
        assert restore == [{"before": "off"}]

    async def test_the_exact_search_runs_with_seqscan_back(self):
        """The exact search reads every chunk: it has to run after the
        setting is back, or the planner would look for any other way."""
        trapped = [_row(100 + i, 0.479 - i * 0.001) for i in range(20)]
        session = _scripted_session(rows=trapped, exact_rows=_GOOD[:20])
        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        kinds = [_kind(s) for s, _ in _statements(session)]
        assert kinds.index("seqscan como estaba") < kinds.index("exacta")
        assert kinds.index("recorrido") < kinds.index("seqscan como estaba")

    async def test_a_failed_walk_leaves_the_setting_to_the_rollback(self):
        """After a failed statement the transaction only accepts a rollback,
        and the rollback undoes the local setting: restoring it there would
        hide the real error behind "current transaction is aborted"."""
        session = _scripted_session(walk_error=RuntimeError("statement timeout"))
        with pytest.raises(RuntimeError, match="statement timeout"):
            await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        kinds = [_kind(s) for s, _ in _statements(session)]
        assert kinds[-2:] == ["seqscan off", "recorrido"]
        assert "seqscan como estaba" not in kinds
        assert "exacta" not in kinds

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
        # Same rule as without a portal: the index or the btree on
        # dataset_id, never a scan of every chunk.
        kinds = [_kind(s) for s, _ in stmts]
        assert kinds[-3:] == ["seqscan off", "recorrido", "seqscan como estaba"]

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
        assert not any("enable_seqscan" in s for s, _ in _statements(session))
        assert "WHERE dc.dataset_id IN (SELECT id FROM datasets WHERE portal = :portal)" in flat
        assert (params["portal"], params["limit"], params["min_sim"]) == ("caba", 7, 0.4)

    async def test_portal_filter_goes_straight_to_the_exact_search_before_pgvector_0_8(self):
        session = _scripted_session(extversion="0.7.4")
        await PgVectorSearchAdapter(session).search_datasets_ann(
            [0.1] * 8, limit=7, portal_filter="caba", min_similarity=0.4
        )

        stmts = _statements(session)
        assert not any(
            "iterative_scan" in s or "ef_search" in s or "enable_seqscan" in s for s, _ in stmts
        )
        [(sql, params)] = _exact_sqls(session)
        assert params["portal"] == "caba"
        assert params["limit"] == 7
        # There the exact search is the answer, not a second opinion: no
        # ceiling, which would turn a slow answer into none.
        assert "savepoint" not in session.events
        assert not any("statement_timeout" in s for s, _ in stmts)

    async def test_without_iterative_scan_and_without_portal_the_index_still_answers(self):
        session = _scripted_session(extversion="0.7.4", rows=_GOOD)
        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        sqls = [s for s, _ in _statements(session)]
        assert not any("iterative_scan" in s for s in sqls)
        assert _hnsw_sql(session)[1]["candidates"] == 400

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


# A trapped walk as prod's gold set saw it on 2026-10-08: full, top 0.489-0.540.
_WEAK = [_row(100 + i, 0.52 - i * 0.001) for i in range(20)]
_EXACT = [_row(i, 0.675 - i * 0.001) for i in range(20)]


class TestExactFallbackCap:
    """The exact search behind the index runs under a ceiling, in a savepoint.

    Prod, 2026-10-08: the fallback took 0.8-1 s warm and 13 s cold, and
    improved the top in 1 of 4 queries. Past the ceiling the index's hits are
    served, and the transaction (which the agent shares across a whole turn)
    must come out of it usable and without the ceiling set."""

    @pytest.fixture(autouse=True)
    def _fresh_version_cache(self, monkeypatch):
        monkeypatch.setattr(PgVectorSearchAdapter, "_pgvector_version", None)

    async def test_under_the_cap_the_exact_search_is_served_as_before(self):
        session = _scripted_session(rows=_WEAK, exact_rows=_EXACT)

        results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert [r.dataset_id for r in results] == [r.dataset_id for r in _EXACT]
        assert session.events[-6:] == [
            "savepoint",
            "lee timeout",
            "timeout tope",
            "exacta",
            "timeout como estaba",
            "release",
        ]
        caps = [p for s, p in _statements(session) if _kind(s) == "timeout tope"]
        assert caps == [{"ms": "1500"}]

    async def test_under_the_cap_the_timeout_is_put_back_before_the_release(self):
        """A released savepoint keeps its SET LOCAL until the transaction
        ends: without putting it back, the rest of the agent's turn would run
        every statement under 1.5 s."""
        session = _scripted_session(rows=_WEAK, exact_rows=_EXACT)

        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        restores = [p for s, p in _statements(session) if _kind(s) == "timeout como estaba"]
        assert restores == [{"before": _SESSION_TIMEOUT}]
        assert session.state == {"statement_timeout": _SESSION_TIMEOUT, "aborted": False}

    async def test_past_the_cap_the_index_hits_are_served(self, caplog):
        session = _scripted_session(rows=_WEAK, exact_error=_statement_timeout_error())

        with caplog.at_level(logging.INFO, logger=_ADAPTER_LOGGER):
            results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert [r.dataset_id for r in results] == [r.dataset_id for r in _WEAK]
        [record] = [r for r in caplog.records if "hnsw→exacta" in r.getMessage()]
        assert record.levelno == logging.WARNING
        message = record.getMessage()
        assert "puntaje bajo" in message
        assert "pasó el tope de 1500 ms" in message
        assert "se sirven los hits del índice" in message

    async def test_past_the_cap_the_session_stays_usable_and_without_the_cap(self):
        session = _scripted_session(rows=_WEAK, exact_error=_statement_timeout_error())

        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert session.events[-5:] == [
            "savepoint",
            "lee timeout",
            "timeout tope",
            "exacta",
            "rollback to savepoint",
        ]
        assert session.state == {"statement_timeout": _SESSION_TIMEOUT, "aborted": False}
        # The turn goes on in the same transaction: the next search runs.
        again = await PgVectorSearchAdapter(session).search_datasets_hnsw([0.1] * 8, limit=20)
        assert [r.dataset_id for r in again] == [r.dataset_id for r in _WEAK]

    async def test_control_without_the_savepoint_the_timeout_breaks_the_session(self):
        """Control for the test above: in this double, as in Postgres, a timed
        out statement outside a savepoint leaves the transaction aborted. If
        it did not, the test above would pass without the savepoint."""
        session = _scripted_session(rows=_WEAK, exact_error=_statement_timeout_error())
        adapter = PgVectorSearchAdapter(session)
        with pytest.raises(OperationalError):
            await adapter.search_datasets_exact([0.1] * 8, limit=20)

        with pytest.raises(InternalError, match="current transaction is aborted"):
            await adapter.search_datasets_hnsw([0.1] * 8, limit=20)

    async def test_too_few_hits_past_the_cap_serves_the_few(self):
        """The same ceiling for "pocos": without a portal or with a big one
        the exact search reads the same table, and a short list beats a cold
        scan."""
        session = _scripted_session(rows=_GOOD[:3], exact_error=_statement_timeout_error())

        results = await PgVectorSearchAdapter(session).search_datasets_ann(
            [0.1] * 8, limit=10, portal_filter="datos_gob_ar"
        )

        assert [r.dataset_id for r in results] == [r.dataset_id for r in _GOOD[:3]]
        assert session.state["aborted"] is False

    async def test_too_few_hits_under_the_cap_serves_the_exact_search(self):
        """A small portal's exact search reads that portal's chunks and ends
        far under the ceiling: there it still completes the list."""
        session = _scripted_session(rows=_GOOD[:3], exact_rows=_GOOD[:10])

        results = await PgVectorSearchAdapter(session).search_datasets_ann(
            [0.1] * 8, limit=10, portal_filter="caba"
        )

        assert len(results) == 10
        assert session.events[-1] == "release"

    async def test_any_other_error_serves_the_index_hits_too(self, caplog):
        session = _scripted_session(
            rows=_WEAK,
            exact_error=OperationalError("SELECT", {}, Exception("could not resize shared memory")),
        )

        with caplog.at_level(logging.WARNING, logger=_ADAPTER_LOGGER):
            results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert [r.dataset_id for r in results] == [r.dataset_id for r in _WEAK]
        assert session.state == {"statement_timeout": _SESSION_TIMEOUT, "aborted": False}
        [record] = [r for r in caplog.records if "hnsw→exacta" in r.getMessage()]
        assert "OperationalError" in record.getMessage()
        assert "tope" not in record.getMessage()

    async def test_a_failed_savepoint_rollback_is_not_hidden(self):
        """If the rollback to the savepoint fails the connection is gone:
        serving the index's hits would hand the next query of the turn a
        transaction nobody can use. The error goes to the caller, whose
        ``reset`` rolls back, as for any failed search."""
        session = _scripted_session(
            rows=_WEAK,
            exact_error=_statement_timeout_error(),
            savepoint_rollback_error=OperationalError(
                "ROLLBACK TO SAVEPOINT", {}, Exception("server closed the connection")
            ),
        )

        with pytest.raises(OperationalError, match="server closed the connection"):
            await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

    async def test_a_cancellation_is_not_swallowed(self):
        """The agent's tool timeout cancels the task: that must reach the
        caller (which rolls back), not come out as the index's hits."""
        session = _scripted_session(rows=_WEAK, exact_error=asyncio.CancelledError())

        with pytest.raises(asyncio.CancelledError):
            await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert "rollback to savepoint" not in session.events
        assert "release" not in session.events

    async def test_a_trusted_answer_opens_no_savepoint(self):
        session = _scripted_session(rows=_GOOD[:10])

        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=10)

        session.begin_nested.assert_not_awaited()
        assert not any("statement_timeout" in s for s, _ in _statements(session))


# "votaciones nominales" on staging, 2026-10-08 (review of #183): with 400
# candidates some embeddings trap the walk at 0.526 ("Legislativas
# provinciales 2017"); with 1000 it reaches "Votaciones Nominales" at 0.675.
_FREED = [_row(200 + i, 0.675 - i * 0.001) for i in range(20)]
# A walk that stays weak however wide: a query OpenArg has no data for.
_STILL_WEAK = [_row(300 + i, 0.53 - i * 0.001) for i in range(20)]


def _walk_candidates(session: AsyncMock) -> list[int]:
    return [p["candidates"] for s, p in _statements(session) if "LIMIT :candidates" in s]


class TestWideWalk:
    """A weak top walks again, wider, before the exact search.

    Review of #183: with 400 candidates real queries end trapped, the exact
    search behind the walk rescued them, and its ceiling made that rescue
    fail cold or under load. The rescue is now a second walk, which reads
    the index and not the table: it does not depend on the ceiling."""

    @pytest.fixture(autouse=True)
    def _fresh_version_cache(self, monkeypatch):
        monkeypatch.setattr(PgVectorSearchAdapter, "_pgvector_version", None)

    def test_the_wide_walk_is_what_prod_walked_and_wider_than_the_first(self):
        assert PgVectorSearchAdapter._ANN_WIDE_CANDIDATES == 1000
        assert PgVectorSearchAdapter._ANN_WIDE_CANDIDATES > PgVectorSearchAdapter._ANN_CANDIDATES

    async def test_a_trapped_walk_is_freed_even_when_the_exact_search_would_be_cut(self, caplog):
        """The finding itself: the exact search would hit the ceiling (cold
        cache, load), and what gets served is not the trapped walk."""
        session = _scripted_session(
            rows=_WEAK, wide_rows=_FREED, exact_error=_statement_timeout_error()
        )

        with caplog.at_level(logging.INFO, logger=_ADAPTER_LOGGER):
            results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert [r.dataset_id for r in results] == [r.dataset_id for r in _FREED]
        assert _walk_candidates(session) == [400, 1000]
        # The wide walk answered: no exact search, no savepoint.
        assert _exact_sqls(session) == []
        session.begin_nested.assert_not_awaited()
        [record] = [r for r in caplog.records if "hnsw ancho" in r.getMessage()]
        assert "candidatos=400→1000" in record.getMessage()
        assert "top=0.520→0.675" in record.getMessage()
        assert not [r for r in caplog.records if "hnsw→exacta" in r.getMessage()]

    async def test_the_wide_walk_runs_with_seqscan_off_and_puts_it_back_too(self):
        session = _scripted_session(rows=_WEAK, wide_rows=_FREED)

        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        walk = ["lee seqscan", "seqscan off", "recorrido", "seqscan como estaba"]
        assert [_kind(s) for s, _ in _statements(session)] == [
            "ef_search",
            "versión",
            "iterative",
            *walk,
            "ef_search",
            "iterative",
            *walk,
        ]

    async def test_still_weak_after_the_wide_walk_the_exact_search_runs(self):
        session = _scripted_session(rows=_WEAK, wide_rows=_STILL_WEAK, exact_rows=_EXACT)

        results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert [r.dataset_id for r in results] == [r.dataset_id for r in _EXACT]
        assert _walk_candidates(session) == [400, 1000]
        # Both walks first, then the exact search in its savepoint.
        walks_and_exact = [k for k in session.events if k in ("recorrido", "savepoint", "exacta")]
        assert walks_and_exact == ["recorrido", "recorrido", "savepoint", "exacta"]

    async def test_past_the_cap_the_wide_walks_hits_are_served(self, caplog):
        """If the exact search is cut, the wider walk's answer is served: it
        holds the first walk's candidates and more."""
        session = _scripted_session(
            rows=_WEAK, wide_rows=_STILL_WEAK, exact_error=_statement_timeout_error()
        )

        with caplog.at_level(logging.WARNING, logger=_ADAPTER_LOGGER):
            results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert [r.dataset_id for r in results] == [r.dataset_id for r in _STILL_WEAK]
        assert session.state == {"statement_timeout": _SESSION_TIMEOUT, "aborted": False}
        [record] = [r for r in caplog.records if "hnsw→exacta" in r.getMessage()]
        assert "top=0.530" in record.getMessage()

    async def test_a_trusted_walk_walks_once(self):
        session = _scripted_session(rows=_GOOD[:20], wide_rows=_FREED)

        await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert _walk_candidates(session) == [400]

    async def test_too_few_datasets_go_to_the_exact_search_without_a_second_walk(self):
        session = _scripted_session(rows=_GOOD[:3], wide_rows=_GOOD[:10], exact_rows=_GOOD[:10])

        results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=10)

        assert len(results) == 10
        assert _walk_candidates(session) == [400]
        assert len(_exact_sqls(session)) == 1

    async def test_with_a_portal_a_weak_top_goes_to_the_exact_search(self):
        """A small portal's exact search reads only its chunks."""
        session = _scripted_session(rows=_WEAK, wide_rows=_FREED, exact_rows=_EXACT)

        results = await PgVectorSearchAdapter(session).search_datasets_ann(
            [0.1] * 8, limit=20, portal_filter="caba"
        )

        assert [r.dataset_id for r in results] == [r.dataset_id for r in _EXACT]
        assert _walk_candidates(session) == [400]

    async def test_without_the_iterative_scan_a_weak_top_does_not_walk_again(self):
        """Before pgvector 0.8 both walks stop at ef_search: the second one
        would be the first again."""
        session = _scripted_session(
            extversion="0.7.4", rows=_WEAK, wide_rows=_FREED, exact_rows=_EXACT
        )

        results = await PgVectorSearchAdapter(session).search_datasets_ann([0.1] * 8, limit=20)

        assert [r.dataset_id for r in results] == [r.dataset_id for r in _EXACT]
        assert _walk_candidates(session) == [400]

    async def test_the_index_search_alone_never_runs_the_exact_search(self):
        """What the canary measures: the index as callers get it before the
        exact search."""
        session = _scripted_session(rows=_WEAK, wide_rows=_STILL_WEAK, exact_rows=_EXACT)

        results = await PgVectorSearchAdapter(session).search_datasets_index([0.1] * 8, limit=10)

        assert [r.dataset_id for r in results] == [r.dataset_id for r in _STILL_WEAK]
        assert _walk_candidates(session) == [400, 1000]
        assert _exact_sqls(session) == []


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
