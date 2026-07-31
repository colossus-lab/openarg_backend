"""Detecting a broken table and still serving it is the same as not detecting it.

The ingestion validators find `html_as_data`, `placeholder_headers`,
`row_count` and friends, write them to `ingestion_findings`, and the promotion
gate keeps a failing table out of `ready`. What that gate cannot do is reach
backwards: a table promoted before a detector existed, or one whose defect the
retrospective sweep found afterwards, keeps being served with the finding
sitting open beside it.

Measured 2026-07-31 — tables in `ready` with an unresolved CRITICAL finding:

    placeholder_headers      131
    row_count                 11
    single_column_html_blob    1

`open_findings_for()` in `validation/findings_repository.py` was written for
precisely this question and had **zero callers anywhere in the repo**. The
detection was built, correct, and never consulted — the same shape as the
twelve defects of 2026-07-30.

The gate runs at execution rather than in discovery for the reason
`test_sandbox_blocked_mart_execution` records: discovery only controls what is
*suggested* to the model, and the model names tables on its own.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import (
    _findings_blocked_error,
    _referenced_data_tables,
)


def _engine_returning(rows: list[tuple[str, str, str]]) -> MagicMock:
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = rows
    return engine


def _engine_that_raises() -> MagicMock:
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("findings table unreachable")
    return engine


class TestReferencedDataTables:
    def test_finds_unqualified_cache_tables(self) -> None:
        assert _referenced_data_tables("SELECT * FROM cache_diputados_1") == {"cache_diputados_1"}

    def test_finds_raw_qualified_tables(self) -> None:
        found = _referenced_data_tables('SELECT * FROM raw."caba__pauta__h__v2"')
        assert found == {"caba__pauta__h__v2"}

    def test_ignores_marts(self) -> None:
        """Marts carry their quality signal in mart_definitions, not findings."""
        assert _referenced_data_tables("SELECT * FROM mart.presupuesto_consolidado") == set()

    def test_finds_unqualified_raw_landings(self) -> None:
        """`search_path = public, raw` makes an unqualified name reach `raw.*`.

        The tables that actually carry findings are versioned raw landings, not
        `cache_*` — the first one found in prod was
        `indec__indec_ipi_manufacturero_produccion_industr__2971d412__v3`. A
        `cache_`-prefixed filter on the unqualified branch would have skipped
        precisely the rows this gate exists for.
        """
        sql = "SELECT * FROM indec__indec_ipi_manufacturero__2971d412__v3"
        assert _referenced_data_tables(sql) == {"indec__indec_ipi_manufacturero__2971d412__v3"}

    def test_finds_tables_behind_a_join(self) -> None:
        sql = "SELECT * FROM cache_a JOIN cache_b ON cache_a.id = cache_b.id"
        assert _referenced_data_tables(sql) == {"cache_a", "cache_b"}

    def test_ignores_a_query_that_reads_nothing(self) -> None:
        assert _referenced_data_tables("SELECT 1") == set()


class TestExecutionIsRefused:
    def test_open_critical_finding_blocks_the_query(self) -> None:
        engine = _engine_returning(
            [("cache_pobreza_2024", "placeholder_headers", "columns are placeholders")]
        )
        error = _findings_blocked_error(engine, "SELECT * FROM cache_pobreza_2024")
        assert error is not None
        assert "cache_pobreza_2024" in error
        assert "placeholder_headers" in error

    def test_the_reason_reaches_the_user(self) -> None:
        """A refusal that does not say why trains people to route around it."""
        engine = _engine_returning([("cache_x", "row_count", "0 rows materialised")])
        error = _findings_blocked_error(engine, "SELECT * FROM cache_x")
        assert "0 rows materialised" in error

    def test_a_clean_table_runs(self) -> None:
        assert _findings_blocked_error(_engine_returning([]), "SELECT * FROM cache_clean") is None

    def test_a_query_touching_no_data_table_skips_the_lookup(self) -> None:
        """No relation, no round trip — this runs on every execution."""
        engine = _engine_returning([])
        assert _findings_blocked_error(engine, "SELECT 1") is None
        engine.connect.assert_not_called()


class TestFailureModeIsDeliberatelyOpen:
    def test_an_unreadable_findings_table_does_not_block(self) -> None:
        """The opposite choice from the mart guard, on purpose.

        `_blocked_mart_error` fails closed: it covers a handful of curated
        views a human explicitly withdrew, so refusing on doubt costs almost
        nothing. This gate sits in front of ~27k ingested tables, where the
        same reflex would take the entire corpus offline over one unreadable
        catalog — a self-inflicted outage in the name of quality.
        """
        assert _findings_blocked_error(_engine_that_raises(), "SELECT * FROM cache_x") is None

    def test_the_mart_guard_still_fails_closed(self) -> None:
        """Asserted here so the asymmetry above is a decision, not a drift."""
        adapter = pytest.importorskip("app.infrastructure.adapters.sandbox.pg_sandbox_adapter")
        refusal = adapter._blocked_mart_error(
            _engine_that_raises(), "SELECT * FROM mart.presupuesto_nacional_ejecutado"
        )
        assert refusal is not None


class TestBothGatesAreWired:
    def test_execute_readonly_consults_the_findings_gate(self) -> None:
        """A guard nobody calls is what this whole change is about."""
        from pathlib import Path

        source = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "app"
            / "infrastructure"
            / "adapters"
            / "sandbox"
            / "pg_sandbox_adapter.py"
        ).read_text(encoding="utf-8")
        execution = source.split("blocked_error =", 1)[1].split("\n", 1)[0]
        assert "_blocked_mart_error" in execution
        assert "_findings_blocked_error" in execution
