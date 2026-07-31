"""A sweep that only ever appends is a log, not a synchronisation.

`ws0_retrospective_sweep` re-runs the detector suite over every materialised
relation every 30 minutes and persists what it finds. It never closed anything.
`persist_findings` re-opens on conflict (`resolved_at = NULL`), and the only
other closer, `_close_resolved_findings_query`, requires the dataset to have
been re-processed *after* the finding (`cd.updated_at > f.found_at`) — which
never happens for a table that was fixed in place, or whose finding was wrong
to begin with, and is simply never re-collected.

Result, measured on prod 2026-07-31: 1459 open retrospective findings, oldest
2026-05-06. Among them tables whose columns are demonstrably fine today —
`arsat__declaraciones_juradas_patrimoniales` carries an open finding while its
columns read `Apellido, Nombre, Cargo, Tipo, Cumplimiento`.

The mart auditor solved this on its side with `resolve_missing`: keep what this
run reported, close the rest, so a partially-fixed resource ends with fewer
open findings rather than the same ones forever. This wires the same contract
into the ingestion sweep.

Scope worth stating: `resolve_missing` is mode-scoped, so this closes
RETROSPECTIVE findings only. The 7145 `pre_parse` and 2696 `post_parse` ones
are historical facts about a download or a parse, and the sweep has neither
bytes nor a parser — `placeholder_headers` in particular is built by hand in
`validate_post_parse` and is never re-evaluated here. Those need a
re-collection to close, which is separate work.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.application.validation.detector import Finding, Mode, Severity


@pytest.fixture
def hooks():
    return pytest.importorskip("app.application.validation.collector_hooks")


def _finding() -> Finding:
    return Finding(
        detector_name="row_count",
        detector_version="1",
        severity=Severity.CRITICAL,
        mode=Mode.RETROSPECTIVE,
        message="0 rows",
    )


class TestStaleFindingsAreClosed:
    def test_a_resource_with_no_findings_closes_everything(self, hooks) -> None:
        """The `arsat` case: detector is quiet now, the old finding must go."""
        with (
            patch.object(hooks, "persist_findings"),
            patch.object(hooks, "resolve_missing") as resolve,
            patch.object(hooks, "get_validator") as get_validator,
        ):
            get_validator.return_value.run.return_value = []
            hooks.validate_retrospective(
                MagicMock(), resolve_stale=True, dataset_id="res-1", table_name="raw.t"
            )
        resolve.assert_called_once()
        assert resolve.call_args.kwargs["mode"] is Mode.RETROSPECTIVE
        assert list(resolve.call_args.kwargs["keep_hashes"]) == []

    def test_a_resource_that_still_reports_keeps_this_runs_finding(self, hooks) -> None:
        """Partial fixes: keep what reproduced, close what did not."""
        with (
            patch.object(hooks, "persist_findings"),
            patch.object(hooks, "resolve_missing") as resolve,
            patch.object(hooks, "get_validator") as get_validator,
        ):
            get_validator.return_value.run.return_value = [_finding()]
            hooks.validate_retrospective(
                MagicMock(), resolve_stale=True, dataset_id="res-1", table_name="raw.t"
            )
        keep = list(resolve.call_args.kwargs["keep_hashes"])
        assert len(keep) == 1 and keep[0], "the current run's hash must survive"

    def test_default_stays_append_only(self, hooks) -> None:
        """Other callers must not start closing findings by accident."""
        with (
            patch.object(hooks, "persist_findings"),
            patch.object(hooks, "resolve_missing") as resolve,
            patch.object(hooks, "get_validator") as get_validator,
        ):
            get_validator.return_value.run.return_value = []
            hooks.validate_retrospective(MagicMock(), dataset_id="res-1", table_name="raw.t")
        resolve.assert_not_called()

    def test_a_resource_without_an_id_is_left_alone(self, hooks) -> None:
        """`raw_table_versions` rows carry no dataset_id.

        Closing "everything for resource None" would be a mass resolve.
        """
        with (
            patch.object(hooks, "persist_findings"),
            patch.object(hooks, "resolve_missing") as resolve,
            patch.object(hooks, "get_validator") as get_validator,
        ):
            get_validator.return_value.run.return_value = []
            hooks.validate_retrospective(
                MagicMock(), resolve_stale=True, dataset_id="", table_name="raw.t"
            )
        resolve.assert_not_called()


class TestBatchIntrospection:
    """The sweep died on its 600s soft limit every run, so the tail of the
    inventory was never validated. Two queries per relation over 27.7k
    relations is the cost; one of them was an exact COUNT(*) on tables holding
    tens of millions of rows.
    """

    @pytest.fixture
    def sweep(self):
        return pytest.importorskip("app.infrastructure.celery.tasks.ingestion_findings_sweep")

    def test_columns_are_fetched_for_the_whole_batch_at_once(self, sweep) -> None:
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = [
            MagicMock(table_schema="raw", table_name="a", column_name="x"),
            MagicMock(table_schema="raw", table_name="a", column_name="y"),
            MagicMock(table_schema="raw", table_name="b", column_name="z"),
        ]
        out = sweep._columns_for_batch(engine, ["raw.a", "raw.b"])
        assert out == {("raw", "a"): ["x", "y"], ("raw", "b"): ["z"]}
        assert conn.execute.call_count == 1, "one query per batch, not per table"

    def test_large_relations_use_the_planner_estimate(self, sweep) -> None:
        """No full scan on a 52-million-row relation to learn it is not empty."""
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        # SimpleNamespace, not MagicMock: `name` is a reserved MagicMock kwarg
        # and sets the mock's own name instead of an attribute, so `r.name`
        # comes back as a repr and the lookup silently misses.
        conn.execute.return_value.fetchall.return_value = [
            SimpleNamespace(schema="raw", name="big", approx=52_000_000)
        ]
        out = sweep._row_counts_for_batch(engine, ["raw.big"])
        assert out == {("raw", "big"): 52_000_000}
        assert conn.execute.call_count == 1, "the estimate must not trigger a COUNT(*)"

    def test_small_relations_still_get_an_exact_count(self, sweep) -> None:
        """0 vs 3 rows is a CRITICAL finding, and reltuples is -1 if unanalysed."""
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = [
            SimpleNamespace(schema="raw", name="small", approx=-1)
        ]
        conn.execute.return_value.scalar.return_value = 3
        out = sweep._row_counts_for_batch(engine, ["raw.small"])
        assert out == {("raw", "small"): 3}
        assert conn.execute.call_count == 2, "estimate, then the exact count"

    def test_an_empty_batch_costs_nothing(self, sweep) -> None:
        engine = MagicMock()
        assert sweep._columns_for_batch(engine, []) == {}
        assert sweep._row_counts_for_batch(engine, []) == {}
        engine.connect.assert_not_called()


class TestTheSweepAsksForClosure:
    def test_the_loop_passes_resolve_stale(self) -> None:
        """The whole point: a flag nobody sets changes nothing."""
        from pathlib import Path

        source = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "app"
            / "infrastructure"
            / "celery"
            / "tasks"
            / "ingestion_findings_sweep.py"
        ).read_text(encoding="utf-8")
        call = source.split("findings = validate_retrospective(", 1)[1].split(")", 1)[0]
        assert "resolve_stale=True" in call
