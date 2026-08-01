"""Silence from a check that was handed nothing is not evidence of health.

`_split_qualified_name` maps an unqualified `cached_datasets.table_name` to the
`public` schema. That was true when `cache_*` lived there. Measured 2026-07-31:
all 25288 ready rows carry an unqualified name and all 25288 relations live in
`raw`, so the sweep resolved every single one against the wrong schema, read no
columns, and validated almost nothing — silently, because "no columns" produces
no findings rather than an error.

Harmless while the sweep only appended. The moment it started *closing*
findings, that silence became an assertion: four genuinely broken tables had
their critical findings resolved on 2026-07-31 20:43 on the strength of columns
nobody had read, and went straight back into serving.

    ciudad_mendoza__nomina_de_funcionarios   ['col_0', 'col_1', 'col_2', ...]
    tucuman__banco_de_proyectos_viales       ['Unnamed: 0', 'Unnamed: 1', ...]

Two defences, and the second matters more than the first. Resolving the schema
correctly fixes this instance. Refusing to conclude anything from an
unobserved relation fixes the class — the next time a lookup returns nothing
for a reason nobody predicted, the sweep will decline to draw a conclusion
rather than quietly certify the table as healthy.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.application.validation.detector import Mode


@pytest.fixture
def sweep():
    return pytest.importorskip("app.infrastructure.celery.tasks.ingestion_findings_sweep")


@pytest.fixture
def hooks():
    return pytest.importorskip("app.application.validation.collector_hooks")


_BROKEN = ["col_0", "col_1", "col_2", "Unnamed: 3", "Unnamed: 4", "col_5"]


class TestSchemaResolution:
    def test_an_unqualified_name_resolves_to_raw(self, sweep) -> None:
        """The real shape of the inventory: 25288 of 25288 live in `raw`."""
        cols = {("raw", "portal__src__hash__v1"): ["a", "b"]}
        assert sweep._resolve_columns(cols, "portal__src__hash__v1") == ["a", "b"]

    def test_an_explicit_schema_still_wins(self, sweep) -> None:
        cols = {("raw", "t"): ["raw_col"], ("public", "t"): ["public_col"]}
        assert sweep._resolve_columns(cols, "public.t") == ["public_col"]
        assert sweep._resolve_columns(cols, "raw.t") == ["raw_col"]

    def test_a_legacy_public_table_is_still_found(self, sweep) -> None:
        """`cache_*` in public must keep working — that is why both are tried."""
        assert sweep._resolve_columns({("public", "cache_foo"): ["x"]}, "cache_foo") == ["x"]

    def test_a_missing_relation_yields_nothing(self, sweep) -> None:
        assert sweep._resolve_columns({}, "gone__hash__v1") == []

    def test_row_counts_resolve_the_same_way(self, sweep) -> None:
        """Otherwise columns come from `raw` and the count from nowhere."""
        counts = {("raw", "portal__src__hash__v1"): 42}
        assert sweep._resolve_row_count(counts, "portal__src__hash__v1") == 42

    def test_a_zero_row_count_is_not_mistaken_for_absent(self, sweep) -> None:
        """0 rows is a CRITICAL finding; `.get() or None` would erase it."""
        assert sweep._resolve_row_count({("raw", "t"): 0}, "t") == 0


class TestUnobservedRelationsConcludeNothing:
    """The class-level defence, independent of the schema bug above."""

    def _run(self, hooks, columns, close_mock):
        with (
            patch.object(hooks, "persist_findings"),
            patch.object(hooks, "resolve_missing") as resolve,
            patch.object(hooks, "_resolve_placeholder_headers", close_mock),
            patch.object(hooks, "get_validator") as get_validator,
        ):
            get_validator.return_value.run.return_value = []
            hooks.validate_retrospective(
                MagicMock(),
                resolve_stale=True,
                dataset_id="res-1",
                table_name="raw.t",
                materialized_columns=columns,
            )
        return resolve

    def test_no_columns_closes_nothing(self, hooks) -> None:
        """The exact 2026-07-31 failure, stated as an invariant."""
        close = MagicMock()
        resolve = self._run(hooks, None, close)
        close.assert_not_called()
        resolve.assert_not_called()

    def test_an_empty_column_list_closes_nothing_either(self, hooks) -> None:
        close = MagicMock()
        resolve = self._run(hooks, [], close)
        close.assert_not_called()
        resolve.assert_not_called()

    def test_observed_and_clean_still_closes(self, hooks) -> None:
        """The fail-safe must not disable the fix it is protecting."""
        close = MagicMock()
        resolve = self._run(hooks, ["Apellido", "Nombre", "Cargo"], close)
        close.assert_called_once()
        resolve.assert_called_once()

    def test_observed_and_broken_keeps_the_finding(self, hooks) -> None:
        close = MagicMock()
        with (
            patch.object(hooks, "persist_findings") as persist,
            patch.object(hooks, "resolve_missing"),
            patch.object(hooks, "_resolve_placeholder_headers", close),
            patch.object(hooks, "get_validator") as get_validator,
        ):
            get_validator.return_value.run.return_value = []
            hooks.validate_retrospective(
                MagicMock(),
                resolve_stale=True,
                dataset_id="res-1",
                table_name="raw.t",
                materialized_columns=_BROKEN,
            )
        close.assert_not_called()
        assert [f.detector_name for f in persist.call_args.args[2]] == ["placeholder_headers"]

    def test_the_check_is_not_run_on_an_unobserved_relation(self, hooks) -> None:
        """Not merely "produces no finding" — it must not be consulted at all."""
        with (
            patch.object(hooks, "persist_findings"),
            patch.object(hooks, "resolve_missing"),
            patch.object(hooks, "_resolve_placeholder_headers"),
            patch.object(hooks, "get_validator") as get_validator,
            patch.object(hooks, "_placeholder_header_finding") as check,
        ):
            get_validator.return_value.run.return_value = []
            hooks.validate_retrospective(
                MagicMock(),
                resolve_stale=True,
                dataset_id="res-1",
                table_name="raw.t",
                materialized_columns=None,
            )
        check.assert_not_called()


class TestTheSweepUsesTheResolver:
    def test_the_loop_does_not_index_the_dict_directly(self) -> None:
        """A `.get(key)` on the assumed schema is what silently returned [].""" ""
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
        loop = source.split("for row in batch:", 1)[1].split("total_scanned", 1)[0]
        assert "_resolve_columns(" in loop
        assert "_resolve_row_count(" in loop
        assert "cols_by_table.get(" not in loop


class TestModeIsUnchanged:
    def test_retrospective_findings_stay_retrospective(self, hooks) -> None:
        """The mode decides what `resolve_missing` may touch."""
        with (
            patch.object(hooks, "persist_findings") as persist,
            patch.object(hooks, "resolve_missing"),
            patch.object(hooks, "_resolve_placeholder_headers"),
            patch.object(hooks, "get_validator") as get_validator,
        ):
            get_validator.return_value.run.return_value = []
            hooks.validate_retrospective(
                MagicMock(),
                resolve_stale=True,
                dataset_id="res-1",
                table_name="raw.t",
                materialized_columns=_BROKEN,
            )
        assert persist.call_args.args[2][0].mode is Mode.RETROSPECTIVE
