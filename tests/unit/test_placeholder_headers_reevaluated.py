"""A finding written once and re-checked never becomes a permanent sentence.

`placeholder_headers` is built by hand inside `validate_post_parse` and was
evaluated nowhere else. Nothing could ever close one: the retrospective sweep
did not run the check, and `_close_resolved_findings_query` needs a
re-collection that never comes for a table whose headers were repaired in
place.

That was survivable while findings only sat in a table nobody read. It stopped
being survivable when the sandbox started refusing tables with an open critical
finding — at which point 121 tables left serving, and re-running the real
detector against their current columns showed **117 of them were clean**:

    arsat__declaraciones_juradas_patrimoniales
        ['Apellido', 'Nombre', 'Cargo', 'Tipo', 'Cumplimiento']
    buenos_aires_prov__producto_bruto_geografico_pbg
        ['actividad_detalle', 'actividad_sector_letra', 'anio', ...]

Withheld since May for a defect fixed months ago. The gate was not wrong to
refuse a table with an open critical finding; the finding was wrong to still be
open, and the gate is what made that visible.

The check needs `materialized_columns` and nothing else — no bytes, no parser —
which the sweep reads straight from `information_schema`. Its confinement to
the parse path was history, not a constraint.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.application.validation.detector import Mode, ResourceContext


@pytest.fixture
def hooks():
    return pytest.importorskip("app.application.validation.collector_hooks")


_CLEAN = ["Apellido", "Nombre", "Cargo", "Tipo", "Cumplimiento"]
_BROKEN = ["col_0", "col_1", "col_2", "Unnamed: 3", "Unnamed: 4", "col_5"]


class TestTheCheckRunsRetrospectively:
    def test_broken_headers_are_still_detected(self, hooks) -> None:
        finding = hooks._placeholder_header_finding(
            ResourceContext(resource_id="r", materialized_columns=_BROKEN),
            mode=Mode.RETROSPECTIVE,
        )
        assert finding is not None
        assert finding.severity.value == "critical"

    def test_clean_headers_produce_nothing(self, hooks) -> None:
        assert (
            hooks._placeholder_header_finding(
                ResourceContext(resource_id="r", materialized_columns=_CLEAN),
                mode=Mode.RETROSPECTIVE,
            )
            is None
        )

    def test_the_sweep_hook_now_runs_it(self, hooks) -> None:
        """It was reachable only from validate_post_parse."""
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
        persisted = persist.call_args.args[2]
        assert [f.detector_name for f in persisted] == ["placeholder_headers"]


class TestStaleOnesAreClosed:
    def test_clean_columns_close_the_finding_across_modes(self, hooks) -> None:
        """The POST_PARSE finding is what blocks; closing only RETROSPECTIVE
        would leave every one of the 117 exactly where it was."""
        with (
            patch.object(hooks, "persist_findings"),
            patch.object(hooks, "resolve_missing"),
            patch.object(hooks, "_resolve_placeholder_headers") as close,
            patch.object(hooks, "get_validator") as get_validator,
        ):
            get_validator.return_value.run.return_value = []
            hooks.validate_retrospective(
                MagicMock(),
                resolve_stale=True,
                dataset_id="res-1",
                table_name="raw.t",
                materialized_columns=_CLEAN,
            )
        close.assert_called_once()
        assert close.call_args.args[1] == "res-1"

    def test_still_broken_columns_keep_the_finding(self, hooks) -> None:
        with (
            patch.object(hooks, "persist_findings"),
            patch.object(hooks, "resolve_missing"),
            patch.object(hooks, "_resolve_placeholder_headers") as close,
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

    def test_nothing_closes_without_resolve_stale(self, hooks) -> None:
        """The collector's own paths must not start resolving by accident."""
        with (
            patch.object(hooks, "persist_findings"),
            patch.object(hooks, "_resolve_placeholder_headers") as close,
            patch.object(hooks, "get_validator") as get_validator,
        ):
            get_validator.return_value.run.return_value = []
            hooks.validate_retrospective(
                MagicMock(), dataset_id="res-1", table_name="raw.t", materialized_columns=_CLEAN
            )
        close.assert_not_called()

    def test_the_close_is_scoped_to_one_detector_and_one_resource(self, hooks) -> None:
        """A cross-mode UPDATE has to be narrow or it becomes a mass resolve."""
        engine = MagicMock()
        conn = engine.begin.return_value.__enter__.return_value
        conn.execute.return_value.rowcount = 3
        assert hooks._resolve_placeholder_headers(engine, "res-1") == 3
        sql = str(conn.execute.call_args.args[0]).lower()
        assert "resource_id = :rid" in sql
        assert "detector_name = 'placeholder_headers'" in sql
        assert "resolved_at is null" in sql
