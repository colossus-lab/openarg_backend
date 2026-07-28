"""Two findings must never race for the same row.

`persist_findings` upserts on (resource_id, detector_name, detector_version,
mode, input_hash). Two findings from one check on one mart that build the same
input_hash overwrite each other, and there is nothing to notice: the upsert is
doing exactly what it was designed to do.

Observed on the auditor's first real run. `mart_hidden_despite_rows` emitted a
CRITICAL — 52.086.049 rows unreachable behind a zeroed counter — and a WARN
about the failed refresh that caused it. Neither carried a `column`, so both
fell back to the mart id, the WARN landed second, and the CRITICAL was gone.
The task's summary reported 41 findings persisted while the table held 40.

The bug is not that one check forgot a key; it is that forgetting was free.
"""

from __future__ import annotations

from app.application.marts.quality import finding_discriminator
from app.application.marts.quality.checks import build_default_mart_checks
from app.application.marts.quality.checks.numeric_typing import NumericTypingCheck
from app.application.marts.quality.checks.row_count_drift import RowCountDriftCheck
from app.application.marts.quality.context import MartAuditContext, MartColumn


def _ctx(**kwargs) -> MartAuditContext:
    base = {"mart_id": "m", "view_name": "m"}
    base.update(kwargs)
    return MartAuditContext(**base)


def _keys(findings, mart_id="m") -> list[str]:
    return [f"{f.detector_name}:{finding_discriminator(f, mart_id)}" for f in findings]


class TestDiscriminator:
    def test_prefers_the_explicit_finding_key(self) -> None:
        findings = RowCountDriftCheck().run(
            _ctx(last_row_count=0, approx_row_count=10, last_refresh_status="refresh_failed")
        )
        assert all("finding_key" in f.payload for f in findings)

    def test_falls_back_to_the_column(self) -> None:
        findings = NumericTypingCheck().run(
            _ctx(columns=(MartColumn(name="monto", pg_type="text"),))
        )
        assert finding_discriminator(findings[0], "m") == "monto"

    def test_falls_back_to_the_mart_id(self) -> None:
        class _Bare:
            detector_name = "x"
            payload: dict = {}

        assert finding_discriminator(_Bare(), "mi_mart") == "mi_mart"


class TestNoCollisions:
    def test_the_measured_collision_is_gone(self) -> None:
        """The exact case that lost the CRITICAL finding."""
        ctx = _ctx(
            mart_id="mediaciones_prejudiciales",
            last_row_count=0,
            approx_row_count=52_086_049,
            last_refresh_status="refresh_failed",
        )
        findings = RowCountDriftCheck().run(ctx)
        keys = _keys(findings, ctx.mart_id)
        assert len(findings) == 2
        assert len(set(keys)) == 2, f"both findings would land on one row: {keys}"

    def test_per_column_findings_stay_distinct(self) -> None:
        ctx = _ctx(
            columns=(
                MartColumn(name="credito_vigente", pg_type="text"),
                MartColumn(name="credito_devengado", pg_type="text"),
                MartColumn(name="anio", pg_type="text"),
            )
        )
        keys = _keys(NumericTypingCheck().run(ctx))
        assert len(set(keys)) == len(keys) == 3

    def test_no_default_check_can_collide_on_one_mart(self) -> None:
        """The general invariant, not the one case.

        A context built to trip everything at once: every finding any check
        produces for a single mart must be separately storable.
        """
        ctx = _ctx(
            mart_id="kitchen_sink",
            last_row_count=0,
            approx_row_count=1_000_000,
            last_refresh_status="refresh_failed",
            candidate_table_count=500,
            kept_table_count=10,
            resolved_sql=(
                'SELECT anio, SUM(credito_vigente::numeric) AS v FROM raw."t" '
                "WHERE credito_devengado::numeric <= credito_vigente::numeric "
                "GROUP BY anio"
            ),
            columns=(
                MartColumn(name="credito_vigente", pg_type="text"),
                MartColumn(name="monto_total", pg_type="text"),
                MartColumn(name="anio", pg_type="text"),
            ),
        )
        findings = [f for check in build_default_mart_checks() for f in check.run(ctx)]
        keys = _keys(findings, ctx.mart_id)
        assert len(findings) > 4, "the fixture must actually trip several checks"
        assert len(set(keys)) == len(keys), (
            f"findings sharing a storage key would overwrite each other: "
            f"{sorted(k for k in keys if keys.count(k) > 1)}"
        )
