"""A mart holding data that discovery cannot reach.

`COALESCE(last_row_count, 0) > 0` hides marts that are empty. The failure
paths of `build_mart` / `refresh_mart` set `last_row_count = 0` on purpose so
a failed build stops being offered — correct when the mart really is empty,
wrong when the previous materialization is still sitting there.

`mart.presupuesto_consolidado` spent months in that state before anyone
noticed. Found again on staging 2026-07-28, in the first audit run that could
have found it: `mediaciones_prejudiciales`, `refresh_failed`,
`last_row_count = 0`, 52.086.049 rows in the view. Nothing anywhere reported
it — the mart simply stopped being an answer.
"""

from __future__ import annotations

from app.application.marts.quality.checks import build_default_mart_checks
from app.application.marts.quality.checks.row_count_drift import RowCountDriftCheck
from app.application.marts.quality.context import MartAuditContext
from app.application.validation.detector import Severity


def _ctx(**kwargs) -> MartAuditContext:
    base = {"mart_id": "m", "view_name": "m"}
    base.update(kwargs)
    return MartAuditContext(**base)


def _by_severity(findings, severity):
    return [f for f in findings if f.severity is severity]


class TestHiddenDespiteRows:
    def test_flags_a_mart_hidden_over_real_data(self) -> None:
        """The measured case: 52M rows behind a zeroed counter."""
        ctx = _ctx(
            mart_id="mediaciones_prejudiciales",
            last_row_count=0,
            approx_row_count=52_086_049,
            last_refresh_status="refresh_failed",
        )
        findings = RowCountDriftCheck().run(ctx)
        critical = _by_severity(findings, Severity.CRITICAL)
        assert len(critical) == 1
        assert critical[0].payload["approx_rows"] == 52_086_049
        assert "52,086,049" in critical[0].message

    def test_a_genuinely_empty_mart_is_not_hidden_data(self) -> None:
        """Counter and view agree: nothing is being concealed."""
        ctx = _ctx(last_row_count=0, approx_row_count=0, last_refresh_status="refreshed")
        assert _by_severity(RowCountDriftCheck().run(ctx), Severity.CRITICAL) == []

    def test_a_healthy_mart_is_silent(self) -> None:
        ctx = _ctx(last_row_count=4961, approx_row_count=4961, last_refresh_status="built")
        assert RowCountDriftCheck().run(ctx) == []

    def test_unknown_estimate_is_not_treated_as_data(self) -> None:
        """`reltuples = -1` (never analysed) arrives as None, not as a count.

        Reporting "hidden despite rows" off an unknown would be inventing the
        evidence for the claim.
        """
        ctx = _ctx(last_row_count=0, approx_row_count=None, last_refresh_status="refreshed")
        assert _by_severity(RowCountDriftCheck().run(ctx), Severity.CRITICAL) == []

    def test_a_deliberately_blocked_mart_is_not_reported(self) -> None:
        """serving_blocked is a documented decision, not an accident."""
        ctx = _ctx(
            last_row_count=0,
            approx_row_count=91_299,
            serving_blocked=True,
            last_refresh_status="build_failed",
        )
        assert RowCountDriftCheck().applicable_to(ctx) is False


class TestFailedRefreshStatus:
    def test_warns_on_a_failed_build(self) -> None:
        ctx = _ctx(last_row_count=100, approx_row_count=100, last_refresh_status="build_failed")
        warns = _by_severity(RowCountDriftCheck().run(ctx), Severity.WARN)
        assert len(warns) == 1
        assert warns[0].payload["last_refresh_status"] == "build_failed"

    def test_warns_on_a_failed_refresh(self) -> None:
        ctx = _ctx(last_row_count=100, approx_row_count=100, last_refresh_status="refresh_failed")
        assert _by_severity(RowCountDriftCheck().run(ctx), Severity.WARN)

    def test_successful_statuses_are_silent(self) -> None:
        for status in ("built", "refreshed"):
            ctx = _ctx(last_row_count=10, approx_row_count=10, last_refresh_status=status)
            assert RowCountDriftCheck().run(ctx) == [], status

    def test_the_measured_case_reports_both_signals(self) -> None:
        """Hidden data and a failed refresh are separate facts about one mart."""
        ctx = _ctx(
            last_row_count=0,
            approx_row_count=52_086_049,
            last_refresh_status="refresh_failed",
        )
        findings = RowCountDriftCheck().run(ctx)
        assert len(findings) == 2
        assert {f.severity for f in findings} == {Severity.CRITICAL, Severity.WARN}


class TestRegistration:
    def test_check_is_registered_first(self) -> None:
        """Reachability precedes column quality: an unreachable mart's types
        are a secondary question."""
        checks = build_default_mart_checks()
        assert checks[0].name == "mart_hidden_despite_rows"

    def test_every_finding_carries_a_remediation(self) -> None:
        ctx = _ctx(
            last_row_count=0,
            approx_row_count=1_000,
            last_refresh_status="refresh_failed",
        )
        assert all(f.payload.get("remediation") for f in RowCountDriftCheck().run(ctx))
