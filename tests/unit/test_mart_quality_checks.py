"""The mart quality auditor: does it find what we found by hand?

Every check here encodes a defect that reached production or staging and was
discovered by a person reading an answer and noticing the number was wrong.
There are 71 marts and nobody knows how many carry the same shapes, which is
the whole reason this sweep exists.

Each test names the incident it generalises.
"""

from __future__ import annotations

from app.application.marts.quality.auditor import _coverage_counts, _source_refs, summarize
from app.application.marts.quality.check import MartCheck
from app.application.marts.quality.checks import build_default_mart_checks
from app.application.marts.quality.checks.numeric_typing import NumericTypingCheck
from app.application.marts.quality.checks.row_filter import RowFilterCheck
from app.application.marts.quality.checks.source_coverage import SourceCoverageCheck
from app.application.marts.quality.context import MartAuditContext, MartColumn, SourceTable
from app.application.validation.detector import Mode, Severity


def _ctx(**kwargs) -> MartAuditContext:
    base = {"mart_id": "m", "view_name": "m"}
    base.update(kwargs)
    return MartAuditContext(**base)


def _cols(*pairs) -> tuple[MartColumn, ...]:
    return tuple(MartColumn(name=n, pg_type=t) for n, t in pairs)


class TestSourceCoverage:
    """presupuesto_nacional_ejecutado served a ranking off 36 of 560 tables."""

    def test_flags_a_mart_built_from_a_fraction_of_its_sources(self) -> None:
        ctx = _ctx(candidate_table_count=560, kept_table_count=36, last_row_count=91_299)
        findings = SourceCoverageCheck().run(ctx)
        assert len(findings) == 1
        assert findings[0].severity is Severity.CRITICAL
        assert findings[0].payload["kept_ratio"] == 0.0643

    def test_full_coverage_is_silent(self) -> None:
        ctx = _ctx(candidate_table_count=12, kept_table_count=12)
        assert SourceCoverageCheck().run(ctx) == []

    def test_a_mild_filter_is_not_reported(self) -> None:
        """Dimension clusters legitimately drop some tables."""
        ctx = _ctx(candidate_table_count=10, kept_table_count=8)
        assert SourceCoverageCheck().run(ctx) == []

    def test_a_moderate_drop_warns_without_crying_critical(self) -> None:
        ctx = _ctx(candidate_table_count=10, kept_table_count=4)
        findings = SourceCoverageCheck().run(ctx)
        assert findings and findings[0].severity is Severity.WARN

    def test_silent_when_the_denominator_is_unknown(self) -> None:
        """A mart built before the coverage marker existed: report nothing.

        Inventing a denominator would produce a confident wrong ratio, which
        is the failure mode the whole audit exists to prevent.
        """
        ctx = _ctx(source_tables=(SourceTable("raw", "t1"),))
        assert SourceCoverageCheck().applicable_to(ctx) is False


class TestNumericTyping:
    """Amounts left as TEXT: the shape behind the original wrong ranking."""

    def test_flags_a_text_amount_column(self) -> None:
        ctx = _ctx(columns=_cols(("credito_vigente", "text")))
        findings = NumericTypingCheck().run(ctx)
        assert len(findings) == 1
        assert findings[0].severity is Severity.CRITICAL
        assert findings[0].payload["column"] == "credito_vigente"

    def test_numeric_amounts_are_clean(self) -> None:
        ctx = _ctx(columns=_cols(("credito_vigente", "numeric"), ("anio", "integer")))
        assert NumericTypingCheck().run(ctx) == []

    def test_reports_every_offending_column_separately(self) -> None:
        """One mart, three bad columns, three findings to fix independently."""
        ctx = _ctx(
            columns=_cols(
                ("credito_vigente", "text"),
                ("credito_devengado", "text"),
                ("monto_total", "text"),
                ("jurisdiccion", "text"),
            )
        )
        findings = NumericTypingCheck().run(ctx)
        assert {f.payload["column"] for f in findings} == {
            "credito_vigente",
            "credito_devengado",
            "monto_total",
        }

    def test_identifier_columns_are_not_amounts(self) -> None:
        """Casting a CUIT or a jurisdiction code would destroy leading zeros."""
        ctx = _ctx(columns=_cols(("jurisdiccion_id", "text"), ("cuit", "text")))
        assert NumericTypingCheck().run(ctx) == []

    def test_a_text_year_warns_rather_than_criticals(self) -> None:
        """`'2.022'` orphaned 1.602 rows, but a TEXT year is survivable."""
        ctx = _ctx(columns=_cols(("anio", "text")))
        findings = NumericTypingCheck().run(ctx)
        assert len(findings) == 1
        assert findings[0].severity is Severity.WARN


class TestRowFilter:
    """The devengado guard that dropped 42 % of source rows before GROUP BY."""

    _BAD_SQL = (
        "SELECT anio, SUM(credito_vigente::numeric) AS v "
        'FROM raw."t" '
        "WHERE ejercicio IS NOT NULL "
        "  AND credito_devengado::numeric <= credito_vigente::numeric "
        "GROUP BY anio"
    )
    _GOOD_SQL = (
        "SELECT anio, SUM(credito_vigente::numeric) AS v "
        'FROM raw."t" WHERE ejercicio IS NOT NULL GROUP BY anio '
        "HAVING SUM(credito_devengado::numeric) <= SUM(credito_vigente::numeric)"
    )

    def test_flags_an_amount_comparison_in_the_where(self) -> None:
        ctx = _ctx(resolved_sql=self._BAD_SQL, last_row_count=4962)
        findings = RowFilterCheck().run(ctx)
        assert len(findings) == 1
        assert findings[0].severity is Severity.CRITICAL

    def test_the_having_form_is_clean(self) -> None:
        """The fix must not keep tripping the check that found the bug."""
        ctx = _ctx(resolved_sql=self._GOOD_SQL, last_row_count=4961)
        assert RowFilterCheck().run(ctx) == []

    def test_ignores_queries_that_do_not_aggregate(self) -> None:
        sql = 'SELECT * FROM raw."t" WHERE credito_devengado <= credito_vigente'
        assert RowFilterCheck().applicable_to(_ctx(resolved_sql=sql)) is False

    def test_a_comparison_inside_a_comment_is_not_a_filter(self) -> None:
        sql = (
            "-- credito_devengado <= credito_vigente se movio a HAVING\n"
            'SELECT anio FROM raw."t" WHERE anio IS NOT NULL GROUP BY anio'
        )
        assert RowFilterCheck().run(_ctx(resolved_sql=sql)) == []

    def test_row_delta_is_context_not_evidence(self) -> None:
        """A GROUP BY collapses rows by design, so the drop cannot stand alone."""
        ctx = _ctx(
            resolved_sql=self._BAD_SQL,
            last_row_count=474,
            source_tables=(SourceTable("raw", "t", row_count=7635),),
        )
        payload = RowFilterCheck().run(ctx)[0].payload
        assert payload["row_delta_pct"] is not None
        assert payload["source_rows"] == 7635


class TestAuditorPlumbing:
    def test_extracts_source_tables_from_resolved_sql(self) -> None:
        sql = (
            'SELECT * FROM (SELECT 1 FROM raw."a__h__v1" UNION ALL SELECT 1 FROM raw."b__h__v2") x'
        )
        assert _source_refs(sql) == [("raw", "a__h__v1"), ("raw", "b__h__v2")]

    def test_reads_the_coverage_marker(self) -> None:
        assert _coverage_counts("(/* macro_coverage: kept 36 of 560 */ SELECT 1)") == (36, 560)

    def test_absent_marker_yields_no_counts(self) -> None:
        assert _coverage_counts("SELECT 1") == (None, None)

    def test_summary_counts_by_severity(self) -> None:
        ctx = _ctx(columns=_cols(("monto", "text"), ("anio", "text")))
        results = [(ctx, NumericTypingCheck().run(ctx)), (_ctx(mart_id="clean"), [])]
        summary = summarize(results)
        assert summary["marts_audited"] == 2
        assert summary["marts_with_findings"] == 1
        assert summary["by_severity"] == {"critical": 1, "warn": 1}


class TestCheckRegistry:
    def test_default_checks_are_registered(self) -> None:
        names = {c.name for c in build_default_mart_checks()}
        assert names == {
            "mart_hidden_despite_rows",
            "mart_source_coverage",
            "mart_amount_filter_before_aggregation",
            "mart_amount_column_is_text",
            "mart_duplicate_rows",
        }

    def test_every_check_is_a_mart_check(self) -> None:
        assert all(isinstance(c, MartCheck) for c in build_default_mart_checks())

    def test_findings_carry_the_audit_mode(self) -> None:
        """They share a table with ingestion findings; the mode keeps them apart."""
        ctx = _ctx(columns=_cols(("monto", "text")))
        findings = NumericTypingCheck().run(ctx)
        assert findings[0].mode is Mode.MART_AUDIT

    def test_findings_carry_a_remediation(self) -> None:
        """A finding nobody knows how to act on is noise."""
        ctx = _ctx(columns=_cols(("monto", "text")))
        assert NumericTypingCheck().run(ctx)[0].payload["remediation"]


# ── DuplicateRowsCheck ───────────────────────────────────────────────────────
# Generalises two defects found on 2026-08-24, both of which built `success`
# and reported a healthy row count.


def test_duplicate_rows_flags_the_snic_shape() -> None:
    """`delitos_argentina_snic`: 348.933 of 1.000.000 rows were exact copies of
    four overlapping SNIC resources, so every crime count ran ~30 % high."""
    from app.application.marts.quality.checks.duplicate_rows import DuplicateRowsCheck

    ctx = _ctx(
        mart_id="delitos_argentina_snic",
        scanned_row_count=1_000_000,
        distinct_row_count=651_067,
        kept_table_count=4,
    )
    findings = DuplicateRowsCheck().run(ctx)
    assert len(findings) == 1
    assert findings[0].payload["duplicate_rows"] == 348_933
    assert findings[0].payload["duplicate_share"] == 0.3489
    assert findings[0].mode is Mode.MART_AUDIT


def test_duplicate_rows_escalates_when_the_mart_is_mostly_copies() -> None:
    """`energia_petroleo_gas_produccion`: 4.103.759 rows, 99.816 distinct."""
    from app.application.marts.quality.checks.duplicate_rows import DuplicateRowsCheck

    ctx = _ctx(scanned_row_count=4_103_759, distinct_row_count=99_816, kept_table_count=43)
    findings = DuplicateRowsCheck().run(ctx)
    assert len(findings) == 1
    assert findings[0].severity is Severity.CRITICAL


def test_duplicate_rows_stays_quiet_on_ordinary_repetition() -> None:
    """A mart projecting few columns from a detailed source repeats rows by
    design. Below the threshold this must say nothing, or the sweep becomes
    noise nobody reads."""
    from app.application.marts.quality.checks.duplicate_rows import DuplicateRowsCheck

    ctx = _ctx(scanned_row_count=100_000, distinct_row_count=95_000)
    assert DuplicateRowsCheck().run(ctx) == []


def test_duplicate_rows_says_nothing_when_it_could_not_measure() -> None:
    """The scan times out or the view is gone. A check that cannot measure has
    to stay silent rather than report zero duplicates as health."""
    from app.application.marts.quality.checks.duplicate_rows import DuplicateRowsCheck

    check = DuplicateRowsCheck()
    ctx = _ctx(scanned_row_count=None, distinct_row_count=None)
    assert check.applicable_to(ctx) is False
    assert check.run(ctx) == []


def test_duplicate_rows_skips_tiny_marts() -> None:
    """On 64 rows a handful of repeats crosses any percentage without meaning
    anything — `mujer_centros_atencion` measures 75 % and is 48 rows."""
    from app.application.marts.quality.checks.duplicate_rows import DuplicateRowsCheck

    assert DuplicateRowsCheck().applicable_to(_ctx(scanned_row_count=64)) is False


def test_duplicate_rows_reports_whether_it_sampled() -> None:
    """A share measured on a sample is a signal, not a count, and the finding
    has to say which it is."""
    from app.application.marts.quality.checks.duplicate_rows import DuplicateRowsCheck

    ctx = _ctx(scanned_row_count=400_000, distinct_row_count=591, duplicate_scan_sampled=True)
    finding = DuplicateRowsCheck().run(ctx)[0]
    assert finding.payload["sampled"] is True
    assert "sample" in finding.message


def test_duplicate_rows_is_in_the_default_sweep() -> None:
    from app.application.marts.quality.checks.duplicate_rows import DuplicateRowsCheck

    assert any(isinstance(c, DuplicateRowsCheck) for c in build_default_mart_checks())
