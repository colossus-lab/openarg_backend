"""Rows a mart serves more than once.

Two marts shipped this defect and neither showed a symptom anyone could see:

  * `delitos_argentina_snic` unioned four overlapping SNIC resources with no
    dedup. 348.933 of its 1.000.000 rows were exact copies, so every crime
    count it served ran about 30 % high — on the most-asked topic in the
    corpus. Robberies for 2024 read as ~900.000 against a real 630.781.
  * `energia_petroleo_gas_produccion` unioned the 43 tables matching its
    resource pattern. 4.103.759 rows, 99.816 of them distinct: 97,6 %
    duplicated, most carrying a NULL quantity because only one of those 43
    tables had the measurement column at all.

Both built `success`, both reported a healthy `last_row_count`, and a bigger
number looks like more data. Nothing in the sweep asked this question until
now.

**A duplicate is not automatically a defect, so this reports and does not
accuse.** Whether two identical rows are one row too many depends on what a
row means: `ddjj_funcionarios_federales` measures 50 % duplicates and is
correct, because a sworn declaration may legitimately list two identical
debts. The share is evidence for a person to judge, which is why the payload
carries the counts and how many tables the mart was built from — snapshot
repetition and a real fan-out look different once you can see both numbers.
"""

from __future__ import annotations

from app.application.marts.quality.check import MartCheck
from app.application.marts.quality.context import MartAuditContext
from app.application.validation.detector import Finding, Severity

# Below this share, repetition is ordinary: a mart projecting few columns from
# a detailed source repeats rows by design.
_WARN_SHARE = 0.20
# Above this, the mart is mostly copies of itself. Both defects found on
# 2026-08-24 were past it.
_CRITICAL_SHARE = 0.50


class DuplicateRowsCheck(MartCheck):
    name = "mart_duplicate_rows"
    version = "1"
    severity = Severity.WARN

    def applicable_to(self, ctx: MartAuditContext) -> bool:
        # No measurement, nothing to say. Also skip trivially small marts,
        # where a handful of repeats crosses any percentage threshold without
        # meaning anything.
        return bool(ctx.scanned_row_count) and ctx.scanned_row_count >= 1000

    def run(self, ctx: MartAuditContext) -> list[Finding]:
        scanned = ctx.scanned_row_count or 0
        distinct = ctx.distinct_row_count
        if distinct is None or scanned <= 0:
            return []
        duplicates = scanned - distinct
        if duplicates <= 0:
            return []
        share = duplicates / scanned
        if share < _WARN_SHARE:
            return []

        severity = Severity.CRITICAL if share >= _CRITICAL_SHARE else Severity.WARN
        scope = (
            f"a {scanned:,}-row sample" if ctx.duplicate_scan_sampled else f"all {scanned:,} rows"
        )
        return [
            self._finding(
                severity=severity,
                message=(
                    f"{ctx.mart_id}: {duplicates:,} of {scope} are exact duplicates "
                    f"({share:.1%}). Whether that is a defect depends on what one row "
                    f"means — check the mart's grain against its source cluster."
                ),
                payload={
                    "scanned_rows": scanned,
                    "distinct_rows": distinct,
                    "duplicate_rows": duplicates,
                    "duplicate_share": round(share, 4),
                    "sampled": ctx.duplicate_scan_sampled,
                    # Snapshot repetition scales with how many tables were
                    # unioned; a fan-out inside one table does not. Both
                    # numbers travel so the difference is visible.
                    "kept_table_count": ctx.kept_table_count,
                    "candidate_table_count": ctx.candidate_table_count,
                },
            )
        ]
