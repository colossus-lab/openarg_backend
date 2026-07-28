"""How much of its own domain a mart actually covers.

`live_tables_by_*` drops any source table missing a required column and logs
"kept N of M". That log line is the only trace: `mart_definitions` records
`last_row_count`, which is happily non-zero for a mart built from 6 % of its
sources. `presupuesto_nacional_ejecutado` served a budget ranking off 36 of 560
tables — 91k of 15.8M rows — and looked healthy by every stored signal.

The number is not a bug on its own: a fact-vs-dimension cluster is *supposed*
to filter. What makes it reportable is that nobody chose it and nobody can see
it.
"""

from __future__ import annotations

from app.application.marts.quality.check import MartCheck
from app.application.marts.quality.context import MartAuditContext
from app.application.validation.detector import Finding, Severity

# Below this share of candidate tables, a mart is answering about a different
# universe than its description implies. Chosen so the measured presupuesto
# case (6,4 %) lands as critical and an ordinary dimension filter does not.
_CRITICAL_RATIO = 0.25
_WARN_RATIO = 0.60


class SourceCoverageCheck(MartCheck):
    name = "mart_source_coverage"
    version = "1"
    severity = Severity.WARN

    def applicable_to(self, ctx: MartAuditContext) -> bool:
        return ctx.candidate_table_count is not None and ctx.candidate_table_count > 1

    def run(self, ctx: MartAuditContext) -> list[Finding]:
        ratio = ctx.kept_ratio
        if ratio is None or ratio >= _WARN_RATIO:
            return []

        kept = ctx.kept_table_count if ctx.kept_table_count is not None else len(ctx.source_tables)
        total = ctx.candidate_table_count
        severity = Severity.CRITICAL if ratio < _CRITICAL_RATIO else Severity.WARN
        return [
            self._finding(
                severity=severity,
                message=(
                    f"{ctx.mart_id} se construye sobre {kept} de {total} tablas "
                    f"candidatas ({ratio:.1%}): el filtro de columnas del macro "
                    f"descartó el resto en silencio"
                ),
                payload={
                    "mart_id": ctx.mart_id,
                    "kept_tables": kept,
                    "candidate_tables": total,
                    "kept_ratio": round(ratio, 4),
                    "last_row_count": ctx.last_row_count,
                    "hits_30d": ctx.hits_30d,
                    "remediation": (
                        "Revisar `expected_columns` / `require_all_columns` en el "
                        "YAML. Si el recorte es intencional (cluster fact-vs-dim), "
                        "documentarlo en `description`; si no, relajar el filtro y "
                        "medir el efecto antes de servir."
                    ),
                },
            )
        ]
