"""A mart holding data that nobody can reach.

Discovery filters on `COALESCE(last_row_count, 0) > 0`. The failure paths of
`build_mart` / `refresh_mart` write `last_row_count = 0` deliberately, so a
mart that failed to build stops being offered. That is the right behaviour
when the mart is empty.

It is the wrong behaviour when the build failed but the previous
materialization is still there: the view keeps its rows, the stored count says
zero, and the mart disappears from serving with nothing reporting it.
`mart.presupuesto_consolidado` sat like that for months. Measured on staging
2026-07-28, `mediaciones_prejudiciales` is in exactly that state right now —
`refresh_failed`, `last_row_count = 0`, and 52.086.049 rows in the view.

Both signals here are about the mart's own bookkeeping rather than its data,
which is why they can be checked nightly for free.
"""

from __future__ import annotations

from app.application.marts.quality.check import MartCheck
from app.application.marts.quality.context import MartAuditContext
from app.application.validation.detector import Finding, Severity

_FAILED_STATUSES = frozenset({"build_failed", "refresh_failed"})


class RowCountDriftCheck(MartCheck):
    name = "mart_hidden_despite_rows"
    version = "1"
    severity = Severity.CRITICAL

    def applicable_to(self, ctx: MartAuditContext) -> bool:
        # A mart withheld on purpose is not "hidden"; that decision is
        # documented in the YAML and has its own reason attached.
        return not ctx.serving_blocked

    def run(self, ctx: MartAuditContext) -> list[Finding]:
        findings: list[Finding] = []
        stored = ctx.last_row_count or 0
        actual = ctx.approx_row_count

        if stored == 0 and actual is not None and actual > 0:
            findings.append(
                self._finding(
                    severity=Severity.CRITICAL,
                    message=(
                        f"{ctx.mart_id} está oculto del discovery "
                        f"(last_row_count={ctx.last_row_count}) pero la vista tiene "
                        f"~{actual:,} filas: el último build falló y dejó el contador "
                        f"en cero sobre datos que siguen ahí"
                    ),
                    payload={
                        "mart_id": ctx.mart_id,
                        "last_row_count": ctx.last_row_count,
                        "approx_rows": actual,
                        "last_refresh_status": ctx.last_refresh_status,
                        "hits_30d": ctx.hits_30d,
                        "remediation": (
                            "Rebuildear el mart y mirar por qué falló. Si el build "
                            "vuelve a fallar, los datos de la vista son de un build "
                            "anterior y su frescura es desconocida: decidir "
                            "explícitamente entre repararlo o bloquearlo con razón "
                            "en el YAML, en vez de dejarlo desaparecido."
                        ),
                    },
                )
            )

        status = (ctx.last_refresh_status or "").strip()
        if status in _FAILED_STATUSES:
            findings.append(
                self._finding(
                    severity=Severity.WARN,
                    message=(
                        f"{ctx.mart_id} quedó en estado '{status}': lo que se sirva "
                        f"—o se deje de servir— viene de un build anterior"
                    ),
                    payload={
                        "mart_id": ctx.mart_id,
                        "last_refresh_status": status,
                        "last_row_count": ctx.last_row_count,
                        "approx_rows": actual,
                        "hits_30d": ctx.hits_30d,
                        "remediation": (
                            "Revisar los logs de build_mart/refresh_mart para este "
                            "mart_id. Un fallo persistente no se reporta en ningún "
                            "otro lado."
                        ),
                    },
                )
            )
        return findings
