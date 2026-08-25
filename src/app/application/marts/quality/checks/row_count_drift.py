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

The mirror case costs a real count and is worth it. `pobreza_indec_aglomerados`
served **zero rows while the registry said 864** — the count left over from a
build months earlier, when the sources still parsed. The mart stayed
discoverable, every question about poverty routed to it, and it answered with
nothing. The `mart_empty` alert could not see it either, because that query
reads `last_row_count` and the registry was the thing that was wrong.

A stored count is a claim about the past. Believing it is how an empty mart
stays invisible.
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
                    key="hidden_despite_rows",
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

        # The mirror of the case above: the registry claims rows and the view
        # has none. Only when the scan read the whole view — above the sampling
        # threshold `scanned_row_count` is a sample and says nothing about the
        # total.
        scanned = ctx.scanned_row_count
        if not ctx.duplicate_scan_sampled and scanned == 0 and stored > 0:
            findings.append(
                self._finding(
                    severity=Severity.CRITICAL,
                    key="empty_despite_stored_count",
                    message=(
                        f"{ctx.mart_id} no tiene ninguna fila pero el registro dice "
                        f"{stored:,}: sigue siendo elegible para el routing y "
                        f"responde vacío. El conteo guardado es de un build anterior"
                    ),
                    payload={
                        "mart_id": ctx.mart_id,
                        "last_row_count": ctx.last_row_count,
                        "actual_rows": 0,
                        "last_refresh_status": ctx.last_refresh_status,
                        "hits_30d": ctx.hits_30d,
                        "remediation": (
                            "Rebuildear y mirar el `macro_coverage` del SQL "
                            "resuelto: si dice `kept 0 of N`, el patrón dejó de "
                            "matchear y el mart no se arregla reconstruyéndolo. "
                            "Medido en `pobreza_indec_aglomerados`: sus 17 tablas "
                            "perdieron el encabezado al colectarse y hay que "
                            "re-colectar el recurso."
                        ),
                    },
                )
            )

        status = (ctx.last_refresh_status or "").strip()
        if status in _FAILED_STATUSES:
            findings.append(
                self._finding(
                    severity=Severity.WARN,
                    key="failed_refresh_status",
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
