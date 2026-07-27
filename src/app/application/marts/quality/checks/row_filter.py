"""Rows the mart's own WHERE discards before aggregating.

Measured 2026-07-27 on `presupuesto_consolidado`: a guard reading
`WHERE credito_devengado <= credito_vigente`, added so an execution dashboard
could not show >100 %, ran per source row and dropped 3.219 of 7.635 (42,2 %),
taking 55,1 % of the year's executed budget with it. Aggregated by the grain
the mart actually serves, 474 of 474 groups satisfied the invariant — the
filter was destroying data to prevent a condition that did not occur.

Two signals, neither conclusive alone:

  * a comparison between two amount columns inside a WHERE of an aggregating
    query — the shape itself, catchable without touching data;
  * source rows in, mart rows out — a large drop is not proof (a GROUP BY
    legitimately collapses rows), so it is reported as context, never alone.
"""

from __future__ import annotations

import re

from app.application.marts.quality.check import MartCheck
from app.application.marts.quality.context import MartAuditContext
from app.application.validation.detector import Finding, Severity

_AMOUNT_COMPARISON = re.compile(
    r"\b(?P<left>\w*(?:devengado|vigente|monto|importe|credito|total)\w*)\s*"
    r"(?:::\w+)?\s*(?P<op><=|<|>=|>)\s*"
    r"(?P<right>\w*(?:devengado|vigente|monto|importe|credito|total)\w*)\b",
    re.IGNORECASE,
)


def _strip_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _where_clause(sql: str) -> str:
    match = re.search(
        r"\bWHERE\b(?P<body>.*?)"
        r"(?=\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|\bWINDOW\b|$)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    return match.group("body") if match else ""


class RowFilterCheck(MartCheck):
    name = "mart_amount_filter_before_aggregation"
    version = "1"
    severity = Severity.CRITICAL

    def applicable_to(self, ctx: MartAuditContext) -> bool:
        sql = (ctx.resolved_sql or "").upper()
        return bool(sql) and "GROUP BY" in sql

    def run(self, ctx: MartAuditContext) -> list[Finding]:
        sql = _strip_comments(ctx.resolved_sql or "")
        matches = _AMOUNT_COMPARISON.findall(_where_clause(sql))
        if not matches:
            return []

        pairs = sorted({f"{left} {op} {right}" for left, op, right in matches})
        source_rows = ctx.source_row_total
        dropped_pct = None
        if source_rows and ctx.last_row_count is not None and source_rows > 0:
            dropped_pct = round(100.0 * (source_rows - ctx.last_row_count) / source_rows, 2)

        return [
            self._finding(
                message=(
                    f"{ctx.mart_id} compara montos en el WHERE de una consulta que "
                    f"agrega ({', '.join(pairs)}): el filtro corre por fila de origen, "
                    f"antes del GROUP BY, y descarta ejecución real"
                ),
                payload={
                    "mart_id": ctx.mart_id,
                    "comparisons": pairs,
                    "source_rows": source_rows,
                    "mart_rows": ctx.last_row_count,
                    # Context, not evidence: a GROUP BY collapses rows by design.
                    "row_delta_pct": dropped_pct,
                    "hits_30d": ctx.hits_30d,
                    "remediation": (
                        "Mover el invariante a HAVING sobre los agregados. Un "
                        "invariante presupuestario se sostiene sobre el total, no "
                        "sobre cada fila: `vigente = 0, devengado > 0` es ejecución "
                        "normal contra crédito reasignado."
                    ),
                },
            )
        ]
