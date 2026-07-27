"""Budget invariants belong in HAVING, never in a row-level WHERE.

`presupuesto_consolidado` v0.2.1 added `WHERE credito_devengado <=
credito_vigente` so an execution dashboard could not show >100 %. The
invariant is real — spending more than the approved credit is legally
impossible — but it is a *budgetary* invariant: it holds over an
aggregate, not over each source row.

Measured against `cache_presupuesto_credito_2024` on 2026-07-26:

  * the row filter dropped 3.219 of 7.635 rows (42,2 %), taking 55,1 %
    of the year's devengado with them — the YAML comment estimated
    "1 row";
  * 1.921 of those rows are `vigente = 0, devengado > 0`, which is
    ordinary execution against reallocated credit, not corrupt data;
  * the universities program was left with 0,03 % of its real execution;
  * aggregated by (jurisdicción, programa), 474 of 474 groups satisfy the
    invariant — zero violations. The filter was destroying data to
    prevent a condition that does not occur at the grain it is served at.

This is the same shape as the NL2SQL lossy-numeric-filter bug: discarding
rows so a constraint passes yields an incomplete total presented as fact.
A `HAVING` cannot do this — it is evaluated after aggregation, so it can
only reject a group that genuinely violates the invariant.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.application.marts.mart import load_all_marts

_MARTS_DIR = Path(__file__).resolve().parents[2] / "config" / "marts"

# Comparisons between two amount-ish columns. A row-level guard of this
# shape is the pattern being banned; the same expression wrapped in
# SUM()/AVG() inside a HAVING is fine.
_AMOUNT_COMPARISON = re.compile(
    r"\b(?P<left>\w*(?:devengado|vigente|monto|importe|credito)\w*)\s*"
    r"(?:::\w+)?\s*(?:<=|<|>=|>)\s*"
    r"(?P<right>\w*(?:devengado|vigente|monto|importe|credito)\w*)\b",
    re.IGNORECASE,
)


def _strip_sql_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _where_clause(sql: str) -> str:
    """The WHERE body, stopping at GROUP BY / HAVING / ORDER BY / LIMIT."""
    match = re.search(
        r"\bWHERE\b(?P<body>.*?)"
        r"(?=\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|\bWINDOW\b|$)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    return match.group("body") if match else ""


def _aggregating_marts() -> list:
    return [m for m in load_all_marts(_MARTS_DIR) if "GROUP BY" in (m.sql or "").upper()]


class TestNoAmountComparisonInWhere:
    @pytest.mark.parametrize("mart", _aggregating_marts(), ids=lambda m: m.id)
    def test_amount_invariants_are_not_row_filters(self, mart) -> None:
        sql = _strip_sql_comments(mart.sql or "")
        offenders = _AMOUNT_COMPARISON.findall(_where_clause(sql))
        assert not offenders, (
            f"{mart.id}: comparing two amount columns in a WHERE drops source rows "
            f"before aggregation. In presupuesto_consolidado this silently removed "
            f"42 % of rows and 55 % of the devengado. Move it to HAVING, where it "
            f"is evaluated on the aggregate the mart actually serves. Found: {offenders}"
        )


class TestConsolidadoKeepsItsGuard:
    """Moving the filter must not mean losing the protection."""

    def _consolidado(self):
        marts = {m.id: m for m in load_all_marts(_MARTS_DIR)}
        mart = marts.get("presupuesto_consolidado")
        assert mart is not None, "mart not found — was it renamed?"
        return mart

    def test_invariant_still_enforced_after_aggregation(self) -> None:
        sql = _strip_sql_comments(self._consolidado().sql or "")
        having = re.search(r"\bHAVING\b(?P<body>.*)", sql, re.IGNORECASE | re.DOTALL)
        assert having, "the devengado <= vigente guard must survive as a HAVING"
        body = having.group("body")
        assert "devengado" in body.lower() and "vigente" in body.lower()
        assert "sum(" in body.lower(), "the guard must compare aggregates, not raw rows"

    def test_version_was_bumped(self) -> None:
        """build_mart keys off the version, and the rows must be rebuilt."""
        assert self._consolidado().version != "0.4.0"
