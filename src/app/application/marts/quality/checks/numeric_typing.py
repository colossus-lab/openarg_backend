"""Amount columns left as TEXT.

`_build_union` casts every column of the UNION to `::text` so heterogeneous
source tables can be stacked. The outer SELECT of the mart is supposed to cast
back. When it does not, a column named `credito_vigente` or `monto` reaches
serving as TEXT holding `'1.234,56'` and `'1234.56'` in the same column, and
every aggregate over it is either an error or a silent undercount.

This is the shape that produced the original incident: amounts stored as TEXT,
summed after a regex filter that dropped the comma-decimal rows.
"""

from __future__ import annotations

import re

from app.application.marts.quality.check import MartCheck
from app.application.marts.quality.context import MartAuditContext
from app.application.validation.detector import Finding, Severity

# Names that denote a quantity in Argentine public data. Matched as substrings
# because the real columns are `credito_devengado`, `monto_total`,
# `importe_facturado`, `cantidad_hechos`.
_AMOUNT_HINTS = (
    "credito",
    "monto",
    "importe",
    "valor",
    "total",
    "cantidad",
    "presupuesto",
    "gasto",
    "recaudacion",
    "saldo",
    "precio",
)
# Names that look numeric but are identifiers: casting them would be wrong, and
# leading zeros are meaningful (CUIT, códigos de jurisdicción).
_ID_HINTS = ("_id", "id_", "codigo", "cuit", "cuil", "dni", "expediente")

_NUMERIC_TYPES = ("numeric", "integer", "bigint", "double precision", "real", "smallint")
_YEAR_RE = re.compile(r"\b(anio|ano|year|ejercicio)\b")


def _is_numeric(pg_type: str) -> bool:
    return any(pg_type.lower().startswith(t) for t in _NUMERIC_TYPES)


def _looks_like_amount(column: str) -> bool:
    lowered = column.lower()
    if any(hint in lowered for hint in _ID_HINTS):
        return False
    return any(hint in lowered for hint in _AMOUNT_HINTS)


class NumericTypingCheck(MartCheck):
    name = "mart_amount_column_is_text"
    version = "1"
    severity = Severity.CRITICAL

    def applicable_to(self, ctx: MartAuditContext) -> bool:
        return bool(ctx.columns)

    def run(self, ctx: MartAuditContext) -> list[Finding]:
        findings: list[Finding] = []
        for column in ctx.columns:
            if _is_numeric(column.pg_type):
                continue
            is_amount = _looks_like_amount(column.name)
            is_year = bool(_YEAR_RE.search(column.name.lower()))
            if not (is_amount or is_year):
                continue
            findings.append(
                self._finding(
                    # A year stored as TEXT is survivable; an amount is not.
                    severity=Severity.CRITICAL if is_amount else Severity.WARN,
                    message=(
                        f"{ctx.qualified}.{column.name} es {column.pg_type} pero "
                        f"parece {'un monto' if is_amount else 'un año'}: "
                        f"agregar sobre texto produce errores o totales incompletos"
                    ),
                    payload={
                        "mart_id": ctx.mart_id,
                        "column": column.name,
                        "pg_type": column.pg_type,
                        "kind": "amount" if is_amount else "year",
                        "hits_30d": ctx.hits_30d,
                        "remediation": (
                            "Castear en el SELECT externo del YAML "
                            "(`SUM(col::numeric)::numeric AS col`). El `::text` del "
                            "UNION es sólo para apilar tablas heterogéneas; la capa "
                            "de arriba tiene que devolver el tipo real."
                        ),
                    },
                )
            )
        return findings
