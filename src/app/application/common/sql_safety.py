"""Shared SQL-safety helpers.

`is_pure_select` parses a SQL string with sqlglot and returns whether it is
a single, top-level SELECT with no embedded DML/DDL anywhere in the AST
(including CTEs and subqueries).

Why an AST check instead of a regex?
The previous Serving Port `query()` rejected writes by `re.search` for
keywords like `insert|update|delete|drop|...`. That regex has two failure
modes any motivated user can hit:
  1. False negatives: SQL comments, string literals containing the
     keyword, or stacked statements that the regex misses.
  2. False positives: a column or alias named `update_at` blocks legitimate
     SELECTs.
sqlglot parses the actual statement, walks subqueries, and only accepts a
node-tree that is structurally a pure read. Same reasoning as the sandbox
adapter (`pg_sandbox_adapter._validate_sql_ast`); this helper is the
generic, allowlist-free version other modules can reuse.

DEBT-016-002.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def is_pure_select(sql: str) -> tuple[bool, str | None]:
    """Return `(ok, reason_if_rejected)` for the given SQL string.

    Accepts a single top-level `SELECT` with no embedded DML/DDL anywhere
    in the AST. Comments, CTEs, subqueries and unions are fine as long as
    every node is read-only.

    On parse failure or any unexpected exception we err on the side of
    rejection — callers downstream are expected to refuse the query.
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return False, "sqlglot is not installed"

    try:
        statements = sqlglot.parse(sql, dialect="postgres")
    except Exception:
        return False, "could not parse SQL"

    if not statements:
        return False, "empty statement"
    if len(statements) > 1:
        return False, "only single SELECT statements are allowed"

    stmt = statements[0]
    if stmt is None:
        return False, "could not parse SQL"

    # Accept SELECT and set operations (UNION / INTERSECT / EXCEPT) at the
    # top level — they are all pure reads. Reject everything else.
    read_only_top = (exp.Select, exp.Union, exp.Intersect, exp.Except)
    if not isinstance(stmt, read_only_top):
        return False, "only SELECT queries are allowed"

    forbidden = (
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Drop,
        exp.Create,
        exp.Alter,
        exp.Command,
    )
    for node in stmt.walk():
        if isinstance(node, forbidden):
            return False, f"forbidden SQL operation: {type(node).__name__}"

    return True, None
