"""Shared SQL-safety helpers.

`is_pure_select` parses a SQL string with sqlglot and returns whether it is
a single, top-level read-only statement (SELECT or set-op of SELECTs) with
no embedded DML/DDL anywhere in the AST (including CTEs and subqueries).

`is_pure_select_for_relation` is the **scope-aware** variant used by the
Serving Port: every `exp.Table` in the AST must resolve to the expected
(schema, table) pair, and no node may reference a table on the internal
blocklist. This closes the round v46 H1+H2 vector where a UNION/JOIN to
`api_keys` / `successful_queries` / `query_analytics` passed the previous
regex-based gate.

Why AST not regex?
The previous Serving Port `query()` rejected writes by `re.search` for
keywords like `insert|update|delete|drop|...`. That regex has two failure
modes any motivated user can hit:
  1. False negatives: SQL comments, string literals containing the
     keyword, or stacked statements that the regex misses.
  2. False positives: a column or alias named `update_at` blocks legitimate
     SELECTs.
sqlglot parses the actual statement, walks subqueries, and only accepts a
node-tree that is structurally a pure read.

DEBT-016-002 (initial regex → AST migration).
Round v46 H1+H2 (UNION/JOIN scope enforcement).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# Internal metadata / PII tables that NL2SQL-generated queries must never
# reach, regardless of schema. Mirrors `_FORBIDDEN_TABLES` in the sandbox
# adapter — the duplicate definition there is kept in sync because the
# adapter ships defense-in-depth at the SQL execution boundary; we apply
# the same set at the Serving Port AST gate.
# Matched on the bare table name, case-insensitive.
INTERNAL_TABLE_BLOCKLIST = frozenset(
    {
        "catalog_resources",
        "raw_table_versions",
        "cached_datasets",
        "mart_definitions",
        "mart_sample_queries",
        "query_analytics",
        "query_cache",
        "table_catalog",
        "dataset_chunks",
        "datasets",
        "parse_repair_audit",
        "successful_queries",
        "user_queries",
        "query_dataset_links",
        "agent_tasks",
        "api_keys",
        "api_usage",
        "users",
        "messages",
        "conversations",
        "sesion_chunks",
        "alembic_version",
    }
)


def is_pure_select(sql: str) -> tuple[bool, str | None]:
    """Return `(ok, reason_if_rejected)` for the given SQL string.

    Accepts a single top-level `SELECT` (or `UNION`/`INTERSECT`/`EXCEPT` of
    SELECTs) with no embedded DML/DDL anywhere in the AST. Comments, CTEs,
    subqueries and unions are fine as long as every node is read-only.

    **WARNING**: this helper does NOT enforce a table allowlist or blocklist.
    A caller that hands SQL to the Serving Port MUST use
    `is_pure_select_for_relation` instead — otherwise a structurally pure
    `SELECT col FROM mart.foo UNION ALL SELECT email FROM api_keys` will
    pass this gate and exfiltrate internal data (round v46 H1+H2).

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


def is_pure_select_for_relation(
    sql: str,
    *,
    expected_schema: str,
    expected_table: str,
    blocklist: frozenset[str] | None = None,
) -> tuple[bool, str | None]:
    """Strict scope-checked variant of `is_pure_select` for the Serving Port.

    Returns `(True, None)` only when ALL of these hold:
      1. The SQL parses to a single top-level read-only statement (SELECT or
         a set-op of SELECTs).
      2. No DML/DDL node appears anywhere in the AST.
      3. EVERY `exp.Table` reference in the AST resolves to the expected
         `(schema, table)` pair. A `public` expected schema also accepts
         unqualified references; medallion schemas (`mart`, `raw`, etc.)
         must be schema-qualified. Any reference whose schema or bare
         table name differs from the expected pair is a leak vector and
         is rejected.
      4. No referenced table sits on the internal blocklist (defense-in-
         depth: even if the resource_id check above mis-qualifies, the
         hard blocklist still wins).

    Closes round v46 H1+H2:
      `SELECT col FROM mart.foo UNION ALL SELECT email FROM api_keys`
    now fails on point (3) AND point (4).
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return False, "sqlglot is not installed"

    blocklist_l = blocklist if blocklist is not None else INTERNAL_TABLE_BLOCKLIST
    expected_schema_l = expected_schema.lower()
    expected_table_l = expected_table.lower()

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

    # H1+H2 fix (round v46): walk EVERY `exp.Table` and assert it points
    # at the expected (schema, table). Mixed-schema unions, sneaky JOINs,
    # CTE aliases that secretly reference `api_keys`, all fail here. We
    # also collect CTE aliases so a `WITH x AS (SELECT ...) SELECT * FROM x`
    # is not falsely rejected on the `x` reference.
    cte_aliases: set[str] = set()
    for node in stmt.walk():
        if isinstance(node, forbidden):
            return False, f"forbidden SQL operation: {type(node).__name__}"
        if isinstance(node, exp.CTE):
            alias = node.alias_or_name
            if alias:
                cte_aliases.add(alias.lower())

    for node in stmt.walk():
        if not isinstance(node, exp.Table):
            continue
        bare = (node.name or "").lower()
        if not bare:
            # Defensive: an empty Table node has no business in our SQL.
            return False, "unparseable table reference"
        # CTE aliases resolve internally — they are not physical tables.
        if bare in cte_aliases:
            continue
        # Defense-in-depth: hard internal blocklist always wins, regardless
        # of schema. Even if the expected_schema is `public` and the
        # caller mistakenly lists `successful_queries` as the expected
        # table, the blocklist still blocks the query.
        if bare in blocklist_l:
            return False, f"forbidden table reference: {bare}"

        schema_l = (node.db or "").lower()
        if expected_schema_l == "public":
            # Public schema accepts unqualified or "public.<table>" forms.
            if schema_l not in ("", "public"):
                return False, (
                    f"out-of-scope table reference: {schema_l}.{bare} "
                    f"(expected public.{expected_table_l})"
                )
        else:
            if schema_l != expected_schema_l:
                return False, (
                    f"out-of-scope table reference: "
                    f"{schema_l or '<unqualified>'}.{bare} "
                    f"(expected {expected_schema_l}.{expected_table_l})"
                )

        if bare != expected_table_l:
            return False, (
                f"out-of-scope table reference: {schema_l or 'public'}.{bare} "
                f"(expected {expected_schema_l}.{expected_table_l})"
            )

    return True, None
