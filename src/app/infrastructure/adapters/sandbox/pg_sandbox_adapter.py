from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.domain.ports.sandbox.sql_sandbox import (
    CachedTableInfo,
    ISQLSandbox,
    SandboxResult,
)

logger = logging.getLogger(__name__)

MAX_ROWS = int(os.getenv("SANDBOX_MAX_ROWS", "1000"))
_SANDBOX_TIMEOUT_MS = int(os.getenv("SANDBOX_TIMEOUT_MS", "10000"))

# Patterns that indicate write/DDL operations (case-insensitive)
_FORBIDDEN_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY|"
    r"EXECUTE|CALL|DO|SET|RESET|VACUUM|ANALYZE|CLUSTER|REINDEX|LOCK|"
    r"COMMENT|SECURITY|LOAD|IMPORT|REFRESH)\b",
    re.IGNORECASE,
)


_ALLOWED_TABLE_PREFIXES = tuple(os.getenv("SANDBOX_TABLE_PREFIX", "cache_").split(","))
# `public` hosts legacy `cache_*` tables — those still require the
# `cache_*` prefix below.
# `mart` and `raw` host curated views and per-version raw landings — both
# emerge from our own pipeline (mart_definitions / raw_table_versions),
# so anything materialized there is already vetted for read-only access.
# We skip the prefix check for those schemas.
_ALLOWED_SCHEMAS = ("public", "mart", "raw")
_PREFIX_FREE_SCHEMAS = ("mart", "raw")

# Tables that sandbox queries are allowed to reference (regex for FROM/JOIN clauses)
_TABLE_REF_PATTERN = re.compile(
    r'\b(?:FROM|JOIN)\s+"?(\w+)"?(?:\."?(\w+)"?)?',
    re.IGNORECASE,
)


def _validate_sql(sql: str) -> str | None:
    """Return an error message if the SQL is not allowed, else None."""
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        return "Empty SQL statement."

    # Remove SQL comments for validation
    no_comments = re.sub(r"--[^\n]*", "", stripped)
    no_comments = re.sub(r"/\*.*?\*/", "", no_comments, flags=re.DOTALL)
    no_comments = no_comments.strip()

    # Must start with SELECT or WITH (CTEs)
    if not re.match(r"^\s*(SELECT|WITH)\b", no_comments, re.IGNORECASE):
        return "Only SELECT queries are allowed."

    # Check for forbidden keywords (defense-in-depth layer 1)
    match = _FORBIDDEN_PATTERNS.search(no_comments)
    if match:
        return f"Forbidden SQL operation: {match.group(0).upper()}"

    # Table allowlist — defense-in-depth layer 2.
    # Public schema: only `cache_*` (or `SANDBOX_TABLE_PREFIX`) tables
    #   are reachable, since other public objects can be sensitive.
    # Mart / raw schemas: every relation is emitted by our own pipeline
    #   (mart_definitions / raw_table_versions). The pipeline already
    #   gates what lands there, so we don't impose a prefix.
    for m in _TABLE_REF_PATTERN.finditer(no_comments):
        first, second = m.group(1), m.group(2)
        if second:
            schema, table = first.lower(), second
            if schema not in _ALLOWED_SCHEMAS:
                return f"Access to schema '{schema}' is not allowed."
            if schema in _PREFIX_FREE_SCHEMAS:
                continue
        else:
            schema, table = "public", first
        if not any(table.lower().startswith(p) for p in _ALLOWED_TABLE_PREFIXES):
            return f"Access to table '{table}' is not allowed. Only cached dataset tables are accessible."

    # AST-level validation with sqlglot (defense-in-depth layer 3)
    error = _validate_sql_ast(stripped)
    if error:
        return error

    return None


def _validate_sql_ast(sql: str) -> str | None:
    """Parse SQL with sqlglot and reject anything that isn't a pure SELECT."""
    try:
        import sqlglot
        from sqlglot import exp

        statements = sqlglot.parse(sql, dialect="postgres")

        if not statements:
            return "Could not parse SQL statement."

        if len(statements) > 1:
            return "Only single SELECT statements are allowed."

        stmt = statements[0]
        if stmt is None:
            return "Could not parse SQL statement."

        # Reject non-SELECT top-level statements
        if not isinstance(stmt, exp.Select):
            return "Only SELECT queries are allowed."

        # Walk the AST to find DML/DDL nodes embedded in CTEs or subqueries
        _DML_DDL = (
            exp.Insert,
            exp.Update,
            exp.Delete,
            exp.Drop,
            exp.Create,
            exp.Alter,
            exp.Command,
        )
        for node in stmt.walk():
            if isinstance(node, _DML_DDL):
                return f"Forbidden SQL operation in subquery: {type(node).__name__}"

            # Table allowlist — catches all references including comma-separated.
            # Mirror the regex-based check above: schemas in _PREFIX_FREE_SCHEMAS
            # (mart, raw) are accepted without imposing the cache_* prefix because
            # everything materialized there comes from our own pipeline.
            if isinstance(node, exp.Table):
                table_name = node.name
                schema_name = node.db if hasattr(node, "db") else None
                if schema_name and schema_name.lower() not in _ALLOWED_SCHEMAS:
                    return f"Access to schema '{schema_name}' is not allowed."
                if schema_name and schema_name.lower() in _PREFIX_FREE_SCHEMAS:
                    continue
                if table_name and not any(
                    table_name.lower().startswith(p) for p in _ALLOWED_TABLE_PREFIXES
                ):
                    return f"Access to table '{table_name}' is not allowed. Only cached dataset tables are accessible."

    except Exception:
        logger.warning("sqlglot validation failed — rejecting query for safety", exc_info=True)
        return "Could not validate SQL safely. Please simplify your query."


class PgSandboxAdapter(ISQLSandbox):
    """Read-only SQL sandbox that executes queries against cached dataset tables."""

    def __init__(self) -> None:
        self._engine: Engine | None = None
        self._executor = ThreadPoolExecutor(max_workers=2)

    @property
    def _db_url(self) -> str:
        """Resolve DB URL lazily so env var overrides (e.g. in tests) take effect."""
        return (
            os.getenv("SANDBOX_DATABASE_URL")
            or os.getenv("DATABASE_URL")
            or "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db"
        )

    def _get_engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(
                self._db_url,
                pool_size=2,
                max_overflow=1,
                pool_pre_ping=True,
            )
        return self._engine

    def _execute_sync(self, sql: str, timeout_seconds: int) -> SandboxResult:
        """Execute the query synchronously in a read-only transaction."""
        # Auto-fix: wrap bare integers in WHERE/AND/OR clauses with quotes
        # Prevents "operator does not exist: text = integer" errors
        # NOTE: This runs BEFORE validation so _validate_sql operates on the
        # final SQL that will actually be executed (SEC-01 audit fix).
        import re

        sql = re.sub(
            r"(=|<>|!=|>=|<=|>|<)\s*(\d{4,})\b(?!')",
            r"\1 '\2'",
            sql,
        )

        validation_error = _validate_sql(sql)
        if validation_error:
            return SandboxResult(
                columns=[],
                rows=[],
                row_count=0,
                truncated=False,
                error=validation_error,
            )

        engine = self._get_engine()
        timeout_ms = timeout_seconds * 1000

        try:
            with engine.connect() as conn:
                # Set the transaction to read-only and apply statement timeout
                conn.execute(text("SET TRANSACTION READ ONLY"))
                conn.execute(text(f"SET statement_timeout = {int(timeout_ms)}"))

                result = conn.execute(text(sql))
                columns = list(result.keys())

                rows_raw = result.fetchmany(MAX_ROWS + 1)
                truncated = len(rows_raw) > MAX_ROWS
                if truncated:
                    rows_raw = rows_raw[:MAX_ROWS]

                rows = [dict(zip(columns, row, strict=False)) for row in rows_raw]

                # Get the true row count if truncated
                row_count = len(rows)

                # Rollback (read-only anyway) to release the connection cleanly
                conn.rollback()

                return SandboxResult(
                    columns=columns,
                    rows=rows,
                    row_count=row_count,
                    truncated=truncated,
                )

        except Exception as exc:
            raw_error = str(exc)
            logger.warning("Sandbox query failed: %s", raw_error)
            # Sanitize error messages — don't expose PostgreSQL internals
            error_lower = raw_error.lower()
            if "statement timeout" in error_lower or "canceling statement" in error_lower:
                error_msg = f"Query timed out after {timeout_seconds} seconds."
            elif "relation" in error_lower and "does not exist" in error_lower:
                error_msg = "The requested table does not exist."
            elif "column" in error_lower and "does not exist" in error_lower:
                error_msg = "One or more referenced columns do not exist."
            elif "syntax error" in error_lower:
                error_msg = "SQL syntax error in the query."
            else:
                error_msg = "Query execution failed. Please check your SQL and try again."
            return SandboxResult(
                columns=[],
                rows=[],
                row_count=0,
                truncated=False,
                error=error_msg,
            )

    async def execute_readonly(
        self, sql: str, timeout_seconds: int = _SANDBOX_TIMEOUT_MS // 1000
    ) -> SandboxResult:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            partial(self._execute_sync, sql, timeout_seconds),
        )

    def _list_tables_sync(self) -> list[CachedTableInfo]:
        engine = self._get_engine()
        with engine.connect() as conn:
            # LEFT JOIN with `raw_table_versions` so raw-layer tables are
            # reported with their qualified `raw.<table>` name. Without
            # this, the cached_datasets row only carries the bare name
            # (no schema), so consumers couldn't tell apart legacy
            # `cache_*` (in public) from raw landings (in raw schema).
            # The COALESCE leaves cache_* legacy alone (rtv.schema_name
            # is NULL for those) and the BARE name is preserved.
            result = conn.execute(
                text(
                    """
                    SELECT CAST(cd.dataset_id AS text) AS dataset_id,
                           CASE
                               WHEN rtv.schema_name IS NOT NULL
                                    AND rtv.schema_name <> 'public'
                                    AND rtv.superseded_at IS NULL
                                   THEN rtv.schema_name || '.' || cd.table_name
                               ELSE cd.table_name
                           END AS table_name,
                           cd.row_count,
                           cd.columns_json
                    FROM cached_datasets cd
                    LEFT JOIN raw_table_versions rtv
                      ON rtv.table_name = cd.table_name
                     AND rtv.superseded_at IS NULL
                    WHERE cd.status = 'ready'
                    ORDER BY table_name
                    """
                )
            )
            tables = []
            registered_names: set[str] = set()
            registered_bare: set[str] = set()
            for row in result.fetchall():
                columns: list[str] = []
                if row.columns_json:
                    try:
                        columns = json.loads(row.columns_json)
                    except (json.JSONDecodeError, TypeError):
                        pass
                tables.append(
                    CachedTableInfo(
                        table_name=row.table_name,
                        dataset_id=row.dataset_id,
                        row_count=row.row_count,
                        columns=columns,
                    )
                )
                registered_names.add(row.table_name)
                # Also track the bare name so the physical-scan path
                # below skips legacy public.cache_* duplicates AND raw
                # landings already reported with their qualified name.
                if "." in row.table_name:
                    registered_bare.add(row.table_name.split(".", 1)[1])
                else:
                    registered_bare.add(row.table_name)

            # MASTERPLAN Fase 1.5 — surface raw layer tables to consumers.
            # Without this, /data/tables (which docstrings as including
            # "raw layer and curated marts") was lying: the sandbox only
            # enumerated public.cache_*. Now we also list every live raw
            # version (superseded versions are pruned by retain_raw_versions
            # so they shouldn't appear here, but we filter explicitly to
            # be safe). Marts are appended by the data_router enrichment
            # path; we don't add them here so the layering stays clean.
            raw_rows = conn.execute(
                text(
                    """
                    SELECT rtv.schema_name, rtv.table_name,
                           COALESCE(rtv.row_count, s.n_live_tup::bigint, 0) AS row_count
                    FROM raw_table_versions rtv
                    LEFT JOIN pg_stat_user_tables s
                      ON s.schemaname = rtv.schema_name
                     AND s.relname = rtv.table_name
                    WHERE rtv.superseded_at IS NULL
                      AND rtv.schema_name = 'raw'
                      AND EXISTS (
                          SELECT 1 FROM information_schema.tables t
                          WHERE t.table_schema = rtv.schema_name
                            AND t.table_name = rtv.table_name
                      )
                    ORDER BY rtv.table_name
                    """
                )
            ).fetchall()
            for raw_row in raw_rows:
                qualified = f"{raw_row.schema_name}.{raw_row.table_name}"
                # Skip when this raw landing was already surfaced via the
                # cached_datasets path (joined with rtv at the top of this
                # function). `registered_bare` holds the un-prefixed
                # `table_name` of every row already emitted, regardless
                # of whether it was emitted with or without schema prefix.
                if qualified in registered_names or raw_row.table_name in registered_bare:
                    continue
                cols_q = conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = :sch AND table_name = :tn "
                        "ORDER BY ordinal_position"
                    ),
                    {"sch": raw_row.schema_name, "tn": raw_row.table_name},
                ).fetchall()
                tables.append(
                    CachedTableInfo(
                        table_name=qualified,
                        dataset_id="",
                        row_count=int(raw_row.row_count or 0),
                        columns=[str(c.column_name) for c in cols_q],
                    )
                )

            conn.rollback()
            return tables

    async def list_cached_tables(self) -> list[CachedTableInfo]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, self._list_tables_sync)

    def _get_column_types_sync(
        self,
        table_names: list[str],
    ) -> dict[str, list[tuple[str, str]]]:
        engine = self._get_engine()
        if not table_names:
            return {}
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT table_name, column_name, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' "
                    "AND table_name = ANY(:tables) "
                    "ORDER BY table_name, ordinal_position"
                ),
                {"tables": table_names},
            )
            types: dict[str, list[tuple[str, str]]] = {}
            for row in result.fetchall():
                types.setdefault(row.table_name, []).append((row.column_name, row.data_type))
            conn.rollback()
            return types

    async def get_column_types(
        self,
        table_names: list[str],
    ) -> dict[str, list[tuple[str, str]]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            partial(self._get_column_types_sync, table_names),
        )
