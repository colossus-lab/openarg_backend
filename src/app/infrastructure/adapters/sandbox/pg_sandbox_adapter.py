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

MAX_ROWS = 1000

# Patterns that indicate write/DDL operations (case-insensitive)
_FORBIDDEN_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY|"
    r"EXECUTE|CALL|DO|SET|RESET|VACUUM|ANALYZE|CLUSTER|REINDEX|LOCK|"
    r"COMMENT|SECURITY|LOAD|IMPORT|REFRESH)\b",
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

    # AST-level validation with sqlglot (defense-in-depth layer 2)
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
            exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create,
            exp.AlterTable, exp.Command,
        )
        for node in stmt.walk():
            if isinstance(node, _DML_DDL):
                return f"Forbidden SQL operation in subquery: {type(node).__name__}"

    except Exception:
        logger.debug("sqlglot validation failed, falling back to regex", exc_info=True)

    return None


class PgSandboxAdapter(ISQLSandbox):
    """Read-only SQL sandbox that executes queries against cached dataset tables."""

    def __init__(self) -> None:
        # Prefer dedicated sandbox URL (read-only role in prod) over main DB
        self._db_url = os.getenv(
            "SANDBOX_DATABASE_URL",
            os.getenv(
                "DATABASE_URL",
                "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db",
            ),
        )
        self._engine: Engine | None = None
        self._executor = ThreadPoolExecutor(max_workers=2)

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
                conn.execute(text("SET statement_timeout = :timeout_ms"), {"timeout_ms": timeout_ms})

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
        self, sql: str, timeout_seconds: int = 10
    ) -> SandboxResult:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            partial(self._execute_sync, sql, timeout_seconds),
        )

    def _list_tables_sync(self) -> list[CachedTableInfo]:
        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT CAST(dataset_id AS text) AS dataset_id, table_name, row_count, columns_json "
                    "FROM cached_datasets "
                    "WHERE status = 'ready' "
                    "ORDER BY table_name"
                )
            )
            tables = []
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
            conn.rollback()
            return tables

    async def list_cached_tables(self) -> list[CachedTableInfo]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, self._list_tables_sync)

    def _get_column_types_sync(
        self, table_names: list[str],
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
                types.setdefault(row.table_name, []).append(
                    (row.column_name, row.data_type)
                )
            conn.rollback()
            return types

    async def get_column_types(
        self, table_names: list[str],
    ) -> dict[str, list[tuple[str, str]]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            partial(self._get_column_types_sync, table_names),
        )
