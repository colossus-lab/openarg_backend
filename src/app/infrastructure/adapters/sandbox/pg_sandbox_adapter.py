from __future__ import annotations

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import asyncio
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

    # Check for forbidden keywords
    match = _FORBIDDEN_PATTERNS.search(no_comments)
    if match:
        return f"Forbidden SQL operation: {match.group(0).upper()}"

    return None


class PgSandboxAdapter(ISQLSandbox):
    """Read-only SQL sandbox that executes queries against cached dataset tables."""

    def __init__(self) -> None:
        self._db_url = os.getenv(
            "DATABASE_URL",
            "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db",
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
                conn.execute(text(f"SET statement_timeout = {timeout_ms}"))

                result = conn.execute(text(sql))
                columns = list(result.keys())

                rows_raw = result.fetchmany(MAX_ROWS + 1)
                truncated = len(rows_raw) > MAX_ROWS
                if truncated:
                    rows_raw = rows_raw[:MAX_ROWS]

                rows = [dict(zip(columns, row)) for row in rows_raw]

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
            error_msg = str(exc)
            # Provide cleaner error messages for common cases
            if "statement timeout" in error_msg.lower() or "canceling statement" in error_msg.lower():
                error_msg = f"Query timed out after {timeout_seconds} seconds."
            logger.warning("Sandbox query failed: %s", error_msg)
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
