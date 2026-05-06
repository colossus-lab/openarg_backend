"""DDL generation for marts.

`build_create_view_sql(mart)` produces the `CREATE OR REPLACE MATERIALIZED
VIEW` statement (Postgres) plus, when a `unique_index` is declared, the
`CREATE UNIQUE INDEX` that enables `REFRESH MATERIALIZED VIEW CONCURRENTLY`.

`build_comment_sql(mart)` produces one `COMMENT ON COLUMN` per canonical
column. The pipeline reads these comments via `information_schema` to
expose semantic hints to the planner without a parallel registry.

Both functions return SQL strings only — they do not execute. The Celery
task wrappers in `infrastructure/celery/tasks/mart_tasks.py` run them.
"""

from __future__ import annotations

from app.application.marts.mart import Mart


def _quoted(name: str) -> str:
    return f'"{name}"'


def build_create_view_sql(mart: Mart) -> list[str]:
    """Return the list of SQL statements to (re)create a mart.

    The list is ordered: DROP first, CREATE second, UNIQUE INDEX third.
    Concatenating with `;\\n` between statements produces a valid script.
    """
    schema = mart.schema_name
    view = mart.view_name
    qualified = f"{schema}.{_quoted(view)}"

    statements: list[str] = []
    # Materialized views can't be CREATE OR REPLACE; we DROP + CREATE.
    # `IF EXISTS` so first-time builds are idempotent.
    statements.append(f"DROP MATERIALIZED VIEW IF EXISTS {qualified} CASCADE")
    statements.append(
        f"CREATE MATERIALIZED VIEW {qualified} AS\n{mart.sql.rstrip(';')}"
    )

    if mart.refresh.unique_index:
        idx_cols = ", ".join(_quoted(c) for c in mart.refresh.unique_index)
        idx_name = _quoted(f"uq_{view}_{'_'.join(mart.refresh.unique_index)}"[:63])
        statements.append(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} ON {qualified} ({idx_cols})"
        )
    return statements


def build_comment_sql(mart: Mart) -> list[str]:
    """Return `COMMENT ON COLUMN` statements for every canonical column.

    Quoting is double-single (PG-safe) for the comment body so descriptions
    can include apostrophes safely.
    """
    schema = mart.schema_name
    view = mart.view_name
    qualified = f"{schema}.{_quoted(view)}"

    statements: list[str] = []
    for col in mart.canonical_columns:
        if not col.description:
            continue
        body = col.description.replace("'", "''")
        statements.append(
            f"COMMENT ON COLUMN {qualified}.{_quoted(col.name)} IS '{body}'"
        )
    return statements


def build_refresh_sql(mart: Mart) -> str:
    """Return the `REFRESH MATERIALIZED VIEW [CONCURRENTLY]` statement.

    `CONCURRENTLY` is used iff a unique index is declared (Postgres
    requirement).
    """
    qualified = mart.qualified_name
    if mart.refresh.unique_index:
        return f"REFRESH MATERIALIZED VIEW CONCURRENTLY {qualified}"
    return f"REFRESH MATERIALIZED VIEW {qualified}"
