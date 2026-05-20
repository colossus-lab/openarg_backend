"""DDL generation for marts.

`build_create_view_sql(mart)` produces the `CREATE OR REPLACE MATERIALIZED
VIEW` statement (Postgres) plus, when a `unique_index` is declared, the
`CREATE UNIQUE INDEX` that enables `REFRESH MATERIALIZED VIEW CONCURRENTLY`.
It then calls `build_index_sql(mart)` for non-unique indexes on common
filter columns (temporal / geographic). Without these, every mart query
ran a parallel sequential scan — verified by EXPLAIN ANALYZE on
`mart.flujo_vehicular_peajes_caba` (7M rows, 608 MB, 0 indexes pre-fix,
1.7s in cache + up to 10s+ on cold disk for a `WHERE fecha = '2023-...'`
filter that returns 0 rows).

`build_comment_sql(mart)` produces one `COMMENT ON COLUMN` per canonical
column. The pipeline reads these comments via `information_schema` to
expose semantic hints to the planner without a parallel registry.

All functions return SQL strings only — they do not execute. The Celery
task wrappers in `infrastructure/celery/tasks/mart_tasks.py` run them.
"""

from __future__ import annotations

import re

from app.application.marts.mart import Mart


def _quoted(name: str) -> str:
    return f'"{name}"'


# Columns that almost always appear in WHERE clauses and benefit from an
# index. Captured patterns:
#   - exact name (e.g. `fecha`, `anio`, `provincia`, `barrio`)
#   - prefixed temporal columns (e.g. `fecha_de_inicio`, `fecha_actualizacion`)
#   - suffixed temporal columns (e.g. `tramite_fecha`, `_anio`)
#   - geo with optional `_nombre` / `_id` / `_desc` suffix
#   - embedded geo (e.g. `dom_fiscal_provincia`, `provincia_nombre`)
_TEMPORAL_COL_RE = re.compile(
    r"^(fecha|anio|año|ano|periodo|per[ií]odo|mes|trimestre|semestre|ejercicio|year|date)$"
    r"|^(fecha|anio|año|periodo|year|date)_"
    r"|_(fecha|anio|año|periodo|year|date)$",
    re.IGNORECASE,
)
_GEOGRAPHIC_COL_RE = re.compile(
    r"^(provincia|barrio|comuna|departamento|municipio|jurisdicci[oó]n|partido|aglomerado)(_nombre|_id|_desc)?$"
    r"|_(provincia|barrio|comuna|departamento|municipio|jurisdicci[oó]n|partido)(_nombre|_id|_desc)?$",
    re.IGNORECASE,
)


def _is_indexable_column(name: str) -> bool:
    """True when the column name matches a temporal or geographic filter pattern."""
    return bool(_TEMPORAL_COL_RE.search(name) or _GEOGRAPHIC_COL_RE.search(name))


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
    statements.extend(build_index_sql(mart))
    return statements


def build_index_sql(mart: Mart) -> list[str]:
    """Generate non-unique `CREATE INDEX` statements for common filter columns.

    Skips columns already covered by the unique index (avoids duplicate
    work) and any column whose name doesn't match a known temporal /
    geographic pattern. Idempotent via `IF NOT EXISTS`. Index names are
    truncated to 63 chars (Postgres NAMEDATALEN-1).

    Driven by the discovery in round v4.4 that no large mart had any
    index, making every query a `Parallel Seq Scan` over the whole view.
    """
    schema = mart.schema_name
    view = mart.view_name
    qualified = f"{schema}.{_quoted(view)}"

    skip = {c.lower() for c in mart.refresh.unique_index}
    statements: list[str] = []
    for col in mart.canonical_columns:
        col_name = col.name
        if col_name.lower() in skip:
            continue
        if not _is_indexable_column(col_name):
            continue
        idx_name = f"ix_{view}_{col_name}"[:63]
        statements.append(
            f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON {qualified} ({_quoted(col_name)})'
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
