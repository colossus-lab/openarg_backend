"""Notice, at write time, that a connector just published nothing.

Every vía-B connector — BCRA, INDEC, georef, presupuesto, senado, the rest —
ends the same way: write a table, then register it. Between those two steps
there is no check at all, which is how the HCDN payroll spent three weeks as one
blank row and how a 500,000-row resource came back as an empty version without
anyone hearing about it.

The connectors are too different to share a schema. What they do share is the
registration call, and by then two facts are available that need nothing
connector-specific:

- **What it wrote before.** The registry already keeps `row_count` per version.
  A resource that goes from 500,000 rows to 12 did not have a quiet week.
- **What is in the table now.** A bounded sample answers "does any column of any
  row carry anything", which is the failure that every other measure of
  emptiness misses: the table exists, the row count is not zero, the status is
  `ready`, the mart builds.

**It reports; it does not refuse.** By the time registration runs the write has
happened, so refusing is not on the table — and pretending otherwise would be
worse than saying so. What it buys is the difference between finding out in
seconds and finding out in three weeks. A connector that can check *before*
writing should still do that: `staff_tasks` refuses on its identity field, which
is strictly better and is why that guard stays where it is.

Costs nothing on the read path. This runs inside the ingest task that just wrote
the table, never on a query.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

# A drop this steep is not a slow news week. Deliberately far from 1.0: real
# datasets shrink, and a threshold that fires on ordinary variation is a
# threshold people learn to ignore.
COLLAPSE_SHARE = 0.10

# Below this the ratio says nothing — a table going from 8 rows to 1 is noise.
COLLAPSE_MIN_PREVIOUS = 100

_SAMPLE_ROWS = 200


@dataclass(frozen=True)
class BatchVerdict:
    """What is wrong with what was just written, if anything."""

    resource_identity: str
    table: str
    reason: str = ""
    previous_rows: int | None = None
    current_rows: int | None = None

    @property
    def ok(self) -> bool:
        return not self.reason


_PREVIOUS_SQL = text(
    """
    SELECT row_count
    FROM public.raw_table_versions
    WHERE resource_identity = :rid AND version < :v AND row_count IS NOT NULL
    ORDER BY version DESC
    LIMIT 1
    """
)

_COLUMNS_SQL = text(
    r"""
    SELECT a.attname AS name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE n.nspname = :schema AND c.relname = :table
      AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attname NOT LIKE '\_%'
    ORDER BY a.attnum
    """
)


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def check_after_write(
    engine: Any,
    *,
    resource_identity: str,
    schema_name: str,
    table_name: str,
    version: int = 1,
    row_count: int | None = None,
) -> BatchVerdict:
    """Look at what a connector just published. Never raises.

    Returns a verdict rather than acting on it, so the caller decides whether a
    finding is worth a message. Any failure inside is swallowed: a check that
    can break the ingest it is watching is worse than no check.
    """
    verdict = BatchVerdict(
        resource_identity=resource_identity,
        table=f"{schema_name}.{table_name}",
        current_rows=row_count,
    )
    try:
        with engine.connect() as conn:
            previous = conn.execute(
                _PREVIOUS_SQL, {"rid": resource_identity, "v": version}
            ).scalar()

            if (
                previous is not None
                and row_count is not None
                and previous >= COLLAPSE_MIN_PREVIOUS
                and row_count < previous * COLLAPSE_SHARE
            ):
                conn.rollback()
                return BatchVerdict(
                    resource_identity=resource_identity,
                    table=f"{schema_name}.{table_name}",
                    reason=(
                        f"la versión anterior traía {previous} filas y esta trae "
                        f"{row_count} ({round(100.0 * row_count / previous, 1)}%)"
                    ),
                    previous_rows=int(previous),
                    current_rows=row_count,
                )

            cols = [
                r.name
                for r in conn.execute(
                    _COLUMNS_SQL, {"schema": schema_name, "table": table_name}
                ).fetchall()
            ]
            if len(cols) < 2:
                conn.rollback()
                return verdict

            expr = " OR ".join(f"NULLIF(BTRIM({_quote(c)}::text), '') IS NOT NULL" for c in cols)
            res = conn.execute(
                text(
                    f"SELECT count(*) FILTER (WHERE {expr}) AS con_datos, count(*) AS vistas "
                    f"FROM (SELECT * FROM {_quote(schema_name)}.{_quote(table_name)} "
                    f"LIMIT {_SAMPLE_ROWS}) s"
                )
            ).fetchone()
            conn.rollback()
    except Exception:
        # A check that can break the ingest it watches is worse than no check.
        logger.debug("batch guard: could not inspect %s", table_name, exc_info=True)
        return verdict

    if res and res.vistas and not res.con_datos:
        return BatchVerdict(
            resource_identity=resource_identity,
            table=f"{schema_name}.{table_name}",
            reason=(
                f"tiene filas y ninguna columna con contenido "
                f"(muestra de {int(res.vistas)}, {len(cols)} columnas)"
            ),
            current_rows=row_count,
        )
    return verdict
