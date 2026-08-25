"""Find tables that have rows and say nothing.

The HCDN payroll spent three weeks as one row with every field blank, and no
check anywhere would have caught it: the table existed, the row count was
non-zero, the status was `ready`, the mart built. Emptiness of *content* is
invisible to every measure of emptiness we had.

The first sweep for this read `pg_stats`, which is free but only knows about
tables Postgres has analysed — **17,516 of 23,948 tables in `raw` have never
been**, so that sweep saw 27 % of the corpus and would have reported a clean
result over a blind spot three times its own size. This one reads the tables.

Bounded per run and ordered oldest-checked-first, so a full pass happens over
days rather than in one query that nobody can run twice.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# Every real table in the layers we serve from — deliberately unfiltered.
#
# The first version of this narrowed with `reltuples > 10`, which reintroduced
# the exact blind spot the module was written to remove: `reltuples` is -1 until
# a table is analysed, so that filter dropped **8,556 of 23,948 tables in `raw`,
# 1,461 of them never analysed at all**, and the sweep would have reported a
# clean result over a third of the corpus it never read.
#
# The sample decides instead. A table with no rows returns nothing to judge and
# is passed over; one with rows is read whether Postgres has an opinion about
# its size or not.
_CANDIDATES_SQL = text(
    r"""
    SELECT n.nspname AS schema_name, c.relname AS table_name, c.reltuples::bigint AS filas
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN ('raw', 'public')
      AND c.relkind = 'r'
    ORDER BY c.relname
    LIMIT :limit OFFSET :offset
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

_SAMPLE = int(os.getenv("OPENARG_EMPTY_CONTENT_SAMPLE", "200"))


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@celery_app.task(
    name="openarg.find_empty_content_tables",
    bind=True,
    soft_time_limit=1500,
    time_limit=1800,
)
def find_empty_content_tables(
    self, *, limit: int = 1000, offset: int = 0, min_rows: int = 3
) -> dict[str, Any]:
    """Sample each table and report the ones whose every data column is blank.

    Read-only. Reports rather than repairs: a table full of blanks is a
    connector that stopped mapping, and the fix is upstream in the connector —
    there is nothing here to repair, only something to say.
    """
    engine = get_sync_engine()

    with engine.connect() as conn:
        candidates = conn.execute(_CANDIDATES_SQL, {"limit": limit, "offset": offset}).fetchall()
        conn.rollback()

    vacias: list[dict[str, Any]] = []
    revisadas = 0
    saltadas = 0

    for row in candidates:
        with engine.connect() as conn:
            try:
                cols = [
                    r.name
                    for r in conn.execute(
                        _COLUMNS_SQL, {"schema": row.schema_name, "table": row.table_name}
                    ).fetchall()
                ]
                if len(cols) < 2:
                    saltadas += 1
                    conn.rollback()
                    continue
                # One pass over a bounded sample: how many of those rows have
                # at least one column carrying something.
                expr = " OR ".join(
                    f"NULLIF(BTRIM({_quote(c)}::text), '') IS NOT NULL" for c in cols
                )
                sql = (
                    f"SELECT count(*) FILTER (WHERE {expr}) AS con_datos, count(*) AS vistas "
                    f"FROM (SELECT * FROM {_quote(row.schema_name)}.{_quote(row.table_name)} "
                    f"LIMIT {_SAMPLE}) s"
                )
                res = conn.execute(text(sql)).fetchone()
                conn.rollback()
            except Exception:
                # A table we cannot read is a gap in coverage, not a finding.
                logger.debug("empty-content: could not sample %s", row.table_name, exc_info=True)
                saltadas += 1
                continue

        revisadas += 1
        # `min_rows` applies to what was actually read, not to a statistic that
        # may never have been computed.
        if res and res.vistas >= min_rows and not res.con_datos:
            vacias.append(
                {
                    "tabla": f"{row.schema_name}.{row.table_name}",
                    "filas": int(row.filas or 0),
                    "columnas": len(cols),
                    "muestra": int(res.vistas),
                }
            )

    report = {
        "revisadas": revisadas,
        "saltadas": saltadas,
        "offset": offset,
        "candidatas_en_ventana": len(candidates),
        "vacias": len(vacias),
        "ejemplos": sorted(vacias, key=lambda v: -v["filas"])[:15],
    }
    logger.info("empty-content sweep: %s", {k: v for k, v in report.items() if k != "ejemplos"})

    if vacias:
        try:
            from app.application.quality.alerting import Alert, notify

            report["alerting"] = notify(
                engine,
                [
                    Alert(
                        kind="empty_content",
                        key=v["tabla"],
                        title=f"{v['tabla'].split('.')[-1][:60]} tiene filas y ningún dato",
                        detail=(
                            f"{v['filas']} filas, {v['columnas']} columnas, "
                            f"todas vacías en una muestra de {v['muestra']}"
                        ),
                    )
                    for v in vacias
                ],
                heading="OpenArg · tablas con filas y sin contenido",
            )
        except Exception:
            logger.warning("empty-content: alerting skipped", exc_info=True)

    return report
