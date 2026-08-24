"""Repair parse defects on a schedule, for the classes that can prove themselves.

The repairs in `parse_repair.py` have existed since May and applied 543 times,
every one of them because a person went to an admin route and asked. Detection
and repair were two halves that never met — the four PAMI findings on
2026-08-21 were `title_as_columns`, a class with a working fix sitting unused.

This is the wire, for the one class that can be trusted to run unattended.

**Why this class and not the others.** An unsplit CSV carries its own proof of
correct repair: the header names N fields, and if every row splits into exactly
N, the split is right. No human has to look. `title_as_columns` and `col_n` have
no such check — a proposal there can be plausible and wrong, and they stay
manual until the verifier in `repair/verify.py` is wired to them.

**What makes it a drift response and not just a cleanup.** When a portal changes
its delimiter tomorrow, the collector will store the file unsplit exactly as it
did for the 211 tables repaired by hand today. This sweep finds that within a
day and fixes it, without anyone noticing it happened — which is the first place
this system repairs itself in response to an upstream change rather than
reporting one.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# One data column whose name still carries a delimiter. The signature of a file
# read with the wrong separator, and specific enough that nothing else matches:
# a legitimate column name does not contain a semicolon or a tab.
_CANDIDATES_SQL = text(
    r"""
    WITH cols AS (
        SELECT c.table_schema, c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema
         AND t.table_name = c.table_name
         AND t.table_type = 'BASE TABLE'
        WHERE c.table_schema = 'raw'
          AND c.column_name NOT LIKE '\_%'
    ),
    single AS (
        SELECT table_schema, table_name, min(column_name) AS header
        FROM cols
        GROUP BY table_schema, table_name
        HAVING count(*) = 1
    )
    SELECT table_schema, table_name, header
    FROM single
    WHERE header ~ '[,;|]' OR position(chr(9) in header) > 0
    ORDER BY table_name
    LIMIT :limit
    """
)

_MAX_PER_RUN = int(os.getenv("OPENARG_PARSE_REPAIR_MAX_PER_RUN", "200"))


@celery_app.task(
    name="openarg.repair_unsplit_csv_tables",
    bind=True,
    soft_time_limit=1500,
    time_limit=1800,
)
def repair_unsplit_csv_tables(
    self, *, limit: int | None = None, dry_run: bool = True
) -> dict[str, Any]:
    """Find tables holding an unsplit CSV and split them.

    Defaults to `dry_run=True`, so running it by hand reports rather than acts.
    The scheduled entry passes `dry_run=False` explicitly — the decision to
    write is visible in the schedule instead of buried in a default.

    Bounded per run. Every repair is recorded in `parse_repair_audit` and is
    therefore reversible through `revert_repair`; a repair that could not record
    itself could not be undone, so the audit is part of the safety and not
    bookkeeping.
    """
    from app.application.repair.parse_repair import repair_unsplit_csv_table

    engine = get_sync_engine()
    run_id = uuid.uuid4()
    cap = limit or _MAX_PER_RUN

    with engine.connect() as conn:
        rows = conn.execute(_CANDIDATES_SQL, {"limit": cap}).fetchall()
        conn.rollback()

    by_reason: dict[str, int] = {}
    repaired: list[str] = []
    for row in rows:
        try:
            outcome = repair_unsplit_csv_table(
                engine,
                table_schema=row.table_schema,
                table_name=row.table_name,
                run_id=run_id,
                dry_run=dry_run,
            )
        except Exception:
            # One table must not cost the batch. It stays broken and the next
            # run picks it up again, which is the shape of every sweep here.
            logger.warning("unsplit-csv repair raised for %s", row.table_name, exc_info=True)
            by_reason["raised"] = by_reason.get("raised", 0) + 1
            continue
        key = outcome.reason.split(":")[0]
        by_reason[key] = by_reason.get(key, 0) + 1
        if outcome.ok and not dry_run:
            repaired.append(row.table_name)

    result = {
        "run_id": str(run_id),
        "candidates": len(rows),
        "dry_run": dry_run,
        "by_reason": by_reason,
        # The declines are the interesting number, not the repairs. They are
        # tables where the delimiter also appears inside a quoted value, which a
        # split would corrupt silently — and they need a real CSV reader rather
        # than a bigger sweep.
        "declined_inconsistent": by_reason.get("inconsistent_field_count", 0),
        "repaired": len(repaired),
        "samples": repaired[:5],
    }
    logger.info("unsplit-csv repair: %s", result)
    return result


# Tables whose columns are one title copied across them. The SQL narrows to
# plausible candidates — several long names ending in `_N` — and the proposer
# decides; asking Postgres for a common-prefix test across 24,000 tables would
# cost more than reading the few hundred that could possibly match.
_SMEARED_CANDIDATES_SQL = text(
    r"""
    WITH cols AS (
        SELECT c.table_schema, c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema AND t.table_name = c.table_name
         AND t.table_type = 'BASE TABLE'
        WHERE c.table_schema IN ('raw', 'public')
          AND c.column_name NOT LIKE '\_%'
          AND length(c.column_name) >= 25
    )
    SELECT table_schema, table_name
    FROM cols
    WHERE column_name ~ '_[0-9]+$'
    GROUP BY table_schema, table_name
    HAVING count(*) >= 2
    -- Widest first. The SQL can only narrow to "several long names ending in
    -- `_N`" — 1,064 tables — and roughly a tenth of those actually carry the
    -- defect. Ordering by name spent the whole budget on the alphabet: a first
    -- run of 300 refused all 300 while the table it was written for sorted
    -- outside the window. Column count is the one signal available here that
    -- correlates with the shape.
    ORDER BY count(*) DESC, table_name
    LIMIT :limit
    """
)


@celery_app.task(
    name="openarg.repair_smeared_title_tables",
    bind=True,
    soft_time_limit=1800,
    time_limit=2400,
)
def repair_smeared_title_tables(
    self, *, limit: int = 1200, dry_run: bool = True
) -> dict[str, Any]:
    """Recover headers pandas smeared across the columns.

    Measured on 2026-08-23: 116 **servable** tables carry this, holding 291,436
    rows — `acceso_de_mujeres_a_la_salud`,
    `casos_penales_contravencionales_violencia`,
    `educacion_sexual_integral`. They are served today with every column named
    after the same sentence, so a person asking about maternal mortality gets a
    table they cannot read.

    Unlike the other sweeps here, this one targets tables that are *working* —
    the resource is `ready` and the data is fine. Only the names are wrong,
    which is why nothing ever flagged them: no status was ever bad.
    """
    import uuid

    from app.application.repair.parse_repair import repair_smeared_title_table

    engine = get_sync_engine()
    run_id = uuid.uuid4()

    with engine.connect() as conn:
        rows = conn.execute(_SMEARED_CANDIDATES_SQL, {"limit": limit}).fetchall()
        conn.rollback()

    by_reason: dict[str, int] = {}
    repaired: list[str] = []
    for row in rows:
        try:
            outcome = repair_smeared_title_table(
                engine,
                table_schema=row.table_schema,
                table_name=row.table_name,
                run_id=run_id,
                dry_run=dry_run,
            )
        except Exception:
            by_reason["raised"] = by_reason.get("raised", 0) + 1
            logger.warning("smeared repair raised for %s", row.table_name, exc_info=True)
            continue
        key = (outcome.reason or "").split(":")[0]
        by_reason[key] = by_reason.get(key, 0) + 1
        if outcome.ok and not dry_run:
            repaired.append(str(row.table_name))

    result = {
        "run_id": str(run_id),
        "candidates": len(rows),
        "dry_run": dry_run,
        "by_reason": by_reason,
        "repaired": len(repaired),
        "samples": repaired[:5],
    }
    logger.info("smeared-title repair: %s", result)
    return result
