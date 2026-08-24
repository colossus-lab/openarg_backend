"""Repair the tables the validator refused, then let the resource back in.

`ingestion_validation_failed:placeholder_headers` means the parse produced names
like `col_3` or a title row smeared across thirty columns. The validator is
right to refuse that, and refusing is where it stops: **the table is
materialised anyway**. Measured on 2026-08-23: 546 resources sit rejected and
every one of them still has its table on disk.

Meanwhile `parse_repair.py` has held fixes for exactly those two shapes since
May, and has applied them thousands of times — always because a person went to
an admin route and asked. The repair and the resource's status never met. A
table could be fixed and the resource would stay `error` forever, because
nothing looked again.

This is the meeting. Repair the table with the tiers that already exist, ask
whether the names are clean now, and only then promote the resource back to
`ready`.

**Promotion is gated on the answer, not on the attempt.** A repair that ran and
changed nothing must leave the resource rejected: flipping it to `ready` on the
strength of having tried would serve `col_3` to a person asking about poverty,
which is worse than serving nothing and saying so.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

_REJECTED_SQL = text(
    r"""
    SELECT cd.id AS cd_id, cd.dataset_id, cd.table_name, t.table_schema
    FROM raw.cached_datasets cd
    JOIN information_schema.tables t
      ON t.table_name = cd.table_name AND t.table_type = 'BASE TABLE'
     AND t.table_schema IN ('raw', 'public')
    WHERE cd.status IN ('error', 'permanently_failed')
      AND cd.error_message LIKE '%placeholder\_headers%'
    ORDER BY cd.updated_at ASC
    LIMIT :limit
    """
)


def _column_names(conn, schema: str, table: str) -> list[str]:
    return [
        str(r.column_name)
        for r in conn.execute(
            text(
                r"""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = :s AND table_name = :t
                  AND column_name NOT LIKE '\_%'
                ORDER BY ordinal_position
                """
            ),
            {"s": schema, "t": table},
        )
    ]


@celery_app.task(
    name="openarg.rescue_rejected_resources",
    bind=True,
    soft_time_limit=2400,
    time_limit=3000,
)
def rescue_rejected_resources(
    self, *, limit: int = 100, dry_run: bool = True
) -> dict[str, Any]:
    """Repair rejected tables and promote the ones that came out clean."""
    from app.application.pipeline.parsers.column_normalization import is_garbage_column
    from app.application.repair.parse_repair import (
        repair_col_n_table,
        repair_title_as_columns_table,
    )

    engine = get_sync_engine()
    run_id = uuid.uuid4()

    with engine.connect() as conn:
        rows = conn.execute(_REJECTED_SQL, {"limit": limit}).fetchall()
        conn.rollback()

    by_reason: dict[str, int] = {}
    promoted: list[str] = []

    for row in rows:
        schema, table = str(row.table_schema), str(row.table_name)
        if not dry_run:
            for repair in (repair_title_as_columns_table, repair_col_n_table):
                try:
                    out = repair(
                        engine, table_schema=schema, table_name=table,
                        run_id=run_id, dry_run=False,
                    )
                    key = (out.reason or "").split(":")[0] or "ok"
                    by_reason[key] = by_reason.get(key, 0) + 1
                    if out.ok:
                        break
                except Exception:
                    by_reason["raised"] = by_reason.get("raised", 0) + 1

        # The question that decides, asked of the table rather than of the
        # repair's own opinion of itself.
        with engine.connect() as conn:
            names = _column_names(conn, schema, table)
            conn.rollback()
        if not names or any(is_garbage_column(n) for n in names):
            by_reason["still_unusable"] = by_reason.get("still_unusable", 0) + 1
            continue

        by_reason["clean"] = by_reason.get("clean", 0) + 1
        if dry_run:
            continue

        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE raw.cached_datasets "
                        "SET status = 'ready', error_message = NULL, "
                        "    error_category = 'success', columns_json = CAST(:c AS jsonb), "
                        "    updated_at = now() "
                        "WHERE id = :i"
                    ),
                    {"i": row.cd_id, "c": __import__("json").dumps(names)},
                )
                conn.execute(
                    text("UPDATE datasets SET is_cached = true WHERE id = :d"),
                    {"d": row.dataset_id},
                )
            promoted.append(table)
        except Exception:
            logger.warning("could not promote %s", table, exc_info=True)
            by_reason["promote_failed"] = by_reason.get("promote_failed", 0) + 1

    result = {
        "dry_run": dry_run,
        "candidates": len(rows),
        "promoted": len(promoted),
        "by_reason": by_reason,
        "samples": promoted[:5],
        "run_id": str(run_id),
    }
    logger.info("rescue rejected: %s", result)
    return result
