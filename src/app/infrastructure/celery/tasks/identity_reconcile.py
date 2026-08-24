"""Give a resource a name that survives the portal renaming it.

CKAN 2.11 regenerated resource ids when datos.gob.ar migrated around
2026-07-29. The same file now arrives under an identifier we have never seen,
nothing links it to the old one, and the catalogue grew a second copy of every
resource that was re-identified.

Measured 2026-08-23 for `datos_gob_ar`: 5,406 URLs carry more than one
`source_id` across 13,433 rows, and **5,293 of those groups share a single
title** — one resource wearing several names. The 113 whose titles differ are
genuinely distinct and this must not touch them.

**Same URL and same title, or nothing.** The URL alone is not enough: two real
resources can be published from one endpoint that takes parameters, and merging
those would fuse datasets that have nothing to do with each other. Requiring the
title to agree as well is what separates a rename from a coincidence, and 113
groups in production are exactly that coincidence.

**This deletes nothing and merges nothing.** It records which rows are the same
resource. What to do about the 7,201 redundant tables and the 593 million
duplicated rows they hold is a decision with an owner, and the right shape for
this task is to make that decision answerable rather than to make it.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# The earliest row of each (url, title) group names the group. Earliest rather
# than newest: the point is a name that predates the renaming, so that a row
# arriving under a fresh CKAN id can be recognised as something we already have.
_RECONCILE_SQL = text(
    """
    WITH grp AS (
        SELECT download_url, title,
               min(created_at) AS first_seen,
               count(DISTINCT source_id) AS ids
        FROM datasets
        WHERE download_url IS NOT NULL AND download_url <> '' AND title IS NOT NULL
        GROUP BY download_url, title
        HAVING count(DISTINCT source_id) > 1
    ),
    anchor AS (
        SELECT DISTINCT ON (d.download_url, d.title)
               d.download_url, d.title, d.source_id AS original
        FROM datasets d
        JOIN grp g ON g.download_url = d.download_url AND g.title = d.title
        ORDER BY d.download_url, d.title, d.created_at ASC, d.source_id ASC
    )
    UPDATE datasets d
       SET original_identifier = a.original
      FROM anchor a
     WHERE d.download_url = a.download_url
       AND d.title = a.title
       AND d.original_identifier IS DISTINCT FROM a.original
    """
)

_COUNT_SQL = text(
    """
    SELECT count(*) AS marcados,
           count(DISTINCT original_identifier) AS recursos
    FROM datasets WHERE original_identifier IS NOT NULL
    """
)


@celery_app.task(
    name="openarg.reconcile_dataset_identities",
    bind=True,
    soft_time_limit=1800,
    time_limit=2400,
)
def reconcile_dataset_identities(self, *, dry_run: bool = True) -> dict[str, Any]:
    """Point every re-identified row at the name it had first."""
    engine = get_sync_engine()

    with engine.connect() as conn:
        would = conn.execute(
            text(
                """
                SELECT count(*) FROM datasets d
                JOIN (
                    SELECT download_url, title FROM datasets
                    WHERE download_url IS NOT NULL AND download_url <> ''
                      AND title IS NOT NULL
                    GROUP BY download_url, title
                    HAVING count(DISTINCT source_id) > 1
                ) g ON g.download_url = d.download_url AND g.title = d.title
                """
            )
        ).scalar()
        conn.rollback()

    if dry_run:
        return {"dry_run": True, "would_mark": int(would or 0), "marked": 0}

    with engine.begin() as conn:
        conn.execute(_RECONCILE_SQL)

    with engine.connect() as conn:
        row = conn.execute(_COUNT_SQL).fetchone()
        conn.rollback()

    result = {
        "dry_run": False,
        "would_mark": int(would or 0),
        "rows_with_identity": int(row.marcados or 0) if row else 0,
        # The number that says what the catalogue really holds: distinct
        # resources, as opposed to distinct identifiers.
        "distinct_resources": int(row.recursos or 0) if row else 0,
    }
    logger.info("identity reconcile: %s", result)
    return result


@celery_app.task(
    name="openarg.cleanup_duplicate_tables",
    bind=True,
    soft_time_limit=2400,
    time_limit=3000,
)
def cleanup_duplicate_tables_task(
    self, *, limit: int = 200, dry_run: bool = True
) -> dict[str, Any]:
    """Drop the redundant copies CKAN's re-identification left behind.

    Deliberately not on the beat schedule. A sweep that drops tables should be
    started by a person who decided to start it — the 2026-08-03 incident was a
    scheduled sweep doing exactly what it was told against a premise that had
    stopped being true.
    """
    from app.application.catalog.duplicate_cleanup import cleanup_duplicate_tables

    engine = get_sync_engine()
    outcome = cleanup_duplicate_tables(
        engine, run_id=uuid.uuid4(), dry_run=dry_run, limit=limit
    )
    result = outcome.as_dict()
    logger.info("duplicate cleanup: %s", result)
    return result
