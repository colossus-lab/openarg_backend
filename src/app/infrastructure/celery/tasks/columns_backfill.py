"""Put the columns where they are used.

`datasets.columns` is empty for 32,086 of 32,571 rows — 98.5 % of the catalogue.
The plan reads that as a CKAN integration: fetch each resource's fields through
`datastore_search`. The measurement says otherwise.

**We already know the columns for 29,001 of them.** We downloaded those files,
parsed them, and wrote their headers to `raw.cached_datasets.columns_json`. The
information is not missing; it is in a different table from the one that uses it.

And it *is* used. The embedding builder skips the column chunk entirely when the
list is empty (`if cols and len(cols) > 0`), so those 29,001 datasets — 89 % of
the catalogue — have never had a column chunk embedded at all. A person asking
about "superficie sembrada" cannot reach a dataset that has a `superficie_sembrada`
column, because nothing ever embedded the fact that it has one.

So this is not bookkeeping. It is the retrieval quality of nine datasets in ten.

**Bounded per run, and the re-index is dispatched here.** Filling this column
changes the embedding signature, which means the next catalogue scrape would
re-embed every row this touched — thousands at once, in the same hour the scrape
already runs. Draining it deliberately, a batch at a time with the re-index
dispatched alongside, spreads the same work and lands the improvement now
instead of tomorrow.

The remaining ~3,000 have never been collected, and for those the plan's answer
is the right one. They are a separate job, and a smaller one than it looked.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

_DEFAULT_BATCH = 2000

# Only rows where we hold a real parsed header. `columns_json` carrying `[]` or
# a null is the same absence written differently, and copying it would turn an
# empty column into an empty column while claiming progress.
_CANDIDATES_SQL = text(
    """
    SELECT d.id, cd.columns_json
    FROM datasets d
    JOIN raw.cached_datasets cd ON cd.dataset_id = d.id
    WHERE (d.columns IS NULL OR d.columns::text IN ('[]', 'null', '""'))
      AND cd.status = 'ready'
      AND cd.columns_json IS NOT NULL
      AND cd.columns_json::text NOT IN ('[]', 'null', '""')
    ORDER BY d.id
    LIMIT :limit
    """
)


@celery_app.task(
    name="openarg.backfill_dataset_columns",
    bind=True,
    soft_time_limit=1800,
    time_limit=2400,
)
def backfill_dataset_columns(
    self, *, limit: int | None = None, dry_run: bool = True, reindex: bool = True
) -> dict[str, Any]:
    """Copy parsed headers into `datasets.columns`, and re-embed what changed."""
    engine = get_sync_engine()
    cap = limit or _DEFAULT_BATCH

    with engine.connect() as conn:
        rows = conn.execute(_CANDIDATES_SQL, {"limit": cap}).fetchall()
        conn.rollback()

    if dry_run or not rows:
        return {"dry_run": dry_run, "candidates": len(rows), "filled": 0}

    filled: list[str] = []
    for row in rows:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE datasets SET columns = CAST(:c AS jsonb), updated_at = NOW() "
                        "WHERE id = :i"
                    ),
                    {"c": str(row.columns_json), "i": row.id},
                )
            filled.append(str(row.id))
        except Exception:
            # One row must not cost the batch; the next run picks it up again.
            logger.warning("columns backfill failed for %s", row.id, exc_info=True)

    dispatched = 0
    if reindex and filled:
        from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

        for did in filled:
            try:
                index_dataset_embedding.delay(did)
                dispatched += 1
            except Exception:
                logger.debug("could not dispatch reindex for %s", did, exc_info=True)

    with engine.connect() as conn:
        remaining = conn.execute(
            text(
                """
                SELECT count(*) FROM datasets d
                JOIN raw.cached_datasets cd ON cd.dataset_id = d.id
                WHERE (d.columns IS NULL OR d.columns::text IN ('[]', 'null', '""'))
                  AND cd.status = 'ready'
                  AND cd.columns_json IS NOT NULL
                  AND cd.columns_json::text NOT IN ('[]', 'null', '""')
                """
            )
        ).scalar()
        conn.rollback()

    result = {
        "dry_run": False,
        "candidates": len(rows),
        "filled": len(filled),
        "reindex_dispatched": dispatched,
        "remaining": int(remaining or 0),
    }
    logger.info("dataset columns backfill: %s", result)
    return result
