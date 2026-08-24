"""Retry the collections that failed because of us, not because of the source.

`cached_datasets.error_category` already separates the two, and nothing ever
read it that way. Measured in production on 2026-08-23: **1,031 resources sit at
`orchestration_recovery_loop` since 2026-05-06** — a hundred and nine days —
every one of them with a download URL, on portals that answered a probe the same
morning. They were never refused by anyone: they got stuck in `downloading`,
a sweep reaped them, and the retry counter hit its ceiling.

**Why this instead of a CKAN integration.** The plan proposes recovering missing
column metadata through `datastore_search`. Probing ten of these against
datos.gob.ar returned fields for one and 404 for eight: the resources are simply
not in CKAN's DataStore. A portal-specific integration would cover roughly a
tenth of the problem, only on CKAN portals, and the single resource that did
answer was one of *these* — a file that downloads fine and that we failed to
collect ourselves.

Re-collecting recovers everything a datastore query would and more: the rows,
the header, the hash, the embeddings. It is portal-agnostic, and it works for
the next portal too.

**What the first run actually found, which was not what this expected.** Of 150
retried, 2 succeeded and 147 failed again — 134 of them at
`ingestion_validation_failed:placeholder_headers`. The files download fine. The
parser cannot name their columns, the validator refuses the result, and that is
correct: `orchestration_recovery_loop` recorded how they died (reaped while
stuck) rather than why. Retrying is not the fix for those; naming their columns
is, which is what the repair tiers exist for.

The sweep is still worth running, and self-limiting: a retry rewrites
`error_category` to the real reason, and the real reasons are not in the list
below. Of the 150, seven remain eligible. It converts an unusable label into a
usable one even when it cannot collect the resource — which is most of the value
here, because a population nobody can characterise is a population nobody fixes.

Eight of the 147 failed at "incoming parse is worse than the stored table" —
the regression guard, firing unprompted in production against the same shape as
the 2026-08-22 degradation, protecting eight good tables.

**The discrimination is the whole design.** Only categories that name our own
orchestration are retried. A `download_http_error` is the source saying no, a
`policy_non_tabular` is us deciding correctly, and retrying either would be a
loop that costs bandwidth to reach the same answer. `validation_failed` is
deliberately excluded despite being tempting: 85 % of those are a portal serving
an auth page, which no amount of retrying fixes.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# Failures that name our own machinery. Everything else is the source's answer
# or our own correct decision, and retrying it is a loop.
OUR_FAULT = (
    "orchestration_recovery_loop",
    "orchestration_table_missing",
    "materialize_table_collision",
    "orchestration_rerouted",
)

_CANDIDATES_SQL = text(
    """
    SELECT cd.id AS cd_id, cd.dataset_id, d.portal
    FROM raw.cached_datasets cd
    JOIN datasets d ON d.id = cd.dataset_id
    WHERE cd.status IN ('error', 'permanently_failed')
      AND cd.error_category = ANY(:cats)
      AND d.download_url IS NOT NULL AND d.download_url <> ''
      -- The age guard belongs only to `error`, which the ordinary retry path
      -- still handles. `permanently_failed` is terminal by definition: nothing
      -- else will ever pick it up, so making it wait means making it wait
      -- forever.
      --
      -- The first version applied the guard to both and matched 4 rows out of
      -- 952. `updated_at` is not "when this last failed" — it is when anything
      -- last wrote the row, and sweeps touch these constantly. Filtering on it
      -- was measuring the wrong thing.
      AND (
          cd.status = 'permanently_failed'
          OR cd.updated_at < now() - interval '24 hours'
      )
    ORDER BY cd.updated_at ASC
    LIMIT :limit
    """
)


@celery_app.task(
    name="openarg.retry_our_own_failures",
    bind=True,
    soft_time_limit=1800,
    time_limit=2400,
)
def retry_our_own_failures(
    self, *, limit: int = 200, dry_run: bool = True
) -> dict[str, Any]:
    """Clear the retry ceiling on our own failures and collect them again."""
    engine = get_sync_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            _CANDIDATES_SQL, {"cats": list(OUR_FAULT), "limit": limit}
        ).fetchall()
        conn.rollback()

    by_portal: dict[str, int] = {}
    for r in rows:
        by_portal[str(r.portal)] = by_portal.get(str(r.portal), 0) + 1

    if dry_run or not rows:
        return {"dry_run": dry_run, "candidates": len(rows), "dispatched": 0,
                "by_portal": dict(sorted(by_portal.items(), key=lambda x: -x[1])[:6])}

    from app.infrastructure.celery.tasks.collector_tasks import collect_dataset

    dispatched = 0
    for r in rows:
        try:
            with engine.begin() as conn:
                # The counter is why they stay dead. Clearing it is the whole
                # intervention; the collector does the rest and will mark them
                # failed again — legitimately — if the source really refuses.
                conn.execute(
                    text(
                        "UPDATE raw.cached_datasets "
                        "SET retry_count = 0, status = 'pending', error_message = NULL "
                        "WHERE id = :i"
                    ),
                    {"i": r.cd_id},
                )
            collect_dataset.delay(str(r.dataset_id))
            dispatched += 1
        except Exception:
            logger.warning("could not requeue %s", r.dataset_id, exc_info=True)

    result = {
        "dry_run": False,
        "candidates": len(rows),
        "dispatched": dispatched,
        "by_portal": dict(sorted(by_portal.items(), key=lambda x: -x[1])[:6]),
    }
    logger.info("retry our failures: %s", result)
    return result
