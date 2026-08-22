"""One place to see whether any of the self-repair machinery is working.

The subsystem built over 2026-08-21/22 — snapshots, the exoneration cascade,
the repair sweeps, the refresh — writes to logs nobody tails and audit tables
nobody queries. It could be working perfectly, or have silently stopped, and
the difference would not be visible from anywhere. That is not a hypothetical:
`cleanup_raw_orphans` reported `{'dropped': 0, 'failed': 10}` and *succeeded*
every hour for three months while doing nothing, and it took reading a log by
hand to notice.

So this endpoint answers four questions that were previously unanswerable
without a psql session:

- **How old is the data we serve?** Users cannot tell that an answer rests on
  May's numbers. Today, most do.
- **How much of the corpus is badly parsed?** Not "is there a defect" but how
  many tables carry one right now.
- **Is anything actually repairing itself?** Counted from the audit, per class,
  including the refusals — a sweep that declines everything is not the same as
  a sweep that is idle, and both look identical from outside.
- **Can drift even be observed yet?** The cascade needs provenance on both sides
  of a pair, and a report of zeros means something very different when zero
  pairs are attributable.

Read-only, admin-gated, and every number comes from a query rather than a
counter, so it cannot drift from reality the way in-memory metrics do.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import text

from app.infrastructure.celery.tasks._db import get_sync_engine
from app.presentation.http.controllers.admin.tasks_router import verify_admin_key

router = APIRouter(prefix="/admin", tags=["admin-data-health"])

# `updated_at` on a cached_datasets row is when we last wrote it, which is the
# closest thing the schema has to "when we last read the source".
_FRESHNESS_SQL = text(
    """
    SELECT
        count(*) AS total,
        count(*) FILTER (WHERE cd.updated_at > now() - interval '7 days')   AS week,
        count(*) FILTER (WHERE cd.updated_at > now() - interval '30 days')  AS month,
        count(*) FILTER (WHERE cd.updated_at > now() - interval '90 days')  AS quarter,
        count(*) FILTER (WHERE cd.updated_at <= now() - interval '90 days') AS older,
        min(cd.updated_at) AS oldest,
        -- What the portals say changed since we read it. This is the queue the
        -- refresh drains, and the honest measure of how stale we are: age alone
        -- says nothing when three quarters of the catalogue never changes.
        count(*) FILTER (WHERE d.last_updated_at > cd.updated_at) AS portal_says_changed
    FROM raw.cached_datasets cd
    JOIN datasets d ON d.id = cd.dataset_id
    WHERE cd.status = 'ready'
    """
)

_PARSE_SQL = text(
    r"""
    WITH cols AS (
        SELECT c.table_name, c.column_name,
               count(*) OVER (PARTITION BY c.table_name) AS n_cols
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema
         AND t.table_name = c.table_name
         AND t.table_type = 'BASE TABLE'
        WHERE c.table_schema = 'raw' AND c.column_name NOT LIKE '\_%'
    ),
    flags AS (
        SELECT table_name, n_cols,
               bool_or(column_name ~ '^col_[0-9]+$')  AS col_n,
               bool_or(column_name ~ '^[Uu]nnamed')   AS unnamed,
               bool_or(length(column_name) > 60)      AS long_name
        FROM cols GROUP BY 1, 2
    )
    SELECT
        count(*)                                      AS tables,
        count(*) FILTER (WHERE col_n)                 AS col_n,
        count(*) FILTER (WHERE unnamed)               AS unnamed,
        count(*) FILTER (WHERE long_name)             AS long_names,
        count(*) FILTER (WHERE n_cols <= 2)           AS one_or_two_columns,
        count(*) FILTER (WHERE col_n OR unnamed OR long_name OR n_cols <= 2) AS any_symptom
    FROM flags
    """
)

_REPAIRS_SQL = text(
    """
    SELECT phase, operation, ok, count(*) AS n,
           max(applied_at) AS last_seen
    FROM parse_repair_audit
    WHERE applied_at > now() - make_interval(days => :days)
    GROUP BY 1, 2, 3
    ORDER BY n DESC
    """
)

_DRIFT_SQL = text(
    """
    SELECT
        count(*) AS snapshots,
        count(DISTINCT (schema_name, table_name)) AS tables,
        -- A placeholder is not provenance. `legacy:unknown` and a bare date
        -- both look like versions and say nothing, and counting them is how a
        -- coverage number reads 26,435 while the figure G1 can use is zero.
        count(*) FILTER (
            WHERE parser_version IS NOT NULL
              AND parser_version <> 'legacy:unknown'
              AND parser_version !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        ) AS with_real_provenance,
        max(captured_at) AS last_capture
    FROM raw.raw_schema_snapshots
    """
)

_ATTRIBUTABLE_SQL = text(
    """
    SELECT count(*) FROM raw.raw_schema_snapshots a
    JOIN raw.raw_schema_snapshots b
      ON b.resource_identity = a.resource_identity AND b.version = a.version + 1
    WHERE a.parser_version LIKE 'p:%' AND b.parser_version LIKE 'p:%'
    """
)


def _pct(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 1) if whole else 0.0


@router.get("/data-health", dependencies=[Depends(verify_admin_key)])
def data_health(repair_window_days: int = 7) -> dict[str, Any]:
    """Freshness, parse quality, repair activity and drift observability."""
    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            fresh = conn.execute(_FRESHNESS_SQL).fetchone()
            parse = conn.execute(_PARSE_SQL).fetchone()
            repairs = conn.execute(_REPAIRS_SQL, {"days": repair_window_days}).fetchall()
            drift = conn.execute(_DRIFT_SQL).fetchone()
            attributable = conn.execute(_ATTRIBUTABLE_SQL).scalar()
            conn.rollback()
    finally:
        engine.dispose()

    total = int(fresh.total or 0)
    by_phase: dict[str, dict[str, int]] = {}
    for r in repairs:
        entry = by_phase.setdefault(r.phase, {"applied": 0, "declined": 0, "dry_run": 0})
        if r.operation == "apply" and r.ok:
            entry["applied"] += r.n
        elif r.operation == "dry_run":
            entry["dry_run"] += r.n
        else:
            # A sweep that declines everything is not a sweep that is idle, and
            # from outside they look the same. Counted so they stop looking the
            # same.
            entry["declined"] += r.n

    return {
        "freshness": {
            "ready_resources": total,
            "collected_last_week": int(fresh.week or 0),
            "collected_last_month": int(fresh.month or 0),
            "older_than_90_days": int(fresh.older or 0),
            "older_than_90_days_pct": _pct(int(fresh.older or 0), total),
            "oldest_collection": fresh.oldest.isoformat() if fresh.oldest else None,
            # The number that matters more than age: what the portals themselves
            # say has moved since we read it.
            "portal_says_changed": int(fresh.portal_says_changed or 0),
        },
        "parse_quality": {
            "tables": int(parse.tables or 0),
            "with_any_symptom": int(parse.any_symptom or 0),
            "with_any_symptom_pct": _pct(int(parse.any_symptom or 0), int(parse.tables or 0)),
            "col_n": int(parse.col_n or 0),
            "unnamed": int(parse.unnamed or 0),
            "long_names": int(parse.long_names or 0),
            "one_or_two_columns": int(parse.one_or_two_columns or 0),
        },
        "repairs": {
            "window_days": repair_window_days,
            "by_phase": by_phase,
            "last_activity": max(
                (r.last_seen for r in repairs if r.last_seen), default=None
            ),
        },
        "drift_observability": {
            "snapshots": int(drift.snapshots or 0),
            "tables_covered": int(drift.tables or 0),
            "with_real_provenance": int(drift.with_real_provenance or 0),
            "last_capture": drift.last_capture.isoformat() if drift.last_capture else None,
            # Zero here means the cascade cannot yet say whose change anything
            # was, and a drift report of zeros means "nothing comparable" rather
            # than "nothing wrong". The distinction is the whole point.
            "attributable_pairs": int(attributable or 0),
        },
    }
