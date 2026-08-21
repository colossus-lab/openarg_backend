"""Turn stored snapshots into the number nobody can produce today.

The question that started this — *how often, and in what way, do the formats
we ingest actually change?* — has never been answerable, because the evidence
was destroyed by the act of handling the change (19,293 `schema_mismatch_
recreate` drops). Migrations 0056 and 0057 keep the evidence. This task reads
it.

**It runs in shadow.** It notifies nobody, writes nothing to
`ingestion_findings`, and changes no behaviour. That is deliberate, not
timidity: this project already has ~22,000 findings that nobody reads and a
nightly mart auditor whose output was never looked at. Industry figures put
typical false-positive rates at 60-80 %, and two of the cascade's gates (G0
identity, G2 sibling) abstain on every call today for want of a producer.
Turning on a notification before the noise is characterised would burn the
attention it is meant to earn — and this codebase has already spent that
attention twice.

What it produces instead is a summary, broken down **per gate**. If G1
exonerates most of the diffs, the noise was ours and that changes what to fix
next. That breakdown is the deliverable; the alert comes later, calibrated
against it.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from app.application.catalog.schema_snapshot import snapshot_from_row
from app.application.drift import classify_change, summarize
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# Snapshots are keyed by physical table name, which is stable across the drops
# we care about. `resource_identity` would be the better key but it is NULL for
# every legacy `cache_*` table — most of production — so pairing on it would
# silently drop the majority of the corpus.
_PAIRS_SQL = text(
    """
    WITH ordered AS (
        SELECT
            s.*,
            LAG(s.id) OVER w AS prev_id
        FROM raw.raw_schema_snapshots s
        WHERE s.captured_at > NOW() - make_interval(days => :days)
        WINDOW w AS (
            PARTITION BY s.schema_name, s.table_name ORDER BY s.captured_at
        )
    )
    SELECT
        cur.id             AS cur_id,
        cur.schema_name, cur.table_name, cur.resource_identity, cur.version,
        cur.row_count_estimate, cur.stats_available, cur.columns_profile,
        cur.parser_version, cur.normalization_version, cur.layout_profile,
        cur.header_quality, cur.is_truncated,
        cur.captured_at, cur.reason,
        prev.schema_name          AS p_schema_name,
        prev.table_name           AS p_table_name,
        prev.resource_identity    AS p_resource_identity,
        prev.version              AS p_version,
        prev.row_count_estimate   AS p_row_count_estimate,
        prev.stats_available      AS p_stats_available,
        prev.columns_profile      AS p_columns_profile,
        prev.parser_version       AS p_parser_version,
        prev.normalization_version AS p_normalization_version,
        prev.layout_profile       AS p_layout_profile,
        prev.header_quality       AS p_header_quality,
        prev.is_truncated         AS p_is_truncated
    FROM ordered cur
    JOIN raw.raw_schema_snapshots prev ON prev.id = cur.prev_id
    ORDER BY cur.captured_at DESC
    LIMIT :limit
    """
)

_COVERAGE_SQL = text(
    """
    SELECT
        count(*)                                        AS snapshots,
        count(DISTINCT (schema_name, table_name))       AS tables,
        count(*) FILTER (WHERE stats_available)         AS with_stats,
        count(*) FILTER (WHERE parser_version IS NOT NULL) AS with_provenance,
        min(captured_at)                                AS first_seen,
        max(captured_at)                                AS last_seen
    FROM raw.raw_schema_snapshots
    WHERE captured_at > NOW() - make_interval(days => :days)
    """
)


class _PrevRow:
    """Adapter so `snapshot_from_row` can read the `p_`-prefixed half of a pair.

    The pair query returns both snapshots flattened into one row. Rather than
    duplicate the rehydration logic for the prefixed columns, present the
    previous half under the names the rehydrator expects.
    """

    _FIELDS = (
        "schema_name",
        "table_name",
        "resource_identity",
        "version",
        "row_count_estimate",
        "stats_available",
        "columns_profile",
        "parser_version",
        "normalization_version",
        "layout_profile",
        "header_quality",
        "is_truncated",
    )

    def __init__(self, row: Any) -> None:
        for name in self._FIELDS:
            setattr(self, name, getattr(row, f"p_{name}", None))


@celery_app.task(
    name="openarg.report_schema_drift",
    bind=True,
    soft_time_limit=600,
    time_limit=900,
)
def report_schema_drift(self, *, days: int = 30, limit: int = 5000) -> dict[str, Any]:
    """Classify every consecutive snapshot pair in the window. Shadow only.

    Returns the summary and logs it. Notifies nobody, persists no findings and
    changes no behaviour — see the module docstring for why that restraint is
    the design rather than an omission.
    """
    engine = get_sync_engine()

    with engine.connect() as conn:
        coverage_row = conn.execute(_COVERAGE_SQL, {"days": days}).fetchone()
        pair_rows = conn.execute(_PAIRS_SQL, {"days": days, "limit": limit}).fetchall()
        conn.rollback()

    coverage = {
        "snapshots": int(coverage_row.snapshots or 0),
        "tables": int(coverage_row.tables or 0),
        "with_stats": int(coverage_row.with_stats or 0),
        "with_provenance": int(coverage_row.with_provenance or 0),
        "first_seen": coverage_row.first_seen.isoformat() if coverage_row.first_seen else None,
        "last_seen": coverage_row.last_seen.isoformat() if coverage_row.last_seen else None,
    }

    verdicts = []
    examples: list[dict[str, Any]] = []
    for row in pair_rows:
        try:
            verdict = classify_change(snapshot_from_row(_PrevRow(row)), snapshot_from_row(row))
        except Exception:
            # One malformed row must not cost the whole report. A snapshot
            # written by an older version, or with a profile we cannot parse,
            # is a gap in coverage — not a reason to produce nothing.
            logger.warning(
                "drift report: could not classify %s.%s",
                row.schema_name,
                row.table_name,
                exc_info=True,
            )
            continue
        verdicts.append(verdict)
        # Keep a handful of actionable cases inline. The whole point of P3 is
        # that a human reads twenty of these by hand to measure precision, and
        # having to write a query first is friction that stops it happening.
        if verdict.is_actionable and len(examples) < 20:
            examples.append(
                {
                    "table": f"{row.schema_name}.{row.table_name}",
                    "resource_identity": row.resource_identity,
                    "change_class": verdict.change_class.value if verdict.change_class else None,
                    "reason_dropped": row.reason,
                    "added": verdict.diff.get("added"),
                    "removed": verdict.diff.get("removed"),
                    "type_changed": verdict.diff.get("type_changed"),
                    "renamed_candidates": verdict.diff.get("renamed_candidates"),
                    "ambiguous_renames": verdict.diff.get("ambiguous_renames"),
                    "gates_not_evaluated": verdict.gates_not_evaluated,
                }
            )

    report: dict[str, Any] = {
        "window_days": days,
        "coverage": coverage,
        "pairs_found": len(pair_rows),
        **summarize(verdicts),
        "examples": examples,
        "mode": "shadow",
    }

    if not pair_rows:
        # The expected outcome early on, and worth saying plainly rather than
        # reporting zeros that read like "nothing is wrong". A table needs two
        # snapshots before anything can be compared, and the second one only
        # arrives when it is dropped again.
        logger.info(
            "drift report: %d snapshot(s) over %d table(s) but no consecutive pair yet — "
            "nothing is comparable until a table is captured twice",
            coverage["snapshots"],
            coverage["tables"],
            extra={"drift_report": report},
        )
        return report

    logger.info("drift report (shadow): %s", {k: v for k, v in report.items() if k != "examples"})
    if report.get("actionable"):
        # Loud, because these are the cases nothing could explain away — and
        # in shadow mode this log line is the only place they surface.
        logger.warning(
            "drift report: %d change(s) survived every gate — %s",
            report["actionable"],
            report.get("actionable_by_class"),
        )
    return report
