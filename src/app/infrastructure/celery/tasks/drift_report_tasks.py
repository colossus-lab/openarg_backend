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
from app.application.drift import DriftContext, classify_change, summarize
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# Two kinds of pair, because a resource changes shape in two distinguishable ways.
#
# `same_table` — the same physical table captured twice. This is a re-ingest
# that overwrote the table in place, or a baseline followed by the drop. Keyed
# on physical name because `resource_identity` is NULL for every legacy
# `cache_*` table, and pairing on it would silently drop most of the corpus.
#
# `version` — two *different* physical tables that the registry says are the
# same resource. This is the more interesting signal and the only one that can
# be read retroactively: where both versions are still on disk, the format
# change already happened and is sitting there waiting to be measured. Staging
# holds 112 such pairs today.
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
        curv.source_url           AS source_url,
        prevv.source_url          AS p_source_url,
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
    -- source_url feeds G2. The registry row is deleted when a table is
    -- dropped, so it is often absent — LEFT JOIN, and the gate abstains
    -- rather than guessing.
    LEFT JOIN public.raw_table_versions curv
           ON curv.schema_name = cur.schema_name AND curv.table_name = cur.table_name
    LEFT JOIN public.raw_table_versions prevv
           ON prevv.schema_name = prev.schema_name AND prevv.table_name = prev.table_name
    ORDER BY cur.captured_at DESC
    LIMIT :limit
    """
)

# Consecutive versions of one resource, oldest→newest, both sides snapshotted.
# `version` is the registry's own ordering, so this reads a real progression
# rather than whatever order the snapshots happened to be captured in.
_VERSION_PAIRS_SQL = text(
    """
    WITH ordered AS (
        SELECT
            s.*,
            LAG(s.id) OVER w AS prev_id
        FROM raw.raw_schema_snapshots s
        WHERE s.resource_identity IS NOT NULL
          AND s.version IS NOT NULL
        WINDOW w AS (PARTITION BY s.resource_identity ORDER BY s.version, s.captured_at)
    )
    SELECT
        cur.schema_name, cur.table_name, cur.resource_identity, cur.version,
        cur.row_count_estimate, cur.stats_available, cur.columns_profile,
        cur.parser_version, cur.normalization_version, cur.layout_profile,
        cur.header_quality, cur.is_truncated,
        cur.captured_at, cur.reason,
        curv.source_url           AS source_url,
        prevv.source_url          AS p_source_url,
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
    -- source_url feeds G2. The registry row is deleted when a table is
    -- dropped, so it is often absent — LEFT JOIN, and the gate abstains
    -- rather than guessing.
    LEFT JOIN public.raw_table_versions curv
           ON curv.schema_name = cur.schema_name AND curv.table_name = cur.table_name
    LEFT JOIN public.raw_table_versions prevv
           ON prevv.schema_name = prev.schema_name AND prevv.table_name = prev.table_name
    -- Only across distinct tables: a same-table progression is already covered
    -- by _PAIRS_SQL, and counting it twice would inflate every rate.
    WHERE prev.table_name <> cur.table_name
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
        -- `legacy:unknown` and a bare date are placeholders, not provenance.
        -- Counting them made the coverage number read 26,435 when the figure
        -- G1 can actually use was zero, which is the kind of comfortable
        -- statistic this whole module exists to stop producing.
        count(*) FILTER (
            WHERE parser_version IS NOT NULL
              AND parser_version <> 'legacy:unknown'
              AND parser_version !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        ) AS with_provenance,
        count(*) FILTER (WHERE parser_version IS NOT NULL) AS with_provenance_incl_placeholder,
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
        "source_url",
    )

    def __init__(self, row: Any) -> None:
        for name in self._FIELDS:
            setattr(self, name, getattr(row, f"p_{name}", None))


def _context_for(kind: str, row: Any) -> DriftContext:
    """Supply what the cascade's gates need, and nothing it would have to guess.

    Both G0 and G2 abstained on every call until now, for want of a producer.
    A pair carries both facts:

    - **G0 (identity).** A `version` pair is built by partitioning on
      `resource_identity`, so the two sides are the same resource by
      construction. A `same_table` pair is keyed on physical name, which is
      reused — if the registry says the name now belongs to a different
      resource, that is a genuine exoneration, not a format change.
    - **G2 (sibling).** Two different files of one dataset differing from each
      other is heterogeneity, not drift. `source_url` separates the cases.

    A `None` on either field means the fact is unavailable — the registry row is
    deleted when a table is dropped — and the gate abstains. That is the honest
    outcome, and materially better than assuming the answer.
    """
    if kind == "version":
        same_identity: bool | None = True
    elif row.resource_identity and row.p_resource_identity:
        same_identity = row.resource_identity == row.p_resource_identity
    else:
        same_identity = None

    same_source_url: bool | None = None
    if row.source_url and row.p_source_url:
        same_source_url = row.source_url == row.p_source_url

    return DriftContext(same_identity=same_identity, same_source_url=same_source_url)


@celery_app.task(
    name="openarg.report_schema_drift",
    bind=True,
    soft_time_limit=600,
    time_limit=900,
)
def report_schema_drift(self, *, days: int = 30, limit: int = 5000) -> dict[str, Any]:
    """Classify every snapshot pair, of both kinds, and report per gate. Shadow only.

    `days` windows the `same_table` pairs only. Version pairs are read in full:
    where two versions of a resource are both still on disk, the change they
    record happened whenever it happened, and discarding it for being recently
    captured would throw away the only retroactive evidence there is.

    Returns the summary and logs it. Notifies nobody, persists no findings and
    changes no behaviour — see the module docstring for why that restraint is
    the design rather than an omission.
    """
    engine = get_sync_engine()

    with engine.connect() as conn:
        coverage_row = conn.execute(_COVERAGE_SQL, {"days": days}).fetchone()
        same_table_rows = conn.execute(_PAIRS_SQL, {"days": days, "limit": limit}).fetchall()
        # Version pairs are deliberately not windowed by `days`: where two
        # versions of a resource are both still on disk, the change they record
        # happened whenever it happened, and excluding it because the capture is
        # recent would throw away the only retroactive evidence there is.
        version_rows = conn.execute(_VERSION_PAIRS_SQL, {"limit": limit}).fetchall()
        conn.rollback()

    coverage = {
        "snapshots": int(coverage_row.snapshots or 0),
        "tables": int(coverage_row.tables or 0),
        "with_stats": int(coverage_row.with_stats or 0),
        "with_provenance": int(coverage_row.with_provenance or 0),
        "with_provenance_incl_placeholder": int(coverage_row.with_provenance_incl_placeholder or 0),
        "first_seen": coverage_row.first_seen.isoformat() if coverage_row.first_seen else None,
        "last_seen": coverage_row.last_seen.isoformat() if coverage_row.last_seen else None,
    }

    verdicts = []
    by_kind: dict[str, list[Any]] = {}
    examples: list[dict[str, Any]] = []
    pair_rows = [("same_table", r) for r in same_table_rows] + [
        ("version", r) for r in version_rows
    ]
    for kind, row in pair_rows:
        try:
            verdict = classify_change(
                snapshot_from_row(_PrevRow(row)),
                snapshot_from_row(row),
                _context_for(kind, row),
            )
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
        by_kind.setdefault(kind, []).append(verdict)
        # Keep a handful of actionable cases inline. The whole point of P3 is
        # that a human reads twenty of these by hand to measure precision, and
        # having to write a query first is friction that stops it happening.
        if verdict.is_actionable and len(examples) < 20:
            examples.append(
                {
                    "pair_kind": kind,
                    "table": f"{row.schema_name}.{row.table_name}",
                    "compared_against": row.p_table_name,
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
        "pairs_by_kind": {
            "same_table": len(same_table_rows),
            "version": len(version_rows),
        },
        **summarize(verdicts),
        # Broken out per kind because they answer different questions. A
        # `same_table` diff says the table was rewritten; a `version` diff says
        # the resource was republished under a new physical name. Collapsing
        # them into one rate would hide which of the two is actually moving.
        "by_kind": {k: summarize(v) for k, v in by_kind.items()},
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
        # Loud, because these are the cases nothing could explain away.
        logger.warning(
            "drift report: %d change(s) survived every gate — %s",
            report["actionable"],
            report.get("actionable_by_class"),
        )
        # §5.5: one human alert per new CRITICAL. This log line used to be the
        # only place an unexplained change surfaced, in a file nobody tails —
        # which is how three marts stayed broken for weeks and were found by
        # accident. Shadow mode still governs whether the report *acts*; it was
        # never a reason not to tell a person.
        try:
            from app.application.quality.alerting import Alert, notify

            alerts = [
                Alert(
                    kind="drift",
                    # Identity of the resource, not of this sighting: the weekly
                    # report re-derives the same finding from the same pair, and
                    # keying on the run would alert every Monday forever.
                    key=str(ex.get("resource_identity") or ex.get("table")),
                    title=f"Cambio sin explicación en {ex.get('table')}",
                    detail=(
                        f"clase: {ex.get('change_class')} · "
                        f"agregadas: {ex.get('added')} · vs {ex.get('compared_against')}"
                    ),
                )
                for ex in report.get("examples", [])
            ]
            report["alerting"] = notify(engine, alerts, heading="OpenArg · deriva sin explicación")
        except Exception:
            # Never let the notification cost the report.
            logger.warning("drift report: alerting skipped", exc_info=True)
    return report
