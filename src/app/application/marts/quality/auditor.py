"""Gather what each mart looks like, then ask the checks about it.

Reads only catalogs — `mart_definitions`, `pg_class`/`pg_attribute`,
`raw_table_versions`, `query_analytics`. It never queries mart contents: a
sweep over 71 marts has to stay cheap enough to run nightly, and every question
the first checks ask is answerable from metadata.

Source tables come from `mart_definitions.sql_definition`, which `build_mart`
persists with the macros already expanded, so the `raw."tabla__hash__vN"`
references are literal text. That avoids re-resolving macros (which needs the
engine, the live-version tables, and a column introspection pass) just to learn
what a mart was built from.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.application.marts.quality.check import MartCheck
from app.application.marts.quality.checks import build_default_mart_checks
from app.application.marts.quality.context import MartAuditContext, MartColumn, SourceTable
from app.application.validation.detector import Finding

logger = logging.getLogger(__name__)

# `raw."name"` / `public."name"` as emitted by the resolved macro output.
_SOURCE_REF_RE = re.compile(r'\b(?P<schema>raw|public)\."(?P<name>[^"]+)"', re.IGNORECASE)

_MART_ROWS_SQL = text(
    "SELECT mart_id, mart_schema, mart_view_name, description, sql_definition, "
    "       last_row_count, last_refresh_status, serving_blocked, yaml_version "
    "FROM mart_definitions ORDER BY mart_id"
)

# Planner estimate per mart relation. `reltuples` is -1 when the relation has
# never been analysed; the caller normalises that to None so "unknown" is not
# mistaken for "empty" — which is the exact distinction this feeds.
_MART_STATS_SQL = text(
    "SELECT c.relname AS view_name, c.reltuples::bigint AS approx_rows "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = 'mart' AND c.relkind = ANY(ARRAY['m', 'v', 'r'])"
)

_COLUMNS_SQL = text(
    "SELECT c.relname AS view_name, a.attname AS column_name, "
    "       format_type(a.atttypid, a.atttypmod) AS pg_type, a.attnum "
    "FROM pg_class c "
    "JOIN pg_namespace n ON n.oid = c.relnamespace "
    "JOIN pg_attribute a ON a.attrelid = c.oid "
    "WHERE n.nspname = 'mart' AND c.relkind = ANY(ARRAY['m', 'v', 'r']) "
    "AND a.attnum > 0 AND NOT a.attisdropped "
    "ORDER BY c.relname, a.attnum"
)

# Live source tables with their planner row estimate. `reltuples` is an
# estimate, which is the right trade here: exact counts over thousands of raw
# tables would make the sweep expensive, and every threshold is a ratio.
_SOURCE_STATS_SQL = text(
    "SELECT n.nspname AS schema, c.relname AS name, "
    "       GREATEST(c.reltuples, 0)::bigint AS approx_rows "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = ANY(ARRAY['raw', 'public']) AND c.relkind = ANY(ARRAY['r', 'p'])"
)

_TRAFFIC_SQL = text(
    "SELECT replace(served_table, 'mart.', '') AS mart_id, count(*) AS hits, "
    "       avg(CASE WHEN success THEN 1.0 ELSE 0.0 END) AS success_rate "
    "FROM query_analytics "
    "WHERE ts > now() - interval '30 days' AND served_table LIKE 'mart.%' "
    "GROUP BY 1"
)


def _source_refs(resolved_sql: str | None) -> list[tuple[str, str]]:
    if not resolved_sql:
        return []
    seen: dict[tuple[str, str], None] = {}
    for match in _SOURCE_REF_RE.finditer(resolved_sql):
        seen.setdefault((match.group("schema").lower(), match.group("name")), None)
    return list(seen)


_COVERAGE_RE = re.compile(r"/\*\s*macro_coverage:\s*kept\s+(?P<kept>\d+)\s+of\s+(?P<of>\d+)\s*\*/")


def _coverage_counts(resolved_sql: str | None) -> tuple[int | None, int | None]:
    """How many tables the macro considered before its column filter.

    Read from the `macro_coverage` marker `_build_union` writes into the
    generated SQL. A mart built before that marker existed has none, and we
    cannot tell "kept everything" from "kept one of hundreds" — so the coverage
    check stays silent rather than inventing a denominator. Rebuilding the mart
    populates it.
    """
    if not resolved_sql:
        return None, None
    match = _COVERAGE_RE.search(resolved_sql)
    if not match:
        return None, None
    return int(match.group("kept")), int(match.group("of"))


# Above this many rows the duplicate scan reads a sample instead of the whole
# view. `count(DISTINCT t.*)` hashes every column of every row, and the sweep
# runs nightly over marts holding tens of millions.
_DUP_SCAN_FULL_LIMIT = 2_000_000
_DUP_SCAN_SAMPLE = 400_000


def _duplicate_counts(
    engine: Engine, schema: str, view: str, approx_rows: int | None
) -> tuple[int | None, int | None, bool]:
    """Rows scanned, distinct rows among them, and whether it was a sample.

    Returns `(None, None, False)` when the scan cannot run — a missing view, a
    timeout. A check that cannot measure must say nothing rather than guess.
    """
    sampled = (approx_rows or 0) > _DUP_SCAN_FULL_LIMIT
    target = (
        f'(SELECT * FROM "{schema}"."{view}" LIMIT {_DUP_SCAN_SAMPLE}) t'
        if sampled
        else f'"{schema}"."{view}" t'
    )
    try:
        with engine.connect() as conn:
            conn.execute(text("SET LOCAL statement_timeout = '45s'"))
            row = conn.execute(
                text(
                    f"SELECT count(*) AS scanned, count(DISTINCT t.*) AS distinct_rows FROM {target}"
                )
            ).fetchone()
            conn.rollback()
    except Exception as exc:
        # One line, no traceback: over 74 marts a nightly sweep would print a
        # stack for every missing view, and a mart registered without a view is
        # already reported by the coverage checks.
        logger.warning("duplicate scan skipped for %s.%s: %s", schema, view, str(exc)[:120])
        return None, None, False
    if row is None:
        return None, None, False
    return int(row.scanned), int(row.distinct_rows), sampled


def collect_contexts(engine: Engine) -> list[MartAuditContext]:
    """One context per registered mart."""
    with engine.connect() as conn:
        mart_rows = conn.execute(_MART_ROWS_SQL).fetchall()
        column_rows = conn.execute(_COLUMNS_SQL).fetchall()
        stat_rows = conn.execute(_MART_STATS_SQL).fetchall()
        source_rows = conn.execute(_SOURCE_STATS_SQL).fetchall()
        try:
            traffic_rows = conn.execute(_TRAFFIC_SQL).fetchall()
        except Exception:
            # query_analytics is created lazily by the write path; a fresh
            # environment has no table yet. Traffic is for prioritising, not
            # for deciding, so its absence must not stop the sweep.
            logger.info("query_analytics unavailable; auditing without traffic data")
            traffic_rows = []
        conn.rollback()

    columns_by_view: dict[str, list[MartColumn]] = {}
    for row in column_rows:
        columns_by_view.setdefault(row.view_name, []).append(
            MartColumn(name=row.column_name, pg_type=row.pg_type)
        )

    rows_by_table = {(r.schema, r.name): int(r.approx_rows) for r in source_rows}
    # reltuples == -1 means "never analysed". Keeping it out of the map makes
    # the lookup return None, so a check sees "unknown" rather than a negative
    # row count it would have to special-case.
    approx_by_view = {r.view_name: int(r.approx_rows) for r in stat_rows if r.approx_rows >= 0}
    traffic = {r.mart_id: (int(r.hits), float(r.success_rate or 0.0)) for r in traffic_rows}

    contexts: list[MartAuditContext] = []
    for row in mart_rows:
        view_name = row.mart_view_name or row.mart_id
        refs = _source_refs(row.sql_definition)
        sources = tuple(
            SourceTable(schema=schema, name=name, row_count=rows_by_table.get((schema, name)))
            for schema, name in refs
        )
        hits, success_rate = traffic.get(row.mart_id, (0, None))
        scanned, distinct_rows, dup_sampled = _duplicate_counts(
            engine, row.mart_schema or "mart", view_name, approx_by_view.get(view_name)
        )
        kept_count, candidate_count = _coverage_counts(row.sql_definition)
        contexts.append(
            MartAuditContext(
                mart_id=row.mart_id,
                view_name=view_name,
                schema=row.mart_schema or "mart",
                description=row.description,
                resolved_sql=row.sql_definition,
                last_row_count=row.last_row_count,
                last_refresh_status=row.last_refresh_status,
                serving_blocked=bool(row.serving_blocked),
                yaml_version=row.yaml_version,
                columns=tuple(columns_by_view.get(view_name, [])),
                approx_row_count=approx_by_view.get(view_name),
                scanned_row_count=scanned,
                distinct_row_count=distinct_rows,
                duplicate_scan_sampled=dup_sampled,
                source_tables=sources,
                candidate_table_count=candidate_count,
                kept_table_count=kept_count,
                hits_30d=hits,
                success_rate_30d=success_rate,
            )
        )
    return contexts


def run_checks(
    ctx: MartAuditContext,
    checks: list[MartCheck] | None = None,
) -> list[Finding]:
    """Every finding for one mart. A failing check never stops the others."""
    findings: list[Finding] = []
    for check in checks if checks is not None else build_default_mart_checks():
        try:
            if not check.applicable_to(ctx):
                continue
            findings.extend(check.run(ctx))
        except Exception:
            logger.exception("mart check %s failed on %s", check.name, ctx.mart_id)
    return findings


def audit_all(
    engine: Engine,
    checks: list[MartCheck] | None = None,
) -> list[tuple[MartAuditContext, list[Finding]]]:
    """Audit every mart. Marts with no findings are included with an empty list.

    Ordered by traffic so the report opens with what users actually hit: a
    broken mart with 67 hits in prod is not the same problem as one with none.
    """
    resolved = checks if checks is not None else build_default_mart_checks()
    results = [(ctx, run_checks(ctx, resolved)) for ctx in collect_contexts(engine)]
    results.sort(key=lambda pair: (-pair[0].hits_30d, pair[0].mart_id))
    return results


def finding_discriminator(finding: Finding, mart_id: str) -> str:
    """What separates two findings of the same check on the same mart.

    Completes the persistence layer's idempotency tuple. Two findings that
    share it overwrite each other — silently, because the upsert is doing
    exactly what it was designed to do. That cost the auditor's first real run
    its most severe finding: a WARN about a failed refresh replaced the
    CRITICAL about 52 million unreachable rows, while the summary went on
    counting both.

    `finding_key` is what a check sets when it emits several findings per mart;
    `column` covers the per-column checks; the mart id is the floor for checks
    that emit exactly one.
    """
    payload = getattr(finding, "payload", None) or {}
    return str(payload.get("finding_key") or payload.get("column") or mart_id)


def summarize(results: list[tuple[MartAuditContext, list[Finding]]]) -> dict[str, Any]:
    findings = [f for _ctx, fs in results for f in fs]
    by_severity: dict[str, int] = {}
    for finding in findings:
        by_severity[finding.severity.value] = by_severity.get(finding.severity.value, 0) + 1
    affected = [ctx.mart_id for ctx, fs in results if fs]
    return {
        "marts_audited": len(results),
        "marts_with_findings": len(affected),
        "findings": len(findings),
        "by_severity": by_severity,
        "affected_marts": affected,
    }
