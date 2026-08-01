"""Modo 3 of WS0 — retrospective sweep.

Walks `cached_datasets` in batches and runs the full detector suite against
each row using only the metadata that's already available (no re-download).
Persists findings + (optionally, behind feature flag) flips
`materialization_status` to `materialization_corrupted`.

Default cadence: every 6h (see celery_app.beat_schedule).
Soft-mode default for first week in prod: register findings, do NOT auto-flip.
"""

from __future__ import annotations

import json
import logging
import os

from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.application.validation.collector_hooks import (
    soft_flip_enabled,
    validate_retrospective,
)
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


# Below this planner estimate the sweep pays for an exact `COUNT(*)`. The
# `row_count` detector calls 0 rows CRITICAL, so the bottom of the range is
# where an estimate would invent or hide a finding; above it, the detector
# only ever asks whether the count is within 50% of the declared one.
_EXACT_COUNT_BELOW = 1000


def _batch_size() -> int:
    try:
        return int(os.getenv("OPENARG_SWEEP_BATCH_SIZE", "500"))
    except ValueError:
        return 500


def _portal_filter() -> list[str] | None:
    raw = os.getenv("OPENARG_SWEEP_PORTALS", "").strip()
    if not raw:
        return None
    return [p.strip() for p in raw.split(",") if p.strip()]


def _load_batch(engine, *, offset: int, limit: int, portals: list[str] | None) -> list[dict]:
    """Inventory of materialized rows the sweep should validate.

    Two sources are unioned because either alone leaves a blind spot:
      1. `cached_datasets` (status=ready/error) — the legacy public.cache_*
         path AND vía-A landings that registered a cd row.
      2. `raw_table_versions` (live, no superseded_at) — every raw
         landing, including the ~7% of rows that don't have a cd entry
         (cleanup_invariants registered them with backfill_postauto::*
         identity, or the cd row was purged by a parallel sweep).
    Without (2), 560 raw tables in staging today fall outside the sweep's
    blast radius and never get retro-validated.
    """
    sql = (
        "WITH from_cached AS ( "
        "  SELECT cd.dataset_id::text AS dataset_id, "
        "         cd.table_name, "
        "         cd.row_count, "
        "         cd.size_bytes, "
        "         cd.columns_json, "
        "         cd.status, "
        "         d.portal, "
        "         d.source_id, "
        "         d.download_url, "
        "         d.format, "
        "         cd.updated_at "
        "  FROM raw.cached_datasets cd "
        "  JOIN datasets d ON d.id = cd.dataset_id "
        "  WHERE cd.status IN ('ready','error') "
        "), from_rtv AS ( "
        "  SELECT NULL::text AS dataset_id, "
        "         rtv.schema_name || '.' || rtv.table_name AS table_name, "
        "         rtv.row_count, "
        "         rtv.size_bytes, "
        "         NULL::text AS columns_json, "
        "         'ready' AS status, "
        "         split_part(rtv.resource_identity, '::', 1) AS portal, "
        "         CASE "
        "             WHEN POSITION('::' IN rtv.resource_identity) > 0 "
        "             THEN substring(rtv.resource_identity FROM POSITION('::' IN rtv.resource_identity) + 2) "
        "             ELSE rtv.resource_identity "
        "         END AS source_id, "
        "         rtv.source_url AS download_url, "
        "         NULL::text AS format, "
        "         rtv.created_at AS updated_at "
        "  FROM raw_table_versions rtv "
        "  LEFT JOIN raw.cached_datasets cd ON cd.table_name = rtv.table_name "
        "  WHERE rtv.superseded_at IS NULL "
        "    AND rtv.schema_name = 'raw' "
        "    AND cd.table_name IS NULL "
        ") "
        "SELECT * FROM ("
        "  SELECT * FROM from_cached "
        "  UNION ALL "
        "  SELECT * FROM from_rtv "
        ") combined "
    )
    params: dict[str, object] = {"limit": limit, "offset": offset}
    if portals:
        sql += "WHERE portal = ANY(:portals) "
        params["portals"] = portals
    sql += "ORDER BY updated_at DESC NULLS LAST LIMIT :limit OFFSET :offset"
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()]


def _split_qualified_name(table_name: str) -> tuple[str, str]:
    """Split a possibly-qualified Postgres relation name into (schema, name).

    Accepts the three shapes that show up in `cached_datasets.table_name`
    and `catalog_resources.materialized_table_name`:

        cache_foo                     → ('public', 'cache_foo')
        raw.cache_foo                 → ('raw',    'cache_foo')
        raw."portal__source__hash__v1" → ('raw',    'portal__source__hash__v1')

    Without this split, the sweep / enrichment queries assumed `public.*`
    and silently produced empty findings for raw / staging / mart tables.
    """
    if not table_name:
        return "public", ""
    if "." not in table_name:
        return "public", table_name
    schema, _, rest = table_name.partition(".")
    # Strip surrounding double-quotes that the qualified writer adds.
    return schema.strip('"'), rest.strip('"')


# Where an unqualified `cached_datasets.table_name` can actually live. The
# split above answers `public` for those, which was true when `cache_*` sat in
# the public schema and has not been true for a while: measured 2026-07-31, all
# 25288 ready rows carry an unqualified name and all 25288 relations are in
# `raw`. Resolving against the wrong schema returned no columns, so the sweep
# has been walking its whole inventory and validating almost none of it —
# silently, because "no columns" produced no findings rather than an error.
# `raw` first, since that is where they are; `public` kept for the legacy shape.
_CANDIDATE_SCHEMAS = ("raw", "public")


def _resolve_columns(cols_by_key: dict[tuple[str, str], list[str]], table_name: str) -> list[str]:
    """Columns for a relation, trying the named schema then the real ones."""
    schema, bare = _split_qualified_name(table_name or "")
    if not bare:
        return []
    found = cols_by_key.get((schema, bare))
    if found:
        return found
    for candidate in _CANDIDATE_SCHEMAS:
        found = cols_by_key.get((candidate, bare))
        if found:
            return found
    return []


def _resolve_row_count(
    counts_by_key: dict[tuple[str, str], int | None], table_name: str
) -> int | None:
    """Row count for a relation, resolved the same way as its columns."""
    schema, bare = _split_qualified_name(table_name or "")
    if not bare:
        return None
    for key in ((schema, bare), *((c, bare) for c in _CANDIDATE_SCHEMAS)):
        if key in counts_by_key:
            return counts_by_key[key]
    return None


def _columns_for_batch(engine, table_names: list[str]) -> dict[tuple[str, str], list[str]]:
    """Column lists for a whole batch in one query, keyed by `(schema, name)`.

    Was one connection and one `information_schema` query per table. Over the
    27.7k relations this sweep walks that is 27.7k round trips before any
    detector runs, and the task was dying on its 600s soft limit every single
    run — so the tail of the inventory was never validated at all.
    """
    pairs = [_split_qualified_name(t) for t in table_names if t]
    if not pairs:
        return {}
    # Search the schemas the names claim AND the ones relations actually live
    # in, because for unqualified names those are not the same set — see
    # `_CANDIDATE_SCHEMAS`. `_resolve_columns` picks between the results.
    schemas = sorted({s for s, _ in pairs} | set(_CANDIDATE_SCHEMAS))
    bares = sorted({b for _, b in pairs})
    out: dict[tuple[str, str], list[str]] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT table_schema, table_name, column_name "
                    "FROM information_schema.columns "
                    "WHERE table_schema = ANY(:schemas) AND table_name = ANY(:bares) "
                    "ORDER BY table_schema, table_name, ordinal_position"
                ),
                {"schemas": schemas, "bares": bares},
            ).fetchall()
            conn.rollback()
    except Exception:
        # Failed introspection means the sweep cannot validate these rows —
        # a real coverage gap, not a missing optional.
        logger.warning("Could not introspect columns for a batch", exc_info=True)
        return {}
    for r in rows:
        out.setdefault((r.table_schema, r.table_name), []).append(r.column_name)
    return out


def _row_counts_for_batch(engine, table_names: list[str]) -> dict[tuple[str, str], int | None]:
    """Row counts for a batch, estimated first and only counted when it matters.

    `COUNT(*)` per table was the sweep's other per-row cost, and it is a full
    scan — one of these relations holds 52 million rows. The planner's
    `reltuples` is free and precise enough for the only question the detectors
    ask of a large table (`RowCountDetector` compares against the declared
    count with a 50% tolerance).

    The estimate is *not* good enough at the bottom of the range, where the
    difference between 0 and 3 rows is a CRITICAL finding, and where
    `reltuples` reports -1 for a relation that was never analysed. Those get
    the exact count — few enough to be affordable, and exactly the ones where
    being wrong would fabricate or hide a finding.
    """
    pairs = [_split_qualified_name(t) for t in table_names if t]
    if not pairs:
        return {}
    counts: dict[tuple[str, str], int | None] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT n.nspname AS schema, c.relname AS name, "
                    "       c.reltuples::bigint AS approx "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = ANY(:schemas) AND c.relname = ANY(:bares) "
                    "  AND c.relkind = ANY(ARRAY['r','p','m','v'])"
                ),
                {
                    "schemas": sorted({s for s, _ in pairs} | set(_CANDIDATE_SCHEMAS)),
                    "bares": sorted({b for _, b in pairs}),
                },
            ).fetchall()
            conn.rollback()
            wanted = {b for _, b in pairs}
            needs_exact: list[tuple[str, str]] = []
            for r in rows:
                key = (r.schema, r.name)
                if r.name not in wanted:
                    continue
                approx = int(r.approx)
                if approx > _EXACT_COUNT_BELOW:
                    counts[key] = approx
                else:
                    needs_exact.append(key)
            for schema, bare in needs_exact:
                safe_schema = schema.replace('"', '""')
                safe_bare = bare.replace('"', '""')
                try:
                    res = conn.execute(
                        text(f'SELECT COUNT(*) FROM "{safe_schema}"."{safe_bare}"')  # noqa: S608
                    )
                    counts[(schema, bare)] = int(res.scalar() or 0)
                except Exception:
                    counts[(schema, bare)] = None
                    conn.rollback()
    except Exception:
        logger.warning("Could not read row counts for a batch", exc_info=True)
        return {}
    return counts


def _maybe_flip_status(engine, dataset_id: str, table_name: str, has_critical: bool) -> None:
    """When auto-flip is enabled and there's a critical finding, flag the row.

    We don't have `materialization_status` on `cached_datasets` (it lives on
    the future `catalog_resources`). For now we mark `error_message` so the
    discovery side can deprioritize the resource. Once WS2 lands, this
    becomes an UPDATE of `catalog_resources.materialization_status` to
    `materialization_corrupted`.
    """
    if not has_critical or not soft_flip_enabled():
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE raw.cached_datasets "
                    "SET error_message = COALESCE(error_message,'') || "
                    "    CASE WHEN POSITION('materialization_corrupted' IN COALESCE(error_message,'')) > 0 "
                    "         THEN '' ELSE ' | materialization_corrupted' END, "
                    "    updated_at = NOW() "
                    "WHERE dataset_id = CAST(:did AS uuid)"
                ),
                {"did": dataset_id},
            )
    except Exception:
        logger.exception(
            "Failed to flip materialization_status for %s (%s)", dataset_id, table_name
        )


def _close_resolved_findings_query(engine) -> int:
    """Close findings whose dataset has transitioned to a healthy state since.

    A finding is considered resolved when:
      - The dataset linked by resource_id is now `cached_datasets.status='ready'`.
      - That `ready` transition is **after** the finding was registered
        (`cd.updated_at > f.found_at`), proving a re-process actually happened.

    Returns the number of findings closed.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                WITH closed AS (
                    UPDATE ingestion_findings f
                    SET resolved_at = NOW()
                    WHERE f.resolved_at IS NULL
                      AND EXISTS (
                          SELECT 1 FROM raw.cached_datasets cd
                          WHERE cd.dataset_id::text = f.resource_id
                            AND cd.status = 'ready'
                            AND cd.updated_at > f.found_at
                      )
                    RETURNING f.id, f.detector_name
                )
                SELECT detector_name, COUNT(*) AS n
                FROM closed
                GROUP BY 1
                ORDER BY 2 DESC
                """
            )
        )
        rows = list(result.fetchall())
        total = sum(int(r.n) for r in rows)
        if rows:
            logger.info(
                "close_resolved_findings closed %d findings: %s",
                total,
                {str(r.detector_name): int(r.n) for r in rows},
            )
        else:
            logger.info("close_resolved_findings: nothing to close")
    return total


@celery_app.task(
    name="openarg.close_resolved_findings",
    bind=True,
    soft_time_limit=120,
    time_limit=180,
)
def close_resolved_findings_task(self) -> dict:
    """Periodic closer for ingestion_findings.

    Pairs with the WS0 retrospective sweep (which writes findings) so the
    `ingestion_findings` table stops being write-only. Runs cheap — a single
    UPDATE with EXISTS subquery — so safe to schedule every 15 minutes.
    """
    engine = get_sync_engine()
    try:
        closed = _close_resolved_findings_query(engine)
        return {"closed": closed}
    finally:
        engine.dispose()


@celery_app.task(
    name="openarg.ws0_retrospective_sweep",
    bind=True,
    soft_time_limit=600,
    time_limit=720,
)
def retrospective_sweep(self, *, max_batches: int | None = None) -> dict:
    """Sweep through cached_datasets and persist findings.

    `max_batches` lets ad-hoc dispatchers cap the run; when omitted, the
    sweep runs to completion (or hits the soft time limit).
    """
    engine = get_sync_engine()
    portals = _portal_filter()
    batch_size = _batch_size()

    total_scanned = 0
    total_findings = 0
    total_critical = 0
    batch_idx = 0
    offset = 0

    try:
        while True:
            batch = _load_batch(engine, offset=offset, limit=batch_size, portals=portals)
            if not batch:
                break
            names = [r["table_name"] for r in batch]
            cols_by_table = _columns_for_batch(engine, names)
            counts_by_table = _row_counts_for_batch(engine, names)
            for row in batch:
                cols_real = _resolve_columns(cols_by_table, row["table_name"] or "")
                rows_real = (
                    _resolve_row_count(counts_by_table, row["table_name"] or "")
                    if cols_real
                    else None
                )
                findings = validate_retrospective(
                    engine,
                    # Close what this resource stopped reporting. Without it the
                    # sweep is append-only: `persist_findings` re-opens on
                    # conflict and nothing ever resolves, so a table that got
                    # fixed keeps its finding forever.
                    resolve_stale=True,
                    dataset_id=row["dataset_id"],
                    portal=row["portal"],
                    source_id=row["source_id"],
                    download_url=row["download_url"],
                    declared_format=row["format"],
                    table_name=row["table_name"],
                    materialized_columns=cols_real or None,
                    materialized_row_count=rows_real,
                    declared_size_bytes=row["size_bytes"] or 0,
                    declared_row_count=row["row_count"] or 0,
                    columns_json=row["columns_json"],
                )
                total_scanned += 1
                total_findings += len(findings)
                has_critical = any(f.severity.value == "critical" for f in findings)
                if has_critical:
                    total_critical += 1
                _maybe_flip_status(engine, row["dataset_id"], row["table_name"], has_critical)
            offset += batch_size
            batch_idx += 1
            if max_batches is not None and batch_idx >= max_batches:
                break
    except SoftTimeLimitExceeded:
        logger.warning(
            "ws0 sweep hit soft time limit at batch %d (scanned=%d)", batch_idx, total_scanned
        )

    summary = {
        "scanned": total_scanned,
        "findings_persisted": total_findings,
        "critical_resources": total_critical,
        "batches": batch_idx,
        "auto_flip_enabled": soft_flip_enabled(),
        "portals_filter": portals,
    }
    logger.info("ws0 retrospective sweep done: %s", json.dumps(summary))
    return summary
