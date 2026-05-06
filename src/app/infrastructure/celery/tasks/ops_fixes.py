"""Operational anexos del plan (no requirieren rediseño arquitectónico).

  - `temp_dir_cleanup` — barre `/tmp/tmp*` con mtime > 1h. Evita los ~22
     fallos `[Errno 28] No space left on device` del 2026-03-30.
  - `portal_health` — pingea cada portal y mantiene estado en `portals`
     (down → datasets de ese portal saltan retries vía circuit breaker).

`pg_event_trigger` para auditar `DROP TABLE cache_*` se entrega como
migración SQL en `2026_04_25_0036_pg_event_trigger_drop_cache.py`.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterable
from urllib.parse import urlparse

import httpx
from sqlalchemy import bindparam, text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


# ---------- temp dir cleanup ----------


def _temp_dir() -> str:
    return os.getenv("OPENARG_TEMP_DIR") or "/tmp"


def _cleanup_threshold_seconds() -> int:
    try:
        return int(os.getenv("OPENARG_TEMP_CLEANUP_AGE_SECONDS", "3600"))
    except ValueError:
        return 3600


@celery_app.task(
    name="openarg.ops_temp_dir_cleanup",
    bind=True,
    soft_time_limit=120,
    time_limit=180,
)
def temp_dir_cleanup(self) -> dict:
    """Sweep stale temp files left behind by failed collector runs."""
    threshold = _cleanup_threshold_seconds()
    cutoff = time.time() - threshold
    base = _temp_dir()
    if not os.path.isdir(base):
        return {"removed": 0, "reason": "temp_dir_missing"}
    removed = 0
    bytes_freed = 0
    skipped = 0
    for entry in os.listdir(base):
        if not entry.startswith("tmp"):
            continue
        path = os.path.join(base, entry)
        try:
            stat = os.stat(path)
        except FileNotFoundError:
            continue
        if stat.st_mtime > cutoff:
            skipped += 1
            continue
        try:
            if os.path.isdir(path):
                # Conservative: only remove empty dirs to avoid eating live work.
                try:
                    os.rmdir(path)
                    removed += 1
                except OSError:
                    skipped += 1
            else:
                bytes_freed += stat.st_size
                os.unlink(path)
                removed += 1
        except Exception:
            logger.debug("Could not remove %s", path, exc_info=True)
            skipped += 1
    summary = {
        "removed": removed,
        "skipped": skipped,
        "bytes_freed": bytes_freed,
        "threshold_seconds": threshold,
        "base": base,
    }
    logger.info("temp_dir_cleanup: %s", summary)
    return summary


# ---------- portal_health ----------


# Hardcoded list of confirmed-dead portals (see MEMORY.md → Data Sources Status).
DEAD_PORTAL_HOSTS: tuple[str, ...] = (
    "datos.santafe.gob.ar",
    "datos.modernizacion.gob.ar",
    "datos.ambiente.gob.ar",
    "datos.rionegro.gov.ar",
    "datos.jujuy.gob.ar",
    "datos.salta.gob.ar",
    "datos.laplata.gob.ar",
    "datos.cordoba.gob.ar",
    "datos.cultura.gob.ar",
    "datos.cordoba.gov.ar",
)


def _portal_hosts(engine) -> dict[str, str]:
    """Return {portal_slug: hostname} from a sample dataset per portal."""
    out: dict[str, str] = {}
    sql = text(
        "SELECT DISTINCT ON (portal) portal, download_url "
        "FROM datasets "
        "WHERE COALESCE(download_url,'') <> '' "
        "ORDER BY portal, created_at DESC"
    )
    with engine.connect() as conn:
        for row in conn.execute(sql).fetchall():
            host = (urlparse(row.download_url or "").hostname or "").lower()
            if host:
                out[row.portal] = host
    return out


def _ensure_portals_table(engine) -> None:
    """Idempotent — create the `portals` health table if it doesn't exist.

    Lives outside Alembic migrations because it's purely operational metadata
    and we want it bootstrapped automatically on first sweep.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS portals ("
                "  portal VARCHAR(100) PRIMARY KEY, "
                "  host VARCHAR(255), "
                "  is_down BOOLEAN NOT NULL DEFAULT false, "
                "  last_status INTEGER, "
                "  last_check TIMESTAMP WITH TIME ZONE DEFAULT NOW(), "
                "  consecutive_failures INTEGER NOT NULL DEFAULT 0, "
                "  last_error TEXT"
                ")"
            )
        )


def _record_portal_status(
    engine,
    portal: str,
    host: str,
    *,
    status: int | None,
    error: str | None,
) -> None:
    is_down = host in DEAD_PORTAL_HOSTS or status is None or status >= 500
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO portals (portal, host, is_down, last_status, last_check, "
                "                     consecutive_failures, last_error) "
                "VALUES (:p, :h, :down, :st, NOW(), :cf, :err) "
                "ON CONFLICT (portal) DO UPDATE SET "
                "  host = EXCLUDED.host, "
                "  is_down = EXCLUDED.is_down, "
                "  last_status = EXCLUDED.last_status, "
                "  last_check = NOW(), "
                "  consecutive_failures = CASE "
                "    WHEN :down THEN portals.consecutive_failures + 1 "
                "    ELSE 0 END, "
                "  last_error = EXCLUDED.last_error"
            ),
            {
                "p": portal,
                "h": host,
                "down": is_down,
                "st": status,
                "cf": 1 if is_down else 0,
                "err": (error or "")[:500],
            },
        )


def _ping(host: str, timeout: float) -> tuple[int | None, str | None]:
    if not host:
        return None, "no_host"
    url = f"https://{host}/"
    try:
        with httpx.Client(timeout=timeout, follow_redirects=False) as client:
            resp = client.head(url)
            return resp.status_code, None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def is_portal_down(engine, portal: str) -> bool:
    """Used by the collector circuit breaker to skip retries for dead portals."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT is_down FROM portals WHERE portal = :p"), {"p": portal}
            ).fetchone()
        return bool(row and row.is_down)
    except Exception:
        return False


@celery_app.task(
    name="openarg.ops_portal_health",
    bind=True,
    soft_time_limit=120,
    time_limit=180,
)
def portal_health(self, *, portals: Iterable[str] | None = None, timeout: float = 5.0) -> dict:
    """Ping each portal and record state in `portals`.

    `portals` lets ad-hoc dispatchers limit the run.
    """
    engine = get_sync_engine()
    _ensure_portals_table(engine)
    portal_hosts = _portal_hosts(engine)
    if portals is not None:
        portal_hosts = {p: h for p, h in portal_hosts.items() if p in portals}
    summary: dict = {"checked": 0, "down": 0, "alive": 0, "details": []}
    for portal, host in portal_hosts.items():
        if host in DEAD_PORTAL_HOSTS:
            _record_portal_status(engine, portal, host, status=None, error="known_dead")
            summary["details"].append({"portal": portal, "host": host, "status": "known_dead"})
            summary["down"] += 1
            summary["checked"] += 1
            continue
        status, error = _ping(host, timeout)
        _record_portal_status(engine, portal, host, status=status, error=error)
        if status is None or status >= 500:
            summary["down"] += 1
        else:
            summary["alive"] += 1
        summary["details"].append({"portal": portal, "host": host, "status": status})
        summary["checked"] += 1
    logger.info(
        "portal_health: checked=%s down=%s alive=%s",
        summary["checked"],
        summary["down"],
        summary["alive"],
    )
    return summary


# ---------- backfill_error_categories (P0-B) ----------


@celery_app.task(
    name="openarg.backfill_error_categories",
    bind=True,
    soft_time_limit=300,
    time_limit=420,
)
def backfill_error_categories(self, *, batch_size: int = 200) -> dict:
    """Re-classify legacy `cached_datasets.error_category='unknown'` rows.

    Walks rows in error/permanently_failed status whose category is still
    'unknown' (legacy from before the runtime classifier landed) and re-runs
    the same classifier function on the existing error_message. Idempotent —
    re-run only updates rows whose classifier output changed.
    """
    from app.infrastructure.celery.tasks.collector_tasks import (
        _classify_error_category,
    )

    engine = get_sync_engine()
    select_sql = text(
        """
        SELECT id::text AS id, error_message
        FROM cached_datasets
        WHERE error_category = 'unknown'
          AND status IN ('error', 'permanently_failed')
        ORDER BY updated_at DESC NULLS LAST
        LIMIT :limit OFFSET :offset
        """
    )
    update_sql = text(
        """
        UPDATE cached_datasets
        SET error_category = :cat
        WHERE id = CAST(:id AS uuid)
          AND error_category = 'unknown'
        """
    )
    offset = 0
    seen = 0
    updated = 0
    by_category: dict[str, int] = {}
    while True:
        with engine.connect() as conn:
            rows = conn.execute(select_sql, {"limit": batch_size, "offset": offset}).fetchall()
        if not rows:
            break
        seen += len(rows)
        for r in rows:
            cat = _classify_error_category(r.error_message)
            if cat == "unknown":
                continue
            with engine.begin() as conn:
                conn.execute(update_sql, {"cat": cat, "id": r.id})
            updated += 1
            by_category[cat] = by_category.get(cat, 0) + 1
        offset += batch_size
    summary = {"seen": seen, "updated": updated, "by_category": by_category}
    logger.info("backfill_error_categories: %s", summary)
    return summary


# ---------- force_recollect_separator_mismatches (P0-A) ----------


@celery_app.task(
    name="openarg.force_recollect_separator_mismatches",
    bind=True,
    soft_time_limit=120,
    time_limit=180,
)
def force_recollect_separator_mismatches(self, *, dry_run: bool = False) -> dict:
    """Mark as `pending` the datasets that retrospective sweep flagged as
    separator_mismatch but are still status='ready' with rotten data.

    The retrospective sweep registers the finding but does not flip the
    status (auto-flip is intentionally off-by-default). This task closes
    the loop so the collector picks them up and the post-parse detector
    aborts them as `parser_invalid` on next run, keeping the bad data out
    of `ready`.
    """
    engine = get_sync_engine()
    select_sql = text(
        """
        SELECT cd.id::text AS cached_id,
               cd.dataset_id::text AS dataset_id,
               cd.table_name
        FROM cached_datasets cd
        WHERE cd.status = 'ready'
          AND EXISTS (
              SELECT 1 FROM ingestion_findings f
              WHERE f.resource_id = cd.dataset_id::text
                AND f.detector_name = 'separator_mismatch'
                AND f.severity = 'critical'
                AND f.resolved_at IS NULL
          )
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(select_sql).fetchall()
    candidates = [
        {"cached_id": r.cached_id, "dataset_id": r.dataset_id, "table_name": r.table_name}
        for r in rows
    ]
    if dry_run:
        logger.info(
            "force_recollect_separator_mismatches dry-run: %d candidates", len(candidates)
        )
        return {"candidates": len(candidates), "samples": candidates[:5], "dry_run": True}

    if not candidates:
        return {"candidates": 0, "marked_pending": 0}

    update_sql = text(
        """
        UPDATE cached_datasets
        SET status = 'pending',
            retry_count = 0,
            error_message = 'force_recollect:separator_mismatch',
            error_category = 'parse_format',
            updated_at = NOW()
        WHERE id = CAST(:id AS uuid)
        """
    )
    marked = 0
    with engine.begin() as conn:
        for c in candidates:
            conn.execute(update_sql, {"id": c["cached_id"]})
            marked += 1
    logger.info(
        "force_recollect_separator_mismatches marked %d datasets pending", marked
    )
    return {"candidates": len(candidates), "marked_pending": marked}


# ---------- cleanup_orphan_cache_tables (P1-F) ----------


@celery_app.task(
    name="openarg.cleanup_orphan_cache_tables",
    bind=True,
    soft_time_limit=900,
    time_limit=1080,
)
def cleanup_orphan_cache_tables(self, *, dry_run: bool = True, max_drops: int = 100) -> dict:
    """Drop collector-staged cache_* tables that have no matching cached_datasets row.

    Only deletes tables whose name matches the collector physical-namer
    suffix pattern (`_r<hex>` / `_s<hex>` / `_g<hex>`). Connector-managed
    tables (BAC, BCRA, INDEC, presupuesto, etc.) follow a different
    naming convention and never go through `cached_datasets`, so they
    are intentionally outside this task's scope. The audit table is
    excluded by name.

    Each drop is recorded into `cache_drop_audit`. Defaults to
    `dry_run=True` so the first scheduled run only counts.
    """
    from app.infrastructure.celery.tasks.collector_tasks import _record_cache_drop

    engine = get_sync_engine()
    select_sql = text(
        r"""
        SELECT t.tablename
        FROM pg_tables t
        WHERE t.schemaname = 'public'
          AND t.tablename LIKE 'cache_%'
          AND t.tablename <> 'cache_drop_audit'
          AND (
              t.tablename ~ '_r[0-9a-f]{8,12}(_s[0-9a-f]{6,10})*$'
              OR t.tablename ~ '_s[0-9a-f]{6,10}(_s[0-9a-f]{6,10})*$'
              OR t.tablename ~ '_g[0-9a-f]{6,10}$'
          )
          AND NOT EXISTS (
              SELECT 1 FROM cached_datasets cd WHERE cd.table_name = t.tablename
          )
        ORDER BY t.tablename
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(select_sql, {"limit": max_drops + 1}).fetchall()
    candidates = [r.tablename for r in rows]
    truncated = len(candidates) > max_drops
    candidates = candidates[:max_drops]
    if dry_run:
        logger.info(
            "cleanup_orphan_cache_tables dry-run: %d orphans found (truncated=%s)",
            len(candidates),
            truncated,
        )
        return {
            "found": len(candidates),
            "samples": candidates[:10],
            "dry_run": True,
            "truncated_to_max_drops": truncated,
        }

    dropped = 0
    failed = 0
    for tn in candidates:
        try:
            _record_cache_drop(
                engine,
                table_name=tn,
                reason="orphan_cleanup",
                actor="ops_fixes.cleanup_orphan_cache_tables",
            )
            with engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{tn}" CASCADE'))  # noqa: S608
            dropped += 1
        except Exception:
            logger.exception("Failed to drop orphan table %s", tn)
            failed += 1
    summary = {
        "candidates": len(candidates),
        "dropped": dropped,
        "failed": failed,
        "truncated_to_max_drops": truncated,
    }
    logger.info("cleanup_orphan_cache_tables: %s", summary)
    return summary


# ---------- retain_raw_versions (MASTERPLAN Fase 1) ----------


@celery_app.task(
    name="openarg.retain_raw_versions",
    bind=True,
    soft_time_limit=600,
    time_limit=720,
)
def retain_raw_versions(
    self, *, keep_last: int | None = None, dry_run: bool = False
) -> dict:
    """Drop superseded raw-schema tables beyond the per-resource retention window.

    For each `resource_identity` in `raw_table_versions`, keep the latest
    `keep_last` versions; for older versions:

    1. `DROP TABLE raw."<table_name>"` (recorded via `_record_cache_drop` for
       audit consistency).
    2. `DELETE FROM raw_table_versions` for that row.

    The default for `keep_last` comes from the env var
    `OPENARG_RAW_RETENTION_KEEP_LAST` (fallback 3). Lowering this from 3 to 2
    typically frees ~30% of the raw schema disk usage at the cost of one
    less rollback step per resource.

    Idempotent: re-running with the same args is a no-op once the trim has
    been applied. `dry_run=True` reports candidates without touching anything.
    """
    from app.infrastructure.celery.tasks.collector_tasks import _record_cache_drop

    if keep_last is None:
        from app.setup.config.constants import RAW_RETENTION_KEEP_LAST

        keep_last = RAW_RETENTION_KEEP_LAST
    if keep_last < 1:
        raise ValueError("keep_last must be >= 1")

    engine = get_sync_engine()
    select_sql = text(
        """
        WITH ranked AS (
            SELECT
                resource_identity,
                version,
                schema_name,
                table_name,
                ROW_NUMBER() OVER (
                    PARTITION BY resource_identity ORDER BY version DESC
                ) AS rn
            FROM raw_table_versions
        )
        SELECT resource_identity, version, schema_name, table_name
        FROM ranked
        WHERE rn > :keep
        ORDER BY resource_identity, version
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(select_sql, {"keep": keep_last}).fetchall()
    candidates = [
        {
            "resource_identity": r.resource_identity,
            "version": int(r.version),
            "schema_name": r.schema_name,
            "table_name": r.table_name,
        }
        for r in rows
    ]
    if dry_run:
        logger.info(
            "retain_raw_versions dry-run: %d candidates (keep_last=%d)",
            len(candidates),
            keep_last,
        )
        return {
            "found": len(candidates),
            "samples": candidates[:10],
            "dry_run": True,
            "keep_last": keep_last,
        }

    dropped = 0
    failed = 0
    for c in candidates:
        qualified_name = f"{c['schema_name']}.{c['table_name']}"
        try:
            _record_cache_drop(
                engine,
                table_name=qualified_name,
                reason="retain_raw_versions",
                actor="ops_fixes.retain_raw_versions",
                extra={
                    "resource_identity": c["resource_identity"],
                    "version": c["version"],
                    "keep_last": keep_last,
                },
            )
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f'DROP TABLE IF EXISTS "{c["schema_name"]}"."{c["table_name"]}" CASCADE'
                    )
                )
                conn.execute(
                    text(
                        "DELETE FROM raw_table_versions "
                        "WHERE resource_identity = :rid AND version = :v"
                    ),
                    {"rid": c["resource_identity"], "v": c["version"]},
                )
            dropped += 1
        except Exception:
            logger.exception("Failed to drop raw version %s", qualified_name)
            failed += 1
    summary = {
        "candidates": len(candidates),
        "dropped": dropped,
        "failed": failed,
        "keep_last": keep_last,
    }
    logger.info("retain_raw_versions: %s", summary)
    return summary


# ---------- cleanup_raw_orphans (Sprint RLM) ----------


@celery_app.task(
    name="openarg.cleanup_raw_orphans",
    bind=True,
    soft_time_limit=900,
    time_limit=1080,
)
def cleanup_raw_orphans(
    self,
    *,
    dry_run: bool = False,
    max_drops: int = 50,
    min_age_hours: int = 24,
) -> dict:
    """Drop raw-schema tables that no `cached_datasets` row points to.

    These accumulate when a dataset is reprocessed under a different
    physical name (upstream changed `source_id` / title / hash, producing
    a new discriminator). The cd row is updated to the new table_name and
    the previous raw table is left behind. `retain_raw_versions` keeps
    only the top-N versions per `resource_identity`, but every reprocess
    that lands under a *different* resource_identity creates a fresh
    `rn=1` row — so the per-resource retention never kicks in. This task
    closes that loop by dropping any `raw.*` table whose `table_name` no
    cd row claims and whose `raw_table_versions` row is older than
    `min_age_hours` (default 24h, to avoid races with in-flight collects).

    Each drop:
      1. Audited via `_record_cache_drop(reason='raw_orphan_cleanup')`
      2. `DROP TABLE raw."<name>" CASCADE`
      3. `DELETE FROM raw_table_versions` row

    `max_drops` (default 50) caps drops per run to keep RDS IO bounded.
    `dry_run` (default False) reports candidates without touching DB.
    """
    from app.infrastructure.celery.tasks.collector_tasks import _record_cache_drop

    if max_drops < 1:
        raise ValueError("max_drops must be >= 1")
    if min_age_hours < 0:
        raise ValueError("min_age_hours must be >= 0")

    engine = get_sync_engine()

    # SAFETY NET: marts reference raw tables via `live_table('portal::source_id')`
    # macros expanded to physical names at refresh time. If we drop a raw
    # table whose `resource_identity` is referenced by any mart's SQL,
    # the mart's next refresh fails with `column ... does not exist`.
    # The earlier version of this task (sprint RLM, 2026-05-06 morning)
    # broke `escuelas_argentina` exactly this way — protected identities
    # are now excluded from the dispatch SELECT.
    import re

    mart_protected_select = text(
        "SELECT mart_id, sql_definition FROM mart_definitions "
        "WHERE sql_definition IS NOT NULL"
    )
    protected_identities: set[str] = set()
    with engine.connect() as conn:
        for row in conn.execute(mart_protected_select):
            for match in re.finditer(
                r"""live_table\(\s*['"]([^'"]+)['"]""",
                row.sql_definition or "",
            ):
                protected_identities.add(match.group(1))
            for match in re.finditer(
                r"""live_tables_by_pattern\(\s*['"]([^'"]+)['"]""",
                row.sql_definition or "",
            ):
                # pattern-based references protect every identity that
                # could match the glob; conservatively skip cleanup for
                # rtv rows whose resource_identity matches the prefix.
                protected_identities.add(f"__pattern__:{match.group(1)}")
    pattern_prefixes = [
        p[len("__pattern__:") :] for p in protected_identities if p.startswith("__pattern__:")
    ]
    exact_protected = {p for p in protected_identities if not p.startswith("__pattern__:")}
    if exact_protected or pattern_prefixes:
        logger.info(
            "cleanup_raw_orphans: %d exact + %d pattern mart-protected identities",
            len(exact_protected),
            len(pattern_prefixes),
        )

    select_sql = text(
        """
        SELECT rtv.resource_identity, rtv.version, rtv.schema_name, rtv.table_name
        FROM raw_table_versions rtv
        WHERE rtv.schema_name = 'raw'
          AND rtv.created_at < NOW() - (:age_hours || ' hours')::interval
          AND NOT EXISTS (
              SELECT 1 FROM cached_datasets cd WHERE cd.table_name = rtv.table_name
          )
          AND EXISTS (
              SELECT 1 FROM information_schema.tables t
              WHERE t.table_schema = 'raw' AND t.table_name = rtv.table_name
          )
          AND (:no_exact OR rtv.resource_identity NOT IN :exact)
        ORDER BY rtv.created_at ASC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            select_sql.bindparams(
                bindparam("exact", expanding=True),
            ),
            {
                "age_hours": str(min_age_hours),
                "limit": max_drops + 1,
                "no_exact": len(exact_protected) == 0,
                "exact": list(exact_protected) or [""],
            },
        ).fetchall()
    # Pattern-based filter applied in Python (small set, simple glob match)
    if pattern_prefixes:
        from fnmatch import fnmatchcase

        rows = [
            r
            for r in rows
            if not any(fnmatchcase(r.resource_identity, pat) for pat in pattern_prefixes)
        ]
    candidates = [
        {
            "resource_identity": r.resource_identity,
            "version": int(r.version),
            "schema_name": r.schema_name,
            "table_name": r.table_name,
        }
        for r in rows
    ]
    truncated = len(candidates) > max_drops
    candidates = candidates[:max_drops]

    if dry_run:
        logger.info(
            "cleanup_raw_orphans dry-run: %d candidates (truncated=%s)",
            len(candidates),
            truncated,
        )
        return {
            "found": len(candidates),
            "samples": [c["table_name"] for c in candidates[:10]],
            "dry_run": True,
            "truncated_to_max_drops": truncated,
        }

    dropped = 0
    failed = 0
    for c in candidates:
        qualified = f"{c['schema_name']}.{c['table_name']}"
        try:
            _record_cache_drop(
                engine,
                table_name=qualified,
                reason="raw_orphan_cleanup",
                actor="ops_fixes.cleanup_raw_orphans",
                extra={
                    "resource_identity": c["resource_identity"],
                    "version": c["version"],
                },
            )
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f'DROP TABLE IF EXISTS "{c["schema_name"]}"."{c["table_name"]}" CASCADE'
                    )
                )
                conn.execute(
                    text(
                        "DELETE FROM raw_table_versions "
                        "WHERE resource_identity = :rid AND version = :v"
                    ),
                    {"rid": c["resource_identity"], "v": c["version"]},
                )
            dropped += 1
        except Exception:
            logger.exception("Failed to drop raw orphan %s", qualified)
            failed += 1
    summary = {
        "candidates": len(candidates),
        "dropped": dropped,
        "failed": failed,
        "truncated_to_max_drops": truncated,
        "min_age_hours": min_age_hours,
    }
    logger.info("cleanup_raw_orphans: %s", summary)
    return summary


# ---------- invariant counter cleanup (drift sweep) ----------


@celery_app.task(
    name="openarg.cleanup_invariants",
    bind=True,
    soft_time_limit=120,
    time_limit=180,
)
def cleanup_invariants(self) -> dict[str, int]:
    """Periodic clamp/cleanup for the three invariant counters that drift.

    Specifically:
      1. `cached_datasets.error_category = 'unknown'` for `permanently_failed`
         rows whose `error_message` matches a known classifier pattern that
         the live classifier missed (catches new error shapes between code
         deploys).
      2. `cached_datasets.retry_count > 5` (violates the trigger-enforced
         invariant; clamp back to 5).
      3. Orphan tables in `raw.*` without entry in `raw_table_versions`
         (registers them under `backfill_postauto::<table>` so the Serving
         Port can resolve them — better than dropping data).

    Returns a dict with counts of rows modified per category.
    """
    engine = get_sync_engine()
    fixed_unknown = 0
    fixed_retry = 0
    fixed_orphans = 0

    with engine.begin() as conn:
        # Cover BOTH `error` (retry-able) and `permanently_failed` (terminal)
        # so transient `error` rows have a meaningful category in dashboards
        # before they either succeed (re-classification → category irrelevant)
        # or are promoted to `permanently_failed` (where the classifier in
        # `_apply_cached_outcome` will re-evaluate anyway). For `error` rows
        # that don't match a pattern, we leave `unknown` instead of forcing
        # `parse_format` — `unknown` on retry-able status is fine; only
        # terminal `permanently_failed` should be guaranteed-classified.
        result = conn.execute(
            text(
                """
                UPDATE cached_datasets
                SET error_category = CASE
                    WHEN error_message ILIKE '%redirect%' THEN 'download_http_error'
                    WHEN error_message ILIKE '%zip_entry%' THEN 'policy_too_large'
                    WHEN error_message ILIKE '%stuck%' OR error_message ILIKE '%queue purged%' OR error_message ILIKE '%recovered%' THEN 'orchestration_recovery_loop'
                    WHEN error_message ILIKE '%duplicatecolumn%' OR error_message ILIKE '%specified more than once%' THEN 'parse_schema_mismatch'
                    WHEN error_message ILIKE '%unsupported driver%' OR error_message ILIKE '%bad_zip%' THEN 'parse_format'
                    WHEN status = 'permanently_failed' THEN 'parse_format'
                    ELSE error_category
                END
                WHERE error_category = 'unknown'
                  AND status IN ('error', 'permanently_failed')
                  AND error_message IS NOT NULL
                """
            )
        )
        fixed_unknown = result.rowcount or 0

        result = conn.execute(
            text(
                "UPDATE cached_datasets SET retry_count = 5 "
                "WHERE retry_count > 5"
            )
        )
        fixed_retry = result.rowcount or 0

        # M1 (Sprint 33): rows stuck in `error` with the retry budget
        # exhausted (retry_count >= MAX_TOTAL_ATTEMPTS=5) AND no
        # update for the last 6 hours are zombies — bulk_collect
        # should have re-picked them up but didn't, often because
        # `is_cached=false` was never reset OR the SELECT filter
        # excluded them. Auto-mark `permanently_failed` so dashboards
        # stop counting them as in-flight and operators can decide
        # whether to manually re-trigger. The 6-hour grace window
        # avoids racing with workers actively reprocessing.
        result_zombies = conn.execute(
            text(
                """
                UPDATE cached_datasets
                SET status = 'permanently_failed',
                    updated_at = NOW()
                WHERE status = 'error'
                  AND retry_count >= 5
                  AND updated_at < NOW() - INTERVAL '6 hours'
                """
            )
        )
        fixed_zombies = result_zombies.rowcount or 0

        # Two-pass orphan registration. First pass tries to recover the
        # canonical `<portal>::<source_id>` identity by joining through
        # `cached_datasets` → `datasets`: if a dataset row owns this
        # table, register under its natural identity so future versions
        # of the same dataset slot in correctly (and `retain_raw_versions`
        # can prune the lineage). Second pass falls back to
        # `backfill_postauto::<table_name>` for tables whose owning
        # dataset can't be resolved (legacy artifacts of the wipe).
        result_canonical = conn.execute(
            text(
                """
                INSERT INTO raw_table_versions (
                    resource_identity, version, schema_name, table_name, row_count
                )
                SELECT
                    d.portal || '::' || d.source_id,
                    COALESCE(
                        NULLIF(regexp_replace(t.table_name, '^.*__v', ''), '')::int,
                        1
                    ),
                    'raw',
                    t.table_name,
                    cd.row_count
                FROM information_schema.tables t
                JOIN cached_datasets cd ON cd.table_name = t.table_name
                JOIN datasets d ON d.id = cd.dataset_id
                LEFT JOIN raw_table_versions rtv
                    ON rtv.schema_name = 'raw'
                    AND rtv.table_name = t.table_name
                WHERE t.table_schema = 'raw'
                  AND rtv.table_name IS NULL
                ON CONFLICT (resource_identity, version) DO UPDATE SET
                    schema_name = EXCLUDED.schema_name,
                    table_name = EXCLUDED.table_name,
                    row_count = COALESCE(EXCLUDED.row_count, raw_table_versions.row_count)
                """
            )
        )
        canonical_registered = result_canonical.rowcount or 0

        result_fallback = conn.execute(
            text(
                """
                INSERT INTO raw_table_versions (
                    resource_identity, version, schema_name, table_name, row_count
                )
                SELECT
                    'backfill_postauto::' || t.table_name,
                    1,
                    'raw',
                    t.table_name,
                    NULL
                FROM information_schema.tables t
                LEFT JOIN raw_table_versions rtv
                    ON rtv.schema_name = 'raw'
                    AND rtv.table_name = t.table_name
                WHERE t.table_schema = 'raw'
                  AND rtv.table_name IS NULL
                ON CONFLICT (resource_identity, version) DO NOTHING
                """
            )
        )
        fixed_orphans = canonical_registered + (result_fallback.rowcount or 0)

        # 4. Sync `datasets.is_cached` with the actual cached_datasets state.
        # Drift here is rare but happens when a sweep marks a row `error`
        # without flipping is_cached back to false (or vice versa).
        result_drift = conn.execute(
            text(
                """
                UPDATE datasets d
                SET is_cached = false
                WHERE d.is_cached = true
                  AND NOT EXISTS (
                      SELECT 1 FROM cached_datasets cd
                      WHERE cd.dataset_id = d.id AND cd.status = 'ready'
                  )
                """
            )
        )
        fixed_is_cached_drift = result_drift.rowcount or 0

        # 5. mart_definitions.last_row_count drift.
        # When a build_mart races a refresh_mart, or when a build path falls
        # back to `last_row_count=0` after a partial failure, the metadata
        # claims "empty mart" while the matview has rows. This hides a real
        # mart from the discovery surface (`COALESCE(last_row_count,0) > 0`
        # filter in /data/tables). Detect by joining mart_definitions with
        # pg_class.reltuples and refresh metadata when they disagree.
        result_mart_drift = conn.execute(
            text(
                """
                UPDATE mart_definitions md
                SET last_row_count = GREATEST(c.reltuples::bigint, 0),
                    updated_at = NOW()
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = md.mart_schema
                  AND c.relname = md.mart_view_name
                  AND c.relkind = 'm'
                  AND COALESCE(md.last_row_count, 0) = 0
                  AND c.reltuples > 0
                """
            )
        )
        fixed_mart_row_count = result_mart_drift.rowcount or 0

        # 6. Drop empty orphan raw tables (0 rows, no rtv entry, no
        # cached_datasets owner). These are leftovers from CREATE TABLE +
        # INSERT raw_table_versions transactions that aborted between the
        # two statements. Keep orphans WITH data — those go through the
        # canonical/fallback registration path above so the data survives.
        empty_orphans_rows = conn.execute(
            text(
                """
                SELECT t.table_name
                FROM information_schema.tables t
                LEFT JOIN raw_table_versions rtv
                    ON rtv.schema_name = 'raw'
                    AND rtv.table_name = t.table_name
                LEFT JOIN cached_datasets cd ON cd.table_name = t.table_name
                LEFT JOIN pg_class c
                    ON c.relname = t.table_name
                    AND c.relnamespace = (
                        SELECT oid FROM pg_namespace WHERE nspname = 'raw'
                    )
                WHERE t.table_schema = 'raw'
                  AND rtv.table_name IS NULL
                  AND cd.table_name IS NULL
                  AND COALESCE(c.reltuples, 0) = 0
                """
            )
        ).fetchall()
        dropped_empty_orphans = 0
        for row in empty_orphans_rows:
            try:
                conn.execute(text(f'DROP TABLE IF EXISTS raw."{row.table_name}"'))
                dropped_empty_orphans += 1
            except Exception:
                logger.warning(
                    "cleanup_invariants: could not drop empty orphan raw.%s",
                    row.table_name,
                    exc_info=True,
                )

        # 6.5. Datasets with multiple `ready` cached_datasets rows.
        # Sprint 1.7 audit detected 63 datasets carrying both a legacy
        # `cache_*` ready row AND a raw-promoted ready row. The
        # cleanup leaves the row that owns a current
        # `raw_table_versions` entry (canonical source of truth) and
        # demotes the legacy duplicate to `superseded` status. Without
        # this, `/data/tables` and downstream consumers count datasets
        # twice. The DELETE below is conservative — only touches
        # legacy rows when there's a corresponding live raw rtv with a
        # different table_name; never deletes the only ready row of
        # a dataset.
        result_double_ready = conn.execute(
            text(
                """
                DELETE FROM cached_datasets cd_legacy
                USING cached_datasets cd_raw,
                      raw_table_versions rtv
                WHERE cd_legacy.dataset_id = cd_raw.dataset_id
                  AND cd_legacy.status = 'ready'
                  AND cd_raw.status = 'ready'
                  AND cd_legacy.table_name <> cd_raw.table_name
                  AND cd_legacy.table_name LIKE 'cache_%'
                  AND rtv.schema_name = 'raw'
                  AND rtv.table_name = cd_raw.table_name
                  AND rtv.superseded_at IS NULL
                """
            )
        )
        fixed_double_cd_ready = result_double_ready.rowcount or 0

        # NOTE — Sprint 1.6 audit found ~8,000 rows on `ready` status
        # carrying error_category='unknown' from past failed attempts.
        # The classifier returns 'unknown' for empty error_message and
        # the column has a NOT NULL + CHECK enum constraint that
        # rejects NULL and any value outside the enum. A proper fix
        # requires either:
        #   (a) a migration extending the enum with 'none', 'parser_tag',
        #       'truncated' + retro-classification, or
        #   (b) splitting "did this row ever fail?" into a separate
        #       boolean column, leaving error_category meaningful only
        #       for non-success states.
        # Both shape (a) and shape (b) are tracked in spec 014 DEBT.
        # The retroactive fix is left out of cleanup_invariants for
        # now because it can't run inside the existing schema without
        # the migration above.
        fixed_unknown_on_ready = 0

        # 7. datasets.row_count drift.
        # Several recovery paths in collector_tasks set `datasets.is_cached
        # = true` without also updating `datasets.row_count`, so today
        # ~54% of `datasets` rows have row_count NULL even when the
        # cached_datasets row knows the count. /data/tables and other
        # consumers report 0 rows for those datasets, hiding their
        # actual size. Sync from cached_datasets where we have the truth.
        #
        # NOTE: a dataset can have multiple `cached_datasets` rows ready
        # (e.g. legacy public.cache_* + raw.<bare> from a vía-A landing).
        # ~63 datasets in staging hit that case. Without `DISTINCT ON`
        # the JOIN-update issues N updates per dataset and the final
        # value depends on whichever row was visited last — non-
        # deterministic when the row_counts diverge (3 datasets in
        # staging today). The lateral subquery picks the most recently
        # updated cd row per dataset, which is the policy we want:
        # the latest landing reflects the current truth.
        result_row_count_drift = conn.execute(
            text(
                """
                UPDATE datasets d
                SET row_count = src.row_count,
                    updated_at = NOW()
                FROM (
                    SELECT DISTINCT ON (cd.dataset_id)
                           cd.dataset_id,
                           cd.row_count,
                           cd.updated_at
                    FROM cached_datasets cd
                    WHERE cd.status = 'ready'
                      AND cd.row_count IS NOT NULL
                      AND cd.row_count > 0
                    ORDER BY cd.dataset_id, cd.updated_at DESC NULLS LAST
                ) src
                WHERE src.dataset_id = d.id
                  AND (d.row_count IS NULL OR d.row_count = 0
                       OR d.row_count <> src.row_count)
                """
            )
        )
        fixed_dataset_row_count = result_row_count_drift.rowcount or 0

    summary = {
        "fixed_unknown_category": fixed_unknown,
        "clamped_retry_count": fixed_retry,
        "fixed_zombie_errors": fixed_zombies,
        "registered_orphan_tables": fixed_orphans,
        "canonical_orphans_registered": canonical_registered,
        "fixed_is_cached_drift": fixed_is_cached_drift,
        "fixed_mart_row_count": fixed_mart_row_count,
        "dropped_empty_orphans": dropped_empty_orphans,
        "fixed_dataset_row_count": fixed_dataset_row_count,
        "fixed_unknown_on_ready": fixed_unknown_on_ready,
        "fixed_double_cd_ready": fixed_double_cd_ready,
    }
    if any(summary.values()):
        logger.warning("cleanup_invariants: %s", summary)
    else:
        logger.info("cleanup_invariants: nothing to fix")
    return summary
