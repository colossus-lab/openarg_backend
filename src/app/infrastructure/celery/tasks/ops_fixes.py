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
import shutil
import time
from collections.abc import Iterable
from urllib.parse import urlparse

import httpx
from sqlalchemy import bindparam, text

from app.application.pipeline.connectors.sandbox import (
    discover_catalog_hints_for_planner,
)
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


def _mart_dependency_guards(engine) -> tuple[set[str], list[str], set[str]]:
    """Return mart-protected identities/patterns/raw tables.

    `mart_definitions.sql_definition` stores rendered SQL, not the original
    YAML with macros. That means we need two protection modes:
      - macro-level guards for older rows that still store macro text
      - raw physical table guards extracted from the resolved SQL
      - raw physical table guards extracted from the ACTUAL materialized
        views currently installed in Postgres (`pg_matviews.definition`)

    The last source matters because `mart_definitions` can describe the
    *target* SQL after a YAML edit while the live matview still points at an
    older raw version if refresh/build hasn't happened yet or failed.
    """
    import re

    mart_definitions_select = text(
        "SELECT mart_id, sql_definition FROM mart_definitions WHERE sql_definition IS NOT NULL"
    )
    live_matviews_select = text("SELECT definition FROM pg_matviews WHERE schemaname = 'mart'")
    protected_identities: set[str] = set()
    protected_raw_tables: set[str] = set()

    def _result_rows(result) -> list:
        if result is None:
            return []
        fetchall = getattr(result, "fetchall", None)
        if callable(fetchall):
            return list(fetchall())
        try:
            return list(result)
        except TypeError:
            return []

    def _collect_from_sql(sql_definition: str) -> None:
        if not sql_definition:
            return
        for match in re.finditer(
            r"""live_table\(\s*['"]([^'"]+)['"]""",
            sql_definition,
        ):
            protected_identities.add(match.group(1))
        for match in re.finditer(
            r"""live_tables_by_pattern\(\s*['"]([^'"]+)['"]""",
            sql_definition,
        ):
            protected_identities.add(f"__pattern__:{match.group(1)}")
        for match in re.finditer(
            r"""raw\."([^"]+)"|raw\.([a-z_][a-z0-9_]*)""",
            sql_definition,
            re.IGNORECASE,
        ):
            protected_raw_tables.add(match.group(1) or match.group(2))

    try:
        with engine.connect() as conn:
            try:
                mart_rows = _result_rows(conn.execute(mart_definitions_select))
                for row in mart_rows:
                    _collect_from_sql(getattr(row, "sql_definition", "") or "")
            except Exception:
                logger.debug("Could not load mart_definitions dependency guards", exc_info=True)
            try:
                live_rows = _result_rows(conn.execute(live_matviews_select))
                for row in live_rows:
                    _collect_from_sql(getattr(row, "definition", "") or "")
            except Exception:
                logger.debug("Could not load pg_matviews dependency guards", exc_info=True)
    except Exception:
        logger.debug("Could not connect to load mart dependency guards", exc_info=True)
    pattern_prefixes = [
        p[len("__pattern__:") :] for p in protected_identities if p.startswith("__pattern__:")
    ]
    exact_protected = {p for p in protected_identities if not p.startswith("__pattern__:")}
    return exact_protected, pattern_prefixes, protected_raw_tables


# ---------- temp dir cleanup ----------


def _temp_dir() -> str:
    return os.getenv("OPENARG_TEMP_DIR") or "/tmp"


def _cleanup_threshold_seconds() -> int:
    try:
        return int(os.getenv("OPENARG_TEMP_CLEANUP_AGE_SECONDS", "3600"))
    except ValueError:
        return 3600


def _path_size_bytes(path: str) -> int:
    """Best-effort recursive size for audit logging.

    Symlinks are ignored so cleanup stays scoped to the temp tree itself.
    """
    try:
        if os.path.islink(path):
            return 0
        if os.path.isfile(path):
            return int(os.path.getsize(path))
        total = 0
        for root, dirs, files in os.walk(path, topdown=True, followlinks=False):
            dirs[:] = [d for d in dirs if not os.path.islink(os.path.join(root, d))]
            for name in files:
                fp = os.path.join(root, name)
                if os.path.islink(fp):
                    continue
                try:
                    total += int(os.path.getsize(fp))
                except OSError:
                    continue
        return total
    except OSError:
        return 0


def _remove_stale_tmp_path(path: str, *, base: str) -> tuple[bool, int]:
    """Remove one stale tmp path safely.

    Only removes paths that still resolve under `base`. Directories are
    deleted recursively because crash leftovers are typically non-empty
    extraction trees.
    """
    try:
        real_base = os.path.realpath(base)
        real_path = os.path.realpath(path)
        if os.path.commonpath([real_base, real_path]) != real_base:
            return False, 0
    except Exception:
        return False, 0

    size_bytes = _path_size_bytes(path)
    try:
        if os.path.islink(path) or os.path.isfile(path):
            os.unlink(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            return False, 0
        return True, size_bytes
    except Exception:
        logger.debug("Could not remove %s", path, exc_info=True)
        return False, 0


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
        deleted, reclaimed = _remove_stale_tmp_path(path, base=base)
        if deleted:
            removed += 1
            bytes_freed += reclaimed
        else:
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
        FROM raw.cached_datasets
        WHERE error_category = 'unknown'
          AND status IN ('error', 'permanently_failed')
        ORDER BY updated_at DESC NULLS LAST
        LIMIT :limit
        """
    )
    update_sql = text(
        """
        UPDATE raw.cached_datasets
        SET error_category = :cat
        WHERE id = CAST(:id AS uuid)
          AND error_category = 'unknown'
        """
    )
    seen = 0
    updated = 0
    by_category: dict[str, int] = {}
    while True:
        with engine.connect() as conn:
            rows = conn.execute(select_sql, {"limit": batch_size}).fetchall()
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
        FROM raw.cached_datasets cd
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
        logger.info("force_recollect_separator_mismatches dry-run: %d candidates", len(candidates))
        return {"candidates": len(candidates), "samples": candidates[:5], "dry_run": True}

    if not candidates:
        return {"candidates": 0, "marked_pending": 0}

    update_sql = text(
        """
        UPDATE raw.cached_datasets
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
    logger.info("force_recollect_separator_mismatches marked %d datasets pending", marked)
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
              SELECT 1 FROM raw.cached_datasets cd WHERE cd.table_name = t.tablename
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
    self,
    *,
    keep_last: int | None = None,
    soak_days: int | None = None,
    dry_run: bool = False,
) -> dict:
    """Drop superseded raw-schema tables beyond the per-resource retention window.

    For each `resource_identity` in `raw_table_versions`, keep the latest
    `keep_last` versions; for older versions:

    1. `DROP TABLE raw."<table_name>"` (recorded via `_record_cache_drop` for
       audit consistency).
    2. `DELETE FROM raw_table_versions` for that row.

    Soak window (DEBT-017-002): a candidate is only eligible to be dropped
    when its `superseded_at` is older than `NOW() - soak_days` (default
    `OPENARG_RAW_RETENTION_SOAK_DAYS=7`). Rows whose `superseded_at IS NULL`
    are also eligible — this preserves the previous behaviour for any rows
    that pre-date the soak guarantee, so they don't accumulate forever.

    The default for `keep_last` comes from the env var
    `OPENARG_RAW_RETENTION_KEEP_LAST` (default 2). That keeps one rollback
    step per resource while materially reducing raw-schema growth versus 3.

    Idempotent: re-running with the same args is a no-op once the trim has
    been applied. `dry_run=True` reports candidates without touching anything.
    """
    from app.infrastructure.celery.tasks.collector_tasks import _record_cache_drop

    if keep_last is None:
        from app.setup.config.constants import RAW_RETENTION_KEEP_LAST

        keep_last = RAW_RETENTION_KEEP_LAST
    if keep_last < 1:
        raise ValueError("keep_last must be >= 1")

    if soak_days is None:
        from app.setup.config.constants import RAW_RETENTION_SOAK_DAYS

        soak_days = RAW_RETENTION_SOAK_DAYS
    if soak_days < 0:
        raise ValueError("soak_days must be >= 0")

    engine = get_sync_engine()
    _exact_protected, _pattern_prefixes, protected_raw_tables = _mart_dependency_guards(engine)
    select_sql = text(
        """
        WITH ranked AS (
            SELECT
                resource_identity,
                version,
                schema_name,
                table_name,
                superseded_at,
                ROW_NUMBER() OVER (
                    PARTITION BY resource_identity ORDER BY version DESC
                ) AS rn
            FROM raw_table_versions
        )
        SELECT resource_identity, version, schema_name, table_name
        FROM ranked
        WHERE rn > :keep
          AND (:no_tables OR table_name NOT IN :protected_tables)
          AND (
              superseded_at IS NULL
              OR superseded_at < NOW() - make_interval(days => :soak_days)
          )
        ORDER BY resource_identity, version
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            select_sql.bindparams(bindparam("protected_tables", expanding=True)),
            {
                "keep": keep_last,
                "soak_days": soak_days,
                "no_tables": len(protected_raw_tables) == 0,
                "protected_tables": list(protected_raw_tables) or [""],
            },
        ).fetchall()
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
            "retain_raw_versions dry-run: %d candidates (keep_last=%d, soak_days=%d)",
            len(candidates),
            keep_last,
            soak_days,
        )
        return {
            "found": len(candidates),
            "samples": candidates[:10],
            "dry_run": True,
            "keep_last": keep_last,
            "soak_days": soak_days,
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
                    text(f'DROP TABLE IF EXISTS "{c["schema_name"]}"."{c["table_name"]}" CASCADE')
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
        "soak_days": soak_days,
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
    exact_protected, pattern_prefixes, protected_raw_tables = _mart_dependency_guards(engine)
    if exact_protected or pattern_prefixes or protected_raw_tables:
        logger.info(
            "cleanup_raw_orphans: %d exact + %d pattern mart-protected identities, %d raw tables",
            len(exact_protected),
            len(pattern_prefixes),
            len(protected_raw_tables),
        )

    select_sql = text(
        """
        SELECT rtv.resource_identity, rtv.version, rtv.schema_name, rtv.table_name
        FROM raw_table_versions rtv
        WHERE rtv.schema_name = 'raw'
          AND rtv.created_at < NOW() - (:age_hours || ' hours')::interval
          AND NOT EXISTS (
              SELECT 1 FROM raw.cached_datasets cd WHERE cd.table_name = rtv.table_name
          )
          AND EXISTS (
              SELECT 1 FROM information_schema.tables t
              WHERE t.table_schema = 'raw' AND t.table_name = rtv.table_name
          )
          AND (:no_exact OR rtv.resource_identity NOT IN :exact)
          AND (:no_tables OR rtv.table_name NOT IN :protected_tables)
        ORDER BY rtv.created_at ASC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            select_sql.bindparams(
                bindparam("exact", expanding=True),
                bindparam("protected_tables", expanding=True),
            ),
            {
                "age_hours": str(min_age_hours),
                "limit": max_drops + 1,
                "no_exact": len(exact_protected) == 0,
                "exact": list(exact_protected) or [""],
                "no_tables": len(protected_raw_tables) == 0,
                "protected_tables": list(protected_raw_tables) or [""],
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
                    text(f'DROP TABLE IF EXISTS "{c["schema_name"]}"."{c["table_name"]}" CASCADE')
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
    from app.infrastructure.celery.tasks.collector_tasks import _record_cache_drop

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
                UPDATE raw.cached_datasets
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
            text("UPDATE raw.cached_datasets SET retry_count = 5 WHERE retry_count > 5")
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
                UPDATE raw.cached_datasets
                SET status = 'permanently_failed',
                    updated_at = NOW()
                WHERE status = 'error'
                  AND retry_count >= 5
                  AND updated_at < NOW() - INTERVAL '6 hours'
                """
            )
        )
        fixed_zombies = result_zombies.rowcount or 0

        # M2 (Sprint 38): rows stuck in `error` with retry_count=0 and a
        # populated error_message older than 24h. These never came through
        # the `_apply_cached_outcome` path (which always increments
        # retry_count) — typically the result of ad-hoc operator UPDATEs
        # that re-labelled error_message without touching retry_count.
        # M1's `retry_count >= 5` filter leaves them invisible, so they
        # accumulate. The 24h grace window lets the natural recycle paths
        # (downloading sweep, reset_failed_collectors) get first crack.
        result_zero_retry_zombies = conn.execute(
            text(
                """
                UPDATE raw.cached_datasets
                SET status = 'permanently_failed',
                    updated_at = NOW()
                WHERE status = 'error'
                  AND retry_count = 0
                  AND error_message IS NOT NULL
                  AND updated_at < NOW() - INTERVAL '24 hours'
                """
            )
        )
        fixed_zero_retry_zombies = result_zero_retry_zombies.rowcount or 0

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
                    -- `substring(... from '__v([0-9]+)$')` returns NULL when the
                    -- name carries no version suffix, which is what the COALESCE
                    -- needs. `regexp_replace` did not: with no match it returns
                    -- the *whole* name unchanged, so NULLIF never fired and the
                    -- `::int` raised InvalidTextRepresentation on the first
                    -- unversioned table it met. Since the whole task runs in one
                    -- `engine.begin()`, that aborted all six invariant repairs —
                    -- measured 2026-08-01 on prod: 6395 of 26862 `raw` tables
                    -- have no `__vN` suffix (the legacy `cache_*_r<hex>` shape),
                    -- so the task had been failing every hour since 2026-05-05.
                    COALESCE(
                        NULLIF(substring(t.table_name from '__v([0-9]+)$'), '')::int,
                        1
                    ),
                    'raw',
                    t.table_name,
                    cd.row_count
                FROM information_schema.tables t
                JOIN raw.cached_datasets cd ON cd.table_name = t.table_name
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
                      SELECT 1 FROM raw.cached_datasets cd
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
                LEFT JOIN raw.cached_datasets cd ON cd.table_name = t.table_name
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
                # This drop used to leave no trace at all: it is the one
                # `DROP TABLE` in the codebase that never called
                # `_record_cache_drop`, so `cache_drop_audit` was silently
                # incomplete and nobody could tell "nothing was dropped"
                # from "something was dropped unaudited".
                _record_cache_drop(
                    engine,
                    table_name=f"raw.{row.table_name}",
                    reason="empty_orphan_invariant",
                    actor="ops_fixes.cleanup_invariants",
                )
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
                DELETE FROM raw.cached_datasets cd_legacy
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
                    FROM raw.cached_datasets cd
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
        "fixed_zero_retry_zombies": fixed_zero_retry_zombies,
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


# ---------- cleanup_empty_raw_tables (Sprint Disk Bloat 2026-05-09) ----------


@celery_app.task(
    name="openarg.cleanup_empty_raw_tables",
    bind=True,
    soft_time_limit=900,
    time_limit=1080,
)
def cleanup_empty_raw_tables(
    self,
    *,
    dry_run: bool = True,
    max_drops: int = 50,
    min_age_hours: int = 24,
    min_size_mb: int = 100,
) -> dict:
    """Drop large raw tables that landed with zero rows and were left behind.

    Failed re-collects sometimes leave a fresh `raw.<...>__vN` table that
    received hundreds of `ALTER TABLE ADD COLUMN` calls (each producing
    page bloat) but no INSERT. The version row in `raw_table_versions`
    has `row_count=0`, the table sits at >100MB on disk, and macros never
    pick it up because newer versions superseded it. `cleanup_raw_orphans`
    skips them because the row IS in the registry.

    Selection criteria (all must hold):
      - schema_name = 'raw'
      - row_count = 0 (or NULL — never updated post-create)
      - created_at < NOW() - `min_age_hours` (default 24h)
      - pg_total_relation_size > `min_size_mb` MB (default 100MB)
      - a newer version (`version` > current AND row_count > 0) exists,
        OR `superseded_at` is already set (would be dropped by retention
        anyway but disk-pressure version)

    Each drop:
      1. `_record_cache_drop(reason='empty_raw_bloat')`
      2. `DROP TABLE raw."<n>" CASCADE`
      3. `DELETE FROM raw_table_versions` row

    `dry_run` (default True) reports candidates without touching DB.
    """
    from app.infrastructure.celery.tasks.collector_tasks import _record_cache_drop

    if max_drops < 1:
        raise ValueError("max_drops must be >= 1")
    if min_age_hours < 0:
        raise ValueError("min_age_hours must be >= 0")
    if min_size_mb < 1:
        raise ValueError("min_size_mb must be >= 1")

    engine = get_sync_engine()

    select_sql = text(
        """
        WITH live_versions AS (
            SELECT resource_identity, MAX(version) AS max_version
            FROM raw_table_versions
            WHERE schema_name = 'raw'
              AND COALESCE(row_count, 0) > 0
            GROUP BY resource_identity
        )
        SELECT rtv.resource_identity,
               rtv.version,
               rtv.schema_name,
               rtv.table_name,
               rtv.superseded_at IS NOT NULL AS already_superseded,
               pg_total_relation_size(format('%I.%I', rtv.schema_name, rtv.table_name)::regclass) AS bytes
        FROM raw_table_versions rtv
        LEFT JOIN live_versions lv USING (resource_identity)
        WHERE rtv.schema_name = 'raw'
          AND COALESCE(rtv.row_count, 0) = 0
          AND rtv.created_at < NOW() - (:age_hours || ' hours')::interval
          AND EXISTS (
              SELECT 1 FROM information_schema.tables t
              WHERE t.table_schema = 'raw' AND t.table_name = rtv.table_name
          )
          AND (
              lv.max_version IS NOT NULL AND rtv.version < lv.max_version
              OR rtv.superseded_at IS NOT NULL
          )
          AND pg_total_relation_size(format('%I.%I', rtv.schema_name, rtv.table_name)::regclass) > :min_bytes
        ORDER BY pg_total_relation_size(format('%I.%I', rtv.schema_name, rtv.table_name)::regclass) DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            select_sql,
            {
                "age_hours": str(min_age_hours),
                "min_bytes": min_size_mb * 1024 * 1024,
                "limit": max_drops + 1,
            },
        ).fetchall()
    candidates = [
        {
            "resource_identity": r.resource_identity,
            "version": int(r.version),
            "schema_name": r.schema_name,
            "table_name": r.table_name,
            "bytes": int(r.bytes),
            "already_superseded": bool(r.already_superseded),
        }
        for r in rows
    ]
    truncated = len(candidates) > max_drops
    candidates = candidates[:max_drops]
    total_bytes = sum(c["bytes"] for c in candidates)

    if dry_run:
        logger.info(
            "cleanup_empty_raw_tables dry-run: %d candidates, %d MB to free (truncated=%s)",
            len(candidates),
            total_bytes // (1024 * 1024),
            truncated,
        )
        return {
            "found": len(candidates),
            "bytes_to_free": total_bytes,
            "samples": [
                {"table": c["table_name"], "mb": c["bytes"] // (1024 * 1024)}
                for c in candidates[:10]
            ],
            "dry_run": True,
            "truncated_to_max_drops": truncated,
        }

    dropped = 0
    failed = 0
    bytes_freed = 0
    for c in candidates:
        qualified = f"{c['schema_name']}.{c['table_name']}"
        try:
            _record_cache_drop(
                engine,
                table_name=qualified,
                reason="empty_raw_bloat",
                actor="ops_fixes.cleanup_empty_raw_tables",
                extra={
                    "resource_identity": c["resource_identity"],
                    "version": c["version"],
                    "bytes": c["bytes"],
                    "already_superseded": c["already_superseded"],
                },
            )
            with engine.begin() as conn:
                conn.execute(
                    text(f'DROP TABLE IF EXISTS "{c["schema_name"]}"."{c["table_name"]}" CASCADE')
                )
                conn.execute(
                    text(
                        "DELETE FROM raw_table_versions "
                        "WHERE resource_identity = :rid AND version = :v"
                    ),
                    {"rid": c["resource_identity"], "v": c["version"]},
                )
            dropped += 1
            bytes_freed += c["bytes"]
        except Exception:
            logger.exception("Failed to drop empty raw table %s", qualified)
            failed += 1
    summary = {
        "candidates": len(candidates),
        "dropped": dropped,
        "failed": failed,
        "bytes_freed": bytes_freed,
        "mb_freed": bytes_freed // (1024 * 1024),
        "truncated_to_max_drops": truncated,
        "min_age_hours": min_age_hours,
        "min_size_mb": min_size_mb,
    }
    logger.info("cleanup_empty_raw_tables: %s", summary)
    return summary


# ---------- cleanup_garbage_cols_in_raw (Schema cleanup, 2026-05-10) ----------


@celery_app.task(
    name="openarg.cleanup_garbage_cols_in_raw",
    bind=True,
    soft_time_limit=2400,
    time_limit=2700,
)
def cleanup_garbage_cols_in_raw(
    self,
    *,
    dry_run: bool = False,
    sample_size: int = 5000,
    max_populated_ratio: float = 0.01,
):
    """Drop garbage cols left behind by parser fallbacks in `raw.*` tables.

    Two cleanup passes (idempotent — re-runs are no-ops on clean schemas):

    1. **UUID-shaped col names**: when the source dataset's metadata UUID
       leaks into the column header (parser bug, ~1.4k tables observed).
       Always safe to drop — the col never carries useful data.

    2. **`col_N` / `Unnamed:N` cols that are ≥99% empty**: trailing
       garbage cols pandas creates when the source CSV had stray commas
       past the last real col. Drop only when populated ratio over
       `sample_size` rows is at most `max_populated_ratio`.

    Both pass results audited via structured log (see
    `parse_repair_audit` for the per-(table,col) trail when the helper
    `_audit` from `parse_repair` runs them — this task uses lighter
    inline logging since it operates at scale).

    Runs Sunday 02:30 ART (overlaps the row_count reconcile, both bounded).
    """
    import re

    from sqlalchemy import text

    from app.infrastructure.celery.tasks._db import get_sync_engine

    engine = get_sync_engine()
    stats = {
        "uuid_dropped": 0,
        "uuid_tables_touched": 0,
        "empty_garbage_dropped": 0,
        "empty_garbage_kept": 0,
        "errors": 0,
        "dry_run": dry_run,
    }

    UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

    # --- Pass 1: UUID col drop ---
    try:
        with engine.connect() as conn:
            uuid_cols = conn.execute(
                text(
                    """
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema='raw'
                      AND column_name ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    """
                )
            ).fetchall()
        seen_tables: set[str] = set()
        for tbl, col in uuid_cols:
            if not UUID_RE.match(col):
                continue
            if dry_run:
                stats["uuid_dropped"] += 1
                seen_tables.add(tbl)
                continue
            try:
                with engine.begin() as conn:
                    conn.execute(text(f'ALTER TABLE raw."{tbl}" DROP COLUMN "{col}"'))
                stats["uuid_dropped"] += 1
                seen_tables.add(tbl)
            except Exception:
                stats["errors"] += 1
        stats["uuid_tables_touched"] = len(seen_tables)
    except Exception:
        stats["errors"] += 1
        logger.exception("cleanup_garbage_cols_in_raw: pass 1 (UUID) failed")

    # --- Pass 2: col_N / Unnamed empty drop ---
    try:
        with engine.connect() as conn:
            candidates = conn.execute(
                text(
                    """
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema='raw'
                      AND (column_name ~ '^col_[0-9]+$'
                           OR column_name ILIKE 'unnamed:%')
                    """
                )
            ).fetchall()
        for tbl, col in candidates:
            try:
                # Sample populated count (excluding string-NaN sentinels)
                with engine.connect() as conn:
                    res = conn.execute(
                        text(
                            f"SELECT count(*), "
                            f'SUM(CASE WHEN "{col}" IS NOT NULL '
                            f"AND LOWER(COALESCE(TRIM(\"{col}\"::text), '')) "
                            f"NOT IN ('', 'none', 'nan', 'null', 'n/a', 'na', '<na>', "
                            f"'-', '--', 's/d', 's.d.', '.') "
                            f"THEN 1 ELSE 0 END) FROM "
                            f'(SELECT "{col}" FROM raw."{tbl}" LIMIT :n) sub'
                        ),
                        {"n": sample_size},
                    ).fetchone()
                total_rows = (res[0] if res else 0) or 0
                populated = (res[1] if res else 0) or 0
                if total_rows == 0:
                    continue
                if populated / total_rows > max_populated_ratio:
                    stats["empty_garbage_kept"] += 1
                    continue
                if dry_run:
                    stats["empty_garbage_dropped"] += 1
                    continue
                with engine.begin() as conn:
                    conn.execute(text(f'ALTER TABLE raw."{tbl}" DROP COLUMN "{col}"'))
                stats["empty_garbage_dropped"] += 1
            except Exception:
                stats["errors"] += 1
    except Exception:
        stats["errors"] += 1
        logger.exception("cleanup_garbage_cols_in_raw: pass 2 (empty garbage) failed")

    logger.info("cleanup_garbage_cols_in_raw: %s", stats)
    return stats


# ---------- prewarm_query_plan_cache (Latency optimization, 2026-05-10) ----------


async def _warm_query_plan_candidate(
    *,
    question: str,
    engine,
    embedder,
    llm,
    sandbox,
    serving_port,
    ttl_seconds: int = 604800,
) -> str:
    """Warm one plan-cache candidate using the same planner inputs as runtime."""
    import hashlib
    import json
    from dataclasses import asdict, is_dataclass

    emb = await embedder.embed(question)
    emb_str = "[" + ",".join(str(x) for x in emb) + "]"
    qhash = hashlib.sha256(question.encode("utf-8")).hexdigest()
    with engine.connect() as conn:
        hit = conn.execute(
            text(
                "SELECT 1 - (embedding <=> CAST(:e AS vector)) AS sim "
                "FROM query_plan_cache "
                "WHERE embedding IS NOT NULL AND expires_at > now() "
                "ORDER BY embedding <=> CAST(:e AS vector) "
                "LIMIT 1"
            ),
            {"e": emb_str},
        ).fetchone()
        conn.rollback()
    if hit and float(hit[0] or 0) >= 0.95:
        return "hit"

    catalog_hints = await discover_catalog_hints_for_planner(
        question,
        sandbox=sandbox,
        embedding=embedder,
        serving_port=serving_port,
        llm=llm,
        precomputed_embedding=emb,
    )
    from app.infrastructure.adapters.connectors.query_planner import generate_plan

    plan = await generate_plan(
        llm,
        question,
        memory_context="",
        catalog_hints=catalog_hints,
        skip_classifier=False,
    )
    if plan is None or plan.intent == "clarification":
        return "error"
    if is_dataclass(plan) and not isinstance(plan, type):
        plan_dict = asdict(plan)
    elif hasattr(plan, "model_dump"):
        plan_dict = plan.model_dump()
    else:
        plan_dict = dict(plan.__dict__)
    plan_json = json.dumps(plan_dict, default=str)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO query_plan_cache "
                "(question_hash, question, embedding, plan_json, "
                " ttl_seconds, expires_at) "
                "VALUES (:h, :q, CAST(:e AS vector), CAST(:p AS jsonb), "
                "        :ttl, now() + (:ttl || ' seconds')::interval) "
                "ON CONFLICT (question_hash) DO UPDATE SET "
                "  plan_json = EXCLUDED.plan_json, "
                "  embedding = EXCLUDED.embedding, "
                "  expires_at = EXCLUDED.expires_at"
            ),
            {
                "h": qhash,
                "q": question,
                "e": emb_str,
                "p": plan_json,
                "ttl": ttl_seconds,
            },
        )
    return "warmed"


@celery_app.task(
    name="openarg.prewarm_query_plan_cache",
    bind=True,
    soft_time_limit=1800,
    time_limit=2100,
)
def prewarm_query_plan_cache(
    self,
    *,
    max_queries: int = 100,
):
    """Pre-populate `query_plan_cache` with plans for common queries so
    real user requests hit the cache and skip the planner LLM (~3-4s).

    Strategy:
      1. Source candidate queries from:
         a. `query_analytics` last 30d by frequency (real user signal)
         b. `mart_sample_queries` (curated authors' samples) — fallback
            when analytics is sparse.
      2. For each candidate, embed + check `query_plan_cache` for a hit
         at threshold 0.95. If a hit exists, skip (already warm).
      3. On miss, build the SAME planner inputs used online
         (`catalog_hints`, classifier behaviour) and store the result.

    Designed to run weekly Sunday 02:45 ART (between row_count reconcile
    at 02:00 and garbage-col cleanup at 02:30 — plan cache warmup is
    cheap so it slots between).
    """
    import asyncio

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

    from app.infrastructure.adapters.llm.bedrock_embedding_adapter import (
        BedrockEmbeddingAdapter,
    )
    from app.infrastructure.adapters.llm.bedrock_llm_adapter import (
        BedrockLLMAdapter,
    )
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter
    from app.infrastructure.adapters.serving.legacy_serving_adapter import (
        LegacyServingAdapter,
    )
    from app.infrastructure.celery.tasks._db import get_sync_engine

    engine = get_sync_engine()
    stats = {
        "candidates": 0,
        "already_cached": 0,
        "warmed": 0,
        "errors": 0,
    }

    # 1. Source candidates
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT question, count(*) AS n
                FROM query_analytics
                WHERE ts > now() - INTERVAL '30 days'
                  AND question IS NOT NULL
                GROUP BY question
                ORDER BY n DESC, max(ts) DESC
                LIMIT :n
                """
            ),
            {"n": max_queries},
        ).fetchall()
    candidates: list[str] = [r[0] for r in rows]
    if len(candidates) < max_queries:
        # Fall back to curated sample queries
        with engine.connect() as conn:
            extra_rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT sample_text
                    FROM mart_sample_queries
                    WHERE sample_text IS NOT NULL
                    LIMIT :n
                    """
                ),
                {"n": max_queries - len(candidates)},
            ).fetchall()
        seen = {q.lower() for q in candidates}
        for r in extra_rows:
            if r[0] and r[0].lower() not in seen:
                candidates.append(r[0])

    stats["candidates"] = len(candidates)

    # 2-3. For each candidate, embed + check cache + warm on miss
    embedder = BedrockEmbeddingAdapter()
    llm = BedrockLLMAdapter()

    async_engine: AsyncEngine | None = None
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url:
        try:
            async_engine = create_async_engine(
                db_url,
                pool_size=1,
                max_overflow=0,
                pool_pre_ping=True,
            )
        except Exception:
            logger.warning(
                "Could not initialize async engine for plan-cache prewarm serving discovery",
                exc_info=True,
            )
            async_engine = None

    sandbox = PgSandboxAdapter()
    serving_port = LegacyServingAdapter(async_engine) if async_engine is not None else None

    async def _run() -> None:
        # Concurrency 4 to avoid hammering Bedrock with 100 calls at once
        sem = asyncio.Semaphore(4)

        async def _bound(q: str) -> str:
            async with sem:
                try:
                    return await _warm_query_plan_candidate(
                        question=q,
                        engine=engine,
                        embedder=embedder,
                        llm=llm,
                        sandbox=sandbox,
                        serving_port=serving_port,
                    )
                except Exception:
                    logger.exception("prewarm_query_plan_cache failed for %s", q[:80])
                    return "error"

        results = await asyncio.gather(*[_bound(q) for q in candidates], return_exceptions=False)
        for r in results:
            if r == "hit":
                stats["already_cached"] += 1
            elif r == "warmed":
                stats["warmed"] += 1
            else:
                stats["errors"] += 1

    try:
        asyncio.run(_run())
    finally:
        if async_engine is not None:
            try:
                asyncio.run(async_engine.dispose())
            except Exception:
                logger.debug("Could not dispose async prewarm engine", exc_info=True)
    logger.info("prewarm_query_plan_cache: %s", stats)
    return stats
