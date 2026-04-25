"""Destructive staging reset — wipe legacy state and rebuild via the new model.

WHY THIS EXISTS
---------------
After WS0/WS0.5/WS2/WS4 land, staging accumulated 12k cached_datasets, 8k
table_catalog rows and 10k cache_* tables built with the old code path.
None of those went through the WS0 detectors, deterministic naming or
hierarchical parser. Rather than retrofit, we wipe staging and let the new
pipeline rebuild from scratch — the "reset reproducible" of the plan.

WHAT IT DROPS
-------------
  * every public.cache_* table (the materialized projections)
  * cached_datasets (operational state)
  * table_catalog (legacy semantic index)
  * dataset_chunks (will be regenerated on re-index)
  * catalog_resources (so the backfill repopulates with the new code)
  * ingestion_findings (no point keeping pre-reset findings)
  * cache_drop_audit (will fill again on the new run)

WHAT IT DOES NOT TOUCH
----------------------
  * datasets — the scrape inventory; truncating it would force the scraper
    to re-discover everything from portals. Not done by default. Pass
    `--reset-datasets` to also wipe it.
  * any table that doesn't match `cache_*` or one of the hard-coded
    operational tables above (so user_queries, messages, api_keys, etc.
    stay intact).
  * production. Refuses to run unless `APP_ENV=staging` *and* the explicit
    `--i-understand-this-deletes-data` flag is passed.

USAGE
-----
  APP_ENV=staging python -m scripts.staging_reset --dry-run
  APP_ENV=staging python -m scripts.staging_reset --i-understand-this-deletes-data
  APP_ENV=staging python -m scripts.staging_reset --i-understand-this-deletes-data \\
      --reset-datasets    # also wipe `datasets`, force full re-scrape
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# Hard guard — these table names are wiped unconditionally. cache_* is handled
# dynamically (the dropper enumerates information_schema).
_TRUNCATE_BASE = (
    "ingestion_findings",
    "catalog_resources",
    "cached_datasets",
    "table_catalog",
    "dataset_chunks",
)
_TRUNCATE_OPTIONAL = (
    "cache_drop_audit",
    "portals",
)
_TRUNCATE_WITH_DATASETS = ("datasets",)


def _is_staging() -> bool:
    env = os.getenv("APP_ENV", "").strip().lower()
    return env in {"staging", "stage"}


def _refuse_in_prod() -> None:
    env = os.getenv("APP_ENV", "").strip().lower()
    if env in {"prod", "production"}:
        raise SystemExit(
            "REFUSED — APP_ENV=prod. This script is destructive and only "
            "runs in staging. Set APP_ENV=staging if you really mean it."
        )
    if not _is_staging():
        raise SystemExit(
            "REFUSED — APP_ENV is not 'staging'. Set APP_ENV=staging "
            "explicitly if you really intend to run this."
        )


@dataclass
class ResetSummary:
    cache_tables_dropped: int
    base_tables_truncated: list[str]
    optional_tables_truncated: list[str]
    datasets_truncated: bool
    dry_run: bool


def _list_cache_tables(engine: Engine) -> list[str]:
    sql = text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' "
        "  AND table_name LIKE 'cache\\_%' ESCAPE '\\' "
        "ORDER BY table_name"
    )
    with engine.connect() as conn:
        return [r.table_name for r in conn.execute(sql).fetchall()]


def _table_exists(engine: Engine, name: str) -> bool:
    with engine.connect() as conn:
        res = conn.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = :n"
            ),
            {"n": name},
        )
        return res.first() is not None


_SAFE_NAME_RE = __import__("re").compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _drop_cache_tables(engine: Engine, tables: list[str], *, dry_run: bool) -> int:
    """Drop tables one statement per `engine.begin()` block.

    Multi-statement strings (`DROP …; DROP …`) executed via SQLAlchemy +
    psycopg are fragile (driver-version dependent), so we keep it boring
    and predictable: one statement per call, batched into transactions
    of 50 to balance throughput vs. lock duration.
    """
    if dry_run or not tables:
        return len(tables)
    dropped = 0
    batch_size = 50
    for i in range(0, len(tables), batch_size):
        batch = tables[i : i + batch_size]
        with engine.begin() as conn:
            for name in batch:
                # Defensive: refuse to interpolate anything that doesn't
                # look like a Postgres identifier. cache_* names are
                # constrained by physical_namer but legacy tables predate
                # those guarantees.
                if not _SAFE_NAME_RE.match(name):
                    logger.warning("skip drop of unsafe table name: %r", name)
                    continue
                conn.execute(text(f'DROP TABLE IF EXISTS "{name}" CASCADE'))
        dropped += len(batch)
        logger.info("dropped %d/%d cache_* tables", dropped, len(tables))
    return dropped


def _toggle_cache_drop_trigger(engine: Engine, *, enabled: bool) -> bool:
    """Best-effort toggle for the cache-drop event trigger during staging wipes."""
    try:
        with engine.begin() as conn:
            exists = conn.execute(
                text(
                    "SELECT EXISTS("
                    "  SELECT 1 FROM pg_event_trigger WHERE evtname = 'trg_audit_cache_drop'"
                    ")"
                )
            ).scalar()
            if not exists:
                return False
            action = "ENABLE" if enabled else "DISABLE"
            conn.execute(text(f"ALTER EVENT TRIGGER trg_audit_cache_drop {action}"))
        logger.info("%sd trg_audit_cache_drop during staging reset", action.lower().capitalize())
        return True
    except Exception:
        logger.info(
            "Could not %s trg_audit_cache_drop during staging reset; continuing",
            "enable" if enabled else "disable",
            exc_info=True,
        )
        return False


def _truncate(engine: Engine, name: str, *, dry_run: bool) -> bool:
    if not _table_exists(engine, name):
        logger.info("skip truncate %s — table not present", name)
        return False
    if dry_run:
        logger.info("[dry-run] would TRUNCATE %s", name)
        return True
    with engine.begin() as conn:
        # `RESTART IDENTITY CASCADE` resets sequences and follows FKs (we
        # already dropped cache_*; this catches any other CASCADE).
        conn.execute(text(f'TRUNCATE TABLE "{name}" RESTART IDENTITY CASCADE'))
    logger.info("truncated %s", name)
    return True


def reset(engine: Engine, *, dry_run: bool, reset_datasets: bool) -> ResetSummary:
    cache_tables = _list_cache_tables(engine)
    logger.info(
        "found %d cache_* tables in public schema (dry_run=%s)",
        len(cache_tables),
        dry_run,
    )
    trigger_disabled = False
    if not dry_run:
        trigger_disabled = _toggle_cache_drop_trigger(engine, enabled=False)
    try:
        dropped = _drop_cache_tables(engine, cache_tables, dry_run=dry_run)
    finally:
        if trigger_disabled:
            _toggle_cache_drop_trigger(engine, enabled=True)
    truncated_base: list[str] = []
    for tbl in _TRUNCATE_BASE:
        if _truncate(engine, tbl, dry_run=dry_run):
            truncated_base.append(tbl)
    truncated_optional: list[str] = []
    for tbl in _TRUNCATE_OPTIONAL:
        if _truncate(engine, tbl, dry_run=dry_run):
            truncated_optional.append(tbl)
    datasets_truncated = False
    if reset_datasets:
        for tbl in _TRUNCATE_WITH_DATASETS:
            if _truncate(engine, tbl, dry_run=dry_run):
                datasets_truncated = True
    return ResetSummary(
        cache_tables_dropped=dropped,
        base_tables_truncated=truncated_base,
        optional_tables_truncated=truncated_optional,
        datasets_truncated=datasets_truncated,
        dry_run=dry_run,
    )


def _trigger_repopulation(*, also_scrape: bool) -> None:
    """Best-effort: kick off scrape (if requested) + catalog backfill via Celery.

    If celery isn't reachable, we just log a hint — the operator can run the
    tasks manually.
    """
    try:
        from app.infrastructure.celery.app import ALL_PORTALS, celery_app  # noqa: F401
        from app.infrastructure.celery.tasks.catalog_backfill import (
            catalog_backfill_task,
        )
        from app.infrastructure.celery.tasks.scraper_tasks import scrape_catalog
    except Exception:
        logger.warning(
            "Celery not importable — skipping auto-dispatch. Run manually: "
            "openarg.scrape_catalog (per portal) and openarg.catalog_backfill"
        )
        return
    try:
        if also_scrape:
            for portal in ALL_PORTALS:
                scrape_catalog.delay(portal)
            logger.info("dispatched scrape_catalog for %d portals", len(ALL_PORTALS))
        # Backfill runs after scrape — give a small countdown so it lands behind.
        countdown = 600 if also_scrape else 30
        catalog_backfill_task.apply_async(countdown=countdown)
        logger.info("dispatched catalog_backfill (countdown=%ds)", countdown)
    except Exception:
        logger.warning(
            "Could not dispatch via Celery — broker unavailable? Run the "
            "tasks manually after the reset.",
            exc_info=True,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Destructive staging reset")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--i-understand-this-deletes-data",
        action="store_true",
        help="Required for non-dry-run mode — proves you read the docstring",
    )
    parser.add_argument(
        "--reset-datasets",
        action="store_true",
        help="Also truncate the datasets table (forces full re-scrape)",
    )
    parser.add_argument(
        "--no-repopulate",
        action="store_true",
        help="Skip the post-reset Celery dispatch (you'll run it manually)",
    )
    parser.add_argument(
        "--no-scrape-trigger",
        action="store_true",
        help="Don't auto-dispatch scrape_catalog after the reset",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="DB URL (defaults to $DATABASE_URL)",
    )
    args = parser.parse_args(argv)

    _refuse_in_prod()

    if not args.dry_run and not args.i_understand_this_deletes_data:
        raise SystemExit(
            "REFUSED — non-dry-run requires --i-understand-this-deletes-data"
        )
    if not args.database_url:
        raise SystemExit("DATABASE_URL not set")

    engine = create_engine(args.database_url, pool_pre_ping=True)
    summary = reset(
        engine, dry_run=args.dry_run, reset_datasets=args.reset_datasets
    )
    print("---")
    print(f"cache_* tables dropped:     {summary.cache_tables_dropped}")
    print(f"base tables truncated:      {summary.base_tables_truncated}")
    print(f"optional tables truncated:  {summary.optional_tables_truncated}")
    print(f"datasets truncated:         {summary.datasets_truncated}")
    print(f"dry_run:                    {summary.dry_run}")

    if not args.dry_run and not args.no_repopulate:
        _trigger_repopulation(also_scrape=args.reset_datasets and not args.no_scrape_trigger)

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
