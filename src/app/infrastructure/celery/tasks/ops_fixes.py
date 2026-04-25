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
from sqlalchemy import text

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
