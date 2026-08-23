"""Watch the things whose failure was invisible, and tell a person.

Every item here is a real incident that went unnoticed, not a hypothetical:

- **Marts that stopped building.** Three sat in `build_failed` for weeks and
  were found by someone reading the database on an unrelated errand. The status
  was recorded correctly the whole time; nothing looked at it.
- **Marts that build fine and hold nothing.** `pobreza_indec_aglomerados` —
  poverty, one of the highest-value datasets there is — sat at `built` with zero
  rows from 2026-08-15 until it was noticed on the 23rd. Its sources had data
  the whole time; the mart simply had not been rebuilt since they changed.

  The serving layer hides an empty mart correctly (`COALESCE(last_row_count,0)
  > 0`, in three places), and that is what made it worse rather than better:
  every question about poverty fell through to a search or a deflection for
  eight days, the system was right to hide it, and it told nobody. A `built`
  that holds nothing is a `built` that lies.
- **A collection pipeline that stopped collecting.** After the 2026-08-03
  incident, sixteen days passed with no collection at all. The absence of work
  produces no error, which is exactly why it needs watching — a failure that
  looks like silence is invisible to anything waiting for a failure.
- **Sweeps that succeed while doing nothing.** `cleanup_raw_orphans` returned
  `{'dropped': 0, 'failed': 10}` and reported success every hour for three
  months.
- **Redis filling up.** The instance holds the Celery broker, the results
  backend and the caches, and it ran with `allkeys-lru` — under pressure it
  would evict whatever was least recently used, including queued task messages,
  which is a task silently ceasing to exist. Switching to `noeviction` makes
  that a loud failure instead, and a loud failure nobody hears is the same as a
  quiet one. Hence this watch.

Each check answers one question with one query, and stays quiet when the answer
is fine.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

_BROKEN_MARTS_SQL = text(
    """
    SELECT mart_id, last_refresh_error
    FROM mart_definitions
    WHERE last_refresh_status = 'build_failed'
    ORDER BY mart_id
    """
)

# Built, unblocked, and empty — so the serving filter hides it and nothing says
# so. The twin of `build_failed`, and the one that had no watcher.
_EMPTY_MARTS_SQL = text(
    """
    SELECT mart_id, last_refreshed_at
    FROM mart_definitions
    WHERE COALESCE(last_row_count, 0) = 0
      AND last_refresh_status IN ('built', 'refreshed')
      AND NOT COALESCE(serving_blocked, FALSE)
    ORDER BY mart_id
    """
)

# The pipeline going quiet is not an error anywhere — it is the absence of rows.
_COLLECTION_STALLED_SQL = text(
    """
    SELECT max(updated_at) AS last_collect FROM raw.cached_datasets
    WHERE status = 'ready'
    """
)

_STALL_HOURS = 36

# Well below the point where writes would start failing. `noeviction` turns a
# full Redis into refused writes — the right trade against silently dropping a
# queued task, but only if the warning arrives with room to act on it. Measured
# 2026-08-23: 38 MB of 512, and zero evictions in the instance's history.
_REDIS_WARN_RATIO = 0.75


@celery_app.task(
    name="openarg.alert_on_quality_signals",
    bind=True,
    soft_time_limit=600,
    time_limit=900,
)
def alert_on_quality_signals(self) -> dict[str, Any]:
    """Check the silent failure modes and notify about anything new."""
    from app.application.quality.alerting import Alert, notify

    engine = get_sync_engine()
    alerts: list[Alert] = []

    try:
        with engine.connect() as conn:
            for row in conn.execute(_BROKEN_MARTS_SQL).fetchall():
                alerts.append(
                    Alert(
                        kind="mart_failed",
                        # Keyed on the mart, so a mart that fails, is fixed, and
                        # fails again months later alerts again — while one that
                        # simply stays broken does not re-alert every hour.
                        key=str(row.mart_id),
                        title=f"Mart caído: {row.mart_id}",
                        detail=(str(row.last_refresh_error) or "")[:180],
                    )
                )
            for row_e in conn.execute(_EMPTY_MARTS_SQL).fetchall():
                alerts.append(
                    Alert(
                        kind="mart_empty",
                        key=str(row_e.mart_id),
                        title=f"Mart vacío (construye pero no tiene filas): {row_e.mart_id}",
                        detail=(
                            "el filtro de servido lo oculta, así que nadie lo ve fallar · "
                            f"último refresh: {row_e.last_refreshed_at:%Y-%m-%d %H:%M}"
                            if row_e.last_refreshed_at
                            else "el filtro de servido lo oculta, así que nadie lo ve fallar"
                        ),
                    )
                )
            row = conn.execute(_COLLECTION_STALLED_SQL).fetchone()
            conn.rollback()
    except Exception:
        logger.warning("quality alerts: could not read signals", exc_info=True)
        return {"error": "read_failed", "sent": 0}

    last = row.last_collect if row else None
    if last is not None:
        from datetime import datetime

        if last.tzinfo is None:
            last = last.replace(tzinfo=UTC)
        hours = (datetime.now(UTC) - last).total_seconds() / 3600
        if hours >= _STALL_HOURS:
            alerts.append(
                Alert(
                    kind="collection_stalled",
                    # Keyed by day so a continuing stall reports once a day
                    # rather than once an hour — present enough to act on,
                    # quiet enough not to be muted.
                    key=f"stalled:{datetime.now(UTC):%Y-%m-%d}",
                    title=f"Sin colectar hace {int(hours)} h",
                    detail=f"última colecta: {last:%Y-%m-%d %H:%M} UTC",
                )
            )

    # Redis is not in the database, so it needs its own look.
    try:
        import redis as _redis

        url = os.getenv("REDIS_CACHE_URL") or os.getenv("CELERY_BROKER_URL") or ""
        if url:
            info = _redis.from_url(url).info("memory")
            used = int(info.get("used_memory", 0))
            cap = int(info.get("maxmemory", 0))
            if cap and used / cap >= _REDIS_WARN_RATIO:
                alerts.append(
                    Alert(
                        kind="redis_pressure",
                        # By day: a sustained condition should say so once a day,
                        # not once an hour.
                        key=f"redis:{used * 100 // cap}pct",
                        title=f"Redis al {used * 100 // cap} % de su techo",
                        detail=(
                            f"{used // (1024 * 1024)} MB de {cap // (1024 * 1024)} MB · "
                            "con noeviction, llenarse significa escrituras rechazadas"
                        ),
                    )
                )
    except Exception:
        logger.debug("quality alerts: could not read redis memory", exc_info=True)

    result = notify(engine, alerts, heading="OpenArg · señales de calidad")
    logger.info("quality alerts: %s", result)
    return result


@celery_app.task(
    name="openarg.check_mart_expectations",
    bind=True,
    soft_time_limit=1800,
    time_limit=2400,
)
def check_mart_expectations(self) -> dict[str, Any]:
    """Evaluate every mart's expectations and alert on what fails.

    Runs after the retry sweep, for the same reason the alert does: a mart whose
    sources moved should be rebuilt, not reported, and what survives a rebuild is
    what a person needs to see.

    Silence when everything holds. A daily "69 marts fine" is the furniture that
    trains people to stop reading the channel.
    """
    from app.application.marts.mart import load_all_marts
    from app.application.quality.alerting import Alert, notify
    from app.application.quality.expectations import check_mart
    from app.infrastructure.celery.tasks.mart_tasks import _DEFAULT_MARTS_DIR

    engine = get_sync_engine()
    if not _DEFAULT_MARTS_DIR.exists():
        return {"error": "marts_dir_missing", "sent": 0}

    with engine.connect() as conn:
        counts = {
            str(r.mart_id): int(r.n or 0)
            for r in conn.execute(
                text("SELECT mart_id, COALESCE(last_row_count, 0) AS n FROM mart_definitions")
            )
        }
        conn.rollback()

    findings = []
    for mart in load_all_marts(_DEFAULT_MARTS_DIR):
        if mart.id not in counts:
            continue
        try:
            findings.extend(check_mart(engine, mart, counts[mart.id]))
        except Exception:
            # One mart's check must not cost the sweep.
            logger.warning("expectations: check failed for %s", mart.id, exc_info=True)

    alerts = [
        Alert(
            kind=f"expectation:{f.rule}",
            # Keyed on mart and rule, so a mart failing two different
            # expectations is two findings and a repeat of the same one is not.
            key=f"{f.mart_id}:{f.rule}",
            title=f"{f.mart_id}: {f.rule}",
            detail=f.detail,
        )
        for f in findings
    ]
    result = notify(engine, alerts, heading="OpenArg · expectativas de marts")
    result["findings"] = len(findings)
    logger.info("mart expectations: %s", result)
    return result
