"""Watch the things whose failure was invisible, and tell a person.

Every item here is a real incident that went unnoticed, not a hypothetical:

- **Marts that stopped building.** Three sat in `build_failed` for weeks and
  were found by someone reading the database on an unrelated errand. The status
  was recorded correctly the whole time; nothing looked at it.
- **A collection pipeline that stopped collecting.** After the 2026-08-03
  incident, sixteen days passed with no collection at all. The absence of work
  produces no error, which is exactly why it needs watching — a failure that
  looks like silence is invisible to anything waiting for a failure.
- **Sweeps that succeed while doing nothing.** `cleanup_raw_orphans` returned
  `{'dropped': 0, 'failed': 10}` and reported success every hour for three
  months.

Each check answers one question with one query, and stays quiet when the answer
is fine.
"""

from __future__ import annotations

import logging
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

# The pipeline going quiet is not an error anywhere — it is the absence of rows.
_COLLECTION_STALLED_SQL = text(
    """
    SELECT max(updated_at) AS last_collect FROM raw.cached_datasets
    WHERE status = 'ready'
    """
)

_STALL_HOURS = 36


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

    result = notify(engine, alerts, heading="OpenArg · señales de calidad")
    logger.info("quality alerts: %s", result)
    return result
