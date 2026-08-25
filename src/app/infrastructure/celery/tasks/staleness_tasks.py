"""Tell a person when a source stopped arriving.

The counterpart to every guard that refuses to write. Refusing is right — a
blank payroll is worse than a stale one — but by itself it trades a loud failure
for a silent one: the mart keeps answering and nothing says the data froze.

Deliberately ranked by **how late relative to its own cadence**, not by age. A
census that arrives yearly and a exchange rate that arrives hourly are both
interesting at very different absolute ages, and sorting by age alone would put
the census at the top forever.
"""

from __future__ import annotations

import logging
from typing import Any

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


@celery_app.task(
    name="openarg.alert_stale_ingests",
    bind=True,
    soft_time_limit=600,
    time_limit=900,
)
def alert_stale_ingests(self, *, multiple: float = 3.0, limit: int = 50) -> dict[str, Any]:
    """Report sources that are late by their own standards."""
    from app.application.quality.heartbeat import find_late

    engine = get_sync_engine()
    late = find_late(engine, multiple=multiple, limit=limit)

    report: dict[str, Any] = {
        "late": len(late),
        "worst": [
            {
                "recurso": item.resource_identity,
                "dias_tarde": round(item.days_late, 1),
                "cadencia_dias": round(item.cadence_days, 2),
            }
            for item in late[:10]
        ],
    }
    logger.info("stale ingests: %s", report)

    if not late:
        return report

    try:
        from app.application.quality.alerting import Alert, notify

        report["alerting"] = notify(
            engine,
            [
                Alert(
                    kind="ingest_late",
                    # Identity of the source. A source that stays late is
                    # reported once, not every morning.
                    key=item.resource_identity,
                    title=f"{item.resource_identity[:60]} dejó de llegar",
                    detail=item.phrase_es(),
                )
                for item in late
            ],
            heading="OpenArg · fuentes que dejaron de llegar",
        )
    except Exception:
        logger.warning("stale ingests: alerting skipped", exc_info=True)
    return report
