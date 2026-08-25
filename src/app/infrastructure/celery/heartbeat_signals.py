"""Record that each scheduled task actually ran.

The dead man's switch. `cleanup_raw_orphans` reported `{'dropped': 0}` and
*succeeded* every hour for three months while doing nothing, and a later variant
cost sixteen days of collection — because a task that stops running and a task
with nothing to do produce exactly the same silence.

Only the tasks the beat schedule names are recorded. Every other task completion
is ordinary work with no expected cadence, and writing a row for each would turn
a small table into a firehose without answering any question.
"""

from __future__ import annotations

import logging

from celery.signals import task_success

logger = logging.getLogger(__name__)

_scheduled: frozenset[str] | None = None


def _scheduled_task_names() -> frozenset[str]:
    """The tasks with an expected cadence, read once from the beat schedule."""
    global _scheduled
    if _scheduled is None:
        try:
            from app.infrastructure.celery.app import celery_app

            _scheduled = frozenset(
                entry["task"]
                for entry in (celery_app.conf.beat_schedule or {}).values()
                if isinstance(entry, dict) and entry.get("task")
            )
        except Exception:
            logger.debug("heartbeat signals: could not read the schedule", exc_info=True)
            _scheduled = frozenset()
    return _scheduled


@task_success.connect
def _record(sender=None, **_kw) -> None:
    """Beat once per successful run of a scheduled task. Never raises."""
    name = getattr(sender, "name", None)
    if not name or name not in _scheduled_task_names():
        return
    try:
        from app.application.quality.heartbeat import record_task_run
        from app.infrastructure.celery.tasks._db import get_sync_engine

        record_task_run(get_sync_engine(), name)
    except Exception:
        # A heartbeat that can fail the task it watches is worse than none.
        logger.debug("heartbeat signals: could not record %s", name, exc_info=True)
