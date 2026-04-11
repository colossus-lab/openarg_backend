"""Fire-and-forget background task registry for the query pipeline.

Both the finalize node (memory update) and the NL2SQL subgraph
(``save_successful_query``) spawn coroutines that should outlive the
request that scheduled them. Naively calling ``asyncio.create_task(...)``
without keeping a reference exposes those tasks to CPython's weak-ref
GC: the task can be collected mid-execution and silently disappear.

This module owns the single strong-ref registry used across the
pipeline, plus the done-callback that surfaces any unhandled exception
as a WARNING log. It is the second consumer of the pattern (after
``nodes/finalize.py``) that justified promoting it to a shared helper —
see constitution §0, regla 2 ("abstractions pay rent only when a second
caller exists"), and ``specs/010-sandbox-sql/010b-nl2sql/plan.md`` §7.

Usage:

    from app.application.pipeline._background_tasks import spawn_background

    spawn_background(
        some_coroutine(arg1, arg2),
        name="module.action",
    )

The ``name`` is required so that any exception logged by the done
callback is attributable to a specific call site.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Module-private strong-ref registry. Callers go through spawn_background(),
# never touch this set directly — that keeps the invariant (add on spawn,
# discard on completion) in a single place.
_background_tasks: set[asyncio.Task[Any]] = set()


def spawn_background(coro: Any, *, name: str) -> asyncio.Task[Any]:
    """Schedule a fire-and-forget coroutine with a strong reference + error log.

    The returned task is held in a module-level set until it completes, so
    CPython's weak-ref task GC can't collect it mid-execution. When the task
    finishes (successfully, with an exception, or cancelled) the done
    callback:

    1. Removes the task from the registry so the set does not grow
       unbounded.
    2. Logs any unhandled exception at WARNING level with the ``name``
       passed at spawn time. Silent swallowing is how DEBT-012 survived
       long enough to reach a reverse-SDD review — this is the bright
       line that stops it happening again.

    Cancellation is not logged — cancellations are expected on graceful
    shutdown and during task abort flows.
    """
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)

    def _done(t: asyncio.Task[Any]) -> None:
        _background_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            logger.warning(
                "Background task %r raised: %s", name, exc, exc_info=exc
            )

    task.add_done_callback(_done)
    return task


def _active_task_count() -> int:
    """Return the number of in-flight background tasks.

    Test-only hook — production code should not depend on the precise
    count. Used by ``tests/unit/application/pipeline/test_background_tasks.py``
    to assert that the registry grows on spawn and shrinks on completion.
    """
    return len(_background_tasks)
