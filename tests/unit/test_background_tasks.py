"""Unit tests for the shared background task registry.

Verifies the contract documented in
``specs/010-sandbox-sql/010b-nl2sql/plan.md`` §7:

1. ``spawn_background`` returns a Task that is held by a module-level
   strong reference until completion (no CPython weak-ref GC race).
2. The done callback removes the task from the registry on completion.
3. An exception raised inside the coroutine is logged as a WARNING,
   not silently swallowed.
4. A cancelled task is removed from the registry without logging a
   warning.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.application.pipeline._background_tasks import (
    _active_task_count,
    spawn_background,
)


@pytest.mark.asyncio
async def test_spawn_background_holds_strong_reference_until_done():
    """While the coroutine is running, the task must be in the registry."""
    started = asyncio.Event()
    allow_finish = asyncio.Event()

    async def _work() -> None:
        started.set()
        await allow_finish.wait()

    count_before = _active_task_count()
    task = spawn_background(_work(), name="test.strong_ref")

    # Wait until the coroutine has actually started running.
    await started.wait()

    assert _active_task_count() == count_before + 1
    assert task in [t for t in asyncio.all_tasks() if t.get_name() == "test.strong_ref"]

    # Release the coroutine and let it complete.
    allow_finish.set()
    await task

    # After completion the done_callback should have removed the task.
    assert _active_task_count() == count_before


@pytest.mark.asyncio
async def test_spawn_background_logs_unhandled_exception(caplog):
    """A coroutine that raises must surface via the done callback as WARNING."""

    async def _boom() -> None:
        raise ValueError("kaboom from test")

    count_before = _active_task_count()
    caplog.set_level(logging.WARNING, logger="app.application.pipeline._background_tasks")

    task = spawn_background(_boom(), name="test.boom")

    # Wait for the task to actually finish — we can't await it directly
    # because it raises, which would propagate here. Use asyncio.wait.
    done, _ = await asyncio.wait({task}, return_when=asyncio.ALL_COMPLETED)
    assert task in done

    # Give the event loop one more tick so add_done_callback fires.
    await asyncio.sleep(0)

    # The registry should have shrunk back.
    assert _active_task_count() == count_before

    # And the warning should have been logged with the name.
    matching = [
        rec
        for rec in caplog.records
        if rec.levelno == logging.WARNING
        and "test.boom" in rec.getMessage()
        and "kaboom from test" in rec.getMessage()
    ]
    assert matching, (
        f"expected a WARNING with 'test.boom' + 'kaboom from test' but "
        f"got: {[r.getMessage() for r in caplog.records]}"
    )


@pytest.mark.asyncio
async def test_spawn_background_cancellation_is_quiet(caplog):
    """A cancelled task must not trigger the unhandled-exception WARNING."""
    started = asyncio.Event()

    async def _wait_forever() -> None:
        started.set()
        await asyncio.Event().wait()  # blocks forever unless cancelled

    count_before = _active_task_count()
    caplog.set_level(logging.WARNING, logger="app.application.pipeline._background_tasks")

    task = spawn_background(_wait_forever(), name="test.cancel")
    await started.wait()
    task.cancel()

    # Wait for cancellation to propagate.
    try:
        await task
    except asyncio.CancelledError:
        pass

    await asyncio.sleep(0)

    assert _active_task_count() == count_before

    cancellation_warnings = [
        rec for rec in caplog.records if rec.levelno == logging.WARNING and "test.cancel" in rec.getMessage()
    ]
    assert not cancellation_warnings, (
        "cancelled tasks must not log a WARNING, but got: "
        f"{[r.getMessage() for r in cancellation_warnings]}"
    )
