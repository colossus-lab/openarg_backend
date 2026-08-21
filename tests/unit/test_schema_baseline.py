"""Tests for the baseline snapshot pass.

The point of this task is that it is safe to run against live tables: it must
never drop, alter or block anything, and a table that fails to capture must
cost only itself.
"""

from __future__ import annotations

from unittest.mock import MagicMock


class _Row:
    def __init__(self, table_name, schema_name="raw", version=1):
        self.table_name = table_name
        self.schema_name = schema_name
        self.resource_identity = f"portal::{table_name}"
        self.version = version


def _run(rows, capture_side_effect, remaining=0):
    from app.infrastructure.celery.tasks import schema_baseline_tasks as mod

    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.side_effect = [
        MagicMock(fetchall=lambda: rows),
        MagicMock(scalar=lambda: remaining),
    ]
    mod.get_sync_engine = lambda: engine

    calls = []

    def _capture(_engine, **kw):
        calls.append(kw)
        return capture_side_effect(kw)

    import app.application.catalog.schema_snapshot as snap

    original = snap.capture_table_snapshot
    snap.capture_table_snapshot = _capture
    try:
        return mod.baseline_schema_snapshots.run(), calls, conn
    finally:
        snap.capture_table_snapshot = original


def test_captures_every_candidate():
    result, calls, _ = _run([_Row("a"), _Row("b")], lambda kw: "snap-id", remaining=7)

    assert result["captured"] == 2
    assert result["skipped"] == 0
    assert result["remaining_without_baseline"] == 7
    assert {c["table_name"] for c in calls} == {"a", "b"}


def test_a_table_that_cannot_be_captured_costs_only_itself():
    """A table can vanish between the SELECT and the capture, and one that was
    never analysed yields no profile. Neither should end the run."""
    result, _, _ = _run(
        [_Row("gone"), _Row("fine")],
        lambda kw: None if kw["table_name"] == "gone" else "snap-id",
    )

    assert result["captured"] == 1
    assert result["skipped"] == 1


def test_the_snapshot_is_marked_as_taken_from_a_living_table():
    """A reader must be able to tell 'this is how it looked while alive' from
    'this is how it looked the moment before it died'."""
    _, calls, _ = _run([_Row("t")], lambda kw: "snap-id")

    assert calls[0]["reason"] == "baseline"
    assert calls[0]["extra"]["alive"] is True
    assert calls[0]["extra"]["resource_identity"] == "portal::t"
    assert calls[0]["schema_name"] == "raw"


def test_nothing_destructive_is_issued():
    """The task reads. Any DROP/DELETE/ALTER here would be a defect, since it
    runs against tables that are still in service."""
    _, _, conn = _run([_Row("t")], lambda kw: "snap-id")

    issued = " ".join(str(c.args[0]) for c in conn.execute.call_args_list).upper()
    for verb in ("DROP ", "DELETE ", "ALTER ", "TRUNCATE ", "UPDATE ", "INSERT "):
        assert verb not in issued, f"baseline pass issued {verb.strip()}"
