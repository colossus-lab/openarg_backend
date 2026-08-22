"""Tests for the scheduled unsplit-CSV repair.

This is the first repair in the project allowed to run unattended, so what
matters is that it stays bounded, that it cannot be switched on by accident,
and that one bad table does not cost the batch.
"""

from __future__ import annotations

from unittest.mock import MagicMock


class _Row:
    def __init__(self, i):
        self.table_schema = "raw"
        self.table_name = f"cache_t{i}"
        self.header = "a;b;c"


class _Outcome:
    def __init__(self, ok=True, reason="split"):
        self.ok = ok
        self.reason = reason


def _run(rows, outcomes, **kw):
    from app.application.repair import parse_repair
    from app.infrastructure.celery.tasks import parse_repair_tasks as mod

    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value = MagicMock(
        fetchall=lambda: rows
    )
    mod.get_sync_engine = lambda: engine

    calls: list[dict] = []
    it = iter(outcomes)

    def _repair(_engine, **kwargs):
        calls.append(kwargs)
        result = next(it)
        if isinstance(result, Exception):
            raise result
        return result

    parse_repair.repair_unsplit_csv_table = _repair
    return mod.repair_unsplit_csv_tables.run(**kw), calls, engine


def test_it_reports_without_acting_by_default():
    """Running it by hand must not write. The schedule states dry_run=False
    explicitly so the decision to act is visible there, not in a default."""
    result, calls, _ = _run([_Row(1)], [_Outcome(reason="dry_run")])

    assert result["dry_run"] is True
    assert calls[0]["dry_run"] is True
    assert result["repaired"] == 0


def test_it_stays_within_its_cap():
    result, _, engine = _run([_Row(i) for i in range(5)], [_Outcome()] * 5, limit=5, dry_run=False)

    params = engine.connect.return_value.__enter__.return_value.execute.call_args.args[1]
    assert params["limit"] == 5
    assert result["repaired"] == 5


def test_declines_are_counted_and_surfaced():
    """The refusals are the interesting number. Those tables hold a delimiter
    inside a quoted value, and a split would corrupt every row after it."""
    outcomes = [
        _Outcome(),
        _Outcome(ok=False, reason="inconsistent_field_count:header=4,rows=5"),
        _Outcome(ok=False, reason="inconsistent_field_count:header=3,rows=4"),
    ]
    result, _, _ = _run([_Row(i) for i in range(3)], outcomes, dry_run=False)

    assert result["repaired"] == 1
    assert result["declined_inconsistent"] == 2
    assert result["by_reason"]["inconsistent_field_count"] == 2


def test_one_table_raising_does_not_cost_the_batch():
    outcomes = [_Outcome(), RuntimeError("locked"), _Outcome()]
    result, _, _ = _run([_Row(i) for i in range(3)], outcomes, dry_run=False)

    assert result["repaired"] == 2
    assert result["by_reason"]["raised"] == 1


def test_every_repair_shares_one_run_id():
    """`revert_repair` works from an audit row, and a shared run_id is what lets
    an operator undo a whole sweep rather than a table at a time."""
    result, calls, _ = _run([_Row(i) for i in range(3)], [_Outcome()] * 3, dry_run=False)

    ids = {c["run_id"] for c in calls}
    assert len(ids) == 1
    assert str(next(iter(ids))) == result["run_id"]
