"""Findings that stop being reported have to stop being open.

`mark_resolved` is all-or-nothing per resource, which fits a re-ingest: the
resource is fine now, close everything. A recurring audit needs the partial
case, and did not have it — findings were only ever closed when a mart came
back completely clean.

So a mart with three badly typed columns, one of which someone fixes, kept
three open findings. Nothing anywhere closed the third: the upsert only
re-opens. Observed on staging 2026-07-28 in a smaller form — changing a
check's key scheme orphaned its previous row, and both the stale and the fresh
finding sat open side by side saying the same thing.

A report that only grows is a report people stop reading, which costs more
than the findings it contains.
"""

from __future__ import annotations

from app.application.validation.detector import Mode
from app.application.validation.findings_repository import resolve_missing


class _Result:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class _Conn:
    def __init__(self, recorder: dict, rowcount: int = 1) -> None:
        self._recorder = recorder
        self._rowcount = rowcount

    def __enter__(self):
        return self

    def __exit__(self, *_a) -> bool:
        return False

    def execute(self, statement, params=None):
        self._recorder["sql"] = str(statement)
        self._recorder["params"] = params or {}
        return _Result(self._rowcount)


class _Engine:
    def __init__(self, recorder: dict, rowcount: int = 1) -> None:
        self._recorder = recorder
        self._rowcount = rowcount

    def begin(self):
        return _Conn(self._recorder, self._rowcount)


class _BrokenEngine:
    def begin(self):
        raise RuntimeError("db down")


class TestResolveMissing:
    def test_keeps_the_hashes_still_reported(self) -> None:
        rec: dict = {}
        resolve_missing(
            _Engine(rec),
            "mart::m",
            mode=Mode.MART_AUDIT,
            keep_hashes=["a", "b"],
        )
        assert "input_hash <> ALL(:keep)" in rec["sql"]
        assert rec["params"]["keep"] == ["a", "b"]

    def test_an_empty_keep_set_closes_everything_for_the_mode(self) -> None:
        """A mart that came back clean: nothing to keep open."""
        rec: dict = {}
        resolve_missing(_Engine(rec), "mart::m", mode=Mode.MART_AUDIT, keep_hashes=[])
        assert "input_hash" not in rec["sql"]
        assert "keep" not in rec["params"]

    def test_scoped_to_one_mode(self) -> None:
        """An audit sweep must not resolve ingestion findings it knows nothing about."""
        rec: dict = {}
        resolve_missing(_Engine(rec), "mart::m", mode=Mode.MART_AUDIT, keep_hashes=["a"])
        assert "mode = :mode" in rec["sql"]
        assert rec["params"]["mode"] == "mart_audit"

    def test_only_touches_open_findings(self) -> None:
        rec: dict = {}
        resolve_missing(_Engine(rec), "mart::m", mode=Mode.MART_AUDIT, keep_hashes=[])
        assert "resolved_at IS NULL" in rec["sql"]

    def test_deduplicates_and_orders_the_keep_set(self) -> None:
        """Same set, same SQL parameters — keeps the statement cacheable."""
        rec: dict = {}
        resolve_missing(_Engine(rec), "mart::m", mode=Mode.MART_AUDIT, keep_hashes=["b", "a", "b"])
        assert rec["params"]["keep"] == ["a", "b"]

    def test_ignores_empty_hashes(self) -> None:
        rec: dict = {}
        resolve_missing(_Engine(rec), "mart::m", mode=Mode.MART_AUDIT, keep_hashes=["a", ""])
        assert rec["params"]["keep"] == ["a"]

    def test_returns_the_number_closed(self) -> None:
        assert (
            resolve_missing(
                _Engine({}, rowcount=4), "mart::m", mode=Mode.MART_AUDIT, keep_hashes=[]
            )
            == 4
        )

    def test_a_failure_does_not_propagate(self) -> None:
        """Losing the bookkeeping must not take the sweep down with it."""
        assert (
            resolve_missing(_BrokenEngine(), "mart::m", mode=Mode.MART_AUDIT, keep_hashes=[]) == 0
        )
