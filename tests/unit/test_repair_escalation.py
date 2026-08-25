"""Tests for the repair ladder.

The ladder's job is not to repair — the rungs already did that. Its job is to
try every rung before waking anybody, to stop when a repair would cost more than
the defect, and to be able to say which rung worked. Those three are what these
tests pin down.

The rungs are faked throughout. Testing `repair_col_n_table` again here would
say nothing about the ladder, and the ladder is what is new.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from app.application.repair import escalation as esc
from app.application.repair.parse_repair import RepairOutcome


def _outcome(ok=True, reason="applied", old=None, new=None):
    return RepairOutcome(
        table_schema="raw",
        table_name="t",
        ok=ok,
        reason=reason,
        old_columns=old or ["col_1", "b"],
        new_columns=new or ["monto", "b"],
    )


class _Rung:
    """A fake repair function that records how it was called."""

    def __init__(self, outcome=None, raises=None):
        self.outcome = outcome
        self.raises = raises
        self.calls: list[bool] = []

    def __call__(self, engine, *, table_schema, table_name, run_id, dry_run):
        self.calls.append(dry_run)
        if self.raises:
            raise self.raises
        return self.outcome


def _tiers(monkeypatch, *pairs):
    monkeypatch.setattr(esc, "heuristic_tiers", lambda: tuple(pairs))


class _Col:
    """A `pg_attribute` row. Not a MagicMock: `name` is reserved there."""

    def __init__(self, name):
        self.name = name


def _engine(columns=("col_1", "b", "_source_dataset_id")):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = [_Col(c) for c in columns]
    return engine


# ── qué columnas cambian ───────────────────────────────────────


def test_a_rename_is_positional():
    assert esc.changed_columns(["a", "b"], ["x", "b"]) == ("a",)


def test_a_drop_is_a_set_difference():
    assert esc.changed_columns(["a", "b", "c"], ["a", "c"]) == ("b",)


def test_nothing_changed_is_empty():
    assert esc.changed_columns(["a", "b"], ["a", "b"]) == ()


# ── el orden de la escalera ────────────────────────────────────


def test_the_first_rung_that_recognises_the_table_wins(monkeypatch):
    first = _Rung(_outcome())
    second = _Rung(_outcome())
    _tiers(monkeypatch, ("first", first), ("second", second))

    r = esc.escalate_table(_engine(), table_schema="raw", table_name="t", dry_run=False)

    assert r.fixed and r.tier == "first"
    # Dry run to see the proposal, then the real apply.
    assert first.calls == [True, False]
    assert second.calls == [], "no debería consultar rungs posteriores"


def test_a_rung_that_declines_lets_the_next_one_try(monkeypatch):
    declined = _Rung(_outcome(ok=False, reason="too_few_cols"))
    worked = _Rung(_outcome())
    _tiers(monkeypatch, ("declined", declined), ("worked", worked))

    r = esc.escalate_table(_engine(), table_schema="raw", table_name="t", dry_run=False)

    assert r.tier == "worked"
    assert [a.tier for a in r.attempts] == ["declined", "worked"]
    assert r.attempts[0].reason == "too_few_cols"


def test_a_rung_that_raises_does_not_stop_the_ladder(monkeypatch):
    boom = _Rung(raises=RuntimeError("x"))
    worked = _Rung(_outcome())
    _tiers(monkeypatch, ("boom", boom), ("worked", worked))

    r = esc.escalate_table(_engine(), table_schema="raw", table_name="t", dry_run=False)

    assert r.fixed and r.tier == "worked"
    assert r.attempts[0].reason == "raised:RuntimeError"


def test_a_dry_run_that_succeeds_but_an_apply_that_fails_keeps_climbing(monkeypatch):
    class _Flaky(_Rung):
        def __call__(self, engine, *, table_schema, table_name, run_id, dry_run):
            self.calls.append(dry_run)
            return _outcome(ok=dry_run, reason="" if dry_run else "lock_timeout")

    flaky = _Flaky()
    worked = _Rung(_outcome())
    _tiers(monkeypatch, ("flaky", flaky), ("worked", worked))

    r = esc.escalate_table(_engine(), table_schema="raw", table_name="t", dry_run=False)

    assert r.tier == "worked"


def test_all_rungs_declining_without_a_model_says_so(monkeypatch):
    _tiers(monkeypatch, ("a", _Rung(_outcome(ok=False, reason="no"))))

    r = esc.escalate_table(_engine(), table_schema="raw", table_name="t", dry_run=False)

    assert not r.fixed
    assert r.needs_a_person
    assert r.reason == "heuristics_declined_and_no_model"


# ── dry run ────────────────────────────────────────────────────


def test_a_dry_run_never_calls_the_apply_path(monkeypatch):
    rung = _Rung(_outcome())
    _tiers(monkeypatch, ("a", rung))

    r = esc.escalate_table(_engine(), table_schema="raw", table_name="t", dry_run=True)

    assert r.fixed and r.reason == "dry_run"
    assert rung.calls == [True], "un dry run no debe escribir"


# ── el freno ───────────────────────────────────────────────────


def test_a_repair_that_would_break_a_mart_is_refused(monkeypatch):
    rung = _Rung(_outcome(old=["col_1", "b"], new=["monto", "b"]))
    _tiers(monkeypatch, ("a", rung))

    r = esc.escalate_table(
        _engine(),
        table_schema="raw",
        table_name="t",
        guard=lambda cols: ["mart_x"] if "col_1" in cols else [],
        dry_run=False,
    )

    assert not r.fixed
    assert r.blocked_by_marts == ("mart_x",)
    assert r.changed_columns == ("col_1",)
    assert rung.calls == [True], "no debe aplicar lo que el freno rechazó"


def test_the_guard_only_sees_the_columns_that_change(monkeypatch):
    # A mart naming an untouched column is not a reason to refuse; otherwise
    # nothing feeding a mart could ever be repaired.
    seen: list[list[str]] = []
    _tiers(monkeypatch, ("a", _Rung(_outcome(old=["col_1", "b"], new=["monto", "b"]))))

    def guard(cols):
        seen.append(list(cols))
        return []

    r = esc.escalate_table(
        _engine(), table_schema="raw", table_name="t", guard=guard, dry_run=False
    )

    assert r.fixed
    assert seen == [["col_1"]]


def test_a_guard_that_raises_refuses_rather_than_guesses(monkeypatch):
    _tiers(monkeypatch, ("a", _Rung(_outcome())))

    def guard(cols):
        raise RuntimeError("db down")

    r = esc.escalate_table(
        _engine(), table_schema="raw", table_name="t", guard=guard, dry_run=False
    )

    assert not r.fixed
    assert r.blocked_by_marts == ("<guard-no-disponible>",)


def test_no_guard_means_no_brake(monkeypatch):
    _tiers(monkeypatch, ("a", _Rung(_outcome())))
    r = esc.escalate_table(_engine(), table_schema="raw", table_name="t", dry_run=False)
    assert r.fixed


# ── el escalón del modelo ──────────────────────────────────────


@pytest.fixture
def _no_heuristics(monkeypatch):
    _tiers(monkeypatch, ("a", _Rung(_outcome(ok=False, reason="declined"))))


def test_the_model_is_asked_only_after_every_heuristic_declined(_no_heuristics, monkeypatch):
    called: list[str] = []

    async def _fake(engine, *, llm, table_schema, table_name, run_id, dry_run, **kw):
        called.append(table_name)
        return _outcome(reason="applied")

    monkeypatch.setattr("app.application.repair.parse_repair.repair_with_llm_assist", _fake)

    r = esc.escalate_table(
        _engine(), table_schema="raw", table_name="t", llm=object(), dry_run=False
    )

    assert r.fixed and r.tier == "llm"
    assert called == ["t"]


def test_columns_a_mart_reads_are_held_back_not_used_to_refuse(_no_heuristics, monkeypatch):
    # `pg_depend` knows which columns a view actually reads, so a table feeding a
    # mart no longer has to be refused wholesale: the spoken-for columns are kept
    # out of the proposal and the rest still get repaired.
    visto: dict = {}

    async def _fake(engine, **kw):
        visto.update(kw)
        return _outcome(reason="applied")

    monkeypatch.setattr("app.application.repair.parse_repair.repair_with_llm_assist", _fake)

    r = esc.escalate_table(
        _engine(columns=("col_1", "empresa", "_source_dataset_id")),
        table_schema="raw",
        table_name="t",
        guard=lambda cols: ["mart_x"] if "empresa" in cols else [],
        llm=object(),
        dry_run=False,
    )

    assert r.fixed, "una columna reservada no cancela la reparación de las otras"
    assert visto["protected_columns"] == frozenset({"empresa"})


def test_a_table_whose_every_column_is_read_is_still_refused(_no_heuristics, monkeypatch):
    async def _fake(engine, **kw):  # pragma: no cover — must not be reached
        raise AssertionError("no queda nada que reparar")

    monkeypatch.setattr("app.application.repair.parse_repair.repair_with_llm_assist", _fake)

    r = esc.escalate_table(
        _engine(columns=("a", "b", "_source_dataset_id")),
        table_schema="raw",
        table_name="t",
        guard=lambda cols: ["mart_x"],
        llm=object(),
        dry_run=False,
    )

    assert not r.fixed
    assert r.blocked_by_marts == ("mart_x",)


def test_the_collector_own_columns_are_never_offered(_no_heuristics, monkeypatch):
    vistas: list[list[str]] = []

    async def _fake(engine, **kw):
        return _outcome(reason="applied")

    monkeypatch.setattr("app.application.repair.parse_repair.repair_with_llm_assist", _fake)

    def guard(cols):
        vistas.append(list(cols))
        return []

    esc.escalate_table(
        _engine(columns=("col_1", "b", "_source_dataset_id")),
        table_schema="raw",
        table_name="t",
        guard=guard,
        llm=object(),
        dry_run=False,
    )

    assert all("_source_dataset_id" not in v for v in vistas)


def test_a_model_call_that_raises_is_recorded_not_swallowed(_no_heuristics, monkeypatch):
    async def _fake(engine, **kw):
        raise TimeoutError("bedrock")

    monkeypatch.setattr("app.application.repair.parse_repair.repair_with_llm_assist", _fake)

    r = esc.escalate_table(
        _engine(), table_schema="raw", table_name="t", llm=object(), dry_run=False
    )

    assert not r.fixed
    assert r.reason == "raised:TimeoutError"
    assert r.attempts[-1].tier == "llm"


def test_a_model_that_declines_leaves_the_table_alone(_no_heuristics, monkeypatch):
    async def _fake(engine, **kw):
        return _outcome(ok=False, reason="verification_refused", old=["a"], new=["a"])

    monkeypatch.setattr("app.application.repair.parse_repair.repair_with_llm_assist", _fake)

    r = esc.escalate_table(
        _engine(), table_schema="raw", table_name="t", llm=object(), dry_run=False
    )

    assert not r.fixed and r.tier is None
    assert r.reason == "verification_refused"


# ── el reporte ─────────────────────────────────────────────────


def test_the_result_says_which_rung_and_what_was_tried(monkeypatch):
    _tiers(
        monkeypatch,
        ("uno", _Rung(_outcome(ok=False, reason="no"))),
        ("dos", _Rung(_outcome())),
    )

    r = esc.escalate_table(
        _engine(), table_schema="raw", table_name="t", run_id=uuid.uuid4(), dry_run=False
    )

    assert r.as_log_dict() == {
        "table": "raw.t",
        "fixed": True,
        "tier": "dos",
        "reason": "applied",
        "tried": ["uno", "dos"],
        "blocked_by_marts": [],
    }
