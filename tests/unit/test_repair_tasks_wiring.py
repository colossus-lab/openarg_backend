"""Tests for the task-level paths that were shipped without any.

Found by asking which new functions no test mentions rather than by assuming the
modules underneath them were enough. `apply_approved_repairs` writes DDL to
production tables and had no test at all, which is the one shape that should
never ship untested.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.infrastructure.celery.tasks import self_repair_tasks as srt


class _Outcome:
    def __init__(self, ok=True):
        self.ok = ok
        self.old_columns = ["col_1"]
        self.new_columns = ["monto"]
        self.rows_deleted = 1
        self.reason = "applied"
        self.error_message = ""


@pytest.fixture
def approved_queue(monkeypatch):
    """One approved proposal, with the rung it names faked out."""
    state: dict = {"aplicadas": [], "cerradas": [], "outcome": _Outcome()}

    engine = MagicMock()
    monkeypatch.setattr(srt, "get_sync_engine", lambda: engine)
    monkeypatch.setattr(
        "app.application.repair.approval.approved",
        lambda e, limit=25: state.get(
            "cola",
            [{"id": "p1", "table_schema": "raw", "table_name": "t", "tier": "col_n"}],
        ),
    )

    def _rung(engine, *, table_schema, table_name, run_id, dry_run):
        state["aplicadas"].append((table_name, dry_run))
        return state["outcome"]

    monkeypatch.setattr(
        "app.application.repair.escalation.heuristic_tiers", lambda: (("col_n", _rung),)
    )
    monkeypatch.setattr(
        srt, "_mark_applied", lambda e, pid, *, ok: state["cerradas"].append((pid, ok))
    )
    return state


# ── aplicar lo aprobado ────────────────────────────────────────


def test_an_approved_proposal_is_applied_for_real(approved_queue):
    r = srt.apply_approved_repairs()
    assert r["aplicadas"] == 1
    assert approved_queue["aplicadas"] == [("t", False)], "aprobado significa escribir"


def test_applying_closes_the_proposal(approved_queue):
    srt.apply_approved_repairs()
    assert approved_queue["cerradas"] == [("p1", True)]


def test_a_failed_apply_is_recorded_as_failed_not_applied(approved_queue):
    approved_queue["outcome"] = _Outcome(ok=False)
    r = srt.apply_approved_repairs()
    assert r["fallidas"] == 1 and r["aplicadas"] == 0
    assert approved_queue["cerradas"] == [("p1", False)]


def test_a_rung_that_no_longer_exists_does_not_crash_the_run(approved_queue, monkeypatch):
    # Un tier renombrado o retirado deja propuestas colgadas apuntando a él.
    monkeypatch.setattr("app.application.repair.escalation.heuristic_tiers", lambda: ())
    r = srt.apply_approved_repairs()
    assert r["fallidas"] == 1
    assert "tier_desconocido" in r["detalle"][0]
    assert approved_queue["cerradas"] == [], "no se cierra lo que no se intentó"


def test_a_rung_that_raises_is_recorded_and_the_run_continues(approved_queue, monkeypatch):
    def _boom(engine, **kw):
        raise RuntimeError("lock timeout")

    monkeypatch.setattr(
        "app.application.repair.escalation.heuristic_tiers", lambda: (("col_n", _boom),)
    )
    r = srt.apply_approved_repairs()
    assert r["fallidas"] == 1 and "raised" in r["detalle"][0]


def test_an_empty_queue_does_nothing(approved_queue):
    approved_queue["cola"] = []
    r = srt.apply_approved_repairs()
    assert r == {"aplicadas": 0, "fallidas": 0, "detalle": []}


def test_marking_a_proposal_never_raises():
    engine = MagicMock()
    engine.begin.side_effect = RuntimeError("db caída")
    srt._mark_applied(engine, "p1", ok=True)  # no debe levantar
