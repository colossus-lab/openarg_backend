"""Tests for gating on what a repair does, not on which rung proposed it.

The line is what `parse_repair_audit` can walk back. A rename is a mistake
somebody can undo; deleted rows do not come back, and no amount of confidence
upstream changes which of those two a given repair was.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.application.repair.approval import (
    IMPLAUSIBLE_MULTIPLE,
    classify,
    decide,
    propose,
    target_set_is_plausible,
)


class _Outcome:
    def __init__(self, old=None, new=None, rows_deleted=0):
        self.old_columns = old or ["a", "b"]
        self.new_columns = new if new is not None else ["x", "b"]
        self.rows_deleted = rows_deleted


# ── clasificar por lo que hace ─────────────────────────────────


def test_a_rename_is_reversible():
    c = classify(_Outcome())
    assert c.name == "rename" and c.reversible


def test_deleting_rows_is_not():
    c = classify(_Outcome(rows_deleted=3))
    assert c.name == "delete_rows" and not c.reversible
    assert "no las puede devolver" in c.detail


def test_dropping_columns_is_reversible_because_the_proof_travels_with_it():
    # La heurística prueba que están >99 % vacías antes de tocarlas.
    c = classify(_Outcome(old=["a", "b", "c"], new=["a", "c"]))
    assert c.name == "drop_empty_columns" and c.reversible


def test_no_change_is_its_own_class():
    assert classify(_Outcome(old=["a"], new=["a"])).name == "none"


def test_deleting_rows_wins_over_renaming():
    # Una reparación que hace las dos cosas se juzga por la irreversible.
    assert classify(_Outcome(rows_deleted=1)).name == "delete_rows"


# ── validar el alcance ─────────────────────────────────────────


def test_a_normal_amount_of_work_is_plausible():
    ok, _ = target_set_is_plausible(40, 40)
    assert ok


def test_an_order_of_magnitude_more_than_expected_is_a_bug():
    ok, motivo = target_set_is_plausible(40 * IMPLAUSIBLE_MULTIPLE + 1, 40)
    assert not ok
    assert "cambió de significado" in motivo


def test_nothing_to_do_is_always_plausible():
    ok, _ = target_set_is_plausible(0, 40)
    assert ok


# ── la cola ────────────────────────────────────────────────────


def _engine(row=("id",), raises=False):
    engine = MagicMock()
    if raises:
        engine.begin.side_effect = RuntimeError("db caída")
        return engine, None
    ctx = engine.begin.return_value.__enter__.return_value
    ctx.execute.return_value.fetchone.return_value = row
    return engine, ctx


def test_a_proposal_returns_its_id():
    engine, _ = _engine()
    pid = propose(
        engine, table_schema="raw", table_name="t", tier="col_n", outcome=_Outcome(rows_deleted=2)
    )
    assert pid is not None


def test_a_table_that_already_has_a_pending_proposal_does_not_get_another():
    # Un barrido diario armaría una cola de filas idénticas que nadie puede leer.
    engine, _ = _engine(row=None)
    assert (
        propose(engine, table_schema="raw", table_name="t", tier="col_n", outcome=_Outcome())
        is None
    )


def test_a_database_error_does_not_break_the_sweep():
    engine, _ = _engine(raises=True)
    assert propose(engine, table_schema="raw", table_name="t", tier="c", outcome=_Outcome()) is None


def test_deciding_something_that_is_not_pending_changes_nothing():
    engine, _ = _engine(row=None)
    assert decide(engine, "abc", approved=True, who="yo") is False


def test_a_decision_records_who_made_it():
    engine, ctx = _engine()
    assert decide(engine, "abc", approved=True, who="lucho") is True
    params = ctx.execute.call_args_list[-1][0][1]
    assert params["who"] == "lucho" and params["status"] == "approved"


def test_rejecting_is_recorded_as_rejected():
    engine, ctx = _engine()
    decide(engine, "abc", approved=False, who="lucho")
    assert ctx.execute.call_args_list[-1][0][1]["status"] == "rejected"


# ── la superficie admin ────────────────────────────────────────


def test_a_malformed_proposal_id_is_a_not_found_not_a_crash(monkeypatch):
    # El binding lo hace seguro igual, pero dejar pasar un id mal formado
    # devolvía un 500 con el error de la base adentro.
    from fastapi import HTTPException

    from app.presentation.http.controllers.admin import repair_approval_router as r

    try:
        r.decide_proposal("no-es-un-uuid", approve=True)
    except HTTPException as exc:
        assert exc.status_code == 404
    else:  # pragma: no cover
        raise AssertionError("debería haber rechazado")


def test_the_decider_name_is_bounded(monkeypatch):
    from app.presentation.http.controllers.admin import repair_approval_router as r

    vistos: list[str] = []
    monkeypatch.setattr(
        "app.application.repair.approval.decide",
        lambda e, pid, *, approved, who: vistos.append(who) or True,
    )
    monkeypatch.setattr(r, "get_sync_engine", lambda: None)

    r.decide_proposal(str(__import__("uuid").uuid4()), approve=True, who="x" * 500)

    assert len(vistos[0]) == 64
