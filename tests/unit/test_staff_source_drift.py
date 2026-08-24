"""Tests for the HCDN payroll mapping and the guard that refuses a blank batch.

Written against a real incident. Until 2026-08-03 the portal shipped `Apellido`
and `Área de Desempeño`; from 2026-08-10 it ships `APELLIDO` and `ESTRUCTURA`.
The lookup asked for two exact spellings, matched neither, and mapped every
field to the empty string — so 3,743 employees became one blank row that the
mart served as the payroll of the Chamber of Deputies for three weeks.

Nothing failed. The download succeeded, the insert succeeded, the mart built.
That is the shape being pinned here: not a break, a success that means nothing.
"""

from __future__ import annotations

from app.infrastructure.celery.tasks.staff_tasks import (
    _degenerate_reason,
    _normalize_record,
)

# Exactly what the portal returned on 2026-08-24.
_HOY = {
    "_id": 1,
    "APELLIDO": "COSTA",
    "NOMBRE": "JUAN PABLO",
    "LEGAJO": 804905,
    "ESCALAFON": "A-3-T",
    "ESTRUCTURA": "AGENTE AFECTADO A BLOQUE POLITICO",
    "CONVENIO": "PLANTA TEMPORARIA (LEY 24.600)",
}

# What it returned until 2026-08-03.
_ANTES = {
    "Legajo": 804905,
    "Apellido": "COSTA",
    "Nombre": "JUAN PABLO",
    "Escalafón": "A-3-T",
    "Área de Desempeño": "AGENTE AFECTADO A BLOQUE POLITICO",
    "Convenio": "PLANTA TEMPORARIA (LEY 24.600)",
}


# ── el mapeo ───────────────────────────────────────────────────


def test_reads_the_uppercase_names_the_portal_ships_today():
    r = _normalize_record(_HOY)
    assert r["legajo"] == "804905"
    assert r["apellido"] == "COSTA"
    assert r["nombre"] == "JUAN PABLO"
    assert r["escalafon"] == "A-3-T"
    assert r["convenio"] == "PLANTA TEMPORARIA (LEY 24.600)"


def test_estructura_is_the_new_name_for_area_de_desempeno():
    assert _normalize_record(_HOY)["area_desempeno"] == "AGENTE AFECTADO A BLOQUE POLITICO"


def test_the_old_names_still_work():
    # Kept alongside the new ones: this field has changed twice and may change
    # back, and a mapping that only knows today's spelling breaks again.
    assert _normalize_record(_ANTES) == _normalize_record(_HOY)


def test_a_name_in_any_case_is_matched():
    assert _normalize_record({"ApElLiDo": "COSTA"})["apellido"] == "COSTA"


def test_surrounding_whitespace_in_a_field_name_does_not_hide_it():
    assert _normalize_record({" LEGAJO ": 7})["legajo"] == "7"


def test_a_missing_field_is_empty_not_absent():
    assert _normalize_record({"LEGAJO": 1})["convenio"] == ""


def test_a_null_value_reads_as_empty():
    assert _normalize_record({"APELLIDO": None})["apellido"] == ""


# ── el guardián ────────────────────────────────────────────────


def test_a_healthy_batch_is_not_refused():
    assert _degenerate_reason([_normalize_record(_HOY)] * 10) is None


def test_a_batch_with_no_legajo_is_refused():
    # The exact incident: without `legajo` the upsert key collapses and a whole
    # payroll becomes one row.
    lote = [_normalize_record({"Apellido": "COSTA"}) for _ in range(100)]
    reason = _degenerate_reason(lote)
    assert reason is not None
    assert "legajo" in reason


def test_the_refusal_names_the_fields_it_actually_received():
    # So the message says what changed, instead of only that something did.
    reason = _degenerate_reason([_normalize_record({"APELLIDO": "X"})])
    assert reason and "apellido" in reason


def test_a_fully_blank_batch_is_refused():
    lote = [{"legajo": "", "apellido": "", "nombre": ""} for _ in range(10)]
    assert _degenerate_reason(lote) is not None


def test_a_minority_of_bad_rows_is_not_a_reason_to_refuse():
    # Real payrolls carry a few incomplete records; that is a bad batch, not a
    # source that stopped mapping.
    buenos = [_normalize_record(_HOY) for _ in range(90)]
    malos = [_normalize_record({"APELLIDO": "X"}) for _ in range(10)]
    assert _degenerate_reason(buenos + malos) is None


def test_an_empty_batch_is_somebody_else_s_problem():
    # `snapshot_staff` returns early on zero records; this guard must not claim
    # that case as well or the reported reason would be wrong.
    assert _degenerate_reason([]) is None
