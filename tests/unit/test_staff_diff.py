"""Unit tests for the staff-snapshot diff (FIX-008).

Covers FR-006 / FR-009 / FR-010 / FR-013 from
``specs/002-connectors/002f-staff/spec.md``:

- Altas, bajas, AND field-change updates are detected between two
  consecutive snapshots.
- Updates only fire for ``area_desempeno``, ``escalafon``, or
  ``convenio`` — never for ``apellido`` / ``nombre`` noise.
- Empty ``changes_json`` never produces an event (FR-010).
- First-run returns an empty event list for all three kinds
  (FR-013).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.infrastructure.celery.tasks.staff_tasks import _compute_staff_changes

_NOW = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)


def _prev(legajo: str, **kwargs) -> dict:
    """Build a previous-snapshot row (dict shape from the SELECT)."""
    return {
        "apellido": kwargs.get("apellido", "APELLIDO"),
        "nombre": kwargs.get("nombre", "Nombre"),
        "area_desempeno": kwargs.get("area_desempeno", "DESPACHO DIPUTADO A"),
        "escalafon": kwargs.get("escalafon", "A-1"),
        "convenio": kwargs.get("convenio", "PERMANENTE"),
    }


def _curr(legajo: str, **kwargs) -> dict:
    """Build a current-snapshot normalized record (as _normalize_record would)."""
    return {
        "legajo": legajo,
        "apellido": kwargs.get("apellido", "APELLIDO"),
        "nombre": kwargs.get("nombre", "Nombre"),
        "escalafon": kwargs.get("escalafon", "A-1"),
        "area_desempeno": kwargs.get("area_desempeno", "DESPACHO DIPUTADO A"),
        "convenio": kwargs.get("convenio", "PERMANENTE"),
    }


# ── First run ───────────────────────────────────────────────


def test_first_run_returns_empty_changes():
    """FR-013: a cold start emits no events regardless of the current payload."""
    current = [_curr("1001"), _curr("1002"), _curr("1003")]
    changes, alta_count, baja_count, update_count = _compute_staff_changes(
        prev_by_legajo={},
        current=current,
        now=_NOW,
        is_first_run=True,
    )
    assert changes == []
    assert alta_count == 0
    assert baja_count == 0
    assert update_count == 0


# ── Altas ───────────────────────────────────────────────────


def test_alta_detected_for_new_legajo():
    """An alta is a legajo present in current but absent in prev."""
    prev = {"1001": _prev("1001")}
    current = [_curr("1001"), _curr("1002")]  # 1002 is new
    changes, alta_count, baja_count, update_count = _compute_staff_changes(
        prev, current, _NOW, is_first_run=False
    )
    altas = [c for c in changes if c["tipo"] == "alta"]
    assert len(altas) == 1
    assert altas[0]["legajo"] == "1002"
    assert altas[0]["changes_json"] is None
    assert alta_count == 1
    assert baja_count == 0
    assert update_count == 0


# ── Bajas ───────────────────────────────────────────────────


def test_baja_detected_for_missing_legajo():
    """A baja is a legajo present in prev but absent in current."""
    prev = {"1001": _prev("1001"), "1002": _prev("1002")}
    current = [_curr("1001")]  # 1002 is gone
    changes, alta_count, baja_count, update_count = _compute_staff_changes(
        prev, current, _NOW, is_first_run=False
    )
    bajas = [c for c in changes if c["tipo"] == "baja"]
    assert len(bajas) == 1
    assert bajas[0]["legajo"] == "1002"
    assert bajas[0]["changes_json"] is None
    assert alta_count == 0
    assert baja_count == 1
    assert update_count == 0


# ── Updates (the point of FIX-008) ─────────────────────────


def test_update_detected_on_area_desempeno_change():
    """FR-009: area_desempeno change on a persisting legajo → update event."""
    prev = {"1001": _prev("1001", area_desempeno="DESPACHO DIPUTADO A")}
    current = [_curr("1001", area_desempeno="DESPACHO DIPUTADO B")]
    changes, _, _, update_count = _compute_staff_changes(
        prev, current, _NOW, is_first_run=False
    )
    updates = [c for c in changes if c["tipo"] == "update"]
    assert len(updates) == 1
    assert update_count == 1
    event = updates[0]
    assert event["legajo"] == "1001"
    assert event["area_desempeno"] == "DESPACHO DIPUTADO B"
    # FR-010: changes_json names exactly the fields that differed.
    assert event["changes_json"] == {
        "area_desempeno": {
            "from": "DESPACHO DIPUTADO A",
            "to": "DESPACHO DIPUTADO B",
        }
    }


def test_update_detected_on_escalafon_change():
    """Promotions (escalafon change) produce an update event."""
    prev = {"1001": _prev("1001", escalafon="B-2")}
    current = [_curr("1001", escalafon="A-1")]
    changes, _, _, _ = _compute_staff_changes(prev, current, _NOW, is_first_run=False)
    updates = [c for c in changes if c["tipo"] == "update"]
    assert len(updates) == 1
    assert updates[0]["changes_json"] == {"escalafon": {"from": "B-2", "to": "A-1"}}


def test_update_detected_on_convenio_change():
    """Contract changes (convenio) produce an update event."""
    prev = {"1001": _prev("1001", convenio="TEMPORARIO")}
    current = [_curr("1001", convenio="PERMANENTE")]
    changes, _, _, _ = _compute_staff_changes(prev, current, _NOW, is_first_run=False)
    updates = [c for c in changes if c["tipo"] == "update"]
    assert len(updates) == 1
    assert updates[0]["changes_json"] == {
        "convenio": {"from": "TEMPORARIO", "to": "PERMANENTE"}
    }


def test_update_records_multiple_fields_in_one_event():
    """A single event captures all the fields that changed at once."""
    prev = {
        "1001": _prev(
            "1001",
            area_desempeno="DESPACHO DIPUTADO A",
            escalafon="B-2",
        )
    }
    current = [
        _curr(
            "1001",
            area_desempeno="DESPACHO DIPUTADO B",
            escalafon="A-1",
        )
    ]
    changes, _, _, _ = _compute_staff_changes(prev, current, _NOW, is_first_run=False)
    updates = [c for c in changes if c["tipo"] == "update"]
    assert len(updates) == 1  # ONE event, not two
    diff = updates[0]["changes_json"]
    assert diff == {
        "area_desempeno": {"from": "DESPACHO DIPUTADO A", "to": "DESPACHO DIPUTADO B"},
        "escalafon": {"from": "B-2", "to": "A-1"},
    }


def test_name_only_change_does_not_produce_update():
    """FR-009: changes in untracked fields (apellido, nombre) MUST NOT emit events.

    The three tracked fields are area_desempeno, escalafon, convenio.
    A casing or whitespace change in name fields is noise and must not
    pollute the changes feed.
    """
    prev = {"1001": _prev("1001", apellido="PEREZ", nombre="Juan")}
    current = [_curr("1001", apellido="Pérez", nombre="JUAN")]
    changes, alta_count, baja_count, update_count = _compute_staff_changes(
        prev, current, _NOW, is_first_run=False
    )
    assert changes == []
    assert update_count == 0
    assert alta_count == 0
    assert baja_count == 0


def test_no_changes_returns_empty_list():
    """Identical snapshots produce zero events."""
    prev = {"1001": _prev("1001"), "1002": _prev("1002")}
    current = [_curr("1001"), _curr("1002")]
    changes, alta_count, baja_count, update_count = _compute_staff_changes(
        prev, current, _NOW, is_first_run=False
    )
    assert changes == []
    assert alta_count == baja_count == update_count == 0


def test_mixed_altas_bajas_updates_in_single_diff():
    """All three event kinds coexist in one diff correctly."""
    prev = {
        "1001": _prev("1001", area_desempeno="DESPACHO A"),  # will update
        "1002": _prev("1002"),  # stays unchanged
        "1003": _prev("1003"),  # will baja
    }
    current = [
        _curr("1001", area_desempeno="DESPACHO B"),  # updated
        _curr("1002"),  # unchanged
        _curr("1004"),  # new alta
    ]
    changes, alta_count, baja_count, update_count = _compute_staff_changes(
        prev, current, _NOW, is_first_run=False
    )
    assert alta_count == 1
    assert baja_count == 1
    assert update_count == 1
    tipos = sorted(c["tipo"] for c in changes)
    assert tipos == ["alta", "baja", "update"]


def test_timestamp_stamped_on_every_event():
    """detected_at is populated from the `now` parameter on all events."""
    prev = {
        "1001": _prev("1001", area_desempeno="DESPACHO A"),
        "1002": _prev("1002"),
    }
    current = [
        _curr("1001", area_desempeno="DESPACHO B"),  # update
        _curr("1003"),  # alta
    ]
    changes, _, _, _ = _compute_staff_changes(prev, current, _NOW, is_first_run=False)
    for c in changes:
        assert c["detected_at"] == _NOW


# ── Sanity / edge cases ─────────────────────────────────────


@pytest.mark.parametrize("empty_field", ["area_desempeno", "escalafon", "convenio"])
def test_none_to_value_detected_as_change(empty_field):
    """None → concrete value counts as a change (FR-009: literal equality)."""
    prev_info = _prev("1001")
    prev_info[empty_field] = None
    prev = {"1001": prev_info}
    current = [_curr("1001")]
    changes, _, _, update_count = _compute_staff_changes(
        prev, current, _NOW, is_first_run=False
    )
    assert update_count == 1
    assert empty_field in changes[0]["changes_json"]
    assert changes[0]["changes_json"][empty_field]["from"] is None
