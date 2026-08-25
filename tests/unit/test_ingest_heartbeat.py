"""Tests for knowing when a source stopped arriving.

The failure this closes: refusing to write a bad batch leaves the good one in
place and nothing says the data froze. The HCDN payroll spent three weeks like
that.

The cadence is learned per resource, so what matters most here is that a weekly
source and an hourly one are judged by different standards, and that a source
with too little history is judged by none.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.application.quality.heartbeat import Late, find_late, record_ingest


class _Row:
    def __init__(self, rid, gap_seconds, cadence_seconds, times_seen=10):
        self.resource_identity = rid
        self.gap_seconds = gap_seconds
        self.cadence_seconds = cadence_seconds
        self.times_seen = times_seen
        self.last_ok_at = None


def _engine(rows=(), raises=False):
    engine = MagicMock()
    ctx = engine.begin.return_value.__enter__.return_value
    if raises:
        engine.begin.side_effect = RuntimeError("db caída")
        return engine, None
    ctx.execute.return_value.fetchall.return_value = list(rows)
    return engine, ctx


# ── registrar ──────────────────────────────────────────────────


def test_an_arrival_is_recorded():
    engine, ctx = _engine()
    record_ingest(engine, "staff_hcdn::snapshots")
    # crea la tabla si falta, y después el latido
    assert ctx.execute.call_count == 2


def test_an_empty_identity_records_nothing():
    engine, ctx = _engine()
    record_ingest(engine, "")
    engine.begin.assert_not_called()


def test_a_database_error_never_reaches_the_connector():
    engine, _ = _engine(raises=True)
    record_ingest(engine, "x")  # no debe levantar


# ── detectar ───────────────────────────────────────────────────


def test_a_late_source_is_reported_with_its_own_cadence():
    semanal = 7 * 86400
    engine, _ = _engine([_Row("staff_hcdn::snapshots", 24 * 86400, semanal)])
    late = find_late(engine)
    assert len(late) == 1
    assert late[0].cadence_days == 7
    assert round(late[0].days_late) == 24
    assert "cada ~7.0 día(s)" in late[0].phrase_es()


def test_nothing_late_reports_nothing():
    engine, _ = _engine([])
    assert find_late(engine) == []


def test_a_database_error_returns_empty_rather_than_raising():
    engine, _ = _engine(raises=True)
    assert find_late(engine) == []


def test_the_query_is_parameterised_by_multiple_and_floor():
    engine, ctx = _engine([])
    find_late(engine, multiple=5.0, floor_hours=48, min_seen=6, limit=7)
    params = ctx.execute.call_args_list[-1][0][1]
    assert params["multiple"] == 5.0
    assert params["floor_seconds"] == 48 * 3600
    assert params["min_seen"] == 6
    assert params["limit"] == 7


def test_the_phrase_is_for_a_person_not_a_machine():
    frase = Late("x", days_late=21.0, cadence_days=7.0, times_seen=9).phrase_es()
    assert "21.0" in frase and "7.0" in frase
