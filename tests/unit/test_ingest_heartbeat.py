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


# ── el dead man's switch ───────────────────────────────────────


def test_a_scheduled_task_beats_under_its_own_prefix():
    from app.application.quality.heartbeat import TASK_PREFIX, record_task_run

    engine, ctx = _engine()
    record_task_run(engine, "openarg.cleanup_raw_orphans")
    params = ctx.execute.call_args_list[-1][0][1]
    assert params["rid"] == f"{TASK_PREFIX}openarg.cleanup_raw_orphans"


def test_a_late_task_is_phrased_as_not_running():
    # `cleanup_raw_orphans` reportaba éxito cada hora sin hacer nada durante tres
    # meses. Una corrida que crasheó y una limpia son silencio idéntico.
    tarde = Late("task:openarg.cleanup_raw_orphans", days_late=16.0, cadence_days=1.0, times_seen=90)
    assert tarde.is_task
    assert "que no corre" in tarde.phrase_es()


def test_a_late_resource_is_phrased_as_not_arriving():
    tarde = Late("staff_hcdn::snapshots", days_late=21.0, cadence_days=7.0, times_seen=9)
    assert not tarde.is_task
    assert "que no llega" in tarde.phrase_es()


def test_only_scheduled_tasks_are_recorded(monkeypatch):
    from app.infrastructure.celery import heartbeat_signals as hs

    monkeypatch.setattr(hs, "_scheduled", frozenset({"openarg.agendada"}))
    grabados: list[str] = []
    monkeypatch.setattr(
        "app.application.quality.heartbeat.record_task_run",
        lambda e, n: grabados.append(n),
    )
    monkeypatch.setattr("app.infrastructure.celery.tasks._db.get_sync_engine", lambda: None)

    hs._record(sender=type("S", (), {"name": "openarg.agendada"})())
    hs._record(sender=type("S", (), {"name": "openarg.cualquiera"})())

    assert grabados == ["openarg.agendada"], "el resto sería una manguera sin pregunta que responda"


def test_a_heartbeat_failure_never_fails_the_task(monkeypatch):
    from app.infrastructure.celery import heartbeat_signals as hs

    monkeypatch.setattr(hs, "_scheduled", frozenset({"openarg.agendada"}))

    def _boom(e, n):
        raise RuntimeError("db caída")

    monkeypatch.setattr("app.application.quality.heartbeat.record_task_run", _boom)
    monkeypatch.setattr("app.infrastructure.celery.tasks._db.get_sync_engine", lambda: None)

    hs._record(sender=type("S", (), {"name": "openarg.agendada"})())  # no debe levantar


def test_a_sender_without_a_name_is_ignored():
    from app.infrastructure.celery import heartbeat_signals as hs

    hs._record(sender=None)
