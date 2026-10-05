"""Las tareas del beat se juzgan por la cadencia que declaran, no por la que se aprende.

`ingest_series_tiempo` corría una vez por mes y no hacía nada. Aun si hubiera
dejado de correr, `find_late` no lo habría notado a tiempo: pedía 4 corridas de
historia (cuatro meses) y después 3 veces la cadencia (90 días). La cadencia de
una tarea agendada está escrita en el beat; leerla de ahí la arma desde la
primera corrida.
"""

from __future__ import annotations

from celery.schedules import crontab

from app.infrastructure.celery.tasks import staleness_tasks as stl

_DIA = 86400.0


def test_diaria():
    assert stl.intervalo_maximo_segundos(crontab(hour=18, minute=45)) == _DIA


def test_mensual_toma_el_mes_mas_largo():
    assert stl.intervalo_maximo_segundos(crontab(day_of_month=1, hour=1, minute=30)) == 31 * _DIA
    assert stl.intervalo_maximo_segundos(crontab(day_of_month=15, hour=1, minute=0)) == 31 * _DIA


def test_semanal():
    assert stl.intervalo_maximo_segundos(crontab(day_of_week=0, hour=3, minute=0)) == 7 * _DIA


def test_varias_por_dia_cuenta_el_hueco_de_la_noche():
    # 01:45, 07:45, 13:45, 19:45: el hueco más largo es 19:45 → 01:45 (6 h).
    assert stl.intervalo_maximo_segundos(crontab(hour="1,7,13,19", minute=45)) == 6 * 3600


def test_cada_quince_minutos():
    assert stl.intervalo_maximo_segundos(crontab(minute="*/15")) == 900


def test_algo_que_no_es_un_crontab_no_declara_nada():
    assert stl.intervalo_maximo_segundos(3600) is None
    assert stl.intervalo_maximo_segundos(None) is None


def test_las_cadencias_del_beat_real():
    from app.infrastructure.celery.app import celery_app

    cadencias = stl.cadencias_declaradas(celery_app.conf.beat_schedule)
    tareas = {e["task"] for e in celery_app.conf.beat_schedule.values()}
    assert {f"task:{t}" for t in tareas} == set(cadencias), "toda tarea agendada declara cadencia"
    assert cadencias["task:openarg.ingest_series_tiempo"] == _DIA
    assert cadencias["task:openarg.check_series_freshness"] == _DIA


def test_una_tarea_en_varias_entradas_se_juzga_por_la_mas_frecuente():
    beat = {
        "a": {"task": "openarg.x", "schedule": crontab(hour=3, minute=0)},
        "b": {"task": "openarg.x", "schedule": crontab(minute=0)},
        "c": {"task": "openarg.y", "schedule": "no es un crontab"},
    }
    assert stl.cadencias_declaradas(beat) == {"task:openarg.x": 3600.0}


def test_la_alarma_pasa_las_cadencias_declaradas(monkeypatch):
    vistos: dict = {}

    def _find_late(engine, **kw):
        vistos.update(kw)
        return []

    monkeypatch.setattr("app.application.quality.heartbeat.find_late", _find_late)
    monkeypatch.setattr(stl, "get_sync_engine", lambda: None)

    stl.alert_stale_ingests.run()

    assert vistos["declared"]["task:openarg.ingest_series_tiempo"] == _DIA
