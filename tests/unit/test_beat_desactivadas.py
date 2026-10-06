"""`OPENARG_BEAT_DESACTIVADAS`: sacar entradas del beat sin tocar el código.

Para desplegar código nuevo sin que el beat dispare solo lo que todavía no se
quiere correr (la ingesta diaria de series reescribe 12 tablas de prod a las
18:45; la alarma de frescura y el snapshot del BCRA también corren solos) y
correrlo a mano cuando se decida. El beat lee `beat_schedule` al arrancar y
borra de su archivo las entradas que ya no están, así que con la variable
puesta la tarea no se despacha.
"""

from __future__ import annotations

import logging

import pytest

from app.infrastructure.celery import app as celery_module

_FRENADAS = ("ingest-series-tiempo", "check-series-freshness", "snapshot-bcra")


@pytest.fixture(autouse=True)
def _la_app_de_verdad_sigue_siendo_la_actual():
    # Cada `create_celery()` se instala como app actual de Celery; que los
    # tests que vienen después sigan viendo la del módulo.
    yield
    celery_module.celery_app.set_current()


def test_sin_la_variable_la_agenda_queda_entera(monkeypatch):
    monkeypatch.delenv("OPENARG_BEAT_DESACTIVADAS", raising=False)
    agenda = celery_module.create_celery().conf.beat_schedule
    assert set(_FRENADAS) <= set(agenda)


def test_la_variable_saca_esas_entradas_y_ninguna_otra(monkeypatch):
    monkeypatch.delenv("OPENARG_BEAT_DESACTIVADAS", raising=False)
    entera = set(celery_module.create_celery().conf.beat_schedule)

    monkeypatch.setenv(
        "OPENARG_BEAT_DESACTIVADAS", " ingest-series-tiempo,check-series-freshness ,,snapshot-bcra"
    )
    agenda = set(celery_module.create_celery().conf.beat_schedule)

    assert agenda == entera - set(_FRENADAS)


def test_un_nombre_que_no_es_una_entrada_se_avisa_como_error(monkeypatch, caplog):
    # Un error de tipeo dejaría corriendo justo la tarea que se quería frenar:
    # que se vea en el log del beat, no que pase en silencio.
    monkeypatch.setenv("OPENARG_BEAT_DESACTIVADAS", "ingest-series-tiempos,snapshot-bcra")
    with caplog.at_level(logging.WARNING, logger=celery_module.logger.name):
        agenda = celery_module.create_celery().conf.beat_schedule

    assert "ingest-series-tiempo" in agenda
    assert "snapshot-bcra" not in agenda
    errores = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errores and "ingest-series-tiempos" in errores[0].getMessage()
    avisos = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("snapshot-bcra" in m for m in avisos), "lo que sí sacó también queda en el log"


@pytest.mark.parametrize("valor", ["", " ", ",,"])
def test_vacia_no_saca_nada(monkeypatch, valor):
    monkeypatch.delenv("OPENARG_BEAT_DESACTIVADAS", raising=False)
    entera = set(celery_module.create_celery().conf.beat_schedule)
    monkeypatch.setenv("OPENARG_BEAT_DESACTIVADAS", valor)
    assert set(celery_module.create_celery().conf.beat_schedule) == entera
