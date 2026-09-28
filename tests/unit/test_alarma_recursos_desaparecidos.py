"""Avisar cuando un portal borra recursos, sin convertirlo en ruido.

Un recurso que el portal eliminó no es un error nuestro y por eso no
aparece en ningún tablero: el colector lo reintenta, recibe 404, lo anota
y sigue. Medido en prod el 2026-09-28, con la query de esta alerta:

    datos_gob_ar          294 recursos
    neuquen_legislatura    72
    pami                   15
    cordoba_estadistica    12

294 recursos del portal principal que ya no existen en el origen — y eso
sólo se veía consultando la base a mano. (Encaja con la migración de
datos.gob.ar a CKAN 2.11.5, que regeneró los identificadores.)

La decisión de diseño que importa es **agrupar por portal**: alertar
recurso por recurso serían 409 mensajes que nadie lee, y el módulo de
alerting existe precisamente porque un canal que se silencia es peor que
no tener canal. Cuatro frases accionables valen más que 409 líneas.
"""

from __future__ import annotations

import pytest

from app.infrastructure.celery.tasks.quality_alert_tasks import _escala


class TestEscala:
    """El `key` de un Alert es la identidad del problema, no la del avistamiento.

    Con el portal solo, un portal que borra recursos avisa una vez y nunca
    más, aunque el mes siguiente borre mil. Con el recuento exacto, avisa
    cada vez que aparece uno nuevo. La escala es el punto medio.
    """

    @pytest.mark.parametrize(
        ("n", "esperado"),
        [(10, 10), (12, 10), (19, 10), (72, 70), (99, 90), (294, 200), (1500, 1000)],
    )
    def test_redondea_al_orden_de_magnitud(self, n, esperado):
        assert _escala(n) == esperado

    def test_un_problema_que_no_cambia_de_tamano_no_vuelve_a_hablar(self):
        """12 y 19 recursos son el mismo problema: no son dos alertas."""
        assert _escala(12) == _escala(19)

    def test_un_problema_que_crece_de_orden_vuelve_a_hablar(self):
        """De 72 a 294 sí cambió el tamaño del problema."""
        assert _escala(72) != _escala(294)

    @pytest.mark.parametrize("n", [0, 1, 9])
    def test_no_explota_con_numeros_chicos(self, n):
        assert isinstance(_escala(n), int)
        assert _escala(n) >= 0


def test_la_alerta_agrupa_por_portal_y_no_por_recurso():
    """409 alertas individuales serían el ruido que el canal evita."""
    from app.infrastructure.celery.tasks.quality_alert_tasks import _VANISHED_BY_PORTAL_SQL

    sql = str(_VANISHED_BY_PORTAL_SQL)
    assert "GROUP BY d.portal" in sql
    assert "HAVING count(*) >= :minimo" in sql, "un portal con 2 recursos no es noticia"


def test_solo_cuenta_lo_que_el_portal_borro():
    """Un 500 o un timeout son problemas distintos y no van en esta alerta."""
    from app.infrastructure.celery.tasks.quality_alert_tasks import _VANISHED_BY_PORTAL_SQL

    sql = str(_VANISHED_BY_PORTAL_SQL)
    assert "'%404%'" in sql and "'%410 gone%'" in sql
    assert "error_category = 'download_http_error'" in sql
    # Y con ventana, para que una purga vieja no alerte para siempre.
    assert ":dias" in sql


def test_la_alerta_esta_conectada_a_un_canal_que_corre():
    """Una alerta en una tarea que nadie agenda es una alerta que no existe.

    Es la misma familia del bug que abrió todo este hilo: `recover_stuck_tasks`
    estuvo 36 días despachándose a una cola sin consumidor.
    """
    from app.infrastructure.celery.app import celery_app

    agendadas = {e["task"] for e in (celery_app.conf.beat_schedule or {}).values()}
    assert "openarg.alert_on_quality_signals" in agendadas

    destino = celery_app.conf.task_routes["openarg.alert_on_quality_signals"]["queue"]
    assert destino == "ingest"
