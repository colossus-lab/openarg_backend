"""Tests para sacar de circulación un mart vacío y devolverlo cuando vuelve.

`row_count_drift` detectaba esto hace semanas y sólo avisaba. Medido en
producción el 2026-09-01, con el aviso en su décima repetición:

    sube_uso_transporte_publico   registro 2.853.291 · filas reales 0
    subsidios_transporte_publico  registro   125.425 · filas reales 0

Un usuario que pregunta por transporte público podía recibir ese mart —elegido
con confianza por sus millones de filas declaradas— y una respuesta vacía.

Lo que más se fija acá es lo que el bloqueo NO debe hacer: pisar a una persona
que bloqueó un mart a mano, y sacar medio catálogo de circulación cuando el
problema es sistémico.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.application.marts.quality.serving_gate import (
    AUTO_PREFIX,
    MAX_PER_RUN,
    block_empty,
    unblock_if_recovered,
)


def _engine(rowcount=1, raises=False):
    engine = MagicMock()
    if raises:
        engine.begin.side_effect = RuntimeError("db caída")
        return engine, None
    ctx = engine.begin.return_value.__enter__.return_value
    ctx.execute.return_value.rowcount = rowcount
    return engine, ctx


# ── bloquear ───────────────────────────────────────────────────


def test_un_mart_vacio_sale_de_circulacion():
    engine, ctx = _engine()
    assert block_empty(engine, "sube_uso_transporte_publico", stored=2_853_291) is True


def test_el_bloqueo_corrige_tambien_el_conteo():
    # Bloquear sin corregir deja la mentira en pie para todo lo demás que lea
    # `last_row_count`.
    engine, ctx = _engine()
    block_empty(engine, "m", stored=100)
    sql = str(ctx.execute.call_args[0][0])
    assert "last_row_count = 0" in sql
    assert "serving_blocked = TRUE" in sql


def test_la_razon_dice_cuanto_decia_el_registro():
    engine, ctx = _engine()
    block_empty(engine, "m", stored=2_853_291)
    razon = ctx.execute.call_args[0][1]["reason"]
    assert razon.startswith(AUTO_PREFIX)
    assert "2,853,291" in razon


def test_no_vuelve_a_bloquear_lo_ya_bloqueado():
    engine, ctx = _engine()
    block_empty(engine, "m", stored=1)
    assert "NOT COALESCE(serving_blocked, FALSE)" in str(ctx.execute.call_args[0][0])


# ── devolver ───────────────────────────────────────────────────


def test_un_mart_que_volvio_a_tener_filas_vuelve_solo():
    # Si no volviera solo, cada recuperación necesitaría a una persona y el
    # bloqueo automático sería una trampa.
    engine, _ = _engine()
    assert unblock_if_recovered(engine, "m") is True


def test_solo_levanta_bloqueos_automaticos():
    # Un mart bloqueado a mano en su YAML conserva la decisión de quien la tomó.
    engine, ctx = _engine()
    unblock_if_recovered(engine, "m")
    params = ctx.execute.call_args[0][1]
    assert params["marca"] == f"{AUTO_PREFIX}%"
    assert "serving_blocked_reason LIKE" in str(ctx.execute.call_args[0][0])


def test_desbloquear_algo_no_bloqueado_no_cambia_nada():
    engine, _ = _engine(rowcount=0)
    assert unblock_if_recovered(engine, "m") is False


# ── no romper lo que vigila ────────────────────────────────────


def test_una_base_caida_no_rompe_la_auditoria():
    engine, _ = _engine(raises=True)
    assert block_empty(engine, "m", stored=1) is False
    assert unblock_if_recovered(engine, "m") is False


def test_hay_un_tope_por_corrida():
    # Más marts vacíos que esto de golpe es un problema sistémico; sacar medio
    # catálogo en silencio lo empeora en vez de contenerlo.
    assert 0 < MAX_PER_RUN <= 10
