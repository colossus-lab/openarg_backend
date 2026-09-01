"""Los hallazgos `warn` de la auditoría de marts tienen que salir del canal.

`caba_presupuesto_ejecutado` estuvo 39 % duplicado —un mart de presupuesto
sirviendo totales inflados— y el sistema lo detectó, lo guardó en
`ingestion_findings` y **nunca lo reportó**: el alerta leía sólo `critical`, y
39 % no llega al umbral de 50 %. El check no faltaba; faltaba que se leyera.
"""

from __future__ import annotations

from app.application.quality.alerting import Alert


def test_un_warn_de_mart_va_digerido_no_listado() -> None:
    """Son decenas y ninguno pide acción inmediata: listarlos gastaría el
    presupuesto de atención del canal."""
    assert Alert(kind="mart_audit_warn", key="m", title="t").digest_only


def test_un_critical_de_mart_se_lista() -> None:
    assert not Alert(kind="mart_audit", key="m", title="t").digest_only


def test_el_digest_nombra_cada_clase_por_separado() -> None:
    """Antes decía "N arreglo(s) automático(s)" para todo lo digerido; con dos
    clases distintas eso sería mentira."""
    import inspect

    from app.application.quality import alerting

    src = inspect.getsource(alerting.notify)
    assert "mart(s) con filas repetidas" in src
    assert "arreglo(s) automático(s)" in src


def test_la_consulta_del_alerta_lee_warn_y_critical() -> None:
    from app.infrastructure.celery.tasks.quality_alert_tasks import (
        _MART_AUDIT_FINDINGS_SQL,
    )

    sql = str(_MART_AUDIT_FINDINGS_SQL)
    assert "'critical', 'warn'" in sql or '"critical", "warn"' in sql
    assert "severity" in sql
    # sigue sin traer los ya resueltos: un mart arreglado deja de alertar solo
    assert "resolved_at IS NULL" in sql
