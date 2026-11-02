"""El colector no baja los portales que tienen su propia tarea de carga."""

from __future__ import annotations

from app.application.catalog.carga_propia import sql_sin_carga_propia, tiene_carga_propia
from app.infrastructure.celery.tasks import refresh_tasks, retry_our_failures


def test_indec_tiene_carga_propia() -> None:
    assert tiene_carga_propia("indec")
    assert tiene_carga_propia(" indec ")


def test_los_demas_portales_no() -> None:
    assert not tiene_carga_propia("datos_gob_ar")
    assert not tiene_carga_propia(None)


def test_la_condicion_usa_el_alias() -> None:
    assert sql_sin_carga_propia("x") == " AND x.portal NOT IN ('indec') "


def test_los_barridos_no_despachan_lo_que_el_colector_va_a_saltear() -> None:
    # `collect_dataset` los saltea igual, pero cada uno gasta un lugar del tope
    # del barrido: alcanza con que haya 50 viejos para que no avance nadie más.
    for sql in (refresh_tasks._CANDIDATES_SQL, retry_our_failures._CANDIDATES_SQL):
        assert "d.portal NOT IN ('indec')" in str(sql)
