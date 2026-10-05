"""`register_via_b_table` dice si registró.

El ETL de series registraba con una identidad que chocaba con el índice UNIQUE
`(schema_name, table_name)` del registro; el `except` de `_db.py` se tragaba el
error y la tarea terminaba en éxito sin latido ni conciliación con
`catalog_resources`. El fallo sigue siendo sólo un log para los demás
conectores, pero ahora quien lo necesita puede verlo.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.infrastructure.celery.tasks import _db


def test_un_insert_que_falla_devuelve_false_sin_levantar():
    engine = MagicMock()
    engine.begin.side_effect = RuntimeError(
        'duplicate key value violates unique constraint "uq_raw_table_versions_table_name"'
    )
    assert (
        _db.register_via_b_table(
            engine,
            resource_identity="series_tiempo::tipo_cambio",
            table_name="cache_series_tipo_cambio",
            schema_name="raw",
            parser_version="p",
            normalization_version="n",
        )
        is False
    )


def test_un_registro_exitoso_devuelve_true(monkeypatch):
    monkeypatch.setattr("app.application.quality.heartbeat.record_ingest", lambda e, r: None)
    monkeypatch.setattr(_db, "_report_if_degenerate", lambda *a, **k: None)
    monkeypatch.setattr(_db, "_trigger_marts_for_portal", lambda *a, **k: None)
    engine = MagicMock()
    assert (
        _db.register_via_b_table(
            engine,
            resource_identity="series_tiempo::series-tiempo-tipo_cambio",
            table_name="cache_series_tipo_cambio",
            schema_name="raw",
            parser_version="p",
            normalization_version="n",
        )
        is True
    )


def test_sin_identidad_no_registra():
    assert _db.register_via_b_table(MagicMock(), resource_identity="", table_name="t") is False
