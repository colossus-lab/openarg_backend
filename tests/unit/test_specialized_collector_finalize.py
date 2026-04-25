from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from app.application.validation.detector import Severity
from app.infrastructure.celery.tasks.bcra_tasks import _register_dataset as register_bcra_dataset
from app.infrastructure.celery.tasks.collector_tasks import _finalize_cached_dataset
from app.infrastructure.celery.tasks.georef_tasks import (
    _register_dataset as register_georef_dataset,
)


class _FetchOneResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


@patch("app.infrastructure.celery.tasks.collector_tasks._ws0_validate_post_parse")
def test_finalize_cached_dataset_persists_ready_when_validator_passes(mock_validate_post_parse):
    mock_validate_post_parse.return_value = None
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    result = _finalize_cached_dataset(
        mock_engine,
        dataset_id="11111111-1111-1111-1111-111111111111",
        portal="bcra",
        source_id="bcra-cotizaciones",
        table_name="cache_bcra_cotizaciones",
        row_count=10,
        columns=["fecha", "valor"],
        declared_format="json",
        download_url="https://www.bcra.gob.ar/Estadisticas/Datos_Abiertos.asp",
    )

    assert result["ok"] is True
    assert mock_conn.execute.call_count == 2


@patch("app.infrastructure.celery.tasks.collector_tasks._set_error_status")
@patch("app.infrastructure.celery.tasks.collector_tasks._ws0_validate_post_parse")
def test_finalize_cached_dataset_marks_error_when_validator_rejects(
    mock_validate_post_parse,
    mock_set_error_status,
):
    mock_validate_post_parse.return_value = SimpleNamespace(
        detector_name="single_column_html_blob",
        message="html blob",
        severity=Severity.CRITICAL,
    )
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    result = _finalize_cached_dataset(
        mock_engine,
        dataset_id="11111111-1111-1111-1111-111111111111",
        portal="bcra",
        source_id="bcra-cotizaciones",
        table_name="cache_bcra_cotizaciones",
        row_count=1,
        columns=["<!DOCTYPE html>"],
        declared_format="json",
    )

    assert result["ok"] is False
    mock_set_error_status.assert_called_once()


@patch("app.infrastructure.celery.tasks.bcra_tasks._finalize_cached_dataset")
def test_bcra_register_dataset_returns_none_when_finalize_rejects(mock_finalize):
    mock_finalize.return_value = {"ok": False, "status": "rejected"}
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = [
        MagicMock(),
        _FetchOneResult(("11111111-1111-1111-1111-111111111111",)),
    ]
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    df = pd.DataFrame({"fecha": ["2026-04-25"], "valor": [1]})
    result = register_bcra_dataset(
        mock_engine,
        "bcra-cotizaciones",
        "Cotizaciones Cambiarias BCRA",
        "cache_bcra_cotizaciones",
        df,
    )

    assert result is None
    mock_finalize.assert_called_once()


@patch("app.infrastructure.celery.tasks.georef_tasks._finalize_cached_dataset")
def test_georef_register_dataset_returns_dataset_id_when_finalize_accepts(mock_finalize):
    mock_finalize.return_value = {"ok": True, "status": "ready"}
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = [
        MagicMock(),
        _FetchOneResult(("22222222-2222-2222-2222-222222222222",)),
    ]
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    df = pd.DataFrame({"nombre": ["Buenos Aires"]})
    result = register_georef_dataset(mock_engine, "provincias", "cache_georef_provincias", df)

    assert result == "22222222-2222-2222-2222-222222222222"
    mock_finalize.assert_called_once()
