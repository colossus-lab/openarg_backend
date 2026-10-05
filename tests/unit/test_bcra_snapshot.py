"""El snapshot diario de cotizaciones del BCRA (ítem 2.4 de la auditoría).

Hasta el 04-oct la tarea pisaba ``raw.cache_bcra_cotizaciones`` con una foto
sin fecha (``to_sql`` replace → TRUNCATE + append) y ponía
``datasets.last_updated_at = now()``. Ahora acumula una fila por
``(fecha, codigoMoneda)`` y la frescura es la fecha del dato.

La parte que depende de Postgres (DDL idempotente, ON CONFLICT, el mart) está
en ``tests/integration/test_bcra_cotizaciones_historia.py``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from app.domain.entities.connectors.data_result import DataResult
from app.infrastructure.celery.tasks import bcra_tasks
from app.infrastructure.celery.tasks.bcra_tasks import (
    _normalize,
    _register_dataset,
    snapshot_bcra,
)

_ART = timezone(timedelta(hours=-3))


def _cotizaciones(fecha: str | None = "2026-10-02") -> DataResult:
    detalle = [
        {
            "codigoMoneda": "USD",
            "descripcion": "DOLAR E.E.U.U.",
            "tipoPase": 0,
            "tipoCotizacion": 1520,
        },
        {
            "codigoMoneda": "REF",
            "descripcion": "DOLAR REFERENCIA COM 3500",
            "tipoPase": "0",
            "tipoCotizacion": "1523.0868",
        },
    ]
    return DataResult(
        source="bcra",
        portal_name="Banco Central de la República Argentina",
        portal_url="https://www.bcra.gob.ar",
        dataset_title="Cotizaciones Cambiarias",
        format="json",
        records=[{"fecha": fecha, **r} for r in detalle],
    )


def test_normalize_deja_afuera_lo_que_no_dice_de_que_dia_es() -> None:
    rows = _normalize(
        [
            {"fecha": "2026-10-02", "codigoMoneda": "USD", "tipoCotizacion": "1520.5"},
            {"fecha": None, "codigoMoneda": "EUR", "tipoCotizacion": 1710.76},
            {"fecha": "2026-10-02", "codigoMoneda": "", "tipoCotizacion": 1},
            {"codigoMoneda": "BRL", "tipoCotizacion": 291.08},
        ]
    )
    assert rows == [
        {
            "fecha": date(2026, 10, 2),
            "codigoMoneda": "USD",
            "descripcion": None,
            "tipoPase": None,
            "tipoCotizacion": 1520.5,
        }
    ]


@patch("app.infrastructure.celery.tasks._db.register_via_b_table")
@patch.object(bcra_tasks, "_register_dataset", return_value=None)
@patch.object(bcra_tasks, "_upsert", return_value=(2, 41, date(2026, 10, 2)))
@patch.object(bcra_tasks, "_fetch_bcra_data")
@patch.object(bcra_tasks, "get_sync_engine")
def test_el_snapshot_acumula_con_fecha_y_no_pisa_la_tabla(
    mock_engine, mock_fetch, mock_upsert, mock_register, mock_rtv
) -> None:
    mock_fetch.return_value = _cotizaciones()
    with patch.object(pd.DataFrame, "to_sql") as to_sql:
        result = snapshot_bcra.run()

    to_sql.assert_not_called()  # ni replace ni TRUNCATE + append
    rows = mock_upsert.call_args.args[1]
    assert {r["fecha"] for r in rows} == {date(2026, 10, 2)}
    assert {r["codigoMoneda"] for r in rows} == {"USD", "REF"}
    assert next(r for r in rows if r["codigoMoneda"] == "REF")["tipoCotizacion"] == 1523.0868

    kwargs = mock_register.call_args.kwargs
    # La frescura es la del dato (viernes 02-oct), no la de la corrida.
    assert kwargs["data_as_of"] == datetime(2026, 10, 2, tzinfo=_ART)
    assert kwargs["row_count"] == 41
    assert "fecha" in list(mock_register.call_args.args[4].columns)
    assert mock_rtv.call_args.kwargs["row_count"] == 41
    assert mock_rtv.call_args.kwargs["schema_name"] == "raw"
    assert result["tables"] == [
        {
            "table": "cache_bcra_cotizaciones",
            "rows": 41,
            "inserted": 2,
            "last_date": "2026-10-02",
        }
    ]


@patch.object(bcra_tasks, "_upsert")
@patch.object(bcra_tasks, "_fetch_bcra_data")
@patch.object(bcra_tasks, "get_sync_engine")
def test_sin_fecha_no_se_escribe_nada(mock_engine, mock_fetch, mock_upsert) -> None:
    mock_fetch.return_value = _cotizaciones(fecha=None)
    result = snapshot_bcra.run()
    mock_upsert.assert_not_called()
    assert result == {"tables": []}


@patch("app.infrastructure.celery.tasks._db.register_via_b_table")
@patch.object(bcra_tasks, "_register_dataset", return_value=None)
@patch.object(bcra_tasks, "_upsert", return_value=(500, 600, date(2026, 10, 2)))
@patch.object(bcra_tasks, "_fetch_historicas")
@patch.object(bcra_tasks, "_fetch_bcra_data")
@patch.object(bcra_tasks, "get_sync_engine")
def test_el_backfill_pide_la_historia_de_cada_moneda(
    mock_engine, mock_fetch, mock_hist, mock_upsert, mock_register, mock_rtv
) -> None:
    mock_fetch.return_value = _cotizaciones()
    mock_hist.return_value = [
        {"fecha": "2026-09-30", "codigoMoneda": "USD", "tipoCotizacion": 1517.0},
        {"fecha": "2026-10-01", "codigoMoneda": "USD", "tipoCotizacion": 1524.5},
    ]
    snapshot_bcra.run(backfill_desde="2025-10-01")

    mock_hist.assert_called_once_with(["REF", "USD"], "2025-10-01", "2026-10-02")
    fechas = sorted({r["fecha"] for r in mock_upsert.call_args.args[1]})
    assert fechas == [date(2026, 9, 30), date(2026, 10, 1), date(2026, 10, 2)]


def _engine() -> tuple[MagicMock, MagicMock]:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = ("11111111-1111-1111-1111-111111111111",)
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


@patch.object(bcra_tasks, "_finalize_cached_dataset", return_value={"ok": True})
def test_last_updated_at_es_la_fecha_del_dato(mock_finalize) -> None:
    engine, conn = _engine()
    as_of = datetime(2026, 10, 2, tzinfo=_ART)
    dataset_id = _register_dataset(
        engine,
        "bcra-cotizaciones",
        "Cotizaciones Cambiarias BCRA",
        "cache_bcra_cotizaciones",
        pd.DataFrame(columns=bcra_tasks.COLUMNS),
        data_as_of=as_of,
        row_count=41,
    )

    assert dataset_id == "11111111-1111-1111-1111-111111111111"
    sql, params = conn.execute.call_args_list[0].args
    assert "last_updated_at = :last_updated" in str(sql)
    assert params["last_updated"] == as_of
    assert params["now"] > as_of  # updated_at sí es la hora de la corrida
    assert params["rows"] == 41
    assert mock_finalize.call_args.kwargs["row_count"] == 41
    assert mock_finalize.call_args.kwargs["columns"] == bcra_tasks.COLUMNS


def test_la_fecha_del_dato_es_la_misma_en_utc_y_en_argentina() -> None:
    as_of = bcra_tasks._data_as_of(date(2026, 10, 2))
    assert as_of is not None
    assert as_of.astimezone(UTC).date() == date(2026, 10, 2)
    assert as_of.astimezone(_ART).date() == date(2026, 10, 2)


@pytest.mark.parametrize("sql_fragment", ['ON CONFLICT (fecha, "codigoMoneda") DO NOTHING'])
def test_el_insert_no_duplica_un_fin_de_semana(sql_fragment: str) -> None:
    """Un sábado la API repite el viernes: el índice único lo absorbe."""
    engine, conn = _engine()
    conn.execute.return_value.one.return_value = (0, None)
    conn.execute.return_value.first.return_value = (1,)
    bcra_tasks._upsert(engine, _normalize(_cotizaciones().records))
    sqls = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any(sql_fragment in s for s in sqls)
    assert not any("TRUNCATE" in s for s in sqls)
    assert any("DELETE FROM raw.cache_bcra_cotizaciones WHERE fecha IS NULL" in s for s in sqls)
