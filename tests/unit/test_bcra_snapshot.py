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


@pytest.fixture
def _hoy(monkeypatch: pytest.MonkeyPatch) -> date:
    hoy = date(2026, 10, 4)
    monkeypatch.setattr(bcra_tasks, "_today_ar", lambda: hoy)
    return hoy


@patch("app.infrastructure.celery.tasks._db.register_via_b_table")
@patch.object(bcra_tasks, "_register_dataset", return_value=None)
@patch.object(bcra_tasks, "_upsert", return_value=(500, 600, date(2026, 10, 2)))
@patch.object(bcra_tasks, "_fetch_historicas")
@patch.object(bcra_tasks, "_fetch_bcra_data")
@patch.object(bcra_tasks, "get_sync_engine")
def test_el_backfill_pide_la_historia_de_cada_moneda(
    mock_engine, mock_fetch, mock_hist, mock_upsert, mock_register, mock_rtv, _hoy
) -> None:
    mock_fetch.return_value = _cotizaciones()
    mock_hist.return_value = (
        [
            {"fecha": "2026-09-30", "codigoMoneda": "USD", "tipoCotizacion": 1517.0},
            {"fecha": "2026-10-01", "codigoMoneda": "USD", "tipoCotizacion": 1524.5},
        ],
        [],
    )
    result = snapshot_bcra.run(backfill_desde="2026-01-02")

    mock_hist.assert_called_once_with(["REF", "USD"], "2026-01-02", "2026-10-02")
    # El día va primero, en su transacción; después el tramo histórico.
    dia, tramo = (c.args[1] for c in mock_upsert.call_args_list)
    assert {r["fecha"] for r in dia} == {date(2026, 10, 2)}
    assert sorted({r["fecha"] for r in tramo}) == [date(2026, 9, 30), date(2026, 10, 1)]
    assert result["backfill"]["inserted"] == 500
    assert result["backfill"]["pendiente"] is None


@patch("app.infrastructure.celery.tasks._db.register_via_b_table")
@patch.object(bcra_tasks, "_register_dataset", return_value=None)
@patch.object(bcra_tasks, "_upsert", return_value=(10, 600, date(2026, 10, 2)))
@patch.object(bcra_tasks, "_fetch_historicas", return_value=([], []))
@patch.object(bcra_tasks, "_fetch_bcra_data")
@patch.object(bcra_tasks, "get_sync_engine")
def test_un_backfill_largo_va_por_tramos_del_mas_nuevo_al_mas_viejo(
    mock_engine, mock_fetch, mock_hist, mock_upsert, mock_register, mock_rtv, _hoy
) -> None:
    """Desde 2002 eran unas 230 mil filas en una sola transacción, contra 300 s."""
    mock_fetch.return_value = _cotizaciones()
    snapshot_bcra.run(backfill_desde="2023-01-01", backfill_hasta="2025-12-31")

    tramos = [(c.args[1], c.args[2]) for c in mock_hist.call_args_list]
    assert tramos == [
        ("2025-01-01", "2025-12-31"),
        ("2024-01-01", "2024-12-31"),
        ("2023-01-01", "2023-12-31"),
    ]
    assert mock_upsert.call_count == 1 + len(tramos)  # una transacción por tramo


@patch("app.infrastructure.celery.tasks._db.register_via_b_table")
@patch.object(bcra_tasks, "_register_dataset", return_value=None)
@patch.object(bcra_tasks, "_upsert", return_value=(2, 41, date(2026, 10, 2)))
@patch.object(bcra_tasks, "_fetch_historicas", return_value=([], []))
@patch.object(bcra_tasks, "_fetch_bcra_data")
@patch.object(bcra_tasks, "get_sync_engine")
def test_si_se_pasa_del_presupuesto_deja_dicho_lo_que_falta(
    mock_engine, mock_fetch, mock_hist, mock_upsert, mock_register, mock_rtv, _hoy, monkeypatch
) -> None:
    monkeypatch.setattr(bcra_tasks, "BACKFILL_PRESUPUESTO_S", -1.0)
    mock_fetch.return_value = _cotizaciones()
    result = snapshot_bcra.run(backfill_desde="2025-01-01")

    mock_hist.assert_not_called()
    assert result["backfill"]["pendiente"] == {
        "backfill_desde": "2025-01-01",
        "backfill_hasta": "2026-10-02",
    }
    assert result["tables"][0]["last_date"] == "2026-10-02"  # el día igual entró


@patch.object(bcra_tasks, "_upsert", return_value=(2, 41, date(2026, 10, 2)))
@patch.object(bcra_tasks, "_fetch_historicas", return_value=([], ["REF", "USD"]))
@patch.object(bcra_tasks, "_fetch_bcra_data")
@patch.object(bcra_tasks, "get_sync_engine")
def test_si_no_responde_ninguna_moneda_la_tarea_falla(
    mock_engine, mock_fetch, mock_hist, mock_upsert, _hoy
) -> None:
    """Antes terminaba "bien" con cero filas históricas y un warning por moneda."""
    mock_fetch.return_value = _cotizaciones()
    with pytest.raises(RuntimeError, match="ninguna moneda"):
        snapshot_bcra.run(backfill_desde="2026-01-01")


@pytest.mark.parametrize(
    ("desde", "hasta"),
    [
        ("01/09/2025", None),  # no ISO: cada moneda fallaba por su lado
        ("2025-9-1", None),
        ("20250901", None),
        ("2026-10-10", None),  # futura
        ("2026-09-01", "2026-08-01"),  # desde > hasta
        ("2002-01-01", None),  # más de cinco años de una
        (None, "2026-09-01"),  # hasta sin desde
    ],
)
@patch.object(bcra_tasks, "_fetch_bcra_data")
@patch.object(bcra_tasks, "get_sync_engine")
def test_un_backfill_mal_pedido_falla_antes_de_tocar_nada(
    mock_engine, mock_fetch, desde, hasta, _hoy
) -> None:
    with pytest.raises(ValueError):
        snapshot_bcra.run(backfill_desde=desde, backfill_hasta=hasta)
    mock_fetch.assert_not_called()
    mock_engine.assert_not_called()


def test_cinco_anios_justos_se_aceptan(_hoy) -> None:
    assert bcra_tasks.parse_backfill("2021-10-05") == (date(2021, 10, 5), None)
    assert bcra_tasks.parse_backfill(None) is None


async def test_el_endpoint_de_admin_rechaza_un_backfill_mal_escrito() -> None:
    from fastapi import HTTPException

    from app.presentation.http.controllers.admin import tasks_router

    with (
        patch.object(tasks_router.celery_app, "send_task") as send,
        pytest.raises(HTTPException) as err,
    ):
        await tasks_router.run_task("snapshot_bcra", {"backfill_desde": "01/09/2025"})
    assert err.value.status_code == 422
    send.assert_not_called()

    with patch.object(tasks_router.celery_app, "send_task") as send:
        await tasks_router.run_task(
            "snapshot_bcra", {"backfill_desde": "2025-10-01", "backfill_hasta": "2026-09-30"}
        )
    assert send.call_args.kwargs["kwargs"] == {
        "backfill_desde": "2025-10-01",
        "backfill_hasta": "2026-09-30",
    }


def test_el_registro_de_admin_dice_la_cola_real() -> None:
    """send_task usa task_routes: la tarea va a `ingest`, no a `collector`."""
    from app.infrastructure.celery.app import celery_app
    from app.presentation.http.controllers.admin.tasks_router import TASK_REGISTRY

    route = celery_app.conf.task_routes["openarg.snapshot_bcra"]
    assert TASK_REGISTRY["snapshot_bcra"]["queue"] == route["queue"] == "ingest"


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


@patch.object(bcra_tasks, "_finalize_cached_dataset", return_value={"ok": True})
def test_la_descripcion_dice_que_es_historia_diaria(mock_finalize) -> None:
    """Después del backfill, un SELECT sin filtro de fecha da un día cualquiera."""
    engine, conn = _engine()
    _register_dataset(
        engine,
        "bcra-cotizaciones",
        "Cotizaciones Cambiarias BCRA",
        "cache_bcra_cotizaciones",
        pd.DataFrame(columns=bcra_tasks.COLUMNS),
        description=bcra_tasks.DESCRIPTION,
    )
    sql, params = conn.execute.call_args_list[0].args
    assert params["desc"] == bcra_tasks.DESCRIPTION
    assert "historia diaria" in params["desc"] and "max(fecha)" in params["desc"]
    # La fila ya existe en staging y prod: el ON CONFLICT la tiene que pisar.
    assert "description = EXCLUDED.description" in str(sql)


@patch("app.infrastructure.celery.tasks._db.register_via_b_table")
@patch.object(bcra_tasks, "_register_dataset", return_value=None)
@patch.object(bcra_tasks, "_upsert", return_value=(2, 41, date(2026, 10, 2)))
@patch.object(bcra_tasks, "_fetch_bcra_data")
@patch.object(bcra_tasks, "get_sync_engine")
def test_el_snapshot_registra_la_descripcion_nueva(
    mock_engine, mock_fetch, mock_upsert, mock_register, mock_rtv
) -> None:
    mock_fetch.return_value = _cotizaciones()
    snapshot_bcra.run()
    assert mock_register.call_args.kwargs["description"] == bcra_tasks.DESCRIPTION


def test_la_nota_del_modo_legacy_avisa_que_hay_varias_fechas() -> None:
    from app.application.pipeline.connectors.cache_table_selection import (
        build_table_compat_notes,
    )

    notas = build_table_compat_notes(["raw.cache_bcra_cotizaciones"])
    assert "max(fecha)" in notas


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
