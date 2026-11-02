"""El mes que el INDEC fechó el 26, y lo que `ingest_indec` hace con él.

En `sh_ipc_aperturas.xls` la columna de marzo de 2026 guarda 2026-03-26 con
formato `mmm-yy`: se ve `mar-26` y en la tabla quedaba el día 26. Pedir marzo con
`periodo = '2026-03-01'` no encontraba nada.
"""

from __future__ import annotations

import io
from datetime import datetime

import pandas as pd
import pytest
from openpyxl import Workbook

from app.infrastructure.celery.tasks import indec_tasks as indec


def _serie_mensual(extras: list[str]) -> pd.DataFrame:
    meses = [f"2025-{m:02d}-01 00:00:00" for m in range(1, 13)]
    periodos = [*meses, *extras]
    return pd.DataFrame(
        {
            "concepto": ["Nivel general"] * len(periodos),
            "periodo": periodos,
            "valor": range(len(periodos)),
        }
    )


def test_el_mes_fechado_el_26_pasa_al_primero_y_conserva_su_valor() -> None:
    df = _serie_mensual(["2026-03-26 00:00:00"])

    salida = indec._snap_monthly_periods_to_first(df)

    marzo = salida[salida["periodo"] == "2026-03-01 00:00:00"]
    assert marzo["valor"].tolist() == [12]
    assert not salida["periodo"].str.contains("-26 ").any()
    assert len(salida) == len(df)


def test_una_serie_diaria_conserva_sus_dias() -> None:
    dias = pd.date_range("2026-01-01", periods=90).strftime("%Y-%m-%d 00:00:00").tolist()
    df = pd.DataFrame({"concepto": "CVS", "periodo": dias, "valor": 1.0})

    assert indec._snap_monthly_periods_to_first(df)["periodo"].tolist() == dias


def test_un_mes_con_dos_columnas_no_se_fusiona() -> None:
    df = _serie_mensual(["2026-03-01 00:00:00", "2026-03-26 00:00:00"])

    salida = indec._snap_monthly_periods_to_first(df)

    assert "2026-03-26 00:00:00" in set(salida["periodo"])


def test_una_serie_fechada_a_fin_de_mes_no_se_toca() -> None:
    fines = (
        pd.date_range("2025-01-31", periods=12, freq="ME").strftime("%Y-%m-%d 00:00:00").tolist()
    )
    df = pd.DataFrame({"concepto": "x", "periodo": fines, "valor": 1.0})

    assert indec._snap_monthly_periods_to_first(df)["periodo"].tolist() == fines


def test_periodos_que_no_son_fechas_no_se_tocan() -> None:
    trimestres = [f"{q}T-{a}" for a in (2023, 2024, 2025) for q in (1, 2, 3, 4)]
    df = pd.DataFrame({"concepto": "x", "periodo": trimestres, "valor": 1.0})

    assert indec._snap_monthly_periods_to_first(df)["periodo"].tolist() == trimestres


def test_el_parser_entrega_el_mes_correcto_con_la_forma_del_archivo_real() -> None:
    wb = Workbook()
    hoja = wb.active
    hoja.title = "Variación mensual aperturas"
    fechas = [datetime(2025, m, 1) for m in range(1, 13)] + [datetime(2026, 3, 26)]
    hoja.append(["Índice de precios al consumidor"])
    hoja.append([])
    hoja.append(["Región GBA", *fechas])
    for celda in hoja[3][1:]:
        celda.number_format = "mmm-yy"
    hoja.append(["Nivel general", *[float(i) for i in range(len(fechas))]])
    hoja.append(["Alimentos y bebidas", *[float(i) for i in range(len(fechas))]])
    buffer = io.BytesIO()
    wb.save(buffer)

    df = indec._parse_xls_sheet(buffer.getvalue(), "Variación mensual aperturas")

    assert df is not None
    assert "2026-03-01 00:00:00" in set(df["periodo"])
    assert "2026-03-26 00:00:00" not in set(df["periodo"])


def test_refresca_los_marts_del_portal_al_terminar(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.infrastructure.celery.tasks import mart_tasks

    pedidos: list[list[str]] = []
    monkeypatch.setattr(mart_tasks.refresh_mart, "apply_async", lambda args: pedidos.append(args))

    indec._refresh_indec_marts()

    assert ["inflacion_argentina"] in pedidos
