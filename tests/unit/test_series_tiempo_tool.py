"""Las herramientas `series_tiempo` y `buscar_series` del agente, en uso.

`series_tiempo` corre con el adaptador real contra la API falsa (ver
``series_tiempo_fake``): se mide lo que lee el modelo, no el adaptador suelto.

- reservas: el modelo ve abril de 2026 con "de un total de 1036", no abril de
  2023 con "las últimas 3 de 1000", y un aviso de que la fuente está parada;
- `agregacion` llega a la API (exportaciones anuales: suma, no promedio);
- `variacion` compone sobre los valores: la acumulada de marzo a agosto de
  2026 es 14,58 % y no la suma de las tasas (13,77);
- `buscar_series` compara sin acentos y por palabra completa, y el catálogo
  ya no rotula el EMAE de comercio como "actividad industrial".
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import ToolContext, ToolInputError
from app.application.answers.tools.conectores import BuscarSeries, SeriesTiempo, _tail_for_model
from app.domain.entities.connectors.data_result import DataResult
from tests.unit.series_tiempo_fake import (
    IPC_ID,
    RESERVAS_ID,
    TIPO_CAMBIO_ID,
    FakeSeriesApi,
    diaria,
    ipc_real,
    reservas_mensuales,
    serie,
)


def _ctx(series: Any) -> ToolContext:
    return ToolContext(deps=SimpleNamespace(series=series), req=EngineRequest("q", "u"))


async def _run(api: FakeSeriesApi, args: dict[str, Any]) -> tuple[dict[str, Any], Any]:
    outcome = await SeriesTiempo().run(args, _ctx(api.adapter()))
    return json.loads(outcome.content), outcome


# ── series_tiempo: lo que ve el modelo ─────────────────────


async def test_reservas_el_modelo_ve_abril_2026_de_un_total_de_1036() -> None:
    payload, outcome = await _run(
        FakeSeriesApi(reservas_mensuales()), {"ids": [RESERVAS_ID], "ultimos": 3}
    )

    assert [f["fecha"] for f in payload["filas"]] == ["2026-02-01", "2026-03-01", "2026-04-01"]
    assert "de un total de 1036" in payload["nota"]
    assert payload["ultima_observacion"] == "2026-04-01"
    assert payload["la_fuente_llega_hasta"] == "2026-04-01"
    assert payload["actualizada_en_fuente"] is False
    assert "no la presentes como el dato actual" in payload["aviso"]
    assert outcome.results[0].metadata["truncada"] is True


async def test_tipo_de_cambio_desde_2020_termina_en_agosto_de_2026() -> None:
    payload, _ = await _run(
        FakeSeriesApi(diaria(2345)),
        {"ids": [TIPO_CAMBIO_ID], "desde": "2020-06-01", "ultimos": 1},
    )
    assert payload["filas"][-1]["fecha"] == "2026-08-31"
    assert payload["frecuencia"] == "diaria"


async def test_la_interanual_llega_en_porcentaje_con_la_escala_dicha() -> None:
    payload, _ = await _run(
        FakeSeriesApi(ipc_real()),
        {"ids": [IPC_ID], "representacion": "percent_change_a_year_ago", "ultimos": 1},
    )
    fila = payload["filas"][-1]
    assert fila["fecha"] == "2026-08-01"
    assert 33.54 in fila.values()
    assert "33,54 %" in payload["escala"]


async def test_la_agregacion_llega_a_la_api_con_la_frecuencia() -> None:
    api = FakeSeriesApi(ipc_real())
    await _run(api, {"ids": [IPC_ID], "frecuencia": "year", "agregacion": "sum"})
    params = api.series_requests()[-1]
    assert params["collapse"] == "year"
    assert params["collapse_aggregation"] == "sum"


async def test_la_agregacion_sin_frecuencia_no_se_manda() -> None:
    api = FakeSeriesApi(ipc_real())
    await _run(api, {"ids": [IPC_ID], "agregacion": "sum"})
    assert "collapse_aggregation" not in api.series_requests()[-1]


async def test_una_agregacion_desconocida_vuelve_al_modelo() -> None:
    with pytest.raises(ToolInputError):
        await _run(
            FakeSeriesApi(ipc_real()),
            {"ids": [IPC_ID], "frecuencia": "year", "agregacion": "total"},
        )


# ── series_tiempo: variación calculada en código ───────────


@pytest.mark.parametrize(
    ("desde", "hasta", "esperado"),
    [
        # Acumulada marzo–agosto: índice ago / índice feb − 1. Sumar las seis
        # tasas mensuales da 13,77 (el «≈14 %» de la batería del 02-oct).
        ("2026-02", "2026-08", 14.58),
        # Acumulada enero–agosto, contra diciembre (como publica el INDEC).
        ("2025-12", "2026-08", 21.3),
        # Interanual de agosto.
        ("2025-08", "2026-08", 33.54),
    ],
)
async def test_la_variacion_compone_sobre_los_valores(
    desde: str, hasta: str, esperado: float
) -> None:
    payload, outcome = await _run(
        FakeSeriesApi(ipc_real()), {"ids": [IPC_ID], "variacion": {"desde": desde, "hasta": hasta}}
    )
    fila = payload["filas"][0]
    assert fila["variacion_pct"] == esperado
    assert fila["desde"].startswith(desde)
    assert fila["hasta"].startswith(hasta)
    # Vuelve como un resultado citable, con el cálculo dicho.
    computed = outcome.results[0]
    assert computed.records[0]["variacion_pct"] == esperado
    assert computed.metadata["unidad"] == "porcentaje"
    assert "valor_hasta / valor_desde" in payload["calculo"]


async def test_la_variacion_no_es_la_suma_de_las_tasas() -> None:
    api = FakeSeriesApi(ipc_real())
    tasas, _ = await _run(
        api, {"ids": [IPC_ID], "representacion": "percent_change", "desde": "2026-03-01"}
    )
    suma = round(sum(next(v for k, v in f.items() if k != "fecha") for f in tasas["filas"]), 2)
    variacion, _ = await _run(
        api, {"ids": [IPC_ID], "variacion": {"desde": "2026-02", "hasta": "2026-08"}}
    )
    assert suma == pytest.approx(13.77, abs=0.02)
    assert variacion["filas"][0]["variacion_pct"] == 14.58


async def test_la_variacion_no_usa_un_mes_que_la_serie_no_publico() -> None:
    with pytest.raises(ToolInputError, match="llega hasta 2026-08-01"):
        await _run(
            FakeSeriesApi(ipc_real()),
            {"ids": [IPC_ID], "variacion": {"desde": "2026-02", "hasta": "2026-09"}},
        )


async def test_la_variacion_real_descuenta_la_inflacion() -> None:
    salario = serie(
        "SAL",
        [("2025-01-01", 100.0), ("2025-06-01", 120.0), ("2026-01-01", 150.0)],
        description="Salario de prueba",
    )
    precios = serie(
        "IPCX",
        [("2025-01-01", 200.0), ("2025-06-01", 220.0), ("2026-01-01", 240.0)],
        description="Precios de prueba",
    )
    payload, _ = await _run(
        FakeSeriesApi(salario, precios),
        {
            "ids": ["SAL"],
            "variacion": {"desde": "2025-01", "hasta": "2026-01", "deflactar_con": "IPCX"},
        },
    )
    fila = payload["filas"][0]
    assert fila["variacion_pct"] == 50.0
    assert fila["inflacion_pct"] == 20.0
    # (1 + 0,50) / (1 + 0,20) − 1
    assert fila["variacion_real_pct"] == 25.0


async def test_la_variacion_en_un_rango_largo_de_una_diaria_encuentra_la_base() -> None:
    # 2345 días hasta 2026-08-31: el rango no entra en una página y la base
    # se pide aparte. El valor de cada día es su número de orden.
    api = FakeSeriesApi(diaria(2345))
    payload, _ = await _run(
        api, {"ids": [TIPO_CAMBIO_ID], "variacion": {"desde": "2020-06-30", "hasta": "2026-08"}}
    )
    fila = payload["filas"][0]
    assert fila["desde"] == "2020-06-30"
    assert fila["hasta"] == "2026-08-31"
    assert fila["valor_hasta"] == 2345.0


async def test_la_variacion_no_se_combina_con_una_representacion() -> None:
    with pytest.raises(ToolInputError):
        await _run(
            FakeSeriesApi(ipc_real()),
            {
                "ids": [IPC_ID],
                "representacion": "percent_change",
                "variacion": {"desde": "2026-02", "hasta": "2026-08"},
            },
        )


# ── _tail_for_model con resultados de otros conectores ─────


def test_un_resultado_sin_contrato_conserva_la_nota_de_siempre() -> None:
    result = DataResult(
        source="argentina_datos",
        portal_name="ArgentinaDatos",
        portal_url="",
        dataset_title="Dólar",
        format="json",
        records=[{"fecha": f"2026-09-{d:02d}", "venta": 1500 + d} for d in range(1, 11)],
    )
    payload = _tail_for_model(result, 3)
    assert payload["nota"] == "Se muestran las últimas 3 de 10 observaciones."
    assert "aviso" not in payload
    assert "la_fuente_llega_hasta" not in payload


# ── buscar_series ──────────────────────────────────────────


async def _buscar(texto: str, found: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    series = SimpleNamespace(search=AsyncMock(return_value=found or []))
    outcome = await BuscarSeries().run({"texto": texto}, _ctx(series))
    return json.loads(outcome.content)


def _ids(payload: dict[str, Any]) -> set[str]:
    return {sid for item in payload.get("verificadas", []) for sid in item["ids"]}


async def test_buscar_series_con_acentos() -> None:
    assert IPC_ID in _ids(await _buscar("¿Cuál fue la inflación de agosto?"))
    assert "45.2_ECTDT_0_T_33" in _ids(await _buscar("tasa de desocupación"))
    assert TIPO_CAMBIO_ID in _ids(await _buscar("¿cuánto vale el dólar?"))


async def test_buscar_series_produccion_industrial_es_el_ipi_y_no_el_emae_comercio() -> None:
    ids = _ids(await _buscar("¿Cuánto creció la producción industrial?"))
    assert "453.1_SERIE_ORIGNAL_0_0_14_46" in ids
    assert "11.3_AGCS_2004_M_41" not in ids


@pytest.mark.parametrize(
    "texto",
    [
        "emisiones de gases",  # "emi" dentro de "emisiones"
        "pandemia",
        "cambio climático",  # "cambio" suelto
        "tasa de política monetaria",  # leliq_pases es un factor de la base
        "tasa de politica monetaria",  # sin acento: antes la encontraba
    ],
)
async def test_buscar_series_no_trae_series_ajenas(texto: str) -> None:
    assert _ids(await _buscar(texto)) == set()


async def test_buscar_series_singular_y_plural() -> None:
    assert "74.3_IET_0_M_16" in _ids(await _buscar("exportación de soja"))


async def test_buscar_series_marca_la_discontinuada_y_dice_hasta_cuando_llega() -> None:
    payload = await _buscar(
        "gasto público",
        found=[{"id": "X", "title": "x", "time_index_end": "2023-01-01"}],
    )
    gasto = next(v for v in payload["verificadas"] if "451.3_GPNGPN_0_0_3_30" in v["ids"])
    assert gasto["discontinuada"] is True
    assert payload["series"][0]["hasta"] == "2023-01-01"


async def test_buscar_series_reservas_ofrece_tambien_la_diaria() -> None:
    ids = _ids(await _buscar("reservas internacionales del BCRA"))
    assert {RESERVAS_ID, "92.2_RESERVAS_IRES_0_0_32_40"} <= ids
