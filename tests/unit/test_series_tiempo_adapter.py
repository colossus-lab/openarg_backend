"""El conector de Series de Tiempo en vivo (el que usa el agente en prod).

Contra una API falsa que se porta como la real (ver ``series_tiempo_fake``).
Lo que se prueba:

- una serie con más observaciones que la página termina en su ÚLTIMO dato,
  no en el 1000°: reservas 174.1 respondía "abril de 2023" y el tipo de
  cambio diario "2005-09-27", con y sin `desde`;
- la cola se pide en orden ascendente con `start`, nunca con `sort=desc`
  (que con una representación pierde los últimos períodos);
- toda representación percent_* llega en % (la interanual llegaba como 0,3354);
- con `desde` y una representación no se pierde el principio de la ventana
  (la interanual con desde=2026-07-01 volvía vacía);
- la acumulada en el año es contra el cierre del año anterior, no contra
  enero como la calcula la API;
- `collapse_aggregation` llega a la API;
- la metadata trae el contrato de frescura.
"""

from __future__ import annotations

import pytest

from tests.unit.series_tiempo_fake import (
    DESEMPLEO_ID,
    EXPO_ID,
    IPC_ID,
    RESERVAS_ID,
    TIPO_CAMBIO_ID,
    FakeSeriesApi,
    desempleo,
    diaria,
    exportaciones_reales,
    ipc_real,
    reservas_mensuales,
)

IPC_LABEL = "IPC. Nivel General Nacional. Base dic 2016. Mensual."


def _sin_orden_inverso(api: FakeSeriesApi) -> None:
    for params in api.series_requests():
        assert "sort" not in params
        assert "last" not in params


async def test_una_serie_de_2345_observaciones_termina_en_su_ultimo_dato() -> None:
    api = FakeSeriesApi(diaria(2345))
    result = await api.adapter().fetch([TIPO_CAMBIO_ID])

    assert result is not None
    assert result.records[-1]["fecha"] == "2026-08-31"
    assert len(result.records) == 1000
    meta = result.metadata
    assert meta["total_fuente"] == 2345
    assert meta["truncada"] is True
    assert meta["ultima_observacion"] == "2026-08-31"
    # Primero la página normal; `count` dice que hay más y se pide la cola.
    requests = api.series_requests()
    assert [r.get("start") for r in requests] == [None, "1345"]
    _sin_orden_inverso(api)


async def test_reservas_mensuales_terminan_en_abril_de_2026_y_no_en_2023() -> None:
    api = FakeSeriesApi(reservas_mensuales())
    result = await api.adapter().fetch([RESERVAS_ID])

    assert result is not None
    assert result.records[-1]["fecha"] == "2026-04-01"
    assert result.metadata["total_fuente"] == 1036
    _sin_orden_inverso(api)


async def test_con_desde_un_rango_mas_largo_que_la_pagina_tambien_trae_la_cola() -> None:
    # Tipo de cambio desde 2020: 2435 observaciones, terminaba en 2022-09-26.
    api = FakeSeriesApi(diaria(2345))
    result = await api.adapter().fetch([TIPO_CAMBIO_ID], start_date="2020-06-01")

    assert result is not None
    assert result.records[-1]["fecha"] == "2026-08-31"
    assert result.metadata["truncada"] is True
    _sin_orden_inverso(api)


async def test_una_serie_corta_es_un_solo_pedido_y_no_esta_truncada() -> None:
    api = FakeSeriesApi(ipc_real())
    result = await api.adapter().fetch([IPC_ID])

    assert result is not None
    assert len(api.series_requests()) == 1
    assert len(result.records) == 117
    assert result.metadata["total_fuente"] == 117
    assert result.metadata["truncada"] is False


async def test_limite_chico_devuelve_las_ultimas() -> None:
    api = FakeSeriesApi(diaria(2345))
    result = await api.adapter().fetch([TIPO_CAMBIO_ID], limit=10)

    assert result is not None
    assert [r["fecha"] for r in result.records][-1] == "2026-08-31"
    assert len(result.records) == 10
    assert result.metadata["truncada"] is True


@pytest.mark.parametrize(
    ("representation", "esperado"),
    [
        # 12.276,766 / 12.076,3937 − 1
        ("percent_change", 1.66),
        # 12.276,766 / 9.193,2441 − 1 (la API daba 0,3354117 con unidades «Índice»)
        ("percent_change_a_year_ago", 33.54),
    ],
)
async def test_el_ipc_con_variaciones_termina_en_agosto_de_2026_y_en_porcentaje(
    representation: str, esperado: float
) -> None:
    api = FakeSeriesApi(ipc_real())
    result = await api.adapter().fetch([IPC_ID], representation=representation)

    assert result is not None
    last = result.records[-1]
    assert last["fecha"] == "2026-08-01"
    assert last[IPC_LABEL] == esperado
    assert result.metadata["unidad"] == "porcentaje"
    assert result.metadata["unit"] == "percent"
    assert "(en %)" in result.metadata["units"]
    _sin_orden_inverso(api)


async def test_la_acumulada_en_el_anio_es_contra_diciembre_y_no_contra_enero() -> None:
    api = FakeSeriesApi(ipc_real())
    result = await api.adapter().fetch(
        [IPC_ID], representation="percent_change_since_beginning_of_year"
    )

    assert result is not None
    last = result.records[-1]
    assert last["fecha"] == "2026-08-01"
    # 12.276,766 / 10.121,3715 (dic-2025) − 1. La API da 17,90: contra enero.
    assert last[IPC_LABEL] == 21.3
    assert result.metadata["unidad"] == "porcentaje"
    # Se calcula sobre los valores: no se le pide la representación a la API.
    assert all("representation_mode" not in r for r in api.series_requests())


async def test_la_interanual_con_desde_reciente_no_vuelve_vacia() -> None:
    api = FakeSeriesApi(ipc_real())
    result = await api.adapter().fetch(
        [IPC_ID], start_date="2026-07-01", representation="percent_change_a_year_ago"
    )

    assert result is not None
    assert [r["fecha"] for r in result.records] == ["2026-07-01", "2026-08-01"]
    assert [r[IPC_LABEL] for r in result.records] == [33.83, 33.54]
    # Se pidió desde 13 meses antes y se recortó acá.
    assert api.series_requests()[0]["start_date"] == "2025-06-01"
    assert result.metadata["truncada"] is False


async def test_la_mensual_con_desde_no_pierde_el_primer_mes() -> None:
    api = FakeSeriesApi(ipc_real())
    result = await api.adapter().fetch(
        [IPC_ID], start_date="2026-06-01", representation="percent_change"
    )

    assert result is not None
    assert [r["fecha"] for r in result.records] == ["2026-06-01", "2026-07-01", "2026-08-01"]
    assert result.records[0][IPC_LABEL] == 1.89


async def test_la_agregacion_llega_a_la_api_sólo_con_collapse() -> None:
    api = FakeSeriesApi(ipc_real())
    adapter = api.adapter()
    await adapter.fetch([IPC_ID], collapse="year", collapse_aggregation="sum")
    await adapter.fetch([IPC_ID], collapse_aggregation="sum")

    con, sin = api.series_requests()
    assert con["collapse"] == "year"
    assert con["collapse_aggregation"] == "sum"
    assert "collapse_aggregation" not in sin


async def test_la_metadata_trae_el_contrato_de_frescura() -> None:
    api = FakeSeriesApi(reservas_mensuales())
    result = await api.adapter().fetch([RESERVAS_ID])

    assert result is not None
    meta = result.metadata
    assert meta["ultima_observacion"] == "2026-04-01"
    assert meta["frecuencia"] == "mensual"
    assert meta["fecha_fin_fuente"] == "2026-04-01"
    assert meta["actualizada_en_fuente"] is False
    assert meta["oficial"] is True
    assert meta["organismo"] == "Banco Central de la República Argentina (BCRA)"
    assert meta["series"][0]["id"] == RESERVAS_ID
    assert "unidad" not in meta  # valores en millones de dólares, no porcentajes


async def test_una_diaria_dice_que_es_diaria() -> None:
    api = FakeSeriesApi(diaria(30))
    result = await api.adapter().fetch([TIPO_CAMBIO_ID])

    assert result is not None
    assert result.metadata["frecuencia"] == "diaria"
    assert result.metadata["truncada"] is False


DESEMPLEO_LABEL = "Tasa de desempleo total. En porcentaje."


@pytest.mark.parametrize(
    ("representation", "esperado", "unidades"),
    [
        # La API da 0,079 con unidades «Porcentaje»: el modelo decía 7,9 % y
        # el respaldo decía 0,079.
        (None, 7.9, "Porcentaje (en %)"),
        # Diferencia en puntos porcentuales: 0,079 − 0,078.
        ("change", 0.1, "(en puntos porcentuales)"),
        # Variación de la tasa: ya entra por percent_*, no se escala dos veces.
        ("percent_change", 1.28, "(en %)"),
    ],
)
async def test_el_desempleo_llega_en_porcentaje_y_no_como_fraccion(
    representation: str | None, esperado: float, unidades: str
) -> None:
    api = FakeSeriesApi(desempleo())
    result = await api.adapter().fetch([DESEMPLEO_ID], representation=representation)

    assert result is not None
    assert result.records[-1]["fecha"] == "2026-04-01"
    assert result.records[-1][DESEMPLEO_LABEL] == esperado
    assert unidades in result.metadata["units"]
    assert result.metadata["unidad"] == "porcentaje"


async def test_collapse_year_suma_y_deja_afuera_el_anio_en_curso() -> None:
    # Medido el 05-oct: exportaciones con collapse=year y sum dan 2023, 2024 y
    # 2025 (79.703,2 y 87.111,2); 2026, incompleto, no aparece.
    api = FakeSeriesApi(exportaciones_reales())
    result = await api.adapter().fetch([EXPO_ID], collapse="year", collapse_aggregation="sum")

    assert result is not None
    label = "Exportaciones totales. En millones de dólares."
    assert [r["fecha"] for r in result.records] == ["2023-01-01", "2024-01-01", "2025-01-01"]
    assert [round(r[label], 1) for r in result.records][1:] == [79703.2, 87111.2]
    assert result.metadata["frecuencia"] == "anual"
    assert result.metadata["agregacion"] == "sum"
    assert result.metadata["truncada"] is False
    assert "unidad" not in result.metadata

    # La ventana se aplica sobre la fecha de cada período ya agregado: desde
    # 2023-06 no aparece 2023, y hasta 2025-06 aparece 2025 entero.
    result = await api.adapter().fetch(
        [EXPO_ID], start_date="2023-06-01", collapse="year", collapse_aggregation="sum"
    )
    assert result is not None
    assert [r["fecha"] for r in result.records] == ["2024-01-01", "2025-01-01"]
    result = await api.adapter().fetch(
        [EXPO_ID],
        start_date="2024-01-01",
        end_date="2025-06-30",
        collapse="year",
        collapse_aggregation="sum",
    )
    assert result is not None
    assert round(result.records[-1][label], 1) == 87111.2
