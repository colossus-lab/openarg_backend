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
- la metadata trae el contrato de frescura;
- las tasas con unidades «Porcentaje» que la API da como fracción llegan en
  % (no sólo el desempleo: las familias verificadas de la EPH y la
  pobreza), y no las que ya vienen en % aunque todo su rango quepa en ±1,5
  (gasto en % del PIB, tasa de Japón); con escalas mixtas cada serie dice
  la suya, y con una representación, la de la representación;
- la fecha de fin de la fuente nunca queda antes del último dato traído
  (la metadata de la API puede estar atrasada);
- series de distinta frecuencia pedidas juntas marcan cuál promedió la API.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from tests.unit.series_tiempo_fake import (
    ACTIVIDAD_ID,
    DESEMPLEO_ID,
    DESOCUPACION_AGLOMERADO_ID,
    EMPLEO_ID,
    EXPO_ID,
    GASTO_PIB_CIENCIA_ID,
    GASTO_PIB_EDUCACION_ID,
    GASTO_PIB_TOTAL_ID,
    GASTO_PIB_UNIVERSIDAD_ID,
    HOGARES_POBRES_ID,
    IPC_ID,
    POBREZA_ID,
    RESERVAS_DIARIAS_ID,
    RESERVAS_ID,
    SALARIOS_ID,
    SUBOCUPACION_ID,
    TIPO_CAMBIO_ID,
    FakeSeriesApi,
    desempleo,
    diaria,
    exportaciones_reales,
    gasto_pib,
    ipc_real,
    plazo_fijo_usd,
    reservas_diarias,
    reservas_mensuales,
    salarios,
    serie,
    tasa,
    tasa_japon,
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


# ── tasas en «Porcentaje» que la API da como fracción (H085/H060) ──

ACTIVIDAD_LABEL = "Tasa de actividad total. En porcentaje."
POBREZA_LABEL = (
    "Población con ingresos debajo de línea de pobreza (%) desde 2003. TOTAL. EPH continua."
)


@pytest.mark.parametrize(
    ("sid", "esperado"),
    [
        # Valores de la API del 06-oct: 0,489, 0,45, 0,115, 0,269 y 0,323
        # con unidades «Porcentaje…». Sólo el desempleo se escalaba.
        (ACTIVIDAD_ID, 48.9),
        (EMPLEO_ID, 45.0),
        (SUBOCUPACION_ID, 11.5),
        (HOGARES_POBRES_ID, 26.9),
        (POBREZA_ID, 32.3),
        # Desocupación por aglomerado 1974-2003: 0,166.
        (DESOCUPACION_AGLOMERADO_ID, 16.6),
    ],
)
async def test_las_tasas_en_porcentaje_dadas_como_fraccion_llegan_en_porcentaje(
    sid: str, esperado: float
) -> None:
    api = FakeSeriesApi(tasa(sid))
    result = await api.adapter().fetch([sid])

    assert result is not None
    assert next(v for k, v in result.records[-1].items() if k != "fecha") == esperado
    assert result.metadata["unidad"] == "porcentaje"
    assert result.metadata["value_scale"] == "percentage_points"
    assert "(en %)" in result.metadata["units"]
    assert result.metadata["series"][0]["escalada_a_porcentaje"] is True


async def test_la_diferencia_de_una_tasa_en_fraccion_va_en_puntos_porcentuales() -> None:
    api = FakeSeriesApi(tasa(ACTIVIDAD_ID))
    result = await api.adapter().fetch([ACTIVIDAD_ID], representation="change")

    assert result is not None
    # 0,489 − 0,486
    assert result.records[-1][ACTIVIDAD_LABEL] == 0.3
    assert "(en puntos porcentuales)" in result.metadata["units"]


def _tasa_0_100() -> dict[str, Any]:
    # «(0-100)» dice la escala aunque los valores sean chicos (y aunque el id
    # sea de una familia que viene como fracción).
    return serie(
        "43.2_TASA_0_100",
        [("2026-01-01", 0.9), ("2026-02-01", 1.1)],
        description="Una tasa en 0-100",
        units="Porcentaje (0-100)",
    )


def _tasa_sin_rango() -> dict[str, Any]:
    # Sin el rango de la serie en la metadata no se sabe: no se escala.
    return serie(
        "43.2_TASA_SIN_RANGO",
        [("2026-01-01", 0.48), ("2026-02-01", 0.49)],
        description="Una tasa sin rango",
        units="Porcentaje",
        with_range=False,
    )


@pytest.mark.parametrize(
    ("factory", "desde"),
    [
        # «Porcentaje» y ya en %: desde 2026-03 vale 1,28 y 1,04, menos de
        # 1,5, pero llegó a 13,75 en 2001 (rango de la metadata).
        (plazo_fijo_usd, "2026-03-01"),
        (_tasa_0_100, None),
        (_tasa_sin_rango, None),
        # Ya en % y con TODO el rango dentro de ±1,5: la ciencia y técnica en
        # % del PIB (0,18 a 0,32) salía 26,74 «(en %)» y la tasa overnight de
        # Japón (−0,1 a 0,75) 75,0. El rango no alcanza: fuera de las familias
        # verificadas como fracción no se escala.
        (lambda: gasto_pib(GASTO_PIB_CIENCIA_ID), None),
        (tasa_japon, None),
    ],
)
async def test_un_porcentaje_que_ya_viene_en_porcentaje_o_sin_rango_no_se_escala(
    factory: Callable[[], dict[str, Any]], desde: str | None
) -> None:
    s = factory()
    result = await FakeSeriesApi(s).adapter().fetch([s["id"]], start_date=desde)

    assert result is not None
    expected = [v for f, v in s["data"] if desde is None or f >= desde]
    assert [next(v for k, v in r.items() if k != "fecha") for r in result.records] == expected
    assert "unidad" not in result.metadata
    assert "(en %)" not in result.metadata["units"]
    assert not result.metadata["series"][0].get("escalada_a_porcentaje")


async def test_el_desempleo_sin_rango_en_la_metadata_se_escala_igual() -> None:
    # Está en la lista verificada a mano: no depende del rango.
    s = desempleo()
    del s["field"]["min_value"], s["field"]["max_value"]
    result = await FakeSeriesApi(s).adapter().fetch([DESEMPLEO_ID])

    assert result is not None
    assert result.records[-1][DESEMPLEO_LABEL] == 7.9


async def test_desempleo_y_actividad_juntas_llegan_las_dos_en_porcentaje() -> None:
    # Antes, la misma fila traía 7,9 y 0,489 bajo una sola unidad «Porcentaje».
    api = FakeSeriesApi(desempleo(), tasa(ACTIVIDAD_ID))
    result = await api.adapter().fetch([DESEMPLEO_ID, ACTIVIDAD_ID], start_date="2026-01-01")

    assert result is not None
    last = result.records[-1]
    assert (last[DESEMPLEO_LABEL], last[ACTIVIDAD_LABEL]) == (7.9, 48.9)
    assert result.metadata["unidad"] == "porcentaje"


async def test_con_escalas_mixtas_cada_serie_dice_la_suya() -> None:
    api = FakeSeriesApi(desempleo(), salarios())
    result = await api.adapter().fetch([DESEMPLEO_ID, SALARIOS_ID], start_date="2025-10-01")

    assert result is not None
    assert result.records[-1][DESEMPLEO_LABEL] == 7.9
    by_id = {s["id"]: s for s in result.metadata["series"]}
    assert by_id[DESEMPLEO_ID]["escalada_a_porcentaje"] is True
    assert "en %" in by_id[DESEMPLEO_ID]["unidades"]
    assert not by_id[SALARIOS_ID].get("escalada_a_porcentaje")
    assert by_id[SALARIOS_ID]["unidades"] == "Índice"
    # No todo el resultado está en %: no se rotula entero.
    assert "unidad" not in result.metadata


async def test_el_gasto_en_porcentaje_del_pib_sale_como_lo_publica_la_api() -> None:
    # En la misma fila salían el total 41,87, educación básica 3,46,
    # universitaria 112,75 y ciencia 26,74: las dos últimas ×100 porque su
    # rango cabe en ±1,5. Las cuatro vienen ya en % del PIB.
    ids = [
        GASTO_PIB_TOTAL_ID,
        GASTO_PIB_EDUCACION_ID,
        GASTO_PIB_UNIVERSIDAD_ID,
        GASTO_PIB_CIENCIA_ID,
    ]
    api = FakeSeriesApi(*(gasto_pib(sid) for sid in ids))
    result = await api.adapter().fetch(ids, start_date="2023-01-01")

    assert result is not None
    assert list(result.records[-1].values()) == [
        "2023-01-01",
        41.86654045227007,
        3.4592789759436595,
        1.1274565845376978,
        0.2673829881871986,
    ]
    assert "unidad" not in result.metadata
    assert not any(s.get("escalada_a_porcentaje") for s in result.metadata["series"])
    assert {s["unidades"] for s in result.metadata["series"]} == {"Porcentaje del PIB"}


@pytest.mark.parametrize(
    ("representation", "unidades"),
    [
        ("percent_change", "Variación porcentual período anterior (en %)"),
        ("percent_change_a_year_ago", "Variación porcentual interanual (en %)"),
        (
            "percent_change_since_beginning_of_year",
            "Variación porcentual acumulada en el año (contra el cierre del año anterior) (en %)",
        ),
    ],
)
async def test_con_una_representacion_cada_serie_lleva_las_unidades_de_la_representacion(
    representation: str, unidades: str
) -> None:
    # IPC + salarios en percent_change: cada serie decía «Índice» y los
    # valores eran variaciones en %.
    api = FakeSeriesApi(ipc_real(), salarios())
    result = await api.adapter().fetch(
        [IPC_ID, SALARIOS_ID], start_date="2026-01-01", representation=representation
    )

    assert result is not None
    assert result.metadata["unidad"] == "porcentaje"
    assert [s["unidades"] for s in result.metadata["series"]] == [unidades, unidades]


async def test_la_diferencia_con_escalas_mixtas_dice_puntos_porcentuales_en_cada_serie() -> None:
    api = FakeSeriesApi(desempleo(), salarios())
    result = await api.adapter().fetch(
        [DESEMPLEO_ID, SALARIOS_ID], start_date="2025-10-01", representation="change"
    )

    assert result is not None
    by_id = {s["id"]: s["unidades"] for s in result.metadata["series"]}
    assert by_id[DESEMPLEO_ID] == (
        "Variación respecto del período anterior "
        "(en puntos porcentuales; la API la da como fracción)"
    )
    assert by_id[SALARIOS_ID] == "Variación respecto del período anterior"


# ── fin de la fuente: nunca antes del último dato (H065) ───


async def test_la_fecha_de_fin_de_la_fuente_no_queda_antes_del_ultimo_dato() -> None:
    # 64.2: la metadata dice time_index_end 2026-01-01 y la API ya trae la
    # fila 2026-07-01 (1er semestre de 2026, 32,3 %). El modelo descartó ese
    # dato porque «la fuente llega hasta 2026-01-01».
    api = FakeSeriesApi(tasa(POBREZA_ID))
    result = await api.adapter().fetch([POBREZA_ID])

    assert result is not None
    meta = result.metadata
    assert meta["ultima_observacion"] == "2026-07-01"
    assert meta["fecha_fin_fuente"] == "2026-07-01"
    assert meta["series"][0]["fecha_fin_fuente"] == "2026-07-01"
    assert result.records[-1][POBREZA_LABEL] == 32.3


async def test_un_rango_pasado_no_achica_la_fecha_de_fin_de_la_fuente() -> None:
    api = FakeSeriesApi(reservas_mensuales())
    result = await api.adapter().fetch([RESERVAS_ID], end_date="2020-12-31")

    assert result is not None
    assert result.metadata["ultima_observacion"] == "2020-12-01"
    assert result.metadata["fecha_fin_fuente"] == "2026-04-01"


# ── frecuencias distintas en un pedido: la API promedia (H086) ──

RESERVAS_DIARIAS_LABEL = "Reservas internacionales del BCRA, en millones de dólares"


async def test_diaria_y_mensual_juntas_sin_frecuencia_marcan_el_promedio() -> None:
    api = FakeSeriesApi(reservas_diarias(), reservas_mensuales())
    result = await api.adapter().fetch([RESERVAS_DIARIAS_ID, RESERVAS_ID], start_date="2026-06-01")

    assert result is not None
    assert result.metadata["frecuencia"] == "mensual"
    last = result.records[-1]
    assert last["fecha"] == "2026-08-01"
    # El promedio de agosto, no el saldo al 31 (48.259).
    assert round(last[RESERVAS_DIARIAS_LABEL], 2) == 49700.26
    by_id = {s["id"]: s for s in result.metadata["series"]}
    assert by_id[RESERVAS_DIARIAS_ID]["promediada_por_api"] is True
    assert not by_id[RESERVAS_ID].get("promediada_por_api")
    assert result.metadata["agregada_por_api"] == "promedio"


async def test_con_frecuencia_pedida_o_una_sola_serie_no_hay_promedio_implicito() -> None:
    api = FakeSeriesApi(reservas_diarias(), reservas_mensuales())
    pedida = await api.adapter().fetch(
        [RESERVAS_DIARIAS_ID, RESERVAS_ID],
        start_date="2026-06-01",
        collapse="month",
        collapse_aggregation="end_of_period",
    )
    sola = await api.adapter().fetch([RESERVAS_DIARIAS_ID])

    assert pedida is not None and sola is not None
    assert pedida.records[-1][RESERVAS_DIARIAS_LABEL] == 48259.0
    assert pedida.metadata["agregacion"] == "end_of_period"
    for result in (pedida, sola):
        assert "agregada_por_api" not in result.metadata
        assert not any(s.get("promediada_por_api") for s in result.metadata["series"])
