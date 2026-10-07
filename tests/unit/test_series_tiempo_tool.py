"""Las herramientas `series_tiempo` y `buscar_series` del agente, en uso.

`series_tiempo` corre con el adaptador real contra la API falsa (ver
``series_tiempo_fake``): se mide lo que lee el modelo, no el adaptador suelto.

- reservas: el modelo ve abril de 2026 con "de un total de 1036", no abril de
  2023 con "las últimas 3 de 1000", y un aviso de que la fuente está parada;
- `agregacion` llega a la API (exportaciones anuales: suma, no promedio);
- con `frecuencia`, el último período completo va con nombre y sus meses
  (series_012: el último año completo de exportaciones es 2025, no 2024), y
  un `hasta` que corta antes no se presenta como el último completo; con
  semestres, max o min no se habla de períodos completos: la API trae el
  período sin terminar;
- `variacion` compone sobre los valores: la acumulada de marzo a agosto de
  2026 es 14,58 % y no la suma de las tasas (13,77); entre años de un flujo
  avisa que diciembre contra diciembre no es el total anual;
- un error de la API que no es el de frecuencia inválida no se reintenta
  sin la agregación pedida;
- con varias series, el aviso de atraso nombra cada serie con su fecha;
- las tasas de la EPH llegan en %, y con escalas mixtas el modelo ve la de
  cada columna; las que ya vienen en % (gasto en % del PIB, tasa de Japón)
  no se tocan, y con una representación cada columna lleva las unidades de
  la representación, no «Índice»;
- `la_fuente_llega_hasta` no es anterior al último dato aunque la metadata
  de la API esté atrasada, y `buscar_series` no presenta ese metadato como
  el fin de la serie;
- la pobreza semestral llega con los semestres de la fuente, con `hasta`,
  antes de que se publique el semestre siguiente y pedida junto con otras
  series, y un id inexistente vuelve como pedido inválido y no como fuente
  caída (nueva_08);
- si la serie publicó algo después de lo traído, se dice cuál es el último
  dato (nueva_12);
- series de distinta frecuencia pedidas juntas avisan que la API promedió;
- `buscar_series` compara sin acentos y por palabra completa, y el catálogo
  ya no rotula el EMAE de comercio como "actividad industrial";
- «inflación mayorista» verifica el IPIM e «inflación Misiones» el IPC del
  Noreste, no el IPC nacional (prueba de calidad del 06-oct, nueva_13 y 15).
"""

from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import ToolContext, ToolInputError
from app.application.answers.tools.conectores import BuscarSeries, SeriesTiempo, _tail_for_model
from app.application.quality.data_age import freshness_notices
from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from tests.unit.series_tiempo_fake import (
    ACTIVIDAD_ID,
    DESEMPLEO_GRAN_ROSARIO_ID,
    DESEMPLEO_ID,
    EXPO_ID,
    GASTO_PIB_CIENCIA_ID,
    GASTO_PIB_EDUCACION_ID,
    GASTO_PIB_TOTAL_ID,
    GASTO_PIB_UNIVERSIDAD_ID,
    IMPO_ID,
    IPC_ID,
    IPC_NORESTE_ID,
    IPIM_ID,
    POBREZA_GRAN_ROSARIO_ID,
    POBREZA_GRAN_ROSARIO_PUNTUAL_ID,
    POBREZA_ID,
    RESERVAS_DIARIAS_ID,
    RESERVAS_ID,
    SALARIOS_ID,
    TASA_JAPON_ID,
    TIPO_CAMBIO_ID,
    FakeSeriesApi,
    desempleo,
    desempleo_gran_rosario,
    diaria,
    exportaciones_reales,
    gasto_pib,
    importaciones_reales,
    ipc_noreste_real,
    ipc_real,
    ipim_real,
    pobreza_con_metadata_atrasada,
    pobreza_gran_rosario_puntual,
    pobreza_gran_rosario_real,
    reservas_diarias,
    reservas_mensuales,
    salarios,
    serie,
    tasa,
    tasa_japon,
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
    payload, _ = await _run(api, {"ids": [IPC_ID], "frecuencia": "year", "agregacion": "sum"})
    params = api.series_requests()[-1]
    assert params["collapse"] == "year"
    assert params["collapse_aggregation"] == "sum"
    # El IPC arranca en 2016-12: el primer año agregado es 2017, entero.
    assert payload["filas"][0]["fecha"] == "2017-01-01"


async def test_con_frecuencia_dice_que_los_periodos_estan_completos() -> None:
    # Después de «exportaciones 2025, year+sum» el modelo pedía los 12 meses
    # para comprobar que el año estaba entero (staging, 04 y 05-oct): una
    # vuelta más. La API sólo agrega períodos completos; se lo dice.
    payload, _ = await _run(
        FakeSeriesApi(exportaciones_reales()),
        {"ids": [EXPO_ID], "frecuencia": "year", "agregacion": "sum", "desde": "2025-01-01"},
    )
    assert [f["fecha"] for f in payload["filas"]] == ["2025-01-01"]
    # La frase sigue siendo la primera; después va el año con nombre (series_012).
    assert payload["periodos"].startswith(
        "Cada fila es un año completo: la API no agrega años sin terminar. "
    )
    assert "El último año completo es 2025" in payload["periodos"]


# ── el último año completo, con nombre (batería v3, series_012) ──


async def test_exportaciones_anuales_nombran_2025_como_el_ultimo_anio_completo() -> None:
    """series_012: «¿Cuánto exportó la Argentina en el último año completo?».

    El agente pidió exactamente esto (year+sum, sin ventana), vio la fila
    2025 con 87.111 y respondió 2024 (79.703): «los datos de 2025 corresponden
    a los meses ya publicados» (05 y 06-oct, tres corridas). Lo que leía:
    `ultima_observacion` 2025-01-01 (un día de enero), `la_fuente_llega_hasta`
    2026-08-01 sobre filas anuales y una nota genérica. La herramienta nombra
    el año y dice por qué 2026 no tiene fila.
    """
    payload, _ = await _run(
        FakeSeriesApi(exportaciones_reales()),
        {"ids": [EXPO_ID], "frecuencia": "year", "agregacion": "sum"},
    )
    ultima = payload["filas"][-1]
    assert ultima["periodo"] == "2025"
    assert round(ultima["Exportaciones totales. En millones de dólares."], 1) == 87111.2
    periodos = payload["periodos"]
    assert periodos.startswith("Cada fila es un año completo: la API no agrega años sin terminar.")
    assert "2025-01-01 es el año 2025 entero" in periodos
    assert "El último año completo es 2025: tiene sus 12 meses, de enero a diciembre." in periodos
    assert (
        "2026 no tiene fila porque todavía no tiene sus 12 meses: la fuente llega hasta "
        "2026-08-01." in periodos
    )


async def test_la_balanza_anual_nombra_el_mismo_anio_para_las_dos_series() -> None:
    # Exportaciones e importaciones (las dos de la balanza, 74.3) terminan en
    # 2026-08: una sola frase, sin repetirla por serie.
    payload, _ = await _run(
        FakeSeriesApi(exportaciones_reales(), importaciones_reales()),
        {"ids": [EXPO_ID, IMPO_ID], "frecuencia": "year", "agregacion": "sum"},
    )
    assert payload["filas"][-1]["periodo"] == "2025"
    periodos = payload["periodos"]
    assert periodos.count("El último año completo es 2025") == 1
    assert "«" not in periodos
    assert "la fuente llega hasta 2026-08-01" in periodos


async def test_un_anio_de_una_trimestral_esta_completo_con_sus_cuatro_trimestres() -> None:
    # Desempleo (trimestral) hasta el 2.º trimestre de 2026: 2025 tiene sus
    # cuatro trimestres y 2026, dos.
    payload, _ = await _run(
        FakeSeriesApi(desempleo()), {"ids": [DESEMPLEO_ID], "frecuencia": "year"}
    )
    periodos = payload["periodos"]
    assert "El último año completo es 2025: tiene sus 4 trimestres" in periodos
    assert "2026 no tiene fila porque todavía no tiene sus 4 trimestres" in periodos


@pytest.mark.parametrize("agregacion", ["sum", "end_of_period"])
async def test_un_trimestre_completo_dice_que_meses_tiene(agregacion: str) -> None:
    payload, _ = await _run(
        FakeSeriesApi(exportaciones_reales()),
        {"ids": [EXPO_ID], "frecuencia": "quarter", "agregacion": agregacion, "ultimos": 2},
    )
    periodos = payload["periodos"]
    assert payload["filas"][-1]["periodo"] == "2026-T2"
    assert "El último trimestre completo es 2026-T2: tiene sus 3 meses, de abril a junio." in (
        periodos
    )
    # Julio y agosto de 2026 no completan el 3.er trimestre.
    assert "2026-T3 no tiene fila porque todavía no tiene sus 3 meses" in periodos


@pytest.mark.parametrize(
    ("hasta", "ultima"),
    [
        ("2023-12-31", "2023"),
        # La API filtra por el primer día del período: 2024 entra entero.
        ("2024-06", "2024"),
    ],
)
async def test_un_hasta_que_corta_antes_no_se_presenta_como_el_ultimo_anio_completo(
    hasta: str, ultima: str
) -> None:
    # El año siguiente no tiene fila porque la ventana termina antes, no porque
    # esté incompleto: decir «el último año completo es 2023» sería falso.
    payload, _ = await _run(
        FakeSeriesApi(exportaciones_reales()),
        {"ids": [EXPO_ID], "frecuencia": "year", "agregacion": "sum", "hasta": hasta},
    )
    assert payload["filas"][-1]["fecha"] == f"{ultima}-01-01"
    periodos = payload["periodos"]
    assert periodos.startswith("Cada fila es un año completo")
    assert "último año completo" not in periodos
    assert "no tiene fila" not in periodos


async def test_la_variacion_hasta_2025_no_dice_cual_es_el_ultimo_anio_completo() -> None:
    # Con `hasta=2025` la ventana termina el 31-12-2025: no se sabe por la
    # respuesta si 2026 está completo, y no hace falta para la variación.
    payload, _ = await _run(
        FakeSeriesApi(exportaciones_reales()),
        {
            "ids": [EXPO_ID],
            "frecuencia": "year",
            "agregacion": "sum",
            "variacion": {"desde": "2024", "hasta": "2025"},
        },
    )
    assert "último año completo" not in payload["periodos"]


async def test_una_serie_que_termina_en_diciembre_no_habla_del_anio_siguiente() -> None:
    meses = [(f"{y}-{m:02d}-01", 100.0) for y in (2024, 2025) for m in range(1, 13)]
    payload, _ = await _run(
        FakeSeriesApi(serie("CERRADA", meses, description="Serie cerrada en 2025")),
        {"ids": ["CERRADA"], "frecuencia": "year", "agregacion": "sum"},
    )
    periodos = payload["periodos"]
    assert "El último año completo es 2025" in periodos
    assert "no tiene fila" not in periodos


async def test_series_que_terminan_en_anios_distintos_nombran_el_de_cada_una() -> None:
    larga = [(f"{y}-{m:02d}-01", 100.0) for y in (2024, 2025) for m in range(1, 13)]
    corta = [(f"2024-{m:02d}-01", 50.0) for m in range(1, 13)] + [("2025-01-01", 50.0)]
    payload, _ = await _run(
        FakeSeriesApi(
            serie("LARGA", larga, description="Serie larga"),
            serie("CORTA", corta, description="Serie corta"),
        ),
        {"ids": ["LARGA", "CORTA"], "frecuencia": "year", "agregacion": "sum"},
    )
    periodos = payload["periodos"]
    assert "El último año completo de «Serie larga» es 2025" in periodos
    assert "El último año completo de «Serie corta» es 2024" in periodos
    assert "2025 no tiene fila de «Serie corta»" in periodos
    assert "la fuente llega hasta 2025-01-01" in periodos


# ── lo que la API sí agrega sin terminar (revisión de #162) ──


@pytest.mark.parametrize(
    ("factory", "args", "fecha", "valor"),
    [
        # Exportaciones: 2026-07-01 es julio más agosto de 2026, como en la API
        # real el 06-oct.
        (
            exportaciones_reales,
            {"ids": [EXPO_ID], "agregacion": "sum"},
            "2026-07-01",
            17736.44,
        ),
        # IPC, que arranca en 2016-12: 2025-07-01 es el promedio de junio a
        # agosto de 2026, y la fuente tiene los seis meses de enero a junio.
        (ipc_real, {"ids": [IPC_ID]}, "2025-07-01", 12059.86),
    ],
)
async def test_con_semestres_no_se_dice_que_esten_completos(
    factory: Any, args: dict[str, Any], fecha: str, valor: float
) -> None:
    """Desde una mensual, la API no recorta el semestre en curso y, si la serie
    no arranca en enero, corre los grupos. «El último semestre completo es
    2026-S2: tiene sus 6 meses» daba julio más agosto por el semestre entero, y
    «2026-S1 no tiene fila porque todavía no tiene sus 6 meses» negaba un
    semestre que la fuente ya tiene. La frase genérica también era falsa."""
    payload, _ = await _run(
        FakeSeriesApi(factory()), {**args, "frecuencia": "semester", "ultimos": 3}
    )
    ultima = payload["filas"][-1]
    assert ultima["fecha"] == fecha
    assert round(list(ultima.values())[-1], 2) == valor
    assert "periodos" not in payload


@pytest.mark.parametrize(
    ("factory", "args", "fecha", "valor"),
    [
        # El máximo de enero a agosto de 2026 (API real, 06-oct: 9.577,82).
        (
            exportaciones_reales,
            {"ids": [EXPO_ID], "frecuencia": "year", "agregacion": "max"},
            "2026-01-01",
            9577.82,
        ),
        # El mínimo de enero a agosto de 2026 (API real: 5.963,34).
        (
            exportaciones_reales,
            {"ids": [EXPO_ID], "frecuencia": "year", "agregacion": "min"},
            "2026-01-01",
            5963.34,
        ),
        # El mínimo de julio y agosto de 2026 (API real: 8.853,86).
        (
            exportaciones_reales,
            {"ids": [EXPO_ID], "frecuencia": "quarter", "agregacion": "min"},
            "2026-07-01",
            8853.86,
        ),
        # Desempleo: el máximo de 2026, con dos trimestres.
        (
            desempleo,
            {"ids": [DESEMPLEO_ID], "frecuencia": "year", "agregacion": "max"},
            "2026-01-01",
            7.9,
        ),
    ],
)
async def test_con_max_o_min_no_se_dice_que_los_periodos_esten_completos(
    factory: Any, args: dict[str, Any], fecha: str, valor: float
) -> None:
    """Con max y min la API calcula al consultar y trae el período en curso:
    «El último año completo es 2026: tiene sus 12 meses» era falso."""
    payload, _ = await _run(FakeSeriesApi(factory()), args)
    ultima = payload["filas"][-1]
    assert ultima["fecha"] == fecha
    assert round(list(ultima.values())[-1], 2) == valor
    assert "periodos" not in payload


async def test_la_variacion_con_max_no_da_por_completo_el_anio_en_curso() -> None:
    payload, _ = await _run(
        FakeSeriesApi(exportaciones_reales()),
        {
            "ids": [EXPO_ID],
            "frecuencia": "year",
            "agregacion": "max",
            "variacion": {"desde": "2025", "hasta": "2026"},
        },
    )
    assert payload["filas"][0]["hasta"] == "2026-01-01"
    assert "periodos" not in payload


async def test_sin_frecuencia_no_habla_de_periodos() -> None:
    payload, _ = await _run(FakeSeriesApi(exportaciones_reales()), {"ids": [EXPO_ID]})
    assert "periodos" not in payload


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
    assert "valor_hasta / valor_desde" in payload["calculo"]
    # valor_desde / valor_hasta están en las unidades de la serie (el índice,
    # 10.121,37): el resultado no se rotula entero como porcentaje, sólo
    # las columnas que lo son.
    assert "unidad" not in computed.metadata
    assert computed.metadata["columnas_porcentaje"] == ["variacion_pct"]
    assert "escala" not in payload


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


async def test_la_variacion_ignora_una_representacion_y_lo_dice() -> None:
    # Visto en staging: el modelo pidió la acumulada en el año junto con la
    # variación; un error le costaba una vuelta.
    api = FakeSeriesApi(ipc_real())
    payload, _ = await _run(
        api,
        {
            "ids": [IPC_ID],
            "representacion": "percent_change_since_beginning_of_year",
            "variacion": {"desde": "2026-02", "hasta": "2026-08"},
        },
    )
    assert payload["filas"][0]["variacion_pct"] == 14.58
    assert "se ignoró `representacion`" in payload["nota"]
    assert all("representation_mode" not in r for r in api.series_requests())


# ── variación entre años: dic. contra dic. no es el total anual ──


async def test_la_variacion_entre_anios_de_un_flujo_sin_frecuencia_lo_avisa() -> None:
    # «¿Cuánto crecieron las exportaciones en 2025 respecto de 2024?»: sin
    # frecuencia compara diciembre contra diciembre (7.049 → 7.482, +6,15 %).
    # Es una cifra que parece del año y no lo es: el total creció 9,29 %.
    payload, outcome = await _run(
        FakeSeriesApi(exportaciones_reales()),
        {"ids": [EXPO_ID], "variacion": {"desde": "2024", "hasta": "2025"}},
    )
    fila = payload["filas"][0]
    assert (fila["desde"], fila["hasta"]) == ("2024-12-01", "2025-12-01")
    assert fila["variacion_pct"] == 6.15
    assert "frecuencia=year y agregacion=sum" in payload["nota"]
    assert "no para el total del año" in payload["nota"]
    assert outcome.results[0].metadata["advertencias"]


async def test_la_variacion_del_total_anual_con_year_y_sum_da_9_29() -> None:
    api = FakeSeriesApi(exportaciones_reales())
    payload, _ = await _run(
        api,
        {
            "ids": [EXPO_ID],
            "frecuencia": "year",
            "agregacion": "sum",
            "variacion": {"desde": "2024", "hasta": "2025"},
        },
    )
    fila = payload["filas"][0]
    assert round(fila["valor_desde"], 1) == 79703.2
    assert round(fila["valor_hasta"], 1) == 87111.2
    assert fila["variacion_pct"] == 9.29
    assert "nota" not in payload
    assert api.series_requests()[0]["collapse_aggregation"] == "sum"
    assert "año completo" in payload["periodos"]


async def test_la_variacion_con_el_anio_final_incompleto_lo_avisa() -> None:
    # IPC 2025 → 2026: el año 2026 llega a agosto. Diciembre contra agosto
    # está bien para precios, pero hay que decir que el año no terminó.
    payload, _ = await _run(
        FakeSeriesApi(ipc_real()),
        {"ids": [IPC_ID], "variacion": {"desde": "2025", "hasta": "2026"}},
    )
    fila = payload["filas"][0]
    assert (fila["desde"], fila["hasta"]) == ("2025-12-01", "2026-08-01")
    assert fila["variacion_pct"] == 21.3
    assert "no está completo en la serie" in payload["nota"]
    assert "2026-08-01" in payload["nota"]


async def test_la_variacion_de_un_mes_completo_no_avisa_nada() -> None:
    payload, _ = await _run(
        FakeSeriesApi(ipc_real()),
        {"ids": [IPC_ID], "variacion": {"desde": "2026-02", "hasta": "2026-08"}},
    )
    assert "nota" not in payload


async def test_la_variacion_de_un_anio_pasado_sin_time_index_end_no_es_un_dato_atrasado() -> None:
    """Revisión de ola 3 (#152 × #146): sin time_index_end, el fin de la fuente
    sale del último dato traído (diciembre de 2019) y la variación lo copia;
    sin la marca de inferido, el aviso de atraso lo tomaba como el fin de la
    serie."""
    ipc = ipc_real()
    ipc["field"].pop("time_index_end")
    payload, outcome = await _run(
        FakeSeriesApi(ipc),
        {"ids": [IPC_ID], "variacion": {"desde": "2018-12", "hasta": "2019-12"}},
    )
    assert payload["filas"][0]["hasta"] == "2019-12-01"
    computed = outcome.results[0]
    q = "¿Cuánto aumentaron los precios en 2019?"
    assert freshness_notices([computed], date(2026, 10, 6), q) == []
    assert computed.metadata["fecha_fin_fuente_inferida"] is True


async def test_la_variacion_anual_con_el_anio_en_curso_explica_por_que_no_hay_dato() -> None:
    # Con collapse=year la API deja afuera 2026, que no terminó.
    with pytest.raises(ToolInputError, match="deja afuera el período que todavía no terminó"):
        await _run(
            FakeSeriesApi(exportaciones_reales()),
            {
                "ids": [EXPO_ID],
                "frecuencia": "year",
                "agregacion": "sum",
                "variacion": {"desde": "2025", "hasta": "2026"},
            },
        )


# ── reintento sin frecuencia: sólo con el 400 de frecuencia inválida ──


async def test_un_timeout_con_frecuencia_no_se_reintenta_sin_agregar() -> None:
    # Antes, cualquier error con `collapse` se reintentaba sin collapse ni
    # agregación: un timeout en exportaciones year+sum devolvía la mensual y
    # la variación salía de diciembre contra diciembre (6,15 en vez de 9,29).
    timeout = ConnectorError(
        error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
        details={"series_ids": [EXPO_ID], "reason": "ReadTimeout('The read operation timed out')"},
    )
    series = SimpleNamespace(fetch=AsyncMock(side_effect=timeout))
    with pytest.raises(ConnectorError):
        await SeriesTiempo().run(
            {
                "ids": [EXPO_ID],
                "frecuencia": "year",
                "agregacion": "sum",
                "variacion": {"desde": "2024", "hasta": "2025"},
            },
            _ctx(series),
        )
    assert series.fetch.await_count == 1


async def test_una_frecuencia_mas_fina_que_la_serie_vuelve_a_la_suya_y_lo_dice() -> None:
    # Mensual sobre el desempleo trimestral: la API da 400 ("Intervalo de
    # collapse inválido…") y se pide sin frecuencia, diciéndolo.
    api = FakeSeriesApi(desempleo())
    payload, _ = await _run(api, {"ids": [DESEMPLEO_ID], "frecuencia": "month", "ultimos": 2})
    assert [r.get("collapse") for r in api.series_requests()] == ["month", None]
    assert "no admite `frecuencia=month`" in payload["nota"]
    assert payload["filas"][-1]["fecha"] == "2026-04-01"
    # Y el desempleo llega en %, no como fracción.
    assert 7.9 in payload["filas"][-1].values()
    assert "33,54 %" in payload["escala"]
    # No se agregó nada: no hay períodos agregados de los que hablar.
    assert "periodos" not in payload


# ── varias series: el atraso de cada una ───────────────────


async def test_con_varias_series_el_aviso_nombra_la_desactualizada_con_su_fecha() -> None:
    # Tipo de cambio (desactualizado en la fuente, llega al 31-08) + IPC (al
    # día, llega a 2026-08-01). El aviso decía «su último dato es del
    # 2026-08-01»: la fecha del IPC por el atraso del tipo de cambio.
    payload, _ = await _run(
        FakeSeriesApi(diaria(400), ipc_real()), {"ids": [TIPO_CAMBIO_ID, IPC_ID], "ultimos": 3}
    )
    assert "Tipo de cambio" in payload["aviso"]
    assert "2026-08-31" in payload["aviso"]
    assert "2026-08-01" not in payload["aviso"]
    assert "IPC" not in payload["aviso"]
    por_serie = {s["serie"][:3]: s for s in payload["por_serie"]}
    assert por_serie["Tip"]["la_fuente_llega_hasta"] == "2026-08-31"
    assert por_serie["Tip"]["actualizada_en_fuente"] is False
    assert por_serie["IPC"]["la_fuente_llega_hasta"] == "2026-08-01"
    assert por_serie["IPC"]["actualizada_en_fuente"] is True
    # El agregado (la fecha de la más atrasada) ya no va suelto.
    assert "la_fuente_llega_hasta" not in payload


# ── escalas: las tasas de la EPH en %, y la de cada columna (H085/H060) ──


async def test_la_tasa_de_actividad_llega_en_porcentaje_con_la_escala_dicha() -> None:
    # La API da 0,489 «Porcentaje»; el modelo podía decir «0,49 %».
    payload, _ = await _run(
        FakeSeriesApi(tasa(ACTIVIDAD_ID)), {"ids": [ACTIVIDAD_ID], "ultimos": 1}
    )
    assert 48.9 in payload["filas"][-1].values()
    assert "33,54 %" in payload["escala"]


async def test_con_escalas_mixtas_el_modelo_ve_la_escala_de_cada_columna() -> None:
    payload, _ = await _run(
        FakeSeriesApi(desempleo(), salarios()),
        {"ids": [DESEMPLEO_ID, SALARIOS_ID], "desde": "2025-10-01"},
    )
    por_serie = {s["serie"]: s for s in payload["por_serie"]}
    assert "en %" in por_serie["Tasa de desempleo total. En porcentaje."]["unidades"]
    assert por_serie["Índice de Salarios"]["unidades"] == "Índice"
    escala = payload["escala"]
    assert "no vienen en la misma escala" in escala
    assert "«Tasa de desempleo total. En porcentaje.» está en %" in escala
    assert "«Índice de Salarios» va en sus unidades (Índice)" in escala


async def test_el_gasto_en_porcentaje_del_pib_llega_como_lo_publica_la_api() -> None:
    # `buscar_series` lleva a estas series («gasto público en ciencia y
    # técnica»). La misma fila traía 3,46 (educación básica), 112,75
    # (universitaria) y 26,74 (ciencia) con una `escala` que decía que las dos
    # últimas estaban en %. Las cuatro vienen ya en % del PIB.
    ids = [
        GASTO_PIB_TOTAL_ID,
        GASTO_PIB_EDUCACION_ID,
        GASTO_PIB_UNIVERSIDAD_ID,
        GASTO_PIB_CIENCIA_ID,
    ]
    payload, _ = await _run(
        FakeSeriesApi(*(gasto_pib(sid) for sid in ids)), {"ids": ids, "ultimos": 1}
    )
    assert [v for k, v in payload["filas"][-1].items() if k not in ("fecha", "periodo")] == [
        41.86654045227007,
        3.4592789759436595,
        1.1274565845376978,
        0.2673829881871986,
    ]
    assert "escala" not in payload


async def test_la_tasa_de_japon_no_se_multiplica_por_cien() -> None:
    # 0,75 es 0,75 %; salía 75,0 con «Los valores ya están en %».
    payload, _ = await _run(FakeSeriesApi(tasa_japon()), {"ids": [TASA_JAPON_ID], "ultimos": 1})
    assert 0.75 in payload["filas"][-1].values()
    assert "escala" not in payload


async def test_con_una_representacion_por_serie_no_dice_indice() -> None:
    # IPC + salarios en percent_change: `por_serie` decía «Índice» en cada
    # serie y las filas traían variaciones en %.
    payload, _ = await _run(
        FakeSeriesApi(ipc_real(), salarios()),
        {"ids": [IPC_ID, SALARIOS_ID], "desde": "2026-01-01", "representacion": "percent_change"},
    )
    assert payload["escala"] == "Los valores ya están en %: 33.54 es 33,54 %."
    unidades = [s["unidades"] for s in payload["por_serie"]]
    assert unidades == ["Variación porcentual período anterior (en %)"] * 2


async def test_con_todas_las_series_en_porcentaje_no_hay_escala_mixta() -> None:
    payload, _ = await _run(
        FakeSeriesApi(desempleo(), tasa(ACTIVIDAD_ID)),
        {"ids": [DESEMPLEO_ID, ACTIVIDAD_ID], "desde": "2026-01-01"},
    )
    assert {7.9, 48.9} <= set(payload["filas"][-1].values())
    assert payload["escala"] == "Los valores ya están en %: 33.54 es 33,54 %."


async def test_la_variacion_de_dos_tasas_en_porcentaje_no_habla_de_escalas_mixtas() -> None:
    # El resultado de la variación no lleva `unidad` (sólo algunas columnas
    # son porcentajes), pero las dos series están en la misma escala.
    payload, _ = await _run(
        FakeSeriesApi(desempleo(), tasa(ACTIVIDAD_ID)),
        {
            "ids": [DESEMPLEO_ID, ACTIVIDAD_ID],
            "variacion": {"desde": "2025-04", "hasta": "2026-04"},
        },
    )
    assert {f["serie"]: f["valor_hasta"] for f in payload["filas"]} == {
        "Tasa de desempleo total. En porcentaje.": 7.9,
        "Tasa de actividad total. En porcentaje.": 48.9,
    }
    assert "escala" not in payload


# ── fin de la fuente: la última observación, no un metadato viejo (H065) ──


async def test_la_fuente_llega_hasta_el_ultimo_dato_aunque_la_metadata_este_atrasada() -> None:
    # Una metadata atrasada de verdad (time_index_end 2026-01-01 con el
    # last_value de esa fila) y la API ya trae 2026-07-01: con «la fuente
    # llega hasta 2026-01-01» el modelo descartaba el último dato. (En la
    # 64.2 eran las filas corridas un semestre: ver el test de abajo.)
    payload, _ = await _run(
        FakeSeriesApi(pobreza_con_metadata_atrasada()), {"ids": [POBREZA_ID], "ultimos": 2}
    )
    assert payload["ultima_observacion"] == "2026-07-01"
    assert payload["la_fuente_llega_hasta"] == "2026-07-01"
    assert 32.3 in payload["filas"][-1].values()


async def test_la_pobreza_64_2_llega_con_las_fechas_de_la_fuente_y_el_1s_2026() -> None:
    # La 64.2 como la da la API: 32,3 % (1er semestre de 2026) en la fila
    # 2026-07-01 con time_index_end 2026-01-01. Sale fechada como la fuente,
    # y el fin de la fuente no queda antes del último dato.
    payload, _ = await _run(FakeSeriesApi(tasa(POBREZA_ID)), {"ids": [POBREZA_ID], "ultimos": 2})
    assert payload["ultima_observacion"] == "2026-01-01"
    assert payload["la_fuente_llega_hasta"] == "2026-01-01"
    assert payload["filas"][-1]["periodo"] == "2026-S1"
    assert 32.3 in payload["filas"][-1].values()


# ── pobreza de Gran Rosario: los semestres no salen corridos (nueva_08) ──


def _semestres(payload: dict[str, Any]) -> list[tuple[str, float]]:
    return [
        (f["periodo"], next(v for k, v in f.items() if k not in ("fecha", "periodo")))
        for f in payload["filas"]
    ]


@pytest.mark.parametrize(
    ("args", "esperado"),
    [
        # El último dato: 23,1 % del 1er semestre de 2026 (y 22,3 % el 2°
        # de 2025), como el CSV de la fuente.
        ({"ultimos": 2}, [("2025-S2", 22.3), ("2026-S1", 23.1)]),
        # Con `hasta` el rótulo salía de la fecha de hoy y quedaba todo
        # corrido: 22,3 % rotulado 2026-S1 y 46,8 % (1S-2024) como 2024-S2.
        (
            {"desde": "2024-01-01", "hasta": "2026-06-30"},
            [
                ("2024-S1", 46.8),
                ("2024-S2", 32.4),
                ("2025-S1", 28.1),
                ("2025-S2", 22.3),
                ("2026-S1", 23.1),
            ],
        ),
        ({"desde": "2025-01-01", "hasta": "2025-12-31"}, [("2025-S1", 28.1), ("2025-S2", 22.3)]),
    ],
)
async def test_la_pobreza_de_gran_rosario_rotula_bien_los_semestres(
    args: dict[str, Any], esperado: list[tuple[str, float]]
) -> None:
    payload, _ = await _run(
        FakeSeriesApi(pobreza_gran_rosario_real()), {"ids": [POBREZA_GRAN_ROSARIO_ID], **args}
    )
    assert _semestres(payload) == esperado


async def test_entre_el_fin_del_semestre_y_su_publicacion_no_se_corre_el_rotulo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """El 15-ene-2027 el 2° semestre de 2026 todavía no se publicó (sale en
    marzo): el rótulo dependía de la fecha de hoy y el 23,1 % pasaba a
    2026-S2."""
    from app.application.answers.tools import conectores

    class _Enero2027(date):
        @classmethod
        def today(cls) -> date:
            return date(2027, 1, 15)

    monkeypatch.setattr(conectores, "date", _Enero2027)
    payload, _ = await _run(
        FakeSeriesApi(pobreza_gran_rosario_real()),
        {"ids": [POBREZA_GRAN_ROSARIO_ID], "ultimos": 1},
    )
    assert _semestres(payload) == [("2026-S1", 23.1)]


async def test_la_variacion_de_la_pobreza_compara_los_semestres_pedidos() -> None:
    # 1er semestre de 2025 (28,1 %) contra 1er semestre de 2026 (23,1 %).
    # Corrida, comparaba 2S-2024 (32,4 %) con 2S-2025 (22,3 %): −31,17 %.
    payload, _ = await _run(
        FakeSeriesApi(pobreza_gran_rosario_real()),
        {"ids": [POBREZA_GRAN_ROSARIO_ID], "variacion": {"desde": "2025-01", "hasta": "2026-06"}},
    )
    fila = payload["filas"][0]
    assert (fila["valor_desde"], fila["valor_hasta"], fila["variacion_pct"]) == (
        28.1,
        23.1,
        -17.79,
    )


GRAN_ROSARIO_LABEL = (
    "Población con ingresos debajo de línea de pobreza (%) desde 2003. Gran Rosario. EPH continua."
)
DESEMPLEO_GRAN_ROSARIO_LABEL = "Tasa de desempleo total Gran Rosario. En porcentaje."


async def test_la_pobreza_pedida_con_otras_series_rotula_los_semestres_como_sola() -> None:
    """Revisión de #166: la corrección de las fechas era todo o nada. Con el
    desempleo trimestral, «pobreza y desempleo en Gran Rosario en 2025» daba
    32,4 % (2S-2024) como 2025-S1; con la 64.1 de 2001-2003 (la que el
    modelo mezcló en nueva_08), la continua salía corrida y la fuente
    «llegaba» a 2026-07-01."""

    def pobreza(payload: dict[str, Any]) -> list[tuple[str, float]]:
        return [
            (f["periodo"], f[GRAN_ROSARIO_LABEL])
            for f in payload["filas"]
            if f.get(GRAN_ROSARIO_LABEL) is not None
        ]

    ids = [POBREZA_GRAN_ROSARIO_ID, DESEMPLEO_GRAN_ROSARIO_ID]
    api = FakeSeriesApi(pobreza_gran_rosario_real(), desempleo_gran_rosario())
    con_desempleo, _ = await _run(api, {"ids": ids, "desde": "2025-01-01", "hasta": "2025-12-31"})
    assert pobreza(con_desempleo) == [("2025-S1", 28.1), ("2025-S2", 22.3)]
    assert [f[DESEMPLEO_GRAN_ROSARIO_LABEL] for f in con_desempleo["filas"]] == [7.4, 7.7]
    # La variación del 1er semestre de 2025 al de 2026, también con las dos.
    variacion, _ = await _run(
        api, {"ids": ids, "variacion": {"desde": "2025-01", "hasta": "2026-06"}}
    )
    fila = next(f for f in variacion["filas"] if f["serie"] == GRAN_ROSARIO_LABEL)
    assert (fila["valor_desde"], fila["valor_hasta"], fila["variacion_pct"]) == (
        28.1,
        23.1,
        -17.79,
    )

    ids = [POBREZA_GRAN_ROSARIO_PUNTUAL_ID, POBREZA_GRAN_ROSARIO_ID]
    api = FakeSeriesApi(pobreza_gran_rosario_puntual(), pobreza_gran_rosario_real())
    con_puntual, _ = await _run(api, {"ids": ids, "desde": "2002-01-01", "hasta": "2004-12-31"})
    assert pobreza(con_puntual)[0] == ("2003-S1", 54.6)
    sin_rango, _ = await _run(api, {"ids": ids, "ultimos": 2})
    assert [s["la_fuente_llega_hasta"] for s in sin_rango["por_serie"]] == [
        "2003-05-01",
        "2026-01-01",
    ]
    assert pobreza(sin_rango) == [("2025-S2", 22.3), ("2026-S1", 23.1)]


async def test_un_id_inexistente_vuelve_como_pedido_invalido_y_no_como_fuente_caida() -> None:
    """nueva_08: el modelo armó el id 64.2_GR_0_0_12. El 400 «Serie
    inexistente» llegaba al motor como ConnectorError y el modelo leía «La
    fuente no respondió. Probá con otra.»: dejó la API y contestó con una
    copia vieja del catálogo, con los semestres corridos."""
    api = FakeSeriesApi(pobreza_gran_rosario_real())
    with pytest.raises(ToolInputError, match="no existe la serie `64.2_GR_0_0_12`") as err:
        await _run(api, {"ids": ["64.2_GR_0_0_12"]})
    assert "buscar_series" in str(err.value)
    # Igual con la variación.
    with pytest.raises(ToolInputError, match="no existe la serie"):
        await _run(
            api, {"ids": ["64.2_GR_0_0_12"], "variacion": {"desde": "2025", "hasta": "2026"}}
        )


async def test_el_motor_le_dice_al_modelo_que_el_id_no_existe() -> None:
    from app.application.answers.agent_engine import AgentEngine
    from app.domain.ports.llm.agent_llm import ToolCall

    api = FakeSeriesApi(pobreza_gran_rosario_real())
    engine = AgentEngine(AsyncMock(), SimpleNamespace(series=api.adapter()))
    call = ToolCall(id="t1", name="series_tiempo", input={"ids": ["64.2_GR_0_0_12"]})
    outcome = await engine._run_tool(SeriesTiempo(), call, _ctx(api.adapter()))

    assert outcome.is_error is True
    assert "La fuente no respondió" not in outcome.content
    assert "no existe la serie `64.2_GR_0_0_12`" in outcome.content


# ── el último dato publicado, aunque lo traído termine antes (nueva_12) ──


@pytest.mark.parametrize(
    "args",
    [
        {"desde": "2025-01-01", "hasta": "2026-06-30"},
        {
            "desde": "2025-01-01",
            "hasta": "2026-06-30",
            "representacion": "percent_change_a_year_ago",
        },
        {"variacion": {"desde": "2025-06", "hasta": "2026-06"}},
    ],
)
async def test_si_la_serie_sigue_despues_de_lo_traido_se_dice_cual_es_el_ultimo_dato(
    args: dict[str, Any],
) -> None:
    """nueva_12: pidió el IPI hasta junio y escribió «Último dato disponible:
    junio 2026» con `la_fuente_llega_hasta` 2026-07-01 al lado. Acá con el
    IPC, que llega a agosto."""
    payload, _ = await _run(FakeSeriesApi(ipc_real()), {"ids": [IPC_ID], **args})
    assert payload["ultima_observacion"] == "2026-06-01"
    assert payload["la_fuente_llega_hasta"] == "2026-08-01"
    assert "es el de 2026-08-01, no el de 2026-06-01" in payload["ultimo_dato_publicado"]


async def test_con_frecuencia_el_ultimo_dato_publicado_es_el_de_la_serie() -> None:
    # Exportaciones con year+sum llegan a 2025 (el año en curso no entra):
    # el último dato publicado es agosto de 2026.
    payload, _ = await _run(
        FakeSeriesApi(exportaciones_reales()),
        {"ids": [EXPO_ID], "frecuencia": "year", "agregacion": "sum"},
    )
    assert payload["ultima_observacion"] == "2025-01-01"
    assert "2026-08-01" in payload["ultimo_dato_publicado"]


async def test_si_lo_traido_llega_al_ultimo_dato_no_se_agrega_nada() -> None:
    sin_rango, _ = await _run(FakeSeriesApi(ipc_real()), {"ids": [IPC_ID], "ultimos": 2})
    semestre, _ = await _run(
        FakeSeriesApi(pobreza_gran_rosario_real()),
        {"ids": [POBREZA_GRAN_ROSARIO_ID], "ultimos": 2},
    )
    assert "ultimo_dato_publicado" not in sin_rango
    assert "ultimo_dato_publicado" not in semestre


# ── frecuencias distintas en un pedido: la API promedia (H086) ──


async def test_series_de_distinta_frecuencia_avisan_que_la_api_promedio() -> None:
    # Reservas diaria + mensual: la diaria llega promediada al mes (49.700
    # en agosto) y el modelo la podía dar como el saldo (48.259 al 31-08).
    payload, _ = await _run(
        FakeSeriesApi(reservas_diarias(), reservas_mensuales()),
        {"ids": [RESERVAS_DIARIAS_ID, RESERVAS_ID], "desde": "2026-06-01"},
    )
    aviso = payload["aviso_agregacion"]
    assert "«Reservas internacionales del BCRA, en millones de dólares» (diaria)" in aviso
    assert "Saldos" not in aviso
    assert "PROMEDIANDO" in aviso
    assert "agregacion=end_of_period" in aviso


async def test_una_serie_sola_no_avisa_promedio() -> None:
    payload, _ = await _run(
        FakeSeriesApi(reservas_diarias()), {"ids": [RESERVAS_DIARIAS_ID], "ultimos": 1}
    )
    assert payload["filas"][-1]["fecha"] == "2026-08-31"
    assert "aviso_agregacion" not in payload


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
    # Es el metadato del catálogo de la API, que puede estar atrasado (la
    # pobreza 64.2 dice 2026-01-01 y ya publicó 2026-07-01): se lo nombra
    # así y no como el fin de la serie.
    assert payload["series"][0]["hasta_segun_catalogo"] == "2023-01-01"
    assert "hasta" not in payload["series"][0]
    assert "puede estar atrasado" in BuscarSeries.spec.description


async def test_buscar_series_inflacion_mayorista_es_el_ipim_y_agosto_da_2_14() -> None:
    """nueva_13 (prueba del 06-oct): «no está disponible», y la fuente tenía 2,14 %.

    buscar_series daba como verificada la serie del IPC nacional (la entrada
    "inflacion" coincidía por la palabra «inflación») y la /search de la API
    no traía el IPIM: el modelo terminó en una copia guardada que llegaba a
    julio.
    """
    payload = await _buscar("¿De cuánto fue la inflación mayorista en agosto de 2026?")
    assert _ids(payload) == {IPIM_ID}
    assert "mayorista" in payload["verificadas"][0]["descripcion"]

    serie_ipim, _ = await _run(
        FakeSeriesApi(ipim_real()),
        {"ids": [IPIM_ID], "representacion": "percent_change", "desde": "2026-08-01"},
    )
    assert [f["fecha"] for f in serie_ipim["filas"]] == ["2026-08-01"]
    assert 2.14 in serie_ipim["filas"][0].values()


async def test_buscar_series_inflacion_de_misiones_es_el_noreste_y_agosto_da_1_75() -> None:
    """nueva_15 (prueba del 06-oct): no hay IPC de Misiones y el del Noreste no aparecía.

    Con la serie nacional como única verificada, el modelo explicó que no hay
    dato provincial y ofreció el del Noreste en vez de darlo.
    """
    payload = await _buscar("inflación Misiones agosto 2026")
    assert _ids(payload) == {IPC_NORESTE_ID}
    descripcion = payload["verificadas"][0]["descripcion"]
    assert "Misiones" in descripcion and "región" in descripcion

    serie_nea, _ = await _run(
        FakeSeriesApi(ipc_noreste_real()),
        {"ids": [IPC_NORESTE_ID], "representacion": "percent_change", "desde": "2026-08-01"},
    )
    assert [f["fecha"] for f in serie_nea["filas"]] == ["2026-08-01"]
    assert 1.75 in serie_nea["filas"][0].values()


@pytest.mark.parametrize(
    ("texto", "sid"),
    [
        ("inflación Corrientes agosto 2026", IPC_NORESTE_ID),
        ("inflación Salta agosto 2026", "148.3_INIVELNOA_DICI_M_21"),
    ],
)
async def test_buscar_series_provincia_sin_preposicion_es_su_region(texto: str, sid: str) -> None:
    """Revisión de #164: nueva_15 está escrita sin preposición a propósito.
    Corrientes y Salta, que también son palabras comunes, pedían «en» o «de»
    delante y seguían verificando sólo el IPC nacional."""
    assert _ids(await _buscar(texto)) == {sid}


async def test_buscar_series_costo_de_vida_sigue_verificando_el_ipc() -> None:
    """Revisión de #164: «costo de vida» es keyword del IPC y de la canasta
    básica. La regla de la genérica dejaba sólo la Canasta Básica Total
    (línea de pobreza), y la /search de la API no trae ningún IPC."""
    assert _ids(await _buscar("costo de vida 2025")) == {IPC_ID, "150.1_LA_POBREZA_0_D_13"}


async def test_buscar_series_reservas_ofrece_primero_la_diaria() -> None:
    payload = await _buscar("reservas internacionales del BCRA")
    assert {RESERVAS_ID, "92.2_RESERVAS_IRES_0_0_32_40"} <= _ids(payload)
    # La que llega más lejos va primero; la mensual está parada en abril.
    assert payload["verificadas"][0]["ids"] == ["92.2_RESERVAS_IRES_0_0_32_40"]
