"""`cotizaciones(indicador='inflacion')`: el IPC mensual de ArgentinaDatos.

En la prueba de prod del 29-oct el agente contestó la inflación de los últimos
12 meses hasta agosto y el fact-check de septiembre con septiembre de 2025: la
API de Series de Tiempo seguía en ago-2026 y ArgentinaDatos ya tenía sep-2026
(1,9 %). El adaptador ya traía la inflación, pero sólo lo usaba el pipeline
viejo.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import ToolContext, ToolInputError
from app.application.answers.tools.catalogo import BuscarDatos
from app.application.answers.tools.conectores import BuscarSeries, Cotizaciones, SeriesTiempo
from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode


def _inflacion(meses: int) -> DataResult:
    records = [
        {"fecha": f"{2024 + (i // 12)}-{i % 12 + 1:02d}-28", "inflacion": round(1 + i / 100, 2)}
        for i in range(meses)
    ]
    records[-1] = {"fecha": "2026-09-30", "inflacion": 1.9}
    return DataResult(
        source="argentina_datos",
        portal_name="ArgentinaDatos API",
        portal_url="https://argentinadatos.com",
        dataset_title="Inflación Mensual — ArgentinaDatos",
        format="time_series",
        records=records,
        metadata={"description": "Inflación mensual (variación % IPC)"},
    )


def _ctx(result: DataResult | None) -> tuple[ToolContext, MagicMock]:
    deps = MagicMock()
    deps.arg_datos.fetch_inflacion = AsyncMock(return_value=result)
    return ToolContext(deps, EngineRequest("q", "u")), deps


async def test_trae_la_inflacion_con_el_ultimo_mes_y_la_deja_como_evidencia() -> None:
    serie = _inflacion(33)
    ctx, deps = _ctx(serie)

    outcome = await Cotizaciones().run({"indicador": "inflacion"}, ctx)

    deps.arg_datos.fetch_inflacion.assert_awaited_once_with()
    filas = json.loads(outcome.content)["filas"]
    assert len(filas) == 30
    assert filas[-1] == {"fecha": "2026-09-30", "inflacion": 1.9}
    assert outcome.results == [serie]


async def test_sin_datos_lo_dice() -> None:
    ctx, _ = _ctx(None)

    outcome = await Cotizaciones().run({"indicador": "inflacion"}, ctx)

    assert json.loads(outcome.content) == {"filas": [], "nota": "Sin datos."}


async def test_un_indicador_desconocido_nombra_los_tres() -> None:
    ctx, _ = _ctx(None)

    with pytest.raises(ToolInputError, match="'inflacion'"):
        await Cotizaciones().run({"indicador": "tasas"}, ctx)


def test_la_descripcion_la_acota_al_mes_que_falta_en_series_tiempo() -> None:
    desc = Cotizaciones.spec.description
    assert "indicador=inflacion" in desc
    assert "series_tiempo no llega" in desc
    assert "ArgentinaDatos con dato del INDEC" in desc
    assert "inflacion" in Cotizaciones.spec.input_schema["properties"]["indicador"]["enum"]
    assert "cotizaciones(indicador='inflacion')" in BuscarSeries.spec.description
    assert Cotizaciones().describe({"indicador": "inflacion"}) == (
        "Consultando la inflación mensual en ArgentinaDatos"
    )


def test_buscar_datos_dice_como_esta_el_presupuesto_por_ejercicio() -> None:
    """e) y j) de la prueba del 29-oct: con la tabla de 2026 cargada, contestó
    con el Transversal Financiero 2025 y dijo que no había presupuesto
    universitario. Los valores son los de la tabla, sin tilde en el programa."""
    desc = BuscarDatos.spec.description
    assert "raw.cache_presupuesto_credito_<año>" in desc
    assert "«Educación y Cultura»" in desc
    assert "«Desarrollo de la Educacion Superior»" in desc
    assert "ejercicio del año de hoy" in desc
    assert "Transversal financiero" in desc


# ── series_tiempo completa el IPC con los meses que faltan ─────────────────

_IPC = "148.3_INIVELNAL_DICI_M_26"
_COLUMNA = "IPC. Nivel General Nacional. Base dic 2016. Mensual."


def _ipc_hasta_agosto() -> DataResult:
    return DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url=f"https://apis.datos.gob.ar/series/api/series/?ids={_IPC}",
        dataset_title="Índice de Precios al Consumidor Nacional (IPC). Base diciembre 2016.",
        format="time_series",
        records=[
            {"fecha": "2026-07-01", _COLUMNA: 2.11},
            {"fecha": "2026-08-01", _COLUMNA: 1.66},
        ],
        metadata={"units": "Variación porcentual período anterior (en %)"},
    )


def _ctx_series(inflacion: DataResult | None) -> tuple[ToolContext, MagicMock]:
    deps = MagicMock()
    deps.series.fetch = AsyncMock(return_value=_ipc_hasta_agosto())
    deps.arg_datos.fetch_inflacion = AsyncMock(return_value=inflacion)
    return ToolContext(deps, EngineRequest("q", "u")), deps


async def test_la_variacion_mensual_del_ipc_trae_septiembre_de_argentinadatos() -> None:
    ctx, _ = _ctx_series(_inflacion(33))

    outcome = await SeriesTiempo().run({"ids": [_IPC], "representacion": "percent_change"}, ctx)

    payload = json.loads(outcome.content)
    assert payload["meses_posteriores"]["filas"] == [{"fecha": "2026-09-30", "inflacion": 1.9}]
    assert "ArgentinaDatos con dato del INDEC" in payload["meses_posteriores"]["nota"]
    assert [r.dataset_title for r in outcome.results] == [
        "Índice de Precios al Consumidor Nacional (IPC). Base diciembre 2016.",
        "Inflación mensual del INDEC — ArgentinaDatos",
    ]
    assert outcome.results[1].records == [{"fecha": "2026-09-30", "inflacion": 1.9}]


@pytest.mark.parametrize(
    "args",
    [
        # El índice, no la variación: no se mezclan unidades.
        {"ids": [_IPC]},
        # Un rango cerrado ya dice hasta dónde.
        {"ids": [_IPC], "representacion": "percent_change", "hasta": "2026-08-01"},
        # Otra serie.
        {"ids": ["148.3_INUCLEONAL_DICI_M_19"], "representacion": "percent_change"},
    ],
)
async def test_fuera_del_ipc_mensual_abierto_no_agrega_nada(args: dict) -> None:
    ctx, deps = _ctx_series(_inflacion(33))

    outcome = await SeriesTiempo().run(args, ctx)

    assert "meses_posteriores" not in json.loads(outcome.content)
    assert len(outcome.results) == 1
    deps.arg_datos.fetch_inflacion.assert_not_awaited()


async def test_si_argentinadatos_no_tiene_nada_nuevo_queda_la_serie_sola() -> None:
    serie = _inflacion(20)
    serie.records = [{"fecha": "2026-08-31", "inflacion": 1.66}]
    ctx, _ = _ctx_series(serie)

    outcome = await SeriesTiempo().run({"ids": [_IPC], "representacion": "percent_change"}, ctx)

    assert "meses_posteriores" not in json.loads(outcome.content)
    assert len(outcome.results) == 1


async def test_argentinadatos_caido_no_rompe_la_serie() -> None:
    ctx, deps = _ctx_series(None)
    deps.arg_datos.fetch_inflacion = AsyncMock(
        side_effect=ConnectorError(error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE)
    )

    outcome = await SeriesTiempo().run({"ids": [_IPC], "representacion": "percent_change"}, ctx)

    assert json.loads(outcome.content)["filas"][-1]["fecha"] == "2026-08-01"
    assert len(outcome.results) == 1
