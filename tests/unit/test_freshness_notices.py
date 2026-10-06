"""El aviso de dato atrasado, calculado sobre la evidencia que se citó.

Casos reales del 04-oct-2026 (API de Series de Tiempo con ``metadata=full`` y
API v4 del BCRA): la reservas 174.1 termina en abril de 2026 con
``is_updated=False``; la diaria 92.2 en el 31-ago; el IPC llega a agosto y el
EMAE a julio, los dos al día según la propia API.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from app.application.quality.data_age import (
    freshness_notices,
    observation_label,
    observation_staleness,
)
from app.domain.entities.connectors.data_result import DataResult

HOY = date(2026, 10, 4)


def _serie(title: str, fechas: list[str], source: str = "series_tiempo", **meta: Any) -> DataResult:
    return DataResult(
        source=source,
        portal_name="API de Series de Tiempo",
        portal_url="https://datos.gob.ar/series/api/series/?ids=x",
        dataset_title=title,
        format="time_series",
        records=[{"fecha": f, "valor": 1.0} for f in fechas],
        metadata=meta,
    )


@pytest.mark.parametrize(
    ("last", "freq", "updated", "stale"),
    [
        # Reservas 174.1: mensual hasta abril, la API dice que no se actualiza.
        (date(2026, 4, 1), "mensual", False, True),
        # Reservas diaria 92.2: 34 días sin datos.
        (date(2026, 8, 31), "diaria", None, True),
        # IPC de agosto: 34 días después del fin del mes.
        (date(2026, 8, 1), "mensual", True, False),
        # EMAE de julio: 65 días después del fin del mes, y la API lo da al día.
        (date(2026, 7, 1), "mensual", True, False),
        # Desempleo del 2° trimestre: 96 días después del 30-jun.
        (date(2026, 4, 1), "trimestral", None, False),
        # DolarApi del viernes, consultado el domingo.
        (date(2026, 10, 2), "diaria", None, False),
        # Reservas BCRA v4 al 30-sep.
        (date(2026, 9, 30), "diaria", None, False),
        # Sin frecuencia sólo cuenta lo que dice la fuente.
        (date(2020, 1, 1), None, None, False),
        (date(2026, 9, 1), None, False, True),
    ],
)
def test_atraso_por_frecuencia(
    last: date, freq: str | None, updated: bool | None, stale: bool
) -> None:
    assert observation_staleness(last, freq, HOY, updated_at_source=updated).stale is stale


def test_la_serie_truncada_de_reservas_lleva_aviso_arriba() -> None:
    """La respuesta falsa de la reproducción: «abril de 2023: USD 35.001 M»."""
    [aviso] = freshness_notices(
        [
            _serie(
                "Series históricas de estadísticas monetarias",
                ["2023-02-01", "2023-03-01", "2023-04-01"],
            )
        ],
        HOY,
    )
    assert "abril de 2023" in aviso
    assert "serie mensual" in aviso
    assert "no refleja el valor actual" in aviso


def test_con_el_contrato_de_metadatos_manda_la_fuente() -> None:
    result = _serie(
        "Series históricas de estadísticas monetarias",
        ["2026-03-01", "2026-04-01"],
        ultima_observacion="2026-04-01",
        frecuencia="mensual",
        fecha_fin_fuente="2026-04-01",
        actualizada_en_fuente=False,
    )
    [aviso] = freshness_notices([result], HOY)
    assert "abril de 2026" in aviso
    assert "la fuente no la actualizó" in aviso


def test_un_dato_al_dia_no_lleva_aviso() -> None:
    ipc = _serie(
        "Índice de Precios al Consumidor Nacional (IPC)",
        ["2026-07-01", "2026-08-01"],
        ultima_observacion="2026-08-01",
        frecuencia="mensual",
        fecha_fin_fuente="2026-08-01",
        actualizada_en_fuente=True,
    )
    emae = _serie("EMAE", ["2026-06-01", "2026-07-01"], actualizada_en_fuente=True)
    assert freshness_notices([ipc, emae], HOY) == []


def test_un_periodo_pasado_pedido_a_proposito_no_es_atraso() -> None:
    """ "¿Cuál fue la inflación de 2020?": la fuente llega a agosto de 2026."""
    result = _serie(
        "IPC",
        ["2020-11-01", "2020-12-01"],
        ultima_observacion="2020-12-01",
        frecuencia="mensual",
        fecha_fin_fuente="2026-08-01",
        actualizada_en_fuente=True,
    )
    assert freshness_notices([result], HOY) == []


def test_sin_fecha_de_fin_una_pregunta_por_un_periodo_no_lleva_aviso() -> None:
    """Calibración del 04-oct: "¿Cuánto exportó Argentina en el primer semestre
    de 2026?" daba «el último dato es de junio de 2026… no refleja el valor
    actual». Sin `fecha_fin_fuente` no se sabe si la serie termina ahí."""
    expo = _serie("Intercambio Comercial Argentino", ["2026-05-01", "2026-06-01"])
    q = "¿Cuánto exportó Argentina en el primer semestre de 2026?"
    assert freshness_notices([expo], HOY, q) == []
    assert (
        freshness_notices([expo], HOY, "¿Qué relación hubo entre inflación y salarios en 2025?")
        == []
    )
    # Si pide el valor actual, sí.
    vieja = _serie("Base monetaria", ["2026-04-01", "2026-05-01"])
    assert freshness_notices([vieja], HOY, "¿Cuál es la base monetaria actual?")
    assert freshness_notices([vieja], HOY, "¿Cómo viene la base monetaria desde 2024?")
    # Con la fecha de fin de la fuente manda la fuente, nombre o no un período.
    con_fin = _serie("Saldo comercial", ["2025-01-01", "2025-02-01"], fecha_fin_fuente="2025-02-01")
    assert freshness_notices([con_fin], HOY, "¿Cuál fue el saldo comercial de 2025?")


def test_los_fragmentos_de_sesiones_no_son_una_serie() -> None:
    """Calibración del 04-oct: «el último dato de "Transcripciones
    parlamentarias" es del 19 de febrero de 2026 (serie semanal)»."""
    sesiones = DataResult(
        source="sesiones",
        portal_name="HCDN",
        portal_url="https://www.hcdn.gob.ar",
        dataset_title='Transcripciones parlamentarias: "educación"',
        format="json",
        records=[{"fecha": "2026-02-12", "texto": "…"}, {"fecha": "2026-02-19", "texto": "…"}],
    )
    assert freshness_notices([sesiones], HOY) == []


def test_las_tablas_del_catalogo_no_entran() -> None:
    """Sus fechas dependen del filtro que eligió el modelo."""
    tabla = _serie("Presupuesto", ["2019-01-01", "2019-02-01"], source="sandbox:raw.x")
    assert freshness_notices([tabla], HOY) == []


def test_dolarapi_del_viernes_no_lleva_aviso_y_uno_viejo_si() -> None:
    def _dolar(fecha: str) -> DataResult:
        return DataResult(
            source="dolarapi",
            portal_name="DolarApi",
            portal_url="https://dolarapi.com",
            dataset_title="Cotización actual Dólar Oficial",
            format="time_series",
            records=[{"fecha": fecha, "compra": 1490, "venta": 1540}],
            metadata={"realtime": True},
        )

    assert freshness_notices([_dolar("2026-10-02T18:55:00.000Z")], HOY) == []
    [aviso] = freshness_notices([_dolar("2026-09-10T18:55:00.000Z")], HOY)
    assert "10 de septiembre de 2026" in aviso


def test_un_aviso_por_titulo_y_como_mucho_dos() -> None:
    viejas = [_serie(f"Serie {i}", ["2020-01-01", "2020-02-01"]) for i in range(4)]
    assert len(freshness_notices(viejas, HOY)) == 2
    misma = [_serie("Serie", ["2020-01-01", "2020-02-01"]) for _ in range(3)]
    assert len(freshness_notices(misma, HOY)) == 1


def test_una_evidencia_rara_no_rompe_nada() -> None:
    assert freshness_notices([object(), _serie("x", [])], HOY) == []


# ── varias series en un mismo pedido (revisión del 05-oct, H020) ──────

IPC_TITULO = "Índice de Precios al Consumidor Nacional (IPC)"


def _varias(columnas: dict[str, dict[str, float]], series: list[dict[str, Any]]) -> DataResult:
    """Como lo arma `series_tiempo_adapter` con varios ids: el título del
    primer dataset, la última fila con algún valor, la fecha de fin de la más
    atrasada y el «desactualizada» de cualquiera de las series."""
    fechas = sorted({f for valores in columnas.values() for f in valores})
    records = [
        {"fecha": f, **{titulo: valores.get(f) for titulo, valores in columnas.items()}}
        for f in fechas
    ]
    flags = [s["actualizada_en_fuente"] for s in series]
    return DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url="https://datos.gob.ar/series/api/series/?ids=a,b",
        dataset_title=IPC_TITULO,
        format="time_series",
        records=records,
        metadata={
            "ultima_observacion": fechas[-1],
            "frecuencia": "mensual",
            "fecha_fin_fuente": min(s["fecha_fin_fuente"] for s in series),
            "actualizada_en_fuente": False
            if any(f is False for f in flags)
            else (True if all(f is True for f in flags) else None),
            "truncada": False,
            "series": series,
        },
    )


def _meses(desde: int, hasta: int, base: float) -> dict[str, float]:
    return {f"2026-{m:02d}-01": base + m for m in range(desde, hasta + 1)}


def test_con_varias_series_el_aviso_nombra_la_atrasada_y_no_la_primera() -> None:
    """La reproducción de la revisión: IPC hasta agosto y salario real hasta
    mayo. El aviso decía «el último dato de "IPC" es de mayo de 2026»: el
    título de la primera serie con la fecha de la más atrasada."""
    result = _varias(
        {"IPC Nivel general": _meses(1, 8, 1.5), "Índice de salarios real": _meses(1, 5, 100.0)},
        [
            {
                "id": "148.3_INIVELNAL_DICI_M_26",
                "titulo": "IPC Nivel general",
                "fecha_fin_fuente": "2026-08-01",
                "actualizada_en_fuente": None,
                "frecuencia": "mensual",
            },
            {
                "id": "149.1_SALARIO_REAL",
                "titulo": "Índice de salarios real",
                "fecha_fin_fuente": "2026-05-01",
                "actualizada_en_fuente": None,
                "frecuencia": "mensual",
            },
        ],
    )
    q = "¿Cómo vienen la inflación y el salario real?"
    [aviso] = freshness_notices([result], HOY, q)
    assert "«Índice de salarios real»" in aviso
    assert "mayo de 2026" in aviso
    assert "IPC" not in aviso


def test_con_varias_series_la_desactualizada_de_una_no_se_le_atribuye_a_otra() -> None:
    """IPC + tipo de cambio contra la API en vivo (verificación de H020): el
    aviso decía que el IPC «la fuente no la actualizó» y la que la fuente marca
    como desactualizada es la del tipo de cambio. Pedidas juntas, la diaria
    llega agregada por mes (fechada el 1.º) y su fecha de fin es el 31-ago:
    eso no es «se pidió un período pasado»."""
    result = _varias(
        {
            "IPC Nivel general": _meses(3, 8, 1.5),
            "Tipo de cambio de valuación": _meses(3, 8, 1300.0),
        },
        [
            {
                "id": "148.3_INIVELNAL_DICI_M_26",
                "titulo": "IPC Nivel general",
                "fecha_fin_fuente": "2026-08-01",
                "actualizada_en_fuente": True,
                "frecuencia": "mensual",
            },
            {
                "id": "92.2_TIPO_CAMBIION_0_0_21_24",
                "titulo": "Tipo de cambio de valuación",
                "fecha_fin_fuente": "2026-08-31",
                "actualizada_en_fuente": False,
                "frecuencia": "diaria",
            },
        ],
    )
    [aviso] = freshness_notices([result], HOY)
    assert "«Tipo de cambio de valuación»" in aviso
    assert "la fuente no la actualizó" in aviso
    assert "IPC" not in aviso


def test_una_diaria_agregada_por_mes_no_es_un_periodo_pedido_a_proposito() -> None:
    """La misma comparación con una sola serie: la diaria agregada por mes
    llega hasta el 1-ago y la fuente, hasta el 31-ago. Se trajo hasta el
    final; la fuente la marca desactualizada y eso se dice."""
    result = _serie(
        "Tipo de cambio de valuación",
        ["2026-07-01", "2026-08-01"],
        ultima_observacion="2026-08-01",
        frecuencia="mensual",
        fecha_fin_fuente="2026-08-31",
        actualizada_en_fuente=False,
    )
    [aviso] = freshness_notices([result], HOY)
    assert "agosto de 2026" in aviso and "la fuente no la actualizó" in aviso
    # Un período pasado pedido a propósito sigue sin aviso.
    pasado = _serie(
        "Tipo de cambio de valuación",
        ["2026-06-01", "2026-07-01"],
        ultima_observacion="2026-07-01",
        frecuencia="mensual",
        fecha_fin_fuente="2026-08-31",
        actualizada_en_fuente=False,
    )
    assert freshness_notices([pasado], HOY) == []


def test_con_varias_series_al_dia_no_hay_aviso() -> None:
    result = _varias(
        {"IPC Nivel general": _meses(3, 8, 1.5), "EMAE": _meses(3, 7, 150.0)},
        [
            {
                "id": "148.3_INIVELNAL_DICI_M_26",
                "titulo": "IPC Nivel general",
                "fecha_fin_fuente": "2026-08-01",
                "actualizada_en_fuente": True,
                "frecuencia": "mensual",
            },
            {
                "id": "143.3_NO_PR_2004_A_21",
                "titulo": "EMAE",
                "fecha_fin_fuente": "2026-07-01",
                "actualizada_en_fuente": True,
                "frecuencia": "mensual",
            },
        ],
    )
    assert freshness_notices([result], HOY) == []


async def test_con_varias_series_del_adaptador_el_aviso_nombra_la_serie_atrasada() -> None:
    """De punta a punta, con la metadata que arma el adaptador contra la API
    falsa: el IPC real (hasta agosto, al día) y una mensual que termina en mayo."""
    from tests.unit.series_tiempo_fake import IPC_ID, FakeSeriesApi, ipc_real, serie

    salario_id = "149.1_SOR_PRIVADO_0_M_23"
    salario = serie(
        salario_id,
        [(f"2025-{m:02d}-01", 100.0 + m) for m in range(1, 13)]
        + [(f"2026-{m:02d}-01", 120.0 + m) for m in range(1, 6)],
        description="Índice de salarios. Sector privado registrado. Mensual.",
        dataset="Índice de salarios",
    )
    api = FakeSeriesApi(ipc_real(), salario)
    result = await api.adapter().fetch([IPC_ID, salario_id])
    assert result is not None
    # El agregado sigue como lo arma el adaptador: lo lee también el modelo.
    assert result.metadata["fecha_fin_fuente"] == "2026-05-01"
    assert result.dataset_title.startswith("Índice de Precios al Consumidor")

    [aviso] = freshness_notices([result], HOY, "¿Cómo vienen la inflación y los salarios?")
    assert "«Índice de salarios. Sector privado registrado. Mensual.»" in aviso
    assert "mayo de 2026" in aviso
    assert "Precios al Consumidor" not in aviso


@pytest.mark.parametrize(
    ("last", "freq", "label"),
    [
        (date(2026, 9, 30), "diaria", "30 de septiembre de 2026"),
        (date(2026, 4, 1), "mensual", "abril de 2026"),
        (date(2026, 4, 1), "trimestral", "el 2.º trimestre de 2026"),
        (date(2026, 1, 1), "semestral", "el 1.er semestre de 2026"),
        (date(2025, 1, 1), "anual", "2025"),
    ],
)
def test_la_fecha_como_la_diria_una_persona(last: date, freq: str, label: str) -> None:
    assert observation_label(last, freq) == label
