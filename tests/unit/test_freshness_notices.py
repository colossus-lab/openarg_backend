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
