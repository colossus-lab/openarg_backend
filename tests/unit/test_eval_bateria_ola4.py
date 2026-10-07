"""Dos fallas de la batería v3 en la prueba de staging del 07-oct-2026 que eran de la prueba.

- ``series_009``: "El último dato disponible es de agosto de 2026 … Inflación
  mensual: 1,66 % (variación respecto a julio 2026) … interanual: 33,54 %
  (variación respecto a agosto 2025)". Las dos cifras son exactas (API de
  Series de Tiempo, 148.3_INIVELNAL_DICI_M_26: 12.276,766 / 12.076,3937 − 1 =
  1,66 %; contra 9.193,2441 de agosto de 2025, 33,54 %). ``fecha_del_dato``
  fechaba el 1,66 en julio: en su renglón el único período es la base de la
  comparación, y ``period_of`` la tomaba igual.
- ``ckan_002``: "El dataset tiene 52.367 registros". Es el ``filas_usadas`` del
  conteo por barrio que cita la respuesta: el mismo SQL, corrido en staging en
  sólo lectura, da los mismos 50 grupos de la evidencia y ``__filas_total`` =
  52.367 = count(*). La batería no guardaba las cuentas de filas de
  ``calcular``, así que la fuente salía "citada sin cifra".

Cada arreglo sigue rechazando la respuesta equivocada que el chequeo tiene que
rechazar: el dato de julio presentado como último, y un total de filas que no
sale de la fuente.

La primera versión del arreglo de series_009 tomaba cualquier período cercano
(revisión del PR #169): una fecha de difusión ("difundido el 9 de septiembre de
2026") fechaba como vigente un dato viejo, y series_014, que sólo tiene oráculo
de período, aprobaba entero con el dato de abril. Ahora sólo cuenta el período
que implica la base (``_period_implied_by_base``).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from app.application.answers.tools.catalogo import Calcular
from app.domain.ports.sandbox.sql_sandbox import MartInfo
from tests.evaluation.engines import evidence_items
from tests.evaluation.quality_checks import (
    JudgeThresholds,
    check_fecha_del_dato,
    dated_periods,
    numbers_in_answer,
    period_of,
)
from tests.evaluation.quality_checks import sources_without_figures as sin_cifra
from tests.evaluation.run_eval import load_golden_dataset, score_run
from tests.unit.test_agent_catalogo_tools import Sandbox, _ctx

EVAL = Path(__file__).parents[1] / "evaluation"
ENTRIES = {e["id"]: e for e in load_golden_dataset(EVAL / "golden_dataset.json")}
OLA4 = json.loads((EVAL / "fixtures" / "bateria_ola4_2026_10_07.json").read_text(encoding="utf-8"))
RUNS = {c["caso"]: c for c in OLA4["corridas"]}


def _score(case: str, run: dict[str, Any]) -> dict[str, Any]:
    return score_run(ENTRIES[case], run, run.get("expectativas"), JudgeThresholds())


# ── series_009: la base de la comparación no es la fecha del dato ──

S009 = RUNS["series_009"]
IPC = S009["expectativas"]["expected_period"]  # agosto de 2026
IPC_VALORES = S009["expectativas"]["expected_values"]  # 1,66 % y 33,54 %


def test_series_009_real_del_07_oct_aprueba() -> None:
    """Re-puntuada como en ``--rescore``: misma respuesta, fuentes, evidencia,
    jueces y cifras de los oráculos de ese día. Falló sólo por la fecha."""
    assert [f.split(":")[0] for f in S009["fallas_ese_dia"]] == ["fecha_del_dato"]
    quality = _score("series_009", S009)
    assert quality["passed"], quality["failures"]
    fecha = next(c for c in quality["checks"] if c["name"] == "fecha_del_dato")
    assert fecha["detail"] == "agosto de 2026"


@pytest.mark.parametrize(
    "texto",
    [
        # Las cifras de agosto rotuladas como de julio: una coincidencia de
        # valor con fecha vieja.
        "El último dato disponible es de **julio de 2026**, según el IPC Nacional (INDEC):\n\n"
        "- **Inflación mensual: 1,66%** (variación respecto a junio 2026)\n"
        "- **Inflación interanual: 33,54%** (variación respecto a julio 2025)",
        # El dato viejo (julio, 2,11 %) presentado como el último.
        "El último dato disponible es de **julio de 2026**:\n\n"
        "- **Inflación mensual: 2,11%** (variación respecto a junio 2026)",
        # Sólo nombra las bases: no dice de cuándo es el dato, y sigue fallando
        # como antes del arreglo.
        "- **Inflación mensual: 1,66%** (variación respecto a julio 2026)\n"
        "- **Inflación interanual: 33,54%** (variación respecto a agosto 2025)",
        # Las cifras de agosto con las bases de julio (contra junio y contra
        # julio de 2025) y la fecha en que se publicaron. La primera versión
        # del arreglo la fechaba el 14 de agosto y el caso entero aprobaba.
        "Según el IPC Nacional del INDEC:\n\n"
        "- **Inflación mensual: 1,66%** (variación respecto a junio 2026)\n"
        "- **Inflación interanual: 33,54%** (variación respecto a julio 2025)\n\n"
        "El INDEC publicó estos datos el 14 de agosto de 2026.",
        # El dato de julio con la fecha de difusión arriba, en vez del mes.
        "El INDEC publicó el 14 de agosto de 2026 el último IPC:\n\n"
        "- **Inflación mensual: 2,11%** (variación respecto a junio 2026)\n"
        "- **Inflación interanual: 36,1%** (variación respecto a julio 2025)",
        # Ídem con un mes en el título, que no es el que implica la base.
        "Según el informe del INDEC de agosto de 2026:\n\n"
        "- **Inflación mensual: 2,11%** (variación respecto a junio 2026)",
    ],
)
def test_series_009_sigue_rechazando_un_dato_que_no_es_el_ultimo(texto: str) -> None:
    assert not check_fecha_del_dato(texto, IPC, IPC_VALORES).ok


def test_series_009_la_base_sigue_sin_tapar_el_dato_en_la_misma_oracion() -> None:
    """Lo de antes no cambia: con el período del dato en la misma oración, la
    base no lo reemplaza, y una base sola no hace pasar un dato viejo."""
    assert check_fecha_del_dato(
        "La inflación de agosto de 2026 fue 1,66 %, respecto de julio de 2026.", IPC, IPC_VALORES
    ).ok
    assert not check_fecha_del_dato(
        "La inflación de julio de 2026 fue 2,11 %, respecto de junio de 2026.", IPC, IPC_VALORES
    ).ok


# ── series_014: una fecha de difusión no es la fecha del dato ──
# Su oráculo sólo tiene el período (sin cifras esperadas): fecha_del_dato es
# lo único que rechaza un dato viejo.

S014 = RUNS["series_014"]
IPI = S014["expectativas"]["expected_period"]  # julio de 2026
# El dato de abril de 2026 (−2,4 % interanual, −2,9 % mensual), con las
# bases que le corresponden y otra fecha cerca.
IPI_ABRIL = (
    "- **IPI manufacturero: −2,4 % interanual** (respecto a abril 2025)\n"
    "- **Variación mensual: −2,9 %** (respecto a marzo 2026)"
)


def test_series_014_real_del_07_oct_aprueba() -> None:
    assert S014["fallas_ese_dia"] == []
    quality = _score("series_014", S014)
    assert quality["passed"], quality["failures"]


@pytest.mark.parametrize(
    "encabezado",
    [
        "Según el informe del INDEC difundido el 9 de septiembre de 2026:",
        "La actividad industrial, con datos actualizados al 30 de septiembre de 2026:",
        "Según el informe del INDEC de septiembre de 2026:",
    ],
)
def test_series_014_una_fecha_cercana_no_fecha_como_vigente_un_dato_viejo(encabezado: str) -> None:
    texto = f"{encabezado}\n\n{IPI_ABRIL}"
    assert not check_fecha_del_dato(texto, IPI).ok
    quality = _score("series_014", {**S014, "answer": texto})
    assert not quality["passed"]
    assert any(f.startswith("fecha_del_dato") for f in quality["failures"])


def test_series_014_el_periodo_que_implica_la_base_si_cuenta() -> None:
    """El mes del título que coincide con las bases (julio de 2026: un mes
    después de junio y un año después de julio de 2025) fecha el dato."""
    texto = (
        "El último dato del IPI manufacturero es de julio de 2026:\n\n"
        "- **Interanual: −4,9 %** (respecto a julio 2025)\n"
        "- **Mensual: −1,5 %** (respecto a junio 2026)"
    )
    fecha = check_fecha_del_dato(texto, IPI)
    assert fecha.ok and fecha.detail == "julio de 2026"


def _periodo(texto: str, cifra: str) -> str | None:
    hoy = date(2026, 10, 7)
    periodos = [p for p in dated_periods(texto, hoy) if p.granularity != "anio"]
    n = next(x for x in numbers_in_answer(texto) if x.raw.startswith(cifra))
    p = period_of(n, periodos, texto)
    return p.raw if p else None


@pytest.mark.parametrize(
    ("texto", "cifra", "periodo"),
    [
        # La base va antes: la cifra es su valor (neutralidad_010, ola 3).
        (
            "| 1° T 2026 | 7,8 % |\n| **2° T 2026** | **7,9 %** |\n\n"
            "El último dato disponible es el **2° trimestre de 2026: 7,9 %**, según el INDEC. "
            "La tasa subió respecto al 4° trimestre de 2023 (5,7 %) y se mantuvo en torno a "
            "7–8 % durante 2024, 2025 y lo que va de 2026.",
            "5,7",
            "4° trimestre de 2023",
        ),
        # Ídem (legacy_normal_x3, argentina_datos_005).
        (
            "**La inflación mensual más reciente disponible es 1.66% en agosto de 2026** "
            "(datos de INDEC a través de datos.gob.ar).\n\n"
            "Esto representa una desaceleración significativa respecto a marzo, cuando "
            "alcanzó el pico de 3.38%.",
            "3.38",
            "marzo",
        ),
        # Otra cifra en el medio: la base es del 1,66, no del 2,11
        # (agent_sonnet_x3, argentina_datos_005).
        (
            "La inflación mensual más reciente fue de **1,66% en agosto de 2026**, según el "
            "Índice de Precios al Consumidor Nacional (INDEC, base diciembre 2016).\n\n"
            "En los últimos tres meses la evolución fue: junio 1,89% → julio 2,11% → agosto "
            "1,66%, lo que muestra una leve desaceleración respecto a julio.",
            "2,11",
            "julio",
        ),
        # Lo de alrededor son días, no el mes que implica la base
        # (neutralidad_002, ola 4).
        (
            "El dólar minorista (BCRA) pasó de $1.104 el 1° de abril de 2025 a **$1.541 el 6 "
            "de octubre de 2026**, una suba de 39,6 %. El mayorista (Com. A 3500) subió de "
            "$1.075 a **$1.520**, un 41,5 %. En los últimos meses el tipo de cambio se mantuvo "
            "relativamente estable, oscilando en torno a los $1.530 desde agosto.\n\n"
            "**Brecha cambiaria**\nEl dólar blue cotizaba a **$1.555 (venta) el 7 de octubre "
            "de 2026** según DolarApi (fuente no oficial).",
            "1.530",
            "agosto",
        ),
    ],
)
def test_una_base_que_no_es_de_la_cifra_no_la_refecha(texto: str, cifra: str, periodo: str) -> None:
    """Respuestas guardadas que la primera versión del arreglo re-fechaba en un
    período más nuevo: se quedan con su base, como antes."""
    assert _periodo(texto, cifra) == periodo


# ── ckan_002: el total de filas que `calcular` le dio al modelo ──

C002 = RUNS["ckan_002"]
MART = "mart.caba_transportes_autorizados"
# Las columnas de la vista en staging (pg_attribute, 07-oct).
TIPOS = [
    ("barrio", "text"),
    ("comuna", "text"),
    ("codigo_postal", "numeric"),
    ("long", "text"),
    ("lat", "text"),
    ("periodo", "text"),
    ("numero_documento", "text"),
]


class MartSandbox(Sandbox):
    """El sandbox de prueba de ``calcular`` sirviendo el mart de ckan_002."""

    async def describe_marts(self, names: list[str]) -> dict[str, Any]:
        titulo = C002["sources"][0]["name"].removesuffix(" — conteo")
        return {MART: MartInfo(MART, "caba_transportes_autorizados", titulo, "transporte", 52367)}

    async def get_column_types(self, names: list[str]) -> dict[str, list[tuple[str, str]]]:
        return {n: self.types for n in names}


async def _calcular_ckan_002() -> Any:
    """Lo que devolvió ``calcular`` en ckan_002, con los grupos de staging."""
    total = C002["filas_total_staging"]
    filas = [{**g, "__filas": g["valor"], "__filas_total": total} for g in C002["grupos_staging"]]
    sandbox = MartSandbox(TIPOS, [("AS valor", filas)])
    out = await Calcular().run(
        {"tabla": MART, "operacion": "conteo", "agrupar_por": ["barrio"]}, _ctx(sandbox)
    )
    assert sandbox.sql_with("AS valor") == C002["sql"]  # el SQL de la corrida
    return out


async def test_ckan_002_el_total_de_filas_es_de_la_fuente_citada() -> None:
    out = await _calcular_ckan_002()
    [result] = out.results
    # Es la cifra que vio el modelo, y no está en las filas del resultado.
    assert json.loads(out.content)["filas_usadas"] == 52367
    assert result.metadata["filas_usadas"] == 52367
    assert all(r["valor"] != 52367 for r in result.records)
    [item] = evidence_items([result])
    assert item["conteos_de_filas"] == [52367.0]
    assert item["numbers"] == C002["evidence_items"][0]["numbers"]  # el resto, igual que ese día

    fuentes = [{"name": result.dataset_title, "url": result.portal_url}]
    assert fuentes[0]["name"] == C002["sources"][0]["name"]
    assert sin_cifra(C002["answer"], fuentes, [item]) == ([], [])
    # Re-puntuada entera con esta evidencia: aprueba.
    quality = _score("ckan_002", {**C002, "sources": fuentes, "evidence_items": [item]})
    assert quality["passed"], quality["failures"]


async def test_ckan_002_sigue_rechazando_un_total_que_no_sale_de_la_fuente() -> None:
    [result] = (await _calcular_ckan_002()).results
    items = evidence_items([result])
    fuentes = C002["sources"]
    inventado = C002["answer"].replace("52.367 registros", "61.204 registros")
    assert sin_cifra(inventado, fuentes, items) == ([fuentes[0]["name"]], [])
    # Y el reporte de ese día, que no guardaba las cuentas de filas, sigue
    # dando lo mismo que dio: el arreglo está en lo que se guarda al correr.
    assert sin_cifra(C002["answer"], fuentes, C002["evidence_items"]) == (
        [fuentes[0]["name"]],
        [],
    )


def test_las_cuentas_de_filas_no_cambian_la_regla_de_los_conteos_chicos() -> None:
    """Una fuente con sólo conteos chicos (los tramos de viaje de complex_003)
    y su ``filas_usadas`` (34 = 10 + 11 + 13): un conteo suelto sigue sin
    alcanzar, dos siguen alcanzando, y el total también es una cifra suya."""
    item = {"title": "Viajes — conteo", "url": "u", "numbers": [10.0, 11.0, 13.0]}
    item["conteos_de_filas"] = [34.0]
    fuentes = [{"name": "Viajes — conteo", "url": "u"}]
    base = "Ana Carla Carrizo pagó $ 8.224.603 de viáticos y "
    assert sin_cifra(base + "registró 10 tramos.", fuentes, [item]) == (["Viajes — conteo"], [])
    assert sin_cifra(base + "registró 10 y 13 tramos.", fuentes, [item]) == ([], [])
    assert sin_cifra(base + "registró 34 tramos.", fuentes, [item]) == ([], [])
