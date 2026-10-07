"""Falsos negativos de la batería v3 en la prueba de staging del 06-oct-2026.

Tres respuestas correctas fallaron sólo por una palabra clave:

- ``series_002`` pedía "dólar" y la respuesta dio la serie del BCRA en "$/USD";
- ``argentina_datos_003`` pedía "CCL" y la respuesta dijo "contado con
  liquidación";
- ``series_006`` pedía "actividad" además de "EMAE".

Las cifras de las tres se cotejaron con la fuente pública (BCRA v4, DolarApi y
API de Series de Tiempo). Los patrones aceptan ahora el sinónimo inequívoco, y
cada uno sigue rechazando una respuesta que habla de otra cosa.

``ckan_004`` (256 diputados contra 257) no es de estos: la HCDN publica 257
diputados en ejercicio y el 256 sale de una copia vieja de la composición por
bloques. Lo cuida ``test_256_legisladores_falla``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.evaluation.quality_checks import JudgeThresholds, assess
from tests.evaluation.run_eval import load_golden_dataset, score_run

EVAL = Path(__file__).parents[1] / "evaluation"
ENTRIES = {e["id"]: e for e in load_golden_dataset(EVAL / "golden_dataset.json")}
OLA3 = json.loads((EVAL / "fixtures" / "bateria_ola3_2026_10_06.json").read_text(encoding="utf-8"))
RUNS = {c["caso"]: c for c in OLA3["corridas"]}


def _palabras_clave_ok(case: str, texto: str) -> bool:
    check = assess(ENTRIES[case], texto, []).get("palabras_clave")
    assert check is not None
    return check.ok


@pytest.mark.parametrize("case", ["series_002", "argentina_datos_003", "series_006"])
def test_la_respuesta_real_del_06_oct_aprueba(case: str) -> None:
    """Re-puntuada como en ``--rescore``: misma respuesta, fuentes, evidencia y
    jueces. Ese día falló sólo por la palabra clave."""
    run = RUNS[case]
    assert [f.split(":")[0] for f in run["fallas_ese_dia"]] == ["palabras_clave"]
    quality = score_run(ENTRIES[case], run, None, JudgeThresholds())
    assert quality["passed"], quality["failures"]


@pytest.mark.parametrize(
    ("case", "texto"),
    [
        # Otro tipo de cambio: el índice real multilateral no es en dólares.
        (
            "series_002",
            "El índice de tipo de cambio real multilateral del BCRA pasó de 79,2 a 95,4 "
            "puntos en 2025.",
        ),
        ("series_002", "El tipo de cambio oficial del euro cerró 2025 en $1.712 por EUR."),
        # Otro dólar: el MEP o el blue no son el contado con liquidación.
        (
            "argentina_datos_003",
            "El dólar MEP cotiza hoy a $1.590,20 (venta), según DolarApi, un agregador no oficial.",
        ),
        (
            "argentina_datos_003",
            "El dólar blue cotiza hoy a $1.640 (venta), según DolarApi, un agregador no oficial.",
        ),
        # La actividad, pero de otro indicador.
        (
            "series_006",
            "El PBI creció 4,5 % en 2025 y la actividad económica sigue firme en 2026, según el "
            "INDEC.",
        ),
        (
            "series_006",
            "La actividad industrial (IPI manufacturero) cayó 2,2 % interanual en julio de 2026.",
        ),
    ],
)
def test_el_patron_sigue_rechazando_una_respuesta_sobre_otra_cosa(case: str, texto: str) -> None:
    assert not _palabras_clave_ok(case, texto)


@pytest.mark.parametrize(
    ("case", "texto"),
    [
        ("series_002", "El tipo de cambio oficial cerró 2025 en $1.459,42 por dólar."),
        ("series_002", "El tipo de cambio oficial cerró 2025 en $1.459,42 por US$."),
        (
            "series_002",
            "El tipo de cambio oficial:\n\n| Mes | Mayorista ($/USD) |\n| Diciembre | 1.459,42 |",
        ),
        ("argentina_datos_003", "El CCL cotiza hoy a $1.610,80."),
        ("argentina_datos_003", "El Contado con Liquidación cotiza hoy a $1.610,80."),
        ("argentina_datos_003", "El contado con liquidacion cotiza hoy a $1.610,80."),
        ("argentina_datos_003", "El contado con liqui cotiza hoy a $1.610,80."),
        ("series_006", "El EMAE cayó 1,44 % interanual en julio de 2026."),
        (
            "series_006",
            "El Estimador Mensual de Actividad Económica cayó 1,44 % interanual en julio de 2026.",
        ),
    ],
)
def test_las_variantes_del_mismo_nombre_aprueban(case: str, texto: str) -> None:
    assert _palabras_clave_ok(case, texto)
