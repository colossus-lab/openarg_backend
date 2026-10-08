"""Falsos negativos de la batería v3 en la prueba de staging del 06-oct-2026.

Tres respuestas correctas fallaron sólo por una palabra clave:

- ``series_002`` pedía "dólar" y la respuesta dio la serie del BCRA en "$/USD";
- ``argentina_datos_003`` pedía "CCL" y la respuesta dijo "contado con
  liquidación";
- ``series_006`` pedía "actividad" además de "EMAE".

Las cifras de las tres se cotejaron con la fuente pública (BCRA v4, DolarApi y
API de Series de Tiempo). Los patrones aceptan ahora el sinónimo inequívoco, y
cada uno sigue rechazando una respuesta que habla de otra cosa. En
``series_002`` entra también la Comunicación A 3500, el único nombre del dólar
que daba una respuesta correcta del 05-oct.

Ninguno de los tres casos controla cifras. En ``series_006`` eso deja pasar una
respuesta que nombra al EMAE y da otro indicador: lo marca el ``xfail`` del
final, que queda así hasta que se decida si el caso lleva un oráculo.

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
        # La respuesta correcta del 05-oct (integ_correct_sub_x2.json, corrida 0),
        # recortada: el resto tampoco nombra la moneda. Sólo la identifica la
        # Comunicación A 3500 del BCRA, que es el dólar mayorista de referencia.
        (
            "series_002",
            "El tipo de cambio oficial **arrancó 2025 en torno a $1.062 (minorista) y cerró "
            "el año en $1.479**, según los datos del BCRA. A continuación, el cierre de cada "
            "mes:\n\n| Mes | Minorista (vend.) | Mayorista (A 3500) |\n|---|---|---|\n"
            "| Enero | $1.079,63 | $1.053,50 |\n| Diciembre | $1.479,28 | $1.459,42 |",
        ),
        ("series_002", "El tipo de cambio oficial mayorista (A3500) cerró 2025 en $1.459,42."),
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


@pytest.mark.xfail(
    strict=True,
    reason=(
        "series_006 sólo pide nombrar el indicador y no controla ninguna cifra: una "
        "respuesta que nombra al EMAE y da otro indicador aprueba. Frenarla exige el "
        "oráculo serie_variacion_interanual sobre 143.3_NO_PR_2004_A_21, que además "
        "rechaza las respuestas trimestrales y las que dan sólo niveles: decisión pendiente."
    ),
)
def test_series_006_rechaza_una_sustitucion_que_nombra_al_emae() -> None:
    """La misma corrida del 06-oct (fuentes y evidencia) con otro texto y sin
    jueces: lo que miran los chequeos fijos. Antes de este PR la frenaba, de
    casualidad, la palabra "actividad"; una sustitución que la escribiera
    ("la actividad industrial cayó…") también aprobaba."""
    run = {k: v for k, v in RUNS["series_006"].items() if k != "judge"}
    run["answer"] = (
        "El EMAE de 2026 todavía no se publicó. El IPI manufacturero cayó 2,2 % "
        "interanual en julio de 2026."
    )
    quality = score_run(ENTRIES["series_006"], run, None, JudgeThresholds())
    assert not quality["passed"]
