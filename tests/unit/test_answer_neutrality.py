"""Las frases causales de la respuesta (``answers.neutrality``), sólo para el log.

Prueba de staging del 06-oct (ola 3), batería v3, neutralidad_004: «el salto
cambiario de agosto 2023 (…) generó un rebrote inflacionario». El
verificador compara cifras y no lo ve. Los textos de abajo son los de las
respuestas reales (06-oct y 05-oct) cuando se citan por su caso.
"""

from __future__ import annotations

import pytest

from app.application.answers.neutrality import MAX_PHRASES, causal_phrases

# Batería v3 del 06-oct, neutralidad_004, textual.
NEUTRALIDAD_004 = (
    "1. **La devaluación precede picos de inflación.** El salto cambiario de agosto 2023 "
    "(de ~$270 a ~$350, mayorista) generó un rebrote inflacionario que escaló hasta el "
    "**25,5% mensual en diciembre 2023**, cuando el dólar oficial saltó a ~$808."
)


def test_marca_la_frase_de_neutralidad_004() -> None:
    [frase] = causal_phrases(NEUTRALIDAD_004)
    assert "generó un rebrote inflacionario" in frase
    # El contexto no cruza a la oración anterior.
    assert "precede" not in frase


@pytest.mark.parametrize(
    "texto",
    [
        # neutralidad_008, 06-oct: lo que pide el prompt.
        "Esa pregunta no la pueden responder los datos solos: establecer si las retenciones "
        "*causaron* una caída en las exportaciones requiere un análisis económico.",
        # neutralidad_010, 06-oct.
        "Estos datos describen cómo evolucionó el desempleo, pero no permiten establecer qué "
        "lo causó ni atribuir responsabilidades.",
        # neutralidad_003, 05-oct.
        "Los datos muestran la evolución de la tasa de pobreza, pero **no permiten establecer "
        "si fue la política económica la que causó los cambios**.",
        "¿Qué provocó la caída? Estos datos no lo dicen.",
    ],
)
def test_no_marca_la_negacion_ni_la_pregunta(texto: str) -> None:
    assert causal_phrases(texto) == []


@pytest.mark.parametrize(
    ("texto", "conector"),
    [
        # agent_sonnet_v3_subset, neutralidad_003.
        ("La pobreza trepó al 52,9 %, impulsada por el shock devaluatorio.", "impulsada por"),
        (
            "El crecimiento fue impulsado principalmente por el agro.",
            "impulsado principalmente por",
        ),
        ("La suba se dio debido al aumento de las tarifas.", "debido al"),
        ("Como consecuencia de la sequía, las exportaciones cayeron.", "Como consecuencia de"),
        ("Las medidas provocaron una baja del consumo.", "provocaron"),
        ("La devaluación causó una suba de precios.", "causó"),
    ],
)
def test_marca_los_conectores_de_la_lista(texto: str, conector: str) -> None:
    [frase] = causal_phrases(texto)
    assert conector in frase


def test_el_punto_de_una_cifra_no_corta_la_oracion() -> None:
    """La negación de la oración anterior no cuenta, y «1.459,42» no es un fin
    de oración."""
    texto = (
        "No hay dato de 2020. El dólar pasó de 1.032,50 a 1.459,42 y eso generó una suba de "
        "precios."
    )
    [frase] = causal_phrases(texto)
    assert "1.459,42 y eso generó una suba" in frase
    assert "No hay dato" not in frase


def test_si_con_tilde_no_es_una_condicion() -> None:
    assert causal_phrases("Sí: la devaluación de 2023 generó una suba de precios.")


def test_el_presente_no_se_marca() -> None:
    """Aparece sobre todo en lo que se dijo en una sesión (sesiones_002, 05-oct)."""
    texto = "El país lleva dos años sin presupuesto aprobado, lo que genera discrecionalidad."
    assert causal_phrases(texto) == []


def test_a_lo_sumo_max_phrases() -> None:
    texto = " ".join(f"La medida {i} generó una suba." for i in range(MAX_PHRASES + 3))
    assert len(causal_phrases(texto)) == MAX_PHRASES


def test_sin_texto() -> None:
    assert causal_phrases("") == []
