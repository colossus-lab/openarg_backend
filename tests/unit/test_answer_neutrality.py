"""Las frases causales de la respuesta (``answers.neutrality``), sólo para el log.

Prueba de staging del 06-oct (ola 3), batería v3, neutralidad_004: «el salto
cambiario de agosto 2023 (…) generó un rebrote inflacionario». El
verificador compara cifras y no lo ve. Los textos de abajo son los de las
respuestas reales (06-oct y 05-oct) cuando se citan por su caso.
"""

from __future__ import annotations

import pytest

from app.application.answers.neutrality import MAX_PHRASES, causal_phrases, scan_causal

# Batería v3 del 06-oct, neutralidad_004, textual.
NEUTRALIDAD_004 = (
    "1. **La devaluación precede picos de inflación.** El salto cambiario de agosto 2023 "
    "(de ~$270 a ~$350, mayorista) generó un rebrote inflacionario que escaló hasta el "
    "**25,5% mensual en diciembre 2023**, cuando el dólar oficial saltó a ~$808."
)


def test_marca_las_dos_frases_de_neutralidad_004() -> None:
    """La regla general del título también (revisión de #163): antes sólo
    marcaba «generó»."""
    [regla, frase] = causal_phrases(NEUTRALIDAD_004)
    assert "La devaluación precede picos de inflación" in regla
    assert "generó un rebrote inflacionario" in frase
    # El contexto no cruza a la oración anterior, tampoco después de la negrita.
    assert "precede" not in frase


@pytest.mark.parametrize(
    "texto",
    [
        # Batería v3 del 06-oct, neutralidad_004, textual: la tabla y el punto 3.
        "| Ago-Sep 2023 | 12,4% y 12,8% (saltos por devaluación) |",
        "| **Dic 2023** | **25,5%** (pico post-devaluación) |",
        "3. **El tipo de cambio como ancla.**",
        "En etapas donde el dólar oficial sube por debajo de la inflación, el tipo de cambio "
        "funciona como ancla nominal.",
        # neutralidad_008, 06-oct, textual.
        "- Las caídas más marcadas se dieron en 2009 (crisis global), 2012–2015, 2020 "
        "(pandemia) y 2023 (sequía histórica), períodos en los que también coincidieron otros "
        "factores.",
        # Línea base agent_sonnet_v3_subset_x1, neutralidad_004 (juez 0,2).
        "La relación es estrecha: las devaluaciones del tipo de cambio suelen impulsar la "
        "inflación, aunque con rezago.",
        "**Devaluaciones grandes generan inflación inmediata y alta.**",
        # Línea base agent_sonnet_v3_subset_x1, neutralidad_008 (juez 0,4).
        "3. **Las retenciones sí deprimen exportaciones *en los márgenes*.**",
        # Revisión de #163: lo que nombra la regla del prompt y el control no veía.
        "La inflación bajó gracias a la política de estabilización.",
        "La devaluación impulsó la inflación de diciembre.",
        "La emisión monetaria provoca inflación.",
        "La baja de la pobreza se debe a la desaceleración de la inflación.",
        "La caída de 2020 se explica por la pandemia.",
        "En 2020 las exportaciones cayeron a USD 54.884 M a raíz de la pandemia.",
        "La recesión fue producto del ajuste.",
        "Las exportaciones cayeron en 2023 por la sequía.",
        "La devaluación precede a la inflación.",
        # La negación o la condición en otra cláusula no apaga la marca.
        "Si bien la base monetaria creció, la devaluación causó la suba de precios.",
        "La inflación no bajó de inmediato: la devaluación provocó un pico en diciembre.",
        "En 2020 no hubo vuelos y la pandemia provocó una caída de las exportaciones.",
    ],
)
def test_marca_lo_que_antes_se_le_escapaba(texto: str) -> None:
    assert causal_phrases(texto), texto


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


@pytest.mark.parametrize(
    "texto",
    [
        # Línea base agent_sonnet_x3, sesiones_002, textual.
        "- Se advirtió que el país lleva **dos años sin presupuesto aprobado por el Congreso**, "
        "lo que genera discrecionalidad, debilita el control parlamentario.",
        # Revisión de #163 (r3): lo que la batería reprueba aunque vaya atribuido.
        "El diputado Gómez sostuvo en la sesión que las retenciones generaron una caída de las "
        "exportaciones.",
        "Según el INDEC, la sequía generó una caída de las exportaciones en 2023.",
        'Un orador planteó: "la presión fiscal provoca la fuga de inversiones".',
        "Para un diputado, «las retenciones deprimen las exportaciones».",
    ],
)
def test_lo_atribuido_va_aparte(texto: str) -> None:
    """El presente se marca (revisión de #163), pero lo dicho en una sesión o lo
    que da la fuente oficial, atribuido, va a ``atribuidas``: el prompt permite
    la cita de la fuente oficial y la batería reprueba las dos."""
    frases, atribuidas = scan_causal(texto)
    assert frases == []
    assert len(atribuidas) == 1


def test_el_presente_sin_atribuir_se_marca() -> None:
    """Antes no se marcaba ningún presente, para no marcar lo dicho en una
    sesión. Eso dejaba afuera las reglas generales que prohíbe el prompt; lo
    dicho en una sesión ahora va a ``atribuidas`` (test de arriba)."""
    texto = "El país lleva dos años sin presupuesto aprobado, lo que genera discrecionalidad."
    assert causal_phrases(texto)


@pytest.mark.parametrize(
    "texto",
    [
        # Línea base agent_sonnet_x3, sesiones_004: «impulsar» una ley.
        "Quienes la impulsaron la presentaron como una modernización del mercado de trabajo.",
        "El oficialismo impulsó el proyecto de ley de presupuesto.",
        # Un propósito, no una causa.
        "Se presentó como una reforma para flexibilizar el mercado y generar empleo.",
        # q25 del 06-oct, nueva_21: es la advertencia que pide el prompt.
        "**El impacto causal** —cuánto de la variación se explica por la AUH— requiere un "
        "modelo contrafáctico.",
        # Describen, no explican.
        "La inflación se frenó en mayo.",
        "La base monetaria frenó su crecimiento.",
        "El dólar se disparó en abril.",
        "En abril el tipo de cambio saltó 5,21 % (coincide con el levantamiento del cepo).",
        "El resultado de la balanza comercial fue positivo.",
        "Defunciones por causa, 2022.",
    ],
)
def test_no_marca_lo_que_no_es_una_causa(texto: str) -> None:
    assert scan_causal(texto) == ([], [])


def test_una_frase_por_oracion() -> None:
    texto = "Cayeron en 2009 (crisis global), 2020 (pandemia) y 2023 (sequía histórica)."
    assert len(causal_phrases(texto)) == 1


def test_a_lo_sumo_max_phrases() -> None:
    texto = " ".join(f"La medida {i} generó una suba." for i in range(MAX_PHRASES + 3))
    assert len(causal_phrases(texto)) == MAX_PHRASES


def test_sin_texto() -> None:
    assert causal_phrases("") == []
