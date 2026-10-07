"""El preámbulo de proceso del modelo no se publica (H063).

Los casos son respuestas reales: la prueba del 06-oct en staging (nueva_16),
las corridas de la revisión del 05-oct (integrado y staging) y la batería.
Los que no cambian son aperturas legítimas de esas mismas corridas, incluidas
las que se parecen a un preámbulo.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.application.answers.monologo import NOMBRES_PUBLICOS, sin_monologo

NUEVA_16_OLA3 = (
    'Todos los registros del padrón 2022 son usuarios "EN PADRON TS" (ninguno está dado de '
    "baja o excluido). El total es claro.\n\n"
    "El padrón de **tarifa social eléctrica en Mendoza en 2022** registraba **110.179 "
    "usuarios**, tanto en el 1er trimestre (julio-202207) como en el 2do trimestre, que tienen "
    "exactamente la misma cantidad.\n\n"
    "El desglose por situación en el 1er trimestre de 2022 es:\n"
    "- Jubilados y pensionados: 35.870\n"
    "- Transitorios: 25.815\n\n"
    "Fuente: **Tarifa Social Eléctrica 2022**, portal de datos abiertos de la provincia de "
    "Mendoza."
)

NUEVA_08_INTEGRADO = (
    "Hmm, hay un dato para 2026-S1 pero la fuente llega hasta 2026-01-01 (que es 2025-S2). El "
    "dato 2026-S1 parece adelantarse a lo que la fuente confirma. Voy a usar el más reciente "
    "confirmado por la fuente, que es 2025-S2 (segundo semestre 2025).\n\n"
    "Según el INDEC (EPH continua), **el 22,3% de la población del Gran Rosario era pobre en el "
    "2° semestre de 2025** — el dato más reciente disponible."
)

NUEVA_09_INTEGRADO = (
    "La serie de base monetaria del INDEC/BCRA via API de series llega solo hasta junio 2026. "
    "Ya tengo los valores puntuales de fin de diciembre 2025 y fin de agosto 2026 directamente "
    "del BCRA, pero no puedo hacer la resta ni el porcentaje yo mismo. Lo que sí puedo es usar "
    "`variables_bcra` que ya me dio los dos cierres. Le pido la variación a series_tiempo "
    "usando la serie diaria de reservas (que sí cubre todo el período) e informo la de base "
    "monetaria con los valores puntuales del BCRA sin calcular el porcentaje yo mismo, usando "
    "la herramienta `calcular` no aplica aquí. Busco si hay otra serie de base monetaria que "
    "llegue a agosto 2026.\n\n"
    "Los datos están completos del BCRA directamente. Tengo:\n\n"
    "- **Reservas**: variación **+17,43%** (de USD 41.095 M a USD 48.259 M), calculada por la "
    "herramienta.\n"
    "- **Base monetaria**: cierre dic-2025 = $42.956.965 M; cierre ago-2026 = $47.451.537 M."
)

NUEVA_12_STAGING = (
    "Tengo todos los datos. Ahora calculo los promedios de cada semestre para dar la "
    "comparación.\n\n"
    "**Semestre enero–junio 2025:** 116.66\n"
    "**Semestre enero–junio 2026:** 114.08"
)

NUEVA_20_STAGING = (
    "Con los datos del SNIC (Sistema Nacional de Información Criminal) ya puedo responder.\n\n"
    "---\n\n"
    "**Los homicidios dolosos en la provincia de Buenos Aires bajaron durante la gestión de "
    "Kicillof, aunque con altibajos.**"
)

COMPLEX_004_BATERIA = (
    'Ya tengo todo. La tabla tiene dos series bajo "Personas": una es la tasa de pobreza y la '
    "otra, la de indigencia. Los últimos 3 períodos son los relevantes. También tengo la CBT "
    "mensual. Armo la respuesta.\n\n"
    "Acá va la evolución de la **tasa de pobreza** (% de personas) junto con el **valor de la "
    "Canasta Básica Total (CBT)**, según el INDEC:\n\n"
    "---\n\n"
    "### Tasa de pobreza — 31 aglomerados urbanos (EPH continua, semestral)"
)


@pytest.mark.parametrize(
    ("texto", "empieza"),
    [
        (NUEVA_16_OLA3, "El padrón de **tarifa social eléctrica en Mendoza en 2022**"),
        (NUEVA_08_INTEGRADO, "Según el INDEC (EPH continua), **el 22,3%"),
        # El segundo párrafo («… Tengo:») queda: un «Tengo:» al final también
        # cierra «No encontré X. Lo que sí tengo:», que es la respuesta.
        (NUEVA_09_INTEGRADO, "Los datos están completos del BCRA directamente. Tengo:"),
        (NUEVA_12_STAGING, "**Semestre enero–junio 2025:** 116.66"),
        (NUEVA_20_STAGING, "**Los homicidios dolosos en la provincia de Buenos Aires"),
        (COMPLEX_004_BATERIA, "Acá va la evolución de la **tasa de pobreza**"),
        (
            "Tengo todos los datos necesarios. Ahora los proceso…\n\n**USD 48.657 millones** "
            "eran las reservas al 2 de octubre.",
            "**USD 48.657 millones**",
        ),
    ],
)
def test_el_preambulo_de_proceso_no_se_publica(texto: str, empieza: str) -> None:
    limpio = sin_monologo(texto)
    assert limpio.startswith(empieza)
    # Lo que sigue queda igual, tal cual lo escribió el modelo.
    assert texto.endswith(limpio)


def test_la_respuesta_de_nueva_16_queda_entera_salvo_el_preambulo() -> None:
    limpio = sin_monologo(NUEVA_16_OLA3)
    assert "El total es claro" not in limpio
    assert "Todos los registros" not in limpio
    assert "**110.179 usuarios**" in limpio
    assert limpio.endswith("portal de datos abiertos de la provincia de Mendoza.")


@pytest.mark.parametrize(
    "texto",
    [
        # Aperturas legítimas de las mismas corridas.
        "Los tres senadores nacionales por Santa Fe, según el listado oficial del Senado de la "
        "Nación, son:\n\n- Carolina Losada (UCR)",
        "No puedo responder esa pregunta de esa manera.\n\nLo que sí muestran los datos es...",
        "No tengo una herramienta que me devuelva directamente el listado completo de "
        "diputados.\n\nPara un listado oficial, la Cámara publica la nómina.",
        "No encontré datos sobre ocupación de Airbnb en Bariloche.\n\nEl EMPROTUR publica...",
        "Acá va la comparación entre la inflación mensual (INDEC) y el dólar blue:\n\n| Mes |",
        "Estos datos no permiten establecer causas ni evaluar políticas. Lo que muestran los "
        "números del INDEC es la siguiente evolución:\n\n- 2024: 211 %",
        # Con un "no" adelante es la respuesta, no el proceso.
        "No voy a usar proyecciones privadas.\n\nEl último dato es del 2 de octubre.",
        "No tengo todos los datos de 2026 todavía.\n\nEl último es de junio.",
        # «No encontré…» es la respuesta que pide el prompt, aunque narre.
        "No encontré la serie de Misiones. Voy a usar la del Noreste:\n\n- **1,75 %** en agosto.",
        "No encontré datos de 2026 para Misiones. Lo que sí tengo:\n\n- Noreste: **1,75 %**.",
        # Todo preámbulo y nada después: mejor el texto entero que nada.
        "Tengo todos los datos. Ahora los proceso…",
        "Con lo que encontré: nada.",
        # La negrita es la respuesta aunque el párrafo diga "ya tengo".
        "Ya tengo el dato: **USD 48.657 millones**.\n\nFuente: BCRA.",
    ],
)
def test_una_respuesta_sin_preambulo_no_cambia(texto: str) -> None:
    assert sin_monologo(texto) == texto


def test_los_nombres_de_herramientas_se_cambian_por_lo_que_son() -> None:
    texto = (
        "**USD 48.657 millones** al 2 de octubre, según `variables_bcra`.\n\n"
        "La serie de series_tiempo llega a junio; con la herramienta `calcular` sale +17,43 %."
    )
    limpio = sin_monologo(texto)
    assert limpio == (
        "**USD 48.657 millones** al 2 de octubre, según la API del BCRA.\n\n"
        "La serie de la API de Series de Tiempo llega a junio; con el cálculo sale +17,43 %."
    )


def test_calcular_y_sesiones_como_palabras_no_se_tocan() -> None:
    texto = "Para calcular la variación hacen falta dos sesiones del Congreso. **12 %**."
    assert sin_monologo(texto) == texto


def test_un_nombre_interno_parecido_no_se_toca() -> None:
    texto = "**3 tablas**: raw.series_tiempo_ipc y obtener_datos_v2 no son herramientas."
    assert sin_monologo(texto) == texto


def test_estan_todas_las_herramientas() -> None:
    """Una herramienta nueva sin nombre público se publicaría tal cual."""
    from app.application.answers.tools import build_tools

    deps = MagicMock()  # con todas las dependencias: todas las herramientas
    assert {t.spec.name for t in build_tools(deps)} == set(NOMBRES_PUBLICOS)
