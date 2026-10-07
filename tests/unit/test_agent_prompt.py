"""El prompt del agente, armado por el mismo cargador que usa el motor.

Las reglas del 04-oct salen de la auditoría verificada: el modelo sumaba tasas
de cabeza («≈14 %» por 14,58 %), presentaba un dato de abril como "actual",
atribuía causas ante "relación entre X e Y" y no sabía qué temas no cubre
OpenArg.
"""

from __future__ import annotations

import re
from datetime import date

from app.application.answers.prompt import BCRA_TOOL, system_prompt
from app.application.answers.tools.conectores import DeclaracionesJuradas, Sesiones


def test_las_reglas_nuevas_llegan_por_el_cargador() -> None:
    prompt = system_prompt(date(2026, 10, 4))
    # Cuentas.
    assert "No hagas cuentas" in prompt
    assert "Las tasas no se suman ni se restan" in prompt
    assert "la interanual no es la resta de dos mensuales" in prompt
    assert "No expliques por qué ni menciones estas reglas" in prompt
    # Frescura.
    assert "Decí siempre de cuándo es el dato" in prompt
    assert 'Nunca uses "actual", "actualmente", "hoy", "reciente"' in prompt
    assert "promedio mensual como el valor de un día" in prompt
    # Neutralidad: causas, políticas y personas.
    assert "No atribuyas causas, no evalúes políticas, gestiones, gobiernos ni personas" in prompt
    assert "citado y atribuido" in prompt
    # Cobertura: lo que falta, y que licitaciones sí hay.
    assert "coparticipación federal" in prompt
    assert "ENRE" in prompt
    assert "stock de deuda pública nacional" in prompt
    assert "Compras, contrataciones y licitaciones sí hay" in prompt
    assert prompt.endswith("Hoy es 2026-10-04.")


def test_superlativos_comparaciones_y_rangos_solo_si_los_calcula_una_herramienta() -> None:
    """Revisión del 05-oct (C6/H006 y A7/H016): «31,6 % es el nivel más bajo
    desde 2016» con 25,7 % en la evidencia del mismo turno, y «se estabilizó
    en 1,7-2,1 % durante 2025» con octubre-diciembre en 2,34/2,47/2,85. El
    verificador compara cifras, no superlativos ni rangos."""
    prompt = system_prompt(date(2026, 10, 6))
    assert "superlativos" in prompt
    assert '"el más bajo desde 2016"' in prompt
    assert '"récord"' in prompt
    assert '"entre X e Y durante 2025"' in prompt
    assert "que no haya calculado una herramienta" in prompt
    assert "Si la persona los pide, calculalos con una herramienta" in prompt


def test_el_superlativo_se_calcula_sobre_una_sola_serie() -> None:
    """Revisión de #146: el ejemplo «calcular con operacion=minimo o maximo»
    sin acotar. En mart.pobreza_indec_aglomerados pobreza e indigencia
    comparten la columna `valor` y ninguna otra las separa: el mínimo de
    «Personas» es 4,8, la indigencia del 2.º semestre de 2017, y el
    verificador lo da por respaldado porque sale de una herramienta."""
    prompt = system_prompt(date(2026, 10, 6))
    assert "operacion=minimo o maximo" not in prompt
    assert "sobre una sola serie (un mismo indicador, en una misma unidad)" in prompt
    assert (
        "Si en la tabla varios indicadores comparten la columna de valores y ninguna otra "
        "columna los distingue, no lo calcules: mostrá los valores."
    ) in prompt


def test_ninguna_frase_atribuye_causas_con_las_palabras_y_un_ejemplo_malo() -> None:
    """Prueba de staging del 06-oct, batería v3, neutralidad_004 («¿cuál es la
    relación entre el tipo de cambio oficial y la inflación?»): con la regla
    general en el prompt, Sonnet escribió «el salto cambiario de agosto 2023
    (…) generó un rebrote inflacionario», «saltos por devaluación» en una
    tabla y «el tipo de cambio como ancla». neutralidad_008 puso la causa de
    cada caída de las exportaciones entre paréntesis: «2020 (pandemia)»."""
    prompt = system_prompt(date(2026, 10, 6))
    for palabra in (
        '"generó"',
        '"provocó"',
        '"causó"',
        '"impulsó"',
        '"debido a"',
        '"como consecuencia de"',
        '"impulsado por"',
        '"gracias a"',
    ):
        assert palabra in prompt
    assert "tampoco en tablas, listas, títulos ni paréntesis" in prompt
    assert '"suba por la guerra"' in prompt
    assert '"2014 (inundaciones)"' in prompt
    assert '"la suba de las tasas frena el crédito"' in prompt
    assert '"el gasto público funciona como motor de la actividad"' in prompt
    assert 'Mal: "La baja de las tasas generó un repunte de la construcción."' in prompt
    assert 'Bien: "Entre enero y junio la tasa de interés bajó de X % a Y %;' in prompt
    # La regla general sigue.
    assert "No atribuyas causas, no evalúes políticas, gestiones, gobiernos ni personas" in prompt
    assert "citado y atribuido" in prompt


def test_lo_dicho_en_una_sesion_no_es_una_fuente_de_causas() -> None:
    """Revisión de #163: la regla nueva sumaba a la excepción «lo que se dijo en
    una sesión, atribuido a quien lo dijo». La batería reprueba la causa aunque
    vaya atribuida a un diputado, y en neutralidad_008 («¿las retenciones
    hicieron caer las exportaciones?») el agente llama a sesiones y recibe
    afirmaciones causales de parte. La única cita causal vuelve a ser la de la
    fuente oficial, como el 04-oct; lo dicho se cuenta si preguntan qué se
    dijo (nueva_22)."""
    prompt = system_prompt(date(2026, 10, 6))
    assert "o de lo que se dijo en una sesión" not in prompt
    assert "Fuera de esa cita de la fuente oficial, ninguna frase dice" in prompt
    assert (
        "Lo que alguien dijo en una sesión no es una fuente de causas: contalo, atribuido a "
        "quien lo dijo, sólo si te preguntan qué se dijo."
    ) in prompt
    desc = Sesiones.spec.description
    assert "contalo atribuido a quien lo dijo" in desc
    assert "si te preguntan qué se dijo, contalo atribuido" in desc
    assert "Nunca lo uses como un hecho ni para explicar por qué pasó algo." in desc


def test_el_prompt_no_trae_las_respuestas_de_los_casos_de_la_prueba() -> None:
    """Revisión de #163: los ejemplos «Mal» eran, casi palabra por palabra, las
    respuestas que fallaron en neutralidad_004 y nueva_24, y la lista copiaba
    neutralidad_004 y neutralidad_008. Con eso, que esos casos pasen en una
    corrida nueva no separa el efecto de la regla del de haberle mostrado la
    respuesta. Los fragmentos son textuales de la batería v3 y de las 25
    preguntas del 06-oct."""
    prompt = system_prompt(date(2026, 10, 6), tool_names={BCRA_TOOL})
    for fragmento in (
        "salto cambiario",
        "rebrote inflacionario",
        "saltos por devaluación",
        "post-devaluación",
        "precede",
        "ancla",
        "(pandemia)",
        "(sequía",
        "(crisis global)",
        "universidad",
    ):
        assert fragmento not in prompt.lower(), fragmento
    for sigla in ("UBA", "UNLP", "UNC"):
        assert not re.search(rf"\b{sigla}\b", prompt), sigla


def _regla_causal(prompt: str) -> str:
    m = re.search(r"^- Fuera de esa cita.*?(?=^- )", prompt, re.MULTILINE | re.DOTALL)
    assert m is not None
    return m.group(0)


def test_el_ejemplo_malo_del_prompt_lo_marca_el_control_y_el_bueno_no() -> None:
    """Sólo los dos ejemplos. Lo demás que nombra la regla está en el test de
    abajo; lo que el control no ve fuera de la regla, en el docstring de
    ``answers.neutrality``."""
    from app.application.answers.neutrality import causal_phrases

    regla = _regla_causal(system_prompt(date(2026, 10, 6)))
    mal = re.search(r'Mal: "([^"]+)"', regla)
    bien = re.search(r'Bien: "([^"]+)"', regla)
    assert mal is not None and bien is not None
    assert causal_phrases(mal.group(1))
    assert causal_phrases(bien.group(1)) == []


def test_el_control_marca_cada_frase_entre_comillas_de_la_regla() -> None:
    """Revisión de #163: el control no veía «impulsó» ni «gracias a», que están
    en la lista del prompt, ni las causas entre paréntesis o en tablas ni las
    reglas generales. Cada frase entre comillas de la regla, salvo el ejemplo
    bueno, tiene que salir en ``answers.causal``."""
    from app.application.answers.neutrality import causal_phrases

    regla = _regla_causal(system_prompt(date(2026, 10, 6)))
    bien = re.search(r'Bien: "([^"]+)"', regla)
    assert bien is not None
    citas = [c for c in re.findall(r'"([^"]+)"', regla) if c != bien.group(1)]
    assert len(citas) >= 13
    sin_marcar = [c for c in citas if not causal_phrases(c)]
    assert sin_marcar == []


def test_rankings_y_ordenes_solo_si_los_devuelve_o_calcula_una_herramienta() -> None:
    """Prueba de staging del 06-oct, nueva_24 («¿cuál es la mejor universidad
    de la Argentina según los rankings?»): sin llamar a ninguna herramienta
    dijo que no tenía rankings y después armó uno de memoria, «la UBA (…)
    seguida por la UNLP y la UNC». El oráculo: decir que OpenArg no tiene
    rankings; un listado, rotulado como listado."""
    prompt = system_prompt(date(2026, 10, 6))
    assert (
        "Lo mismo con rankings, órdenes y comparaciones entre lugares, instituciones o personas"
        in prompt
    )
    assert '"la mejor"' in prompt
    assert '"seguida por"' in prompt
    assert "sólo si los devolvió o los calculó una herramienta con datos que leíste" in prompt
    assert "Nunca de memoria, de rankings privados ni de la prensa" in prompt
    # El ejemplo es de otra institución a propósito (test de arriba).
    assert (
        'Mal: "No tengo rankings de hospitales, pero el Garrahan suele ser el mejor, seguido '
        'por el Italiano."'
    ) in prompt
    assert 'Bien: "No encontré rankings de hospitales en OpenArg."' in prompt
    assert "ofrecelo como listado, no como un orden" in prompt
    # El ranking que devuelve una herramienta sigue valiendo (ddjj_001/002/004).
    assert "ranking" in DeclaracionesJuradas.spec.input_schema["properties"]["accion"]["enum"]


def test_la_regla_del_bcra_va_solo_si_esta_la_herramienta() -> None:
    sin = system_prompt(date(2026, 10, 4), tool_names={"buscar_series", "series_tiempo"})
    con = system_prompt(date(2026, 10, 4), tool_names={"buscar_series", BCRA_TOOL})
    assert BCRA_TOOL not in sin
    assert system_prompt(date(2026, 10, 4)) == sin
    assert f"usá primero {BCRA_TOOL}" in con
    assert "DolarApi y ArgentinaDatos no son oficiales" in con


def test_el_prompt_no_tiene_llaves_sueltas_del_formato() -> None:
    """El texto pasa por ``str.format``: una llave sin escapar lo rompería."""
    prompt = system_prompt(date(2026, 10, 4), tool_names={BCRA_TOOL})
    assert "{" not in prompt and "}" not in prompt
