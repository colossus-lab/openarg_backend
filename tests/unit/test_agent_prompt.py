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
    assert '"saltos por devaluación"' in prompt
    assert '"2020 (pandemia)"' in prompt
    assert '"el dólar funciona como ancla"' in prompt
    assert 'Mal: "El salto cambiario de agosto de 2023 generó un rebrote inflacionario."' in prompt
    assert 'Bien: "En agosto de 2023 el dólar mayorista pasó de $A a $B;' in prompt
    # La regla general sigue, y la cita atribuida también vale para lo que se
    # dijo en una sesión: sin eso, la regla nueva le prohibiría contarlo.
    assert "No atribuyas causas, no evalúes políticas, gestiones, gobiernos ni personas" in prompt
    assert "citado y atribuido" in prompt
    assert "o de lo que se dijo en una sesión, atribuido a quien lo dijo" in prompt


def test_el_ejemplo_malo_del_prompt_lo_marca_el_control_y_el_bueno_no() -> None:
    """El prompt y el control en sombra (``answers.neutrality``) dicen lo mismo."""
    from app.application.answers.neutrality import causal_phrases

    prompt = system_prompt(date(2026, 10, 6))
    mal = re.search(r'Mal: "(El salto cambiario[^"]+)"', prompt)
    bien = re.search(r'Bien: "(En agosto de 2023[^"]+)"', prompt)
    assert mal is not None and bien is not None
    assert causal_phrases(mal.group(1))
    assert causal_phrases(bien.group(1)) == []


def test_lo_que_dijo_un_orador_va_atribuido_y_no_como_causa() -> None:
    """La otra mitad de la excepción: la herramienta de sesiones pide atribuir."""
    desc = Sesiones.spec.description
    assert "Lo que dice un orador es suyo: contalo atribuido a quien lo dijo" in desc
    assert "no como un hecho ni como la causa de algo" in desc


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
    assert (
        'Mal: "No tengo rankings de universidades, pero la UBA suele ser la mejor posicionada, '
        'seguida por la UNLP y la UNC."'
    ) in prompt
    assert 'Bien: "No encontré rankings de universidades en OpenArg."' in prompt
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
