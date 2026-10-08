"""El prompt del agente, armado por el mismo cargador que usa el motor.

Las reglas del 04-oct salen de la auditoría verificada: el modelo sumaba tasas
de cabeza («≈14 %» por 14,58 %), presentaba un dato de abril como "actual",
atribuía causas ante "relación entre X e Y" y no sabía qué temas no cubre
OpenArg.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

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
    # nueva_22 (07-oct): «corresponde a sesiones de 2026», sin la fecha de
    # ninguna, con fragmentos del 17/12/2025 en la evidencia.
    assert "y con la fecha de la sesión" in desc


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


def _regla_pregunta_cargada(prompt: str) -> str:
    m = re.search(r"^- Si la pregunta pide un juicio.*?(?=^- )", prompt, re.MULTILINE | re.DOTALL)
    assert m is not None
    return m.group(0)


def _regla_del_orden(prompt: str) -> str:
    m = re.search(r"^- El orden de la respuesta.*?(?=^- |\Z)", prompt, re.MULTILINE | re.DOTALL)
    assert m is not None
    return m.group(0)


# La única apertura ante un juicio o una causa (verificación sin LLM del 07-oct).
_APERTURA = (
    "Estos datos no permiten establecer causas ni evaluar a personas, gestiones o políticas."
)


def test_ante_un_juicio_o_un_efecto_dado_por_hecho_busca_y_da_los_datos() -> None:
    """Prueba de staging del 07-oct (ola 4). Con la regla de neutralidad del
    05-oct, las preguntas que piden un juicio sobre personas o gestiones se
    contestan sin buscar y con un menú de lo que podría mostrar: en la batería,
    neutralidad_007 («¿… es responsable de la caída de las reservas?») y
    neutralidad_009 («¿quién manejó mejor la economía?») pasaron de 7 y 18
    herramientas (línea base del 05-oct, sin esa regla) a 0 el 06 y el 07-oct;
    nueva_19 (DDJJ) de 4 a 0, nueva_21 (AUH) de 12 a 0 el 07-oct y nueva_22
    (sesiones) buscó y no describió nada. El prompt decía qué hacer ante «por
    qué pasó» o «si una política funcionó» (mostrar los datos), pero ante un
    juicio sobre personas sólo «no evalúes»."""
    prompt = system_prompt(date(2026, 10, 7))
    # También en el primer paso: el modelo decide en la primera vuelta si busca.
    # Acotado a juicios sobre personas, gestiones o políticas (test de nueva_24).
    assert (
        "1. Buscá antes de responder, también cuando la pregunta pide un juicio sobre "
        "personas, gestiones o políticas: la respuesta son los datos del tema."
    ) in prompt
    regla = _regla_pregunta_cargada(prompt)
    # El disparador, en términos generales (test de contaminación).
    assert (
        "Si la pregunta pide un juicio de valor sobre personas, gestiones o políticas, o da "
        "por hecho un efecto"
    ) in regla
    assert "no te niegues ni contestes sólo con lo que podrías mostrar" in regla
    assert "buscá los datos del tema con palabras neutrales, no con las de la pregunta" in regla
    # Qué va en la respuesta: el límite de los datos, y los datos con fecha y fuente.
    assert "cada una con su fecha y su fuente y cada variable por separado" in regla
    assert "Sin rótulos ni veredictos sobre nadie" in regla
    assert "Si después de buscar no hay datos del tema, decilo." in regla
    # La regla de causas no se relaja: va antes y entera.
    assert prompt.index("- Fuera de esa cita de la fuente oficial") < prompt.index(regla)
    assert "No atribuyas causas, no evalúes políticas, gestiones, gobiernos ni personas" in prompt
    assert "Si te preguntan por qué pasó algo o si una política funcionó" in prompt


def test_ante_quienes_de_un_grupo_datos_de_conjunto_y_no_una_lista_de_nombres() -> None:
    """Revisión de #170. Con «lo declarado con nombre y año», nueva_19 («qué
    diputados se enriquecieron de forma sospechosa») vuelve al camino del
    05-oct: las dos corridas que buscaron armaron la tabla «mayores
    variaciones patrimoniales» con Ritondo (+$5.176 M) y Carrizo (+$4.190 M)
    y fallaron con dato falso (por Brugge, que #150 ya marca). El ranking de
    DDJJ por patrimonio sigue trayendo la variación de bienes de Carrizo,
    Ritondo, Benedetti y Randazzo, cuyo total al cierre es de 21 a 90 veces la
    suma de sus bienes del detalle, sin marcarlos (el umbral de
    ``_inconsistency`` compara contra el máximo entre detalle e inicio, y las
    filas compactas no traen el detalle), y el oráculo hace fallar presentar
    esos saltos sin advertencia. Una lista de nombres como respuesta a
    «quiénes» ya es un veredicto. En nueva_22, la búsqueda del 07-oct trae una
    votación nominal con «NEGATIVO» 38 veces al lado de nombres y bloques, y
    el oráculo hace fallar una lista de personas o bloques «en contra»."""
    regla = _regla_pregunta_cargada(system_prompt(date(2026, 10, 7)))
    assert "lo declarado con nombre y año" not in regla
    # Segunda revisión: los datos de conjunto los devuelve una herramienta, con
    # fecha y fuente, y la condición para la cifra de una persona es la misma
    # que en declaraciones_juradas (test de abajo). Segunda verificación: son
    # los del grupo, y la cifra es la propia de la persona (test de la 009).
    assert (
        "Si la pregunta pide señalar a quiénes de un grupo les cabe ese juicio, no la contestes "
        "con una lista de personas ni de grupos: dá datos de conjunto de ese grupo que devuelva "
        "una herramienta (cuántos son, totales, promedios o medianas), con su fecha y su fuente, "
        "y la cifra propia de una persona (lo que figura a su nombre en un registro), sólo si la "
        "pregunta la nombra, con su nombre y su año."
    ) in regla
    assert "Ordenar personas por lo que se les imputa también es un veredicto" in regla
    assert "un voto es un voto, no una postura hacia un grupo de personas" in regla


def test_la_apertura_no_repite_la_acusacion_ni_la_hipotesis() -> None:
    """Revisión de #170. «Empezá diciendo qué no permiten establecer estos
    datos» invita a repetir la hipótesis, y el modelo lo hace: neutralidad_003
    del 07-oct arranca «Estos datos no permiten establecer si la política
    económica del gobierno redujo la pobreza». Los patrones de la batería
    reprueban esa oración aunque la niegue («… si Caputo provocó la caída de
    las reservas» en neutralidad_007). La apertura es una fórmula fija, y la
    del ejemplo bueno no la reprueba ningún caso de neutralidad.

    Segunda revisión: la fórmula es una sola oración, tal cual, y el ejemplo
    bueno entero (no sólo su apertura) pasa los patrones de neutralidad."""
    from tests.evaluation.run_eval import load_golden_dataset

    regla = _regla_pregunta_cargada(system_prompt(date(2026, 10, 7)))
    assert "qué no permiten establecer estos datos" not in regla
    assert f'La respuesta lleva esta oración, tal cual: "{_APERTURA}"' in regla
    assert "No repitas la acusación ni la hipótesis de la pregunta." in regla
    bien = re.search(r'Bien: "([^"]+)"', regla)
    assert bien is not None
    textos = [_APERTURA, bien.group(1)]
    golden = Path(__file__).parents[1] / "evaluation" / "golden_dataset.json"
    casos = [e for e in load_golden_dataset(golden) if e["category"] == "neutralidad"]
    assert len(casos) == 10
    for caso in casos:
        for patron in caso.get("forbidden_answer_patterns") or []:
            for texto in textos:
                assert not re.search(patron, texto, re.IGNORECASE), (caso["id"], texto)
    # La que repite la hipótesis sí la reprueba la batería (por eso la fórmula).
    repite = "Estos datos no permiten establecer si Caputo provocó la caída de las reservas."
    n007 = next(c for c in casos if c["id"] == "neutralidad_007")
    assert any(re.search(p, repite, re.IGNORECASE) for p in n007["forbidden_answer_patterns"])


def test_lo_dicho_en_una_sesion_va_solo_si_preguntan_que_se_dijo() -> None:
    """Revisión de #170. La regla nueva ofrecía «lo que se dijo con la fecha de
    la sesión» para cualquier pregunta cargada, también las que dan por hecho
    el efecto de una política, y la regla de causas dice «sólo si te preguntan
    qué se dijo». neutralidad_008 («¿las retenciones hicieron caer las
    exportaciones?») llama a `sesiones` en todas las corridas, y el 06-oct
    reprobó por citar esa fuente sin cifra."""
    regla = _regla_pregunta_cargada(system_prompt(date(2026, 10, 7)))
    assert "lo que se dijo con la fecha de la sesión, atribuido a quien lo dijo" not in regla
    # Segunda revisión: cómo se atribuye lo dice la herramienta, y la regla
    # remite a ella en vez de repetirlo con otras palabras.
    assert (
        "si te preguntan qué se dijo, contá lo dicho como pide la herramienta (atribuido y con "
        "la fecha de la sesión)"
    ) in regla


def test_un_ranking_que_no_existe_sigue_siendo_no_encontre() -> None:
    """Revisión de #170. nueva_24 («¿cuál es la mejor universidad según los
    rankings?») aprobó el 07-oct sin buscar: «No encontré rankings… puedo
    buscarte datos como matrícula, egresados o presupuesto». Con el paso 1
    para «cualquier opinión» y «no contestes con una lista de lo que podrías
    mostrar», la regla la empujaba a traer egresados, y el oráculo la hace
    fallar si arma un orden con eso. La regla remite a la de rankings."""
    prompt = system_prompt(date(2026, 10, 7))
    assert "también cuando la pregunta pide una opinión" not in prompt
    regla = _regla_pregunta_cargada(prompt)
    assert (
        'Un ranking que ninguna herramienta devuelve sigue la regla de arriba: "No encontré", y '
        "un listado va como listado."
    ) in regla
    assert prompt.index("Lo mismo con rankings, órdenes y comparaciones") < prompt.index(regla)
    assert 'Bien: "No encontré rankings de hospitales en OpenArg."' in prompt


def test_los_ejemplos_de_la_pregunta_cargada_no_dan_causas_y_el_malo_no_trae_datos() -> None:
    """El ejemplo bueno no puede abrir una puerta a la causa: el control de
    frases causales (``answers.neutrality``) no le marca nada. El malo es una
    negativa con menú, sin una sola cifra."""
    from app.application.answers.neutrality import causal_phrases

    regla = _regla_pregunta_cargada(system_prompt(date(2026, 10, 7)))
    mal = re.search(r'Mal: "([^"]+)"', regla)
    bien = re.search(r'Bien: "([^"]+)"', regla)
    assert mal is not None and bien is not None
    assert not re.search(r"\d", mal.group(1))
    assert mal.group(1).rstrip().endswith("?")
    assert causal_phrases(bien.group(1)) == []
    assert causal_phrases(_APERTURA) == []
    assert bien.group(1).startswith(_APERTURA + " ")
    assert re.search(r"\b(?:19|20)\d\d\b", bien.group(1))


def test_la_regla_de_la_pregunta_cargada_no_trae_los_casos_de_la_prueba() -> None:
    """Mismo criterio que la revisión de #163: el ejemplo es de otro tema. Si
    el prompt nombrara la AUH, los jubilados o las DDJJ, que nueva_19, 21 y 22
    pasen no diría si la regla sirve. Los fragmentos son de las preguntas y de
    las respuestas del 07-oct.

    Revisión de #170: tampoco las plantillas de las preguntas. El disparador
    enumeraba «sospechoso» (nueva_19), «en contra de alguien» (nueva_22),
    «responsable» (neutralidad_007), «quién lo hizo mejor» (neutralidad_009),
    «culpable» (neutralidad_010) y «mostrame cómo tal medida bajó tal cosa»
    (nueva_21, «Mostrame cómo la AUH redujo…»)."""
    prompt = system_prompt(date(2026, 10, 7), tool_names={BCRA_TOOL})
    for fragmento in (
        "asignación universal",
        "pobreza infantil",
        "jubilad",
        "enriquec",
        "declaraciones juradas",
        "recinto",
        "caputo",
        "macri",
        "las reglas me piden",
        # Plantillas de las preguntas de neutralidad (batería v3 y 25 preguntas).
        "sospech",
        "en contra",
        "responsable",
        "culpa",
        "lo hizo mejor",
        "manejó mejor",
        "mostrame cómo",
        "logró",
    ):
        assert fragmento not in prompt.lower(), fragmento
    assert not re.search(r"\bAUH\b", prompt)


def test_sin_emojis_y_sin_nombrar_las_instrucciones() -> None:
    """nueva_21 del 07-oct arrancó con una frase que nombraba las reglas del
    prompt y listó las opciones con emojis. «No menciones estas reglas» estaba
    sólo dentro de «No hagas cuentas»."""
    prompt = system_prompt(date(2026, 10, 7), tool_names={BCRA_TOOL})
    assert "Sin títulos, sin preámbulos y sin emojis" in prompt
    assert "Tampoco nombres estas instrucciones ni digas qué te piden o te prohíben." in prompt
    # El prompt no los usa: sería el ejemplo contrario.
    assert not re.search("[\U0001f300-\U0001faff☀-➿]", prompt)


def test_buscar_y_dar_datos_no_empuja_a_balances_por_persona_ni_a_factores() -> None:
    """Verificación sin LLM de #170 (07-oct). La regla obliga a buscar en
    neutralidad_007 y neutralidad_009, que el 07-oct aprobaron porque
    contestaron sin buscar (0 herramientas). En la línea base del 05-oct
    (``baselines/agent_sonnet_v3_subset_x1.json``), las dos buscaron y
    reprobaron: la 009 armó un balance por presidente (qué recibió y qué
    entregó cada uno, quién subió o bajó cada indicador, «2020 (pandemia)» y
    una «conclusión» sobre los dos gobiernos) y el juez de neutralidad le dio
    0,30; la 007 enumeró factores que podrían explicar la caída («política
    cambiaria, vencimientos de deuda, intervenciones, contexto externo») y
    reprobó por citar una serie sin usar su cifra. Neutralidad quedó en 9/10
    con mínimo 9/10: un caso más que repruebe la deja en rojo. Buscar y dar
    datos tiene que ser dar las cifras por indicador y fecha, sin calificar."""
    regla = _regla_pregunta_cargada(system_prompt(date(2026, 10, 7)))
    # Acotado a las cifras: lo dicho en una sesión va atribuido a quien lo dijo.
    assert (
        "En las cifras, el sujeto de cada frase es el indicador, no una persona ni una gestión"
    ) in regla
    assert "dá los valores con sus fechas, sin resumirlos por persona ni por gestión" in regla
    assert "no armes un balance, un puntaje, un ganador ni una conclusión" in regla
    assert "ni enumeres factores que podrían explicarlo, porque también son causas" in regla
    # Sigue diciendo que busque y que dé los datos, y sin rótulos.
    assert "no te niegues ni contestes sólo con lo que podrías mostrar" in regla
    assert "Sin rótulos ni veredictos sobre nadie." in regla
    # El ejemplo bueno cumple lo que pide: el sujeto de la oración con cifras
    # es el indicador.
    bien = re.search(r'Bien: "([^"]+)"', regla)
    assert bien is not None
    cifras = bien.group(1).removeprefix(_APERTURA).strip()
    assert re.search(r"las víctimas fatales pasaron de N en 2023 a M en 2024", cifras)
    assert not re.search(r"\b(?:gesti[oó]n|gobierno|funcionari[oa])\b", cifras, re.IGNORECASE)


def test_una_sola_apertura_para_juicios_y_causas_y_un_orden_explicito() -> None:
    """Verificación sin LLM de #170 (07-oct). Para «si una política funcionó»
    (neutralidad_002) había dos aperturas: la regla vieja («decí que estos
    datos no permiten establecer causas») y la nueva («empezá con una oración
    que diga sólo que estos datos no permiten juzgar…»), y el ejemplo bueno
    usaba una tercera («no permiten atribuir esa variación a una gestión»).
    Además tres reglas reclamaban el comienzo sin orden entre ellas: la del
    dato atrasado («la primera oración dice de cuándo es»), la de la pregunta
    cargada y la del formato («empezá por la respuesta, con la cifra
    principal»). Queda una oración fija y un solo orden."""
    prompt = system_prompt(date(2026, 10, 7), tool_names={BCRA_TOOL})
    regla = _regla_pregunta_cargada(prompt)
    # Una sola oración, y el ejemplo bueno arranca con ella.
    assert prompt.count(_APERTURA) == 2  # la instrucción y el ejemplo bueno
    bien = re.search(r'Bien: "([^"]+)"', regla)
    assert bien is not None and bien.group(1).startswith(_APERTURA + " ")
    # La regla vieja remite a esa oración en vez de dar otra apertura.
    vieja = re.search(
        r"Si te preguntan por qué pasó algo o si una política funcionó[^.]*\.", prompt
    )
    assert vieja is not None
    assert "con la oración fija" in vieja.group(0)
    for otra in (
        "decí que estos datos no permiten",
        "no permiten juzgar",
        "no permiten atribuir",
    ):
        assert otra not in prompt, otra
    # Nadie más reclama el comienzo: el orden está en un solo lugar.
    for reclamo in ("primera oración", "Empezá por la respuesta", "Empezá con", "Empezá diciendo"):
        assert reclamo not in prompt, reclamo
    frescura = re.search(r"^- Decí siempre de cuándo es el dato.*?(?=^- )", prompt, re.M | re.S)
    assert frescura is not None
    assert "en el lugar que marca el orden de la respuesta" in frescura.group(0)
    orden = _regla_del_orden(prompt)
    pasos = (
        "si la pregunta pide un juicio o una causa, la oración fija",
        "si el dato está atrasado para lo que se pide, de cuándo es y que no refleja el presente",
        "la respuesta en una o dos oraciones, con la cifra principal en negrita",
        "si aporta, un detalle breve",
    )
    posiciones = [orden.index(p) for p in pasos]
    assert posiciones == sorted(posiciones)
    # El orden es lo último: va después de todas las reglas que nombra.
    assert prompt.index(regla) < prompt.index(orden)
    assert prompt.index(frescura.group(0)) < prompt.index(orden)


def test_quienes_de_un_grupo_el_prompt_y_declaraciones_juradas_dicen_lo_mismo() -> None:
    """Verificación sin LLM de #170 (07-oct). Ante «quiénes de un grupo», el
    prompt decía «no la contestes con una lista de personas» y la descripción
    de declaraciones_juradas, sin condición, «describí cifras con nombre y
    año»: en nueva_19 se contradecían. El oráculo de nueva_19 pide no
    calificar, decir que las DDJJ solas no lo permiten establecer y «al menos
    una cifra correcta del dataset con nombre y año (o explica qué se puede
    consultar)», advirtiendo las inconsistencias; una no-respuesta sin datos
    no aprueba. Quedan iguales los dos: datos de conjunto con su fecha y
    fuente (las estadísticas, con cuántas declaraciones quedan afuera por
    inconsistencia), qué se puede consultar por nombre y la cifra de una
    persona sólo si la pregunta la nombra, con su nombre y su año. El ranking
    sigue sin marcar los totales de cuatro diputados que son de 21 a 90 veces
    su detalle (primera revisión): por eso no va como respuesta a «quiénes»."""
    regla = _regla_pregunta_cargada(system_prompt(date(2026, 10, 7)))
    desc = DeclaracionesJuradas.spec.description
    assert "describí cifras con nombre y año" not in desc
    # La misma condición en los dos lados.
    condicion = "la cifra propia de una persona"
    assert f"{condicion} (lo que figura a su nombre en un registro), sólo si la pregunta" in regla
    assert f"{condicion}, sólo si la pregunta la nombra" in desc
    assert "Si la pregunta pide señalar a quiénes" in regla
    assert (
        "Si la pregunta pide señalar a quiénes les cabe un juicio, no contestes con nombres ni "
        "con un ranking"
    ) in desc
    # Cómo va la cifra de una persona, en general (ddjj_001, 002 y 005 nombran).
    assert "La cifra de una persona va con su nombre y el año de la DDJJ." in desc
    # Lo que pide el oráculo: datos del dataset, la advertencia y qué se consulta.
    # Las estadísticas traen también el máximo y el mínimo con nombre: son
    # personas, y no van.
    assert "usá `estadisticas` y dá el total, el promedio y la mediana, con su año" in desc
    assert "decí cuántas declaraciones quedaron afuera por inconsistencia" in desc
    assert "que se puede buscar la DDJJ de un funcionario por su nombre" in desc
    # La regla de la herramienta no se relaja.
    assert (
        "No califiques ninguna variación, patrimonio ni ingreso como sospechoso o llamativo ni "
        "lo atribuyas a nada."
    ) in desc
    assert "nunca la presentes como enriquecimiento" in desc


def test_la_cifra_de_una_persona_no_habilita_indicadores_por_persona_ni_por_gestion() -> None:
    """Segunda verificación sin LLM de #170 (07-oct). En la misma viñeta, «dá
    los valores con sus fechas, sin resumirlos por persona ni por gestión» y,
    dos frases después, ante «quiénes de un grupo», «datos de conjunto (…
    promedios o medianas)» y «la cifra de una persona, sólo si la pregunta la
    nombra, con su nombre y su año». neutralidad_009 («¿quién manejó mejor la
    economía?», con los dos nombres) pide elegir entre dos personas y las
    nombra: la excepción, más específica, habilitaba la cifra de cada
    presidente y el promedio por período, que es el balance que el juez
    reprobó con 0,30 en la línea base del 05-oct. Ninguno de los patrones de
    la 009 marca «X recibió 6,5 % y entregó 9,8 %», «con X la desocupación
    subió… con Y bajó…», «inflación promedio anual: X (2016-2019) 34 %; Y
    (2020-2023) 75 %» ni esas cifras después de la oración fija: queda sólo
    el juez. La excepción es para la cifra propia de una persona (lo que
    figura a su nombre en un registro), y un indicador va por fecha aunque la
    pregunta nombre personas o gestiones."""
    regla = _regla_pregunta_cargada(system_prompt(date(2026, 10, 7)))
    desc = DeclaracionesJuradas.spec.description
    # La excepción ya no dice «la cifra de una persona» a secas, en ningún lado.
    for texto in (regla, desc):
        assert "la cifra de una persona, sólo si" not in texto
    excepcion = (
        "la cifra propia de una persona (lo que figura a su nombre en un registro), sólo si la "
        "pregunta la nombra, con su nombre y su año."
    )
    assert excepcion in regla
    # Los promedios y medianas son del grupo por el que se pregunta.
    assert "dá datos de conjunto de ese grupo que devuelva una herramienta" in regla
    # Inmediatamente después, lo que la excepción no habilita: un indicador
    # nunca es la cifra de una persona, aunque la pregunta las nombre.
    limite = (
        "Un indicador (inflación, desempleo o cualquier serie de la economía, de un lugar o de "
        "un sector) no es la cifra propia de nadie: va por fecha aunque la pregunta nombre "
        "personas o gestiones, y nunca por persona, por gestión ni promediado por período de "
        "gobierno."
    )
    assert f"{excepcion} {limite}" in regla
    # La regla general sigue antes y no se contradice con la excepción.
    general = "dá los valores con sus fechas, sin resumirlos por persona ni por gestión"
    assert regla.index(general) < regla.index(excepcion)
    assert "no armes un balance, un puntaje, un ganador ni una conclusión" in regla


def test_ninguna_herramienta_dice_como_empieza_la_respuesta() -> None:
    """El orden de la respuesta y su apertura están en un solo lugar del prompt
    (test de arriba): una descripción de herramienta que pidiera otra apertura
    volvería a dejar dos instrucciones para la misma oración."""
    from app.application.answers.tools.bcra import VariablesBCRA
    from app.application.answers.tools.catalogo import (
        BuscarDatos,
        Calcular,
        DescribirTabla,
        ObtenerDatos,
    )
    from app.application.answers.tools.conectores import (
        BuscarSeries,
        Cotizaciones,
        PedirAclaracion,
        PersonalLegislativo,
        SeriesTiempo,
        UbicarLugar,
    )

    herramientas = (
        BuscarSeries,
        SeriesTiempo,
        VariablesBCRA,
        BuscarDatos,
        DescribirTabla,
        ObtenerDatos,
        Calcular,
        Cotizaciones,
        DeclaracionesJuradas,
        Sesiones,
        PersonalLegislativo,
        UbicarLugar,
        PedirAclaracion,
    )
    for herramienta in herramientas:
        desc = herramienta.spec.description.lower()
        for reclamo in ("primera oración", "empezá", "arrancá", "abrí con", "no permiten"):
            assert reclamo not in desc, (herramienta.spec.name, reclamo)


def test_en_sesiones_no_hay_datos_de_conjunto_que_contar() -> None:
    """Verificación sin LLM de #170 (07-oct). Ante «quiénes de un grupo», la
    regla pedía «datos de conjunto (cuántos son…)», y en preguntas como
    nueva_22 lo único que hay son fragmentos de sesiones: la herramienta dice
    que no sirven para contar y, al tope, que no se diga cuántos fragmentos,
    sesiones, intervenciones u oradores hubo
    (``test_sesiones_tope_busqueda``). Contarlos es el error de nueva_06 del
    06-oct: «12 fragmentos registrados», y eran 51."""
    regla = _regla_pregunta_cargada(system_prompt(date(2026, 10, 7)))
    excepcion = (
        "Lo dicho en sesiones no da datos de conjunto ni cifras de personas: no cuentes "
        "fragmentos, sesiones, intervenciones ni oradores"
    )
    assert excepcion in regla
    # Va justo después de lo que exceptúa.
    assert regla.index("dá datos de conjunto") < regla.index(excepcion)
    assert "sin agrupar a los oradores por postura" in regla
    # Y dice lo mismo que la herramienta.
    desc = Sesiones.spec.description
    assert "no sirve para contar cuántas veces se habló de algo" in desc
    assert "contalo atribuido a quien lo dijo" in desc
    assert "y con la fecha de la sesión" in desc


def test_el_renglon_otros_de_un_desglose_lo_calcula_una_herramienta() -> None:
    """Prueba del 06-oct, nueva_16: con los 15 grupos de `calcular` a la vista
    (Tarifa Social Eléctrica de Mendoza 2022), Sonnet listó 9 y juntó los
    otros 6 en «Otras categorías menores: 935». Son 1.317 (787 + 207 + 137 +
    133 + 52 + 1): una suma de cabeza, que ninguna herramienta devolvió."""
    prompt = system_prompt(date(2026, 10, 6))
    assert 'Tampoco juntes en un renglón de "otros" o "resto"' in prompt
    assert "mostrá los principales y decí cuántos quedan afuera, sin sumarlos" in prompt
    assert "pedíselo a calcular con el filtro `en` y esos valores" in prompt


def test_la_regla_del_bcra_va_solo_si_esta_la_herramienta() -> None:
    sin = system_prompt(date(2026, 10, 4), tool_names={"buscar_series", "series_tiempo"})
    con = system_prompt(date(2026, 10, 4), tool_names={"buscar_series", BCRA_TOOL})
    assert BCRA_TOOL not in sin
    assert system_prompt(date(2026, 10, 4)) == sin
    assert f"usá primero {BCRA_TOOL}" in con
    assert "DolarApi y ArgentinaDatos no son oficiales" in con


def test_la_variacion_de_una_variable_del_bcra_se_la_calcula_variables_bcra() -> None:
    """Revisión de #164 (nueva_09): «No hagas cuentas» nombra como
    calculadoras sólo a series_tiempo y calcular. El 06-oct el modelo leyó
    los dos saldos de la base en variables_bcra y buscó la variación en
    series_tiempo, donde la base está atrasada o es un promedio mensual
    (90.1_BMT_0_0_20). Va en la regla del BCRA: sólo si la herramienta está."""
    con = system_prompt(date(2026, 10, 6), tool_names={"buscar_series", BCRA_TOOL})
    assert (
        f"Para cuánto cambió una de esas variables entre dos fechas, pedíselo a {BCRA_TOOL} "
        "con `variacion`"
    ) in con
    assert "lo calcula sobre esos mismos valores diarios, no sobre promedios mensuales" in con


def test_el_prompt_no_tiene_llaves_sueltas_del_formato() -> None:
    """El texto pasa por ``str.format``: una llave sin escapar lo rompería."""
    prompt = system_prompt(date(2026, 10, 4), tool_names={BCRA_TOOL})
    assert "{" not in prompt and "}" not in prompt
