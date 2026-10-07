"""Frases causales en la respuesta, sólo para el log (``answers.causal``).

El prompt prohíbe atribuir causas, y aun así la prueba de staging del 06-oct
encontró «el salto cambiario de agosto 2023 (…) generó un rebrote
inflacionario» (batería v3, neutralidad_004). El verificador
(``answers.verification``) compara cifras: esto no lo ve.

Se marca todo lo que nombra la regla del prompt (``answers.prompt``; un test
lo recorre): los verbos en pasado y en presente («generó», «generan»,
«impulsó», «frena»…, y el infinitivo de una regla general: «suelen
impulsar»), los conectores («debido a», «gracias a», «como consecuencia de»,
«impulsado por», «se debe a», «se explica por», «a raíz de», «producto
de»…), la causa pegada al dato («suba por la guerra», «2020 (pandemia)», «pico
post-devaluación») y las reglas generales («funciona como ancla», «precede»).

No se marca cuando, en la misma cláusula (hasta el punto, «:» o «;»), la
frase es una pregunta o una condición («¿», «si», «qué», «cuánto»), cuando la
negación va pegada al conector («no fue impulsado por») o cuando niega poder
establecerlo («estos datos no permiten establecer…»): eso es lo que pide el
prompt. «si bien» no es una condición.

Lo atribuido va aparte: si antes en la oración hay un «según», un «dijo», un
«sostuvo»… o la frase está entre comillas, va a ``atribuidas`` y no a
``frases``. El prompt permite la causa que da la fuente oficial, citada; la
vara de la batería la reprueba igual, atribuida o no. Por eso van las dos.

Es una cota inferior, no un conteo. Sobre las 545 respuestas distintas que
hay guardadas (batería, 25 preguntas y líneas base) ve 11 de las 14 que
reprueban los patrones de neutralidad de la batería y 7 de las 8 que el juez
puso debajo de 0,5. Se le escapan las causas sin conector de la lista («bajó
por la apreciación del peso», «tiende a desacelerarse», «asociado a») y las
evaluaciones («los objetivos se cumplieron»). Y marca de más: «esto generó
un superávit comercial» es una cuenta, no una causa. Nunca cambia la
respuesta, en ningún modo: con esa precisión, borrar o reescribir sería peor
que el error. Sirve para medir, junto con la batería y su juez, antes de
decidir si hace falta una vuelta correctiva.
"""

from __future__ import annotations

import re

# Los eventos que aparecen como causa pegada al dato: «2020 (pandemia)»,
# «saltos por devaluación», «pico post-devaluación».
_EVENTOS = (
    r"(?:devaluaci[oó]n|pandemia|covid|cuarentena|sequ[ií]a|crisis|cepo|guerra"
    r"|inundaci[oó]n(?:es)?|recesi[oó]n)"
)
_CAUSAL = re.compile(
    # Los verbos, en pasado y en presente. «causa» sola no: casi siempre es el
    # sustantivo («la causa», «defunciones por causa»). «impulsar» un proyecto
    # o una ley («quienes la impulsaron») es otra cosa.
    r"\b(?:"
    r"(?:gener|provoc|desencaden)(?:ó|aron|an?)"
    r"|caus(?:ó|aron|an)"
    r"|deprim(?:e|en|ió|ieron)"
    # «la inflación se frenó», «la base frenó su crecimiento» y «el dólar se
    # disparó» describen.
    r"|(?<!\bse\s)fren(?:a|an|ó|aron)(?!\s+sus?\b)"
    r"|(?<!\bse\s)dispar(?:ó|aron)"
    r"|presion(?:a|an|ó|aron)\s+sobre"
    r"|(?<!\bla\s)(?<!\blo\s)(?<!\blas\s)(?<!\blos\s)impuls(?:ó|aron|an?)"
    r"(?!\s+(?:(?:el|la|los|las|un|una|su|sus|este|esta)\s+)?"
    r"(?:proyecto|ley|iniciativa|reforma|dictamen|pedido|debate|tratamiento)s?\b)"
    # El infinitivo, sólo como regla general: «suelen impulsar», «puede
    # generar». Solo es casi siempre un propósito («para generar empleo»).
    r"|(?:suelen?|pueden?|tienden?\s+a)\s+(?:gener|provoc|caus|impuls|deprim|fren)(?:ar|ir)"
    # Los conectores.
    r"|debid[oa]s?\s+al?"
    r"|gracias\s+al?"
    r"|a\s+ra[ií]z\s+del?"
    r"|a\s+causa\s+del?"
    r"|(?:como\s+)?(?:consecuencia|producto)\s+(?:direct[oa]\s+)?del?"
    r"|como\s+resultado\s+del?"  # sin «como»: «el resultado de la balanza»
    r"|se\s+deb(?:e|en|i[oó]|ieron)\s+(?:\w+\s+){0,2}?al?"
    r"|se\s+explic(?:a|an|ó|aron)\s+(?:\w+\s+){0,2}?por"
    r"|(?:impulsad|provocad|causad|generad|motivad|explicad|empujad)[oa]s?\s+(?:\w+\s+){0,2}?por"
    # La causa pegada al dato.
    r"|por\s+(?:la\s+|el\s+)?" + _EVENTOS + r"|post?[-\s]?" + _EVENTOS + r""
    # Las reglas generales: «funciona como ancla», «precede a la inflación».
    r"|funcion(?:a|an)\s+como|ancla|preceden?"
    r")\b"
    # Un evento entre paréntesis: «2020 (pandemia)», «(crisis global)». No si
    # dice que coincide: es lo que pide el prompt.
    r"|\((?![^()\n]*coincid)[^()\n]{0,40}?\b" + _EVENTOS + r"\b[^()\n]{0,40}\)",
    re.IGNORECASE,
)
# Pregunta o condición, en la misma cláusula: «no permiten establecer qué lo
# causó», «si las retenciones causaron», «cuánto se explica por». «sí» con
# tilde y «si bien» no cuentan.
_QUESTION = re.compile(r"¿|\b(?:qué|cuánt[oa]s?|si(?!\s+bien\b))\b", re.IGNORECASE)
# La negación pegada al conector: «no fue impulsado por», «ni lo causó».
_NEG_CLOSE = re.compile(r"\b(?:no|ni|nunca)\s+(?:\S+\s+){0,2}$", re.IGNORECASE)
# Negar que se pueda establecer: «estos datos no permiten establecer…».
_NEG_EPISTEMIC = re.compile(
    r"\b(?:no|ni|sin)\b.*?\b(?:permit\w*|establec\w*|afirm\w*|determin\w*|atribu\w*|"
    r"conclu\w*|dec[ií]r|asegur\w*|prueb\w*|demuestr\w*|saber|sabe\w*|alcanz\w*|posible|"
    r"aislar|separar|identificar)\b",
    re.IGNORECASE | re.DOTALL,
)
# Lo atribuido, antes en la misma oración.
_ATTRIBUTION = re.compile(
    r"\b(?:según|dijo|dijeron|sostuvo|sostuvieron|afirmó|afirmaron|señaló|señalaron|"
    r"planteó|plantearon|expresó|expresaron|argumentó|argumentaron|advirtió|consideró|"
    r"denunció|opinó|aseguró|remarcó|criticó|cuestionó|mencionó|reclamó|atribuyó)\b",
    re.IGNORECASE,
)
# Fin de oración: un punto seguido de espacio (no el de «1.459,42»), también
# antes de un cierre de negrita («…inflación.** El salto»), un salto de línea
# o una celda de tabla.
_SENTENCE_END = re.compile(r"[.!?…](?=[*_]*\s)|\n|\|")
# Fin de cláusula, para la pregunta y la negación.
_CLAUSE_END = re.compile(r"[:;]")
# Cuánto texto alrededor de la frase va al log.
_CONTEXT = 60
MAX_PHRASES = 5


def _last_end(pattern: re.Pattern[str], text: str, start: int, pos: int) -> int:
    out = start
    for m in pattern.finditer(text, start, pos):
        out = m.end()
    return out


def _quoted(before: str) -> bool:
    """Si ``before`` deja abierta una cita entre comillas («…», “…” o "…")."""
    return (
        before.count("«") > before.count("»")
        or before.count("“") > before.count("”")
        or before.count('"') % 2 == 1
    )


def scan_causal(text: str) -> tuple[list[str], list[str]]:
    """Las frases de ``text`` con un conector causal, con un poco de contexto.

    Devuelve ``(frases, atribuidas)``: las propias y las que van atribuidas a
    alguien o entre comillas. A lo sumo ``MAX_PHRASES`` en cada lista.
    """
    text = text or ""
    own: list[str] = []
    attributed: list[str] = []
    last_start = -1
    for m in _CAUSAL.finditer(text):
        start = _last_end(_SENTENCE_END, text, 0, m.start())
        if start == last_start:  # una frase por oración
            continue
        clause = text[_last_end(_CLAUSE_END, text, start, m.start()) : m.start()]
        if _QUESTION.search(clause) or _NEG_CLOSE.search(clause) or _NEG_EPISTEMIC.search(clause):
            continue
        end_m = _SENTENCE_END.search(text, m.end())
        end = end_m.start() if end_m else len(text)
        a = max(start, m.start() - _CONTEXT)
        b = min(end, m.end() + _CONTEXT)
        phrase = " ".join(text[a:b].split())
        last_start = start
        before = text[start : m.start()]
        target = attributed if _ATTRIBUTION.search(before) or _quoted(before) else own
        if len(target) < MAX_PHRASES:
            target.append(phrase)
        if len(own) >= MAX_PHRASES and len(attributed) >= MAX_PHRASES:
            break
    return own, attributed


def causal_phrases(text: str) -> list[str]:
    """Las frases causales propias de ``text`` (sin las atribuidas).

    A lo sumo ``MAX_PHRASES``; vacía si no hay ninguna.
    """
    return scan_causal(text)[0]
