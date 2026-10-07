"""Frases causales en la respuesta, sólo para el log (``answers.causal``).

El prompt prohíbe atribuir causas, y aun así la prueba de staging del 06-oct
encontró «el salto cambiario de agosto 2023 (…) generó un rebrote
inflacionario» (batería v3, neutralidad_004). El verificador
(``answers.verification``) compara cifras: esto no lo ve.

Acá se marca una lista corta de conectores causales: «generó», «provocó» y
«causó» (y sus plurales), «debido a», «como consecuencia de» e «impulsado
por». No se marca cuando antes, en la misma oración, hay una pregunta, un
«si», un «qué», un «no» o un «ni»: «estos datos no permiten establecer si las
retenciones causaron…» es justamente lo que pide el prompt.

Nunca cambia la respuesta, en ningún modo. Una lista de palabras no distingue
una causa propia de una citada y atribuida a la fuente oficial (que el prompt
permite) ni de lo que dijo un orador en una sesión: con esa precisión, borrar
o reescribir sería peor que el error. Sirve para medir cuántas respuestas
reales los usan antes de decidir si hace falta una vuelta correctiva.
"""

from __future__ import annotations

import re

# Los conectores. «generó» y no «genera»: el presente aparece sobre todo en
# lo que se dijo en una sesión («lo que genera incertidumbre»).
_CAUSAL = re.compile(
    r"\b(?:"
    r"gener(?:ó|aron)|provoc(?:ó|aron)|caus(?:ó|aron)"
    r"|debid[oa]s?\s+al?"
    r"|como\s+consecuencia\s+del?"
    r"|impulsad[oa]s?\s+(?:\w+\s+){0,2}?por"
    r")\b",
    re.IGNORECASE,
)
# Lo que, antes en la misma oración, vuelve la frase una pregunta o una
# negación: «no permiten establecer qué lo causó», «si las retenciones
# causaron». «sí» con tilde no cuenta.
_HEDGE = re.compile(r"¿|\b(?:no|ni|si|qué)\b", re.IGNORECASE)
# Fin de oración: un punto seguido de espacio (no el de «1.459,42») o un
# salto de línea.
_SENTENCE_END = re.compile(r"[.!?…](?=\s)|\n")
# Cuánto texto alrededor de la frase va al log.
_CONTEXT = 60
MAX_PHRASES = 5


def _sentence_start(text: str, pos: int) -> int:
    start = 0
    for m in _SENTENCE_END.finditer(text, 0, pos):
        start = m.end()
    return start


def causal_phrases(text: str) -> list[str]:
    """Las frases de ``text`` con un conector causal, con un poco de contexto.

    A lo sumo ``MAX_PHRASES``; vacía si no hay ninguna.
    """
    out: list[str] = []
    for m in _CAUSAL.finditer(text or ""):
        start = _sentence_start(text, m.start())
        if _HEDGE.search(text, start, m.start()):
            continue
        end_m = _SENTENCE_END.search(text, m.end())
        end = end_m.start() if end_m else len(text)
        a = max(start, m.start() - _CONTEXT)
        b = min(end, m.end() + _CONTEXT)
        out.append(" ".join(text[a:b].split()))
        if len(out) >= MAX_PHRASES:
            break
    return out
