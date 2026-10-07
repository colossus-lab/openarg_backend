"""Lo que el modelo escribe antes de la respuesta no se publica (H063).

La respuesta es el texto entero del turno final (``agent_engine._answer_of``)
y lo único que se limpiaba eran los nombres internos de tablas. A veces el
modelo arranca narrando su proceso, y eso salía publicado:

- «Todos los registros del padrón 2022 son usuarios "EN PADRON TS" (ninguno
  está dado de baja o excluido). El total es claro.» (nueva_16, prueba del
  06-oct en staging);
- «Hmm, hay un dato para 2026-S1 pero la fuente llega hasta 2026-01-01…
  Voy a usar el más reciente confirmado por la fuente…» (nueva_08, revisión
  del 05-oct);
- «… Lo que sí puedo es usar `variables_bcra` que ya me dio los dos cierres.
  Le pido la variación a series_tiempo…» (nueva_09): además, nombres de
  herramientas;
- «Con los datos del SNIC (…) ya puedo responder.» y una raya antes de la
  respuesta (nueva_20);
- en prod, 2 de 44 respuestas del chat desde el 03-oct («Tengo todos los
  datos necesarios. Ahora los proceso…»).

Pasa sobre todo cuando se agota el presupuesto de herramientas y la última
vuelta va sin ellas: el modelo narra lo que ya no puede pedir. El prompt ya
lo prohíbe («sin contar tu proceso») y no alcanza.

Qué se saca, conservador a propósito (perder una oración de la respuesta es
peor que dejar un preámbulo):

- los párrafos del principio que narran el proceso (``_PROCESO``, o que
  nombran una herramienta), si no tienen negrita (la cifra principal va en
  negrita, por el prompt) ni dicen «No encontré…», y si después queda algo
  que no es preámbulo;
- la raya que separaba ese preámbulo de la respuesta;
- en el resto, los nombres de herramientas, cambiados por lo que son para
  quien lee.

Probado contra las 728 respuestas guardadas (las corridas del 05-oct, la
prueba del 06-oct y las líneas de base de la batería): ver el test.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterator

# Narrar el proceso, sobre el párrafo en minúsculas y sin tildes. Un "no"
# adelante es una respuesta, no un proceso: «No tengo todos los datos de
# 2026», «No voy a usar proyecciones privadas».
_NO = r"(?<!\bno )(?<!\bnunca )"
_PROCESO = re.compile(
    r"\bhmm+\b"
    r"|^(?:perfecto|excelente|listo)\b"
    r"|\b(?:ahora|ya) tengo\b"
    rf"|{_NO}\btengo (?:todo|todos los datos|toda la informacion|los datos|lo necesario)\b"
    r"|\b(?:lo|todo lo) que necesito\b"
    rf"|{_NO}\bvoy a (?:usar|consolidar|construir|calcular|buscar|consultar|pedir|revisar"
    r"|verificar|armar|redactar|preparar|procesar|tomar)\b"
    r"|\bahora (?:calculo|busco|pido|reviso|consulto|armo|proceso|los proceso|las proceso"
    r"|redacto|verifico|puedo)\b"
    r"|\b(?:le pido|busco si|noto que)\b"
    r"|\b(?:ya|ahora) puedo responder\b"
    r"|\b(?:necesaria|necesario|suficiente) para responder\b"
    r"|\b(?:armo|armar|redacto|redactar|preparo|preparar|aclaro|incluyo) (?:la|en la) respuesta\b"
    r"|\b(?:aqui|aca) (?:va|esta|tenes) la respuesta\b"
    r"|\brespuesta final\b"
    rf"|^con (?:los|estos|esos|todos los) datos\b[^.]*{_NO}\bpuedo\b"
    r"|\b(?:el|la) (?:total|dato|respuesta|cifra) (?:es|esta|queda) clar[oa]\b"
    r"|\bturnos? de herramientas\b"
)
_NO_ENCONTRE = re.compile(r"\bno (?:lo |la |los |las )?encontre\b")

# Lo que es cada herramienta para quien lee. Un test exige que estén todas.
NOMBRES_PUBLICOS = {
    "buscar_series": "el buscador de series",
    "series_tiempo": "la API de Series de Tiempo",
    "variables_bcra": "la API del BCRA",
    "buscar_datos": "el buscador del catálogo",
    "describir_tabla": "la tabla",
    "obtener_datos": "la tabla",
    "calcular": "el cálculo",
    "cotizaciones": "las cotizaciones",
    "declaraciones_juradas": "las declaraciones juradas",
    "sesiones": "las versiones taquigráficas",
    "personal_legislativo": "la nómina del Congreso",
    "ubicar_lugar": "el buscador de lugares",
    "pedir_aclaracion": "una aclaración",
}
# Los nombres con guion bajo no son palabras: se reconocen solos. "calcular",
# "sesiones" y "cotizaciones" sí lo son: sólo entre comillas invertidas.
_TODOS = "|".join(sorted(NOMBRES_PUBLICOS, key=len, reverse=True))
_COMPUESTOS = "|".join(n for n in sorted(NOMBRES_PUBLICOS, key=len, reverse=True) if "_" in n)
_HERRAMIENTA = re.compile(
    rf"(?:\b[Ll]a herramienta\s+)?(?:`(?P<citado>{_TODOS})`|\b(?P<suelto>{_COMPUESTOS})\b)"
)
_RAYA = re.compile(r"(?:-{3,}|\*{3,}|_{3,})")


def _plano(texto: str) -> str:
    sin_tildes = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in sin_tildes if not unicodedata.combining(c)).lower().strip()


def _bloques(texto: str) -> Iterator[tuple[int, int]]:
    """``(inicio, fin)`` de cada párrafo; una raya sola es un párrafo aparte."""
    pos = 0
    inicio: int | None = None
    for linea in texto.splitlines(keepends=True):
        contenido = linea.strip()
        if not contenido or _RAYA.fullmatch(contenido):
            if inicio is not None:
                yield inicio, pos
                inicio = None
            if contenido:
                yield pos, pos + len(linea)
        elif inicio is None:
            inicio = pos
        pos += len(linea)
    if inicio is not None:
        yield inicio, pos


def _es_proceso(parrafo: str) -> bool:
    plano = _plano(parrafo)
    # Con negrita o con «No encontré…» (lo que pide el prompt cuando falta el
    # dato) el párrafo es la respuesta, aunque además narre.
    if "**" in parrafo or _NO_ENCONTRE.search(plano):
        return False
    return bool(_PROCESO.search(plano) or _HERRAMIENTA.search(parrafo))


def _inicio_de_la_respuesta(texto: str) -> int:
    """Dónde empieza la respuesta: después del preámbulo de proceso, si lo hay."""
    saco_algo = False
    for inicio, fin in _bloques(texto):
        parrafo = texto[inicio:fin].strip()
        if _RAYA.fullmatch(parrafo):
            continue
        if _es_proceso(parrafo):
            saco_algo = True
            continue
        return inicio if saco_algo else 0
    # Todo era preámbulo (o nada lo era): mejor el texto entero que nada.
    return 0


def _nombre_publico(m: re.Match[str]) -> str:
    return NOMBRES_PUBLICOS[m.group("citado") or m.group("suelto")]


def sin_monologo(texto: str) -> str:
    """El texto que se publica: sin el preámbulo de proceso ni nombres de herramientas."""
    respuesta = texto[_inicio_de_la_respuesta(texto) :]
    return _HERRAMIENTA.sub(_nombre_publico, respuesta).strip()
