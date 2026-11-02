"""¿En qué codificación está este CSV?

El colector le preguntaba a `charset_normalizer`, que elige entre un centenar de
codificaciones por coherencia de idioma. Con archivos argentinos adivina mal, y
no de vez en cuando: todo archivo latin-1 o windows-1252 en castellano salía
cp1250, así que «Diseño» quedaba «Diseńo», «Año» como columna «Ańo», «Nº» como
«Nş» y «¿» como «ż». Las vocales acentuadas coinciden entre las dos
codificaciones y por eso tardó en verse (educación superior, 30-oct-2026).
En staging (02-nov): 954 tablas con ń/Ń, de 242 datasets, contando sólo nombres
de columna y valores de `pg_stats`. También salía báltico (cp775, iso8859_13) y,
en archivos que eran UTF-8 válido, cirílico (cp1251) y coreano (cp949).

Los portales argentinos publican en muy pocas codificaciones, así que acá no se
adivina entre todas. Sobre una muestra de 102 CSV reales, en este orden:

1. Marca de orden de bytes (BOM): manda.
2. UTF-16 sin BOM: la mitad de los bytes son ceros.
3. UTF-8 si los bytes lo son. Un texto en otra codificación con acentos casi
   nunca forma secuencias válidas de UTF-8.
4. Si no, es de un byte, y sólo aparecieron dos familias: windows-1252 (19 de
   28) y cp850, la de DOS (9 de 28: «COMPA¥ÖA» es COMPAÑÍA, «C¢rdoba» es
   Córdoba). Gana la que convierte más bytes en letras del castellano. Empate:
   windows-1252.

Lo que esto no arregla: los archivos rotos en origen. «ContrataciÃ³n» guardado
en UTF-8 es UTF-8 válido y se lee tal como está. Eran 12 de las 16 tablas con
«Ã» que se revisaron contra su archivo: los bytes ya vienen doblemente
codificados.

Un archivo windows-1252 cuyos únicos bytes altos son espacios duros se lee como
cp850 («Buenos áAires»). Es el costo de leer bien el caso inverso, que sí
apareció: dos archivos de VIH cuyo único byte alto es la «á» de «más» en cp850.

Módulo listado en `parser_fingerprint._PARSER_MODULES`.
"""

from __future__ import annotations

import codecs
from collections import Counter
from typing import BinaryIO

UTF_8 = "utf-8-sig"
WINDOWS_1252 = "windows-1252"
LATIN_1 = "latin-1"
CP850 = "cp850"

_TAM_BLOQUE = 1 << 20
# Hasta dónde se busca el primer byte que no sea ASCII. Un archivo cuyo primer
# acento aparece más abajo se lee como UTF-8, y si no lo es, el cargador
# reintenta en latin-1.
_MAX_BYTES = 64 << 20
# Con esta cantidad de bytes altos ya no cambia quién gana entre las familias.
_ALTOS_SUFICIENTES = 20_000

# UTF-32 antes que UTF-16: su BOM little-endian empieza igual.
_BOMS = (
    (codecs.BOM_UTF8, UTF_8),
    (codecs.BOM_UTF32_LE, "utf-32"),
    (codecs.BOM_UTF32_BE, "utf-32"),
    (codecs.BOM_UTF16_LE, "utf-16"),
    (codecs.BOM_UTF16_BE, "utf-16"),
)

_ASCII = bytes(range(0x80))
_CASTELLANO = frozenset("áéíóúüñÁÉÍÓÚÜÑ¿¡ºª°")
# Comillas y guiones de Word: dan puntos a windows-1252, donde sí son tipografía.
_TIPOGRAFIA = frozenset("‘’“”–—…•€")
# Bytes sin definir en windows-1252: Python falla al decodificarlos.
_SIN_DEFINIR_1252 = frozenset({0x81, 0x8D, 0x8F, 0x90, 0x9D})


def _puntaje(altos: Counter[int], codificacion: str, validos: frozenset[str]) -> int:
    return sum(
        n for byte, n in altos.items() if bytes([byte]).decode(codificacion, "ignore") in validos
    )


def _de_un_byte(altos: Counter[int]) -> str:
    """La familia de un byte que mejor explica los bytes altos del archivo."""
    windows = _puntaje(altos, WINDOWS_1252, _CASTELLANO | _TIPOGRAFIA)
    if _puntaje(altos, CP850, _CASTELLANO) > windows:
        return CP850
    return LATIN_1 if _SIN_DEFINIR_1252 & altos.keys() else WINDOWS_1252


def _utf16_sin_bom(inicio: bytes) -> str | None:
    """Texto mayormente latino en UTF-16: un byte de cada dos es cero."""
    if len(inicio) < 4:
        return None
    pares, impares = inicio[0::2], inicio[1::2]
    if impares.count(0) >= 0.4 * len(impares):
        return "utf-16-le"
    if pares.count(0) >= 0.4 * len(pares):
        return "utf-16-be"
    return None


def detectar_codificacion(archivo: BinaryIO) -> str:
    """El nombre de la codificación, para `open` y `pd.read_csv`. Lee por bloques."""
    bloque = archivo.read(_TAM_BLOQUE)
    for bom, codificacion in _BOMS:
        if bloque.startswith(bom):
            return codificacion
    if utf16 := _utf16_sin_bom(bloque):
        return utf16

    decodificador = codecs.getincrementaldecoder("utf-8")()
    es_utf8 = True
    altos: Counter[int] = Counter()
    leidos = 0
    while bloque and leidos < _MAX_BYTES:
        altos.update(bloque.translate(None, _ASCII))
        if es_utf8:
            try:
                decodificador.decode(bloque)
            except UnicodeDecodeError:
                es_utf8 = False
        if not es_utf8 and sum(altos.values()) >= _ALTOS_SUFICIENTES:
            break
        leidos += len(bloque)
        bloque = archivo.read(_TAM_BLOQUE)
    return UTF_8 if es_utf8 else _de_un_byte(altos)
