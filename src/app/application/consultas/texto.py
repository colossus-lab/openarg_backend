"""Comparar textos sin distinguir mayúsculas ni acentos, igual en Python y en SQL.

La base no tiene ``unaccent`` (disponible pero no instalada ni en staging ni en
prod, 04-oct) y ``similarity`` de ``pg_trgm`` no pasa el validador del
sandbox. ``translate`` + ``lower`` no necesitan extensión, son IMMUTABLE y el
validador los acepta.

La ``ñ`` NO se pliega a ``n`` para filtrar («Peña» no es «Pena»): sólo para
ordenar sugerencias (``plegar_suelto``).
"""

from __future__ import annotations

import re
import unicodedata

_CON_ACENTO = "áàâäãéèêëíìîïóòôöõúùûüçÁÀÂÄÃÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÇ"
_SIN_ACENTO = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"
_TABLA = str.maketrans(_CON_ACENTO, _SIN_ACENTO)

_NO_ALFANUM = re.compile(r"[^0-9a-z]+")

# Los blancos que colapsa ``str.split()`` (los de ``str.isspace()``: espacio,
# tabulador, saltos de línea, NBSP, los espacios tipográficos…). La expresión
# de SQL usa exactamente estos, escritos como escapes ``\uXXXX`` de las
# expresiones regulares de Postgres, para que los dos lados colapsen lo mismo.
_BLANCOS = (
    "\t\n\x0b\x0c\r\x1c\x1d\x1e\x1f \x85\xa0\u1680"
    "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
    "\u2028\u2029\u202f\u205f\u3000"
)
_BLANCOS_SQL = "[" + "".join(f"\\u{ord(c):04x}" for c in _BLANCOS) + "]+"


def plegar(valor: object) -> str:
    """Minúsculas, sin acentos y con los blancos colapsados (NBSP incluido).

    Es la contraparte de ``plegar_sql``: para cualquier texto las dos dan lo
    mismo.
    """
    return " ".join(str(valor).translate(_TABLA).lower().split())


def plegar_sql(expr: str) -> str:
    """La expresión SQL equivalente a ``plegar``.

    Antes sólo hacía ``btrim`` y no colapsaba los blancos internos: un valor
    real con dos espacios o un NBSP («Hosp. Zonal Gral. de Ag.  Prof. Dr. R.
    Carrillo», 13 filas en staging) no se encontraba ni copiándolo exacto,
    porque del lado del pedido sí se colapsaban (revisión independiente del
    05-oct, H011). El ``regexp_replace`` por fila sólo corre en las tablas
    donde se pliega cada fila (``consultas.preparar.es_tolerante``).
    """
    colapsado = f"regexp_replace({expr}, '{_BLANCOS_SQL}', ' ', 'g')"
    return f"lower(translate(btrim({colapsado}), '{_CON_ACENTO}', '{_SIN_ACENTO}'))"


def plegar_suelto(valor: object) -> str:
    """Para ordenar sugerencias: además pliega la ñ y quita la puntuación."""
    texto = unicodedata.normalize("NFKD", str(valor).lower())
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return " ".join(_NO_ALFANUM.sub(" ", texto).split())


def escapar_like(valor: str) -> str:
    """Escapa los comodines de LIKE con la barra, que es el ESCAPE por defecto."""
    return valor.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
