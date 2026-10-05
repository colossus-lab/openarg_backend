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


def plegar(valor: object) -> str:
    """Minúsculas, sin acentos y con los espacios colapsados.

    Es la contraparte de ``plegar_sql``: para un mismo texto sin espacios
    repetidos adentro, las dos dan lo mismo.
    """
    return " ".join(str(valor).translate(_TABLA).lower().split())


def plegar_sql(expr: str) -> str:
    """La expresión SQL equivalente a ``plegar`` (sin colapsar espacios internos).

    Colapsar espacios internos en SQL obliga a un ``regexp_replace`` por fila;
    del lado del valor pedido sí se colapsan, que es donde aparecen.
    """
    return f"lower(translate(btrim({expr}), '{_CON_ACENTO}', '{_SIN_ACENTO}'))"


def plegar_suelto(valor: object) -> str:
    """Para ordenar sugerencias: además pliega la ñ y quita la puntuación."""
    texto = unicodedata.normalize("NFKD", str(valor).lower())
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return " ".join(_NO_ALFANUM.sub(" ", texto).split())


def escapar_like(valor: str) -> str:
    """Escapa los comodines de LIKE con la barra, que es el ESCAPE por defecto."""
    return valor.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
