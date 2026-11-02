"""Los portales que el colector genérico no baja porque los carga su propia tarea.

`ingest_indec` registra un dataset por hoja de cada Excel, todos con la misma
`download_url`, y escribe cada hoja en su `raw.cache_indec_*`. Cuando la puerta
WS0 rechazaba una de esas tablas, el dataset quedaba con `is_cached = false` y el
colector lo tomaba como pendiente. Pero el colector no sabe de hojas: lee la
primera del archivo y la publica con el título del dataset. Medido en staging el
02-nov-2026: las tres `indec__indec_ipc_*` tenían la variación mensual, también
la de interanual y la de índices, y las 17 del informe de pobreza tenían el
Cuadro 1.

Darle al colector el nombre de la hoja no alcanza: el parser genérico lee el
Cuadro 1 del PIB como 23 filas sin el año, y `ingest_indec` como 988 con período.
Las 14 hojas que la puerta rechaza con un parser las rechaza también con el otro.
"""

from __future__ import annotations

PORTALES_CON_CARGA_PROPIA: frozenset[str] = frozenset({"indec"})


def tiene_carga_propia(portal: str | None) -> bool:
    return (portal or "").strip() in PORTALES_CON_CARGA_PROPIA


def sql_sin_carga_propia(alias: str = "d") -> str:
    """Condición SQL (empieza con AND) que deja afuera a esos portales."""
    portales = ", ".join(f"'{p}'" for p in sorted(PORTALES_CON_CARGA_PROPIA))
    return f" AND {alias}.portal NOT IN ({portales}) "
