"""El barrido de huérfanas del catálogo miraba un solo schema.

`cleanup_orphan_catalog_entries` corre todas las noches a las 3:30 y borra
las filas de `table_catalog` cuya tabla física ya no existe. Comparaba
contra `information_schema.tables WHERE table_schema = 'public'`, que
devuelve nombres **pelados**. Una fila `raw.foo__ab12__v3` no podía estar
nunca en ese conjunto, así que la tarea borraba el 100 % de la capa raw con
sus tablas vivas.

Medido en staging el 2026-09-08, antes del arreglo: `table_catalog` tenía 10
filas, el DELETE marcaba **las 10**, y las 10 tenían su tabla física
existente. Por eso la tabla nunca pasaba de un puñado de filas: se vaciaba
sola cada 24 h y el colector la volvía a llenar de a poco.

Estos tests son de inspección de la consulta, como los de
`test_cleanup_raw_orphans_guard.py`: el DELETE corre sobre
`information_schema` y no hay forma de ejercitarlo sin una base real. Lo que
fijan es la propiedad que se rompió — que el schema salga de la fila, no de
una constante.
"""

from __future__ import annotations

from pathlib import Path

_SQL = Path("src/app/infrastructure/celery/tasks/cache_cleanup_tasks.py").read_text(
    encoding="utf-8"
)


def _delete_block() -> str:
    """El DELETE de huérfanas del catálogo."""
    i = _SQL.index("DELETE FROM table_catalog")
    return _SQL[i : i + 900]


def test_el_schema_sale_de_la_fila_y_no_de_una_constante() -> None:
    bloque = _delete_block()

    assert "split_part(tc.table_name, '.', 1)" in bloque, (
        "el schema tiene que derivarse del propio nombre: si se compara "
        "contra un schema fijo, toda fila calificada parece huérfana"
    )


def test_no_se_compara_contra_public_a_secas() -> None:
    """El bug exacto, escrito para que un reordenamiento futuro lo despierte."""
    bloque = _delete_block()

    assert "WHERE table_schema = 'public'" not in bloque, (
        "filtrar `information_schema` por 'public' vuelve a marcar como huérfana toda la capa raw"
    )


def test_una_fila_sin_schema_sigue_buscandose_en_public() -> None:
    """Las legacy vienen peladas y viven en `public`: no romper ese caso."""
    bloque = _delete_block()

    assert "ELSE 'public'" in bloque


def test_se_toleran_las_comillas() -> None:
    """`catalog_backfill` escribe `raw."tabla"`, con comillas adentro."""
    bloque = _delete_block()

    assert "btrim" in bloque, "hay que sacar las comillas antes de comparar"


# -- enrich_all_tables: seleccion y despacho -----------------------------
#
# La misma familia de bug, en la tarea que llena el catalogo. Tenia DOS
# fallas encadenadas, las dos por comparar/despachar la forma equivocada del
# nombre:
#   1. el LEFT JOIN comparaba `table_catalog.table_name` (calificado) contra
#      `cached_datasets.table_name` (pelado), asi que "ya esta enriquecida?"
#      nunca acertaba y la tarea volvia a tomar las mismas N tablas.
#   2. despachaba el nombre pelado, y `_enrich_table` filtra
#      `information_schema` por schema: con el pelado busca en `public`, no
#      encuentra la tabla raw y devuelve False sin gastar Bedrock ni
#      enriquecer nada.
#
# Verificado contra staging tras el arreglo: la consulta devuelve 30.909
# pendientes, todas calificadas, con cero solapamiento contra las ya
# enriquecidas.

_ENRICH = Path("src/app/infrastructure/celery/tasks/catalog_enrichment_tasks.py").read_text(
    encoding="utf-8"
)


def _seleccion_block() -> str:
    """La consulta de selección DE `enrich_all_tables`.

    Anclada en la definición de la función: el archivo tiene otra consulta
    sobre `raw.cached_datasets` más arriba (el fallback de
    `_resolve_resource_identity_for_table`) y buscar por el FROM traía esa.
    """
    i = _ENRICH.index("def enrich_all_tables")
    return _ENRICH[i : i + 2400]


def test_el_join_compara_por_nombre_pelado() -> None:
    bloque = _seleccion_block()

    assert "split_part(tc.table_name, '.', 2)" in bloque, (
        "sin despelar `table_catalog.table_name`, `tc.id IS NULL` es siempre "
        "verdadero y la tarea nunca avanza"
    )


def test_lo_que_se_despacha_va_calificado() -> None:
    bloque = _seleccion_block()

    assert "rtv.schema_name || '.' || cd.table_name" in bloque, (
        "`_enrich_table` busca las columnas por schema: con el nombre pelado "
        "no encuentra una tabla de `raw` y no enriquece nada"
    )


def test_la_version_viva_es_la_que_manda_el_schema() -> None:
    bloque = _seleccion_block()

    assert "superseded_at IS NULL" in bloque, (
        "una version superada puede apuntar a otro schema; hay que tomar la viva"
    )
