"""El reciclador de descargas tiene que buscar la tabla donde vive.

`_recycle_stuck_downloads` decide si una fila trabada en `downloading` se
promueve a `ready` —porque su tabla existe y tiene filas— o se degrada a
`error` y se re-despacha. Esa decisión se tomaba mirando **sólo**
`table_schema = 'public'`, cuando las tablas viven en `raw` desde el
cutover de la capa raw.

Medido en staging el 2026-09-09: de 85 filas stale con `table_name`,
encontraba **0**; con los cuatro schemas encontraba **3**. Cada fallo
degrada y re-descarga una tabla que estaba perfectamente materializada.

Es la tercera aparición de la misma familia en un mes —los globs
`cache_*` que no matcheaban `raw.`, el DELETE nocturno de `table_catalog`,
y esto—: **dos formas del mismo nombre, y una comparación que asume una.**

También se calificaron el `COUNT(*)` y el listado de columnas. Sin eso, el
conteo dependía del `search_path` de la conexión —el mismo mecanismo que
rompió el sandbox en el PR #20— y las columnas de un homónimo en otro
schema podían mezclarse con las de la tabla real.
"""

from __future__ import annotations

import re
from pathlib import Path

_FUENTE = Path("src/app/infrastructure/celery/tasks/collector_tasks.py").read_text(encoding="utf-8")


def _cuerpo_del_reciclador() -> str:
    i = _FUENTE.index("def _recycle_stuck_downloads(")
    j = _FUENTE.index("\ndef ", i + 10)
    return _FUENTE[i:j]


def test_la_tabla_se_busca_en_los_cuatro_schemas() -> None:
    cuerpo = _cuerpo_del_reciclador()

    assert "table_schema = 'public'" not in cuerpo, (
        "buscar sólo en `public` da 0 aciertos: las tablas viven en `raw`"
    )
    assert "IN ('public', 'raw', 'staging', 'mart')" in cuerpo


def test_el_schema_se_resuelve_y_no_se_asume() -> None:
    """Se necesita el schema real, no un booleano: hace falta para calificar."""
    cuerpo = _cuerpo_del_reciclador()

    assert "SELECT table_schema FROM information_schema.tables" in cuerpo
    assert "if table_schema:" in cuerpo


def test_un_homonimo_en_dos_schemas_resuelve_siempre_igual() -> None:
    """Sin `ORDER BY`, `LIMIT 1` devuelve lo que el plan quiera ese día."""
    cuerpo = _cuerpo_del_reciclador()
    i = cuerpo.index("SELECT table_schema FROM information_schema.tables")
    # Ventana generosa: la consulta lleva un comentario en el medio.
    consulta = cuerpo[i : i + 900]

    assert "ORDER BY" in consulta and "LIMIT 1" in consulta
    assert "array_position" in consulta


def test_el_conteo_de_filas_va_calificado() -> None:
    """Sin el schema, el COUNT lo decide el `search_path` de la conexión."""
    cuerpo = _cuerpo_del_reciclador()

    assert 'FROM "{table_schema}"."{row.table_name}"' in cuerpo
    assert not re.search(r'SELECT COUNT\(\*\) FROM "\{row\.table_name\}"', cuerpo), (
        "un COUNT sin calificar es el bug del `search_path` otra vez (PR #20)"
    )


def test_las_columnas_se_leen_del_schema_correcto() -> None:
    """Un homónimo en otro schema aportaría sus columnas a esta fila."""
    cuerpo = _cuerpo_del_reciclador()
    i = cuerpo.index("SELECT column_name FROM information_schema.columns")
    consulta = cuerpo[i : i + 300]

    assert "table_schema = :ts" in consulta
