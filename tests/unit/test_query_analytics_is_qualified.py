"""`query_analytics` se nombra siempre con `public.`.

Sin esquema, cada escritura va a la tabla que indique el search_path de esa
conexión, y el pool no lo mantiene estable. En staging (2026-09-30) quedaron
dos tablas: 883 filas en `public`, la que lee `/admin/analytics`, y 1.189 en
`raw`, adonde iba todo después de un reinicio y nadie lo leía. Misma trampa
que `test_credits_tables_are_qualified.py` y `test_registry_is_qualified.py`.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

# Un uso en SQL: después de FROM, JOIN, INTO, UPDATE, TABLE, EXISTS u ON. En
# mayúsculas a propósito: el SQL del código va así, y un comentario que dice
# "into query_analytics" no es un uso.
_SQL_USE = re.compile(r"\b(FROM|JOIN|INTO|UPDATE|TABLE|EXISTS|ON)\s+(?P<name>[\w.]+)\b")


def _files_that_mention_it() -> list[str]:
    out = subprocess.run(
        ["git", "grep", "-l", "query_analytics", "--", "src"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [f for f in out.stdout.splitlines() if "/alembic/versions/" not in f]


def test_hay_archivos_para_revisar() -> None:
    """Si el grep no encuentra nada, el test de abajo no prueba nada."""
    files = _files_that_mention_it()
    assert "src/app/application/pipeline/history.py" in files
    assert "src/app/presentation/http/controllers/admin/query_analytics_router.py" in files


def test_todo_uso_en_sql_lleva_esquema() -> None:
    sin_esquema = []
    for path in _files_that_mention_it():
        source = Path(path).read_text(encoding="utf-8")
        for m in _SQL_USE.finditer(source):
            if m.group("name") == "query_analytics":
                line = source.count("\n", 0, m.start()) + 1
                sin_esquema.append(f"{path}:{line}: {m.group(0)}")
    assert not sin_esquema, "query_analytics sin `public.`:\n" + "\n".join(sin_esquema)


def test_la_migracion_une_las_dos_tablas() -> None:
    source = Path(
        "src/app/infrastructure/persistence_sqla/alembic/versions/"
        "2026_10_01_0065_query_analytics_a_public.py"
    ).read_text(encoding="utf-8")
    assert "to_regclass('raw.query_analytics') IS NULL" in source
    assert "ALTER TABLE raw.query_analytics SET SCHEMA public" in source
    assert "INSERT INTO public.query_analytics" in source
    assert "DROP TABLE raw.query_analytics" in source
