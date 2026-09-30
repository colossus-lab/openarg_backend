"""Las tablas de Fundadores y créditos se nombran siempre con `public.`.

El search_path por defecto de la base es `raw, public`: un CREATE sin esquema
cae en `raw` (pasó en staging con la 0063 original), y una consulta sin
esquema depende del search_path de cada conexión, que el pool puede resetear.
Es la misma trampa que rompió ~4.4k tablas `cache_*`. Ver también
`test_registry_is_qualified.py`.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_TABLES = ("api_supporters", "api_credit_balances", "api_credit_movements")
_FILES = (
    "src/app/infrastructure/persistence_sqla/alembic/versions/2026_09_30_0063_fundadores_y_creditos.py",
    "src/app/infrastructure/adapters/credits/credit_repository_sqla.py",
    "src/app/presentation/http/controllers/admin/supporters_router.py",
)
# Una tabla usada en SQL: después de FROM, JOIN, INTO, UPDATE, TABLE, ON o EXISTS.
_SQL_USE = re.compile(
    r"\b(FROM|JOIN|INTO|UPDATE|TABLE|ON|EXISTS)\s+(?P<name>[\w.]+)", re.IGNORECASE
)


@pytest.mark.parametrize("path", _FILES)
def test_every_sql_use_is_schema_qualified(path: str) -> None:
    source = Path(path).read_text(encoding="utf-8")
    unqualified = [
        m.group(0)
        for m in _SQL_USE.finditer(source)
        if m.group("name") in _TABLES or m.group("name") == "users"
    ]
    assert not unqualified, f"{path}: sin esquema: {unqualified}"


def test_the_move_to_public_only_acts_when_needed() -> None:
    source = Path(
        "src/app/infrastructure/persistence_sqla/alembic/versions/2026_10_01_0064_creditos_a_public.py"
    ).read_text(encoding="utf-8")
    assert "to_regclass('raw.{table}') IS NOT NULL" in source
    assert "to_regclass('public.{table}') IS NULL" in source
    assert "SET SCHEMA public" in source
