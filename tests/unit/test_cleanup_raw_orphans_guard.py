"""Tests for the guard that decides which orphaned raw tables may be dropped.

Background: an automated cleanup dropped a live table in August and cost sixteen
days of collection. The guard was added right after, to keep a table that still
holds rows out of the candidate set.

It didn't work for the largest population. `pg_class.reltuples` is **-1** for a
table Postgres has never analysed — not 0 — so `<= 0` made every never-analysed
table eligible regardless of its contents. Measured in production on 2026-08-25:
1,460 tables in `raw` sat at -1 and **41 of them held real data**, while the
sweep runs every six hours with writing enabled.

The distinction is invisible unless you know that -1 is a sentinel, which is why
these tests state it out loud.
"""

from __future__ import annotations

import re
from pathlib import Path

_SQL = Path("src/app/infrastructure/celery/tasks/ops_fixes.py").read_text(encoding="utf-8")


def _guard_block() -> str:
    """The reltuples comparison inside the orphan-candidate query."""
    i = _SQL.index("SELECT c.reltuples FROM pg_class c")
    return _SQL[i : i + 400]


def test_the_guard_compares_against_zero_exactly():
    bloque = _guard_block()
    assert re.search(r"\)\s*,\s*0\s*\)\s*=\s*0", bloque), (
        "el guard tiene que ser `= 0`: `<= 0` incluye el -1 de las tablas "
        "nunca analizadas, que es la población más grande"
    )


def test_the_guard_does_not_use_less_than_or_equal():
    # El bug exacto, escrito para que un reordenamiento futuro lo despierte.
    assert "<= 0" not in _guard_block()


def test_the_reason_is_written_next_to_the_comparison():
    # Un `= 0` sin explicación se "corrige" de vuelta a `<= 0` en la próxima
    # pasada de prolijidad, porque leído solo parece un caso de borde olvidado.
    bloque = _SQL[max(0, _SQL.index("SELECT c.reltuples FROM pg_class c") - 700) :][:900]
    assert "-1" in bloque and "analys" in bloque.lower()


def test_the_sweep_still_bounds_itself():
    # El guard no es la única defensa y no debe quedarse solo.
    i = _SQL.index("SELECT c.reltuples FROM pg_class c")
    assert "LIMIT :limit" in _SQL[i : i + 600]
