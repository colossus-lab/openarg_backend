"""Every SQL reference to the registry must name its schema.

Production carries `raw_table_versions` in both schemas — `public` with the
live rows and `raw` with a stale 166-row shadow — and the application reaches
Postgres through PGBouncer in transaction pooling, where a session-level
`SET search_path` does not survive the connection returning to the pool.
Measured 2026-08-22: of twelve consecutive connections, one resolved `public`
first and eleven resolved `raw`.

An unqualified reference is therefore a coin flip, and it has already cost
real outages twice: 23,445 snapshots captured without an identity, and three
marts down for a column that existed the whole time — `live_table()` resolved
to its empty placeholder nine times out of ten.

This test exists because both were found by accident, weeks apart.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

# Blocklist entries and prose mention the table by bare name on purpose:
# `_FORBIDDEN_TABLES` matches what a user might type, and qualifying those
# would defeat the check that BUG-014 added. Only SQL positions are inspected.
_SQL_REF = re.compile(r"\b(FROM|JOIN|INTO|UPDATE|DELETE\s+FROM)\s+raw_table_versions\b")

_SRC = Path(__file__).resolve().parents[2] / "src" / "app"

# Migrations run with whatever search_path Alembic was given and predate the
# duplication; rewriting historical migrations would change what already ran.
_EXEMPT = ("alembic",)


def _python_files():
    for path in _SRC.rglob("*.py"):
        if any(part in path.parts for part in _EXEMPT):
            continue
        yield path


def test_no_sql_reads_the_registry_without_naming_its_schema():
    offenders = []
    for path in _python_files():
        text = path.read_text(encoding="utf-8")
        for match in _SQL_REF.finditer(text):
            line = text[: match.start()].count("\n") + 1
            offenders.append(f"{path.relative_to(_SRC)}:{line}")

    assert not offenders, (
        "unqualified registry reference — through PGBouncer this resolves to the "
        "stale shadow most of the time:\n  " + "\n  ".join(offenders)
    )


def test_the_blocklists_still_use_the_bare_name():
    """The security check matches what a user types, not what SQL resolves to.
    Qualifying these would let `raw_table_versions` through the sandbox."""
    from app.application.pipeline.nodes.analyst import _INTERNAL_NAMES as analyst_blocked

    assert "raw_table_versions" in {t.lower() for t in analyst_blocked}


@pytest.mark.parametrize("schema", ["public", "raw"])
def test_both_schemas_are_spelled_out_somewhere(schema):
    """A sanity check on the premise: the codebase knows both exist."""
    found = any(
        f"{schema}.raw_table_versions" in p.read_text(encoding="utf-8")
        for p in _python_files()
    )
    if schema == "public":
        assert found, "the live registry must be addressed explicitly"
