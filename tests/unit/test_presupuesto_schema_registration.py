"""Regression test: presupuesto tables must register the schema they land in.

Found on staging 2026-07-26: `mart.presupuesto_consolidado` sat in
`build_failed` with

    relation "public.cache_presupuesto_credito_2016" does not exist

because `ingest_presupuesto` wrote the tables with `to_sql(schema="raw")` but
registered them in `raw_table_versions` with a hard-coded
`schema_name="public"`. The mart macros render `{schema_name}."{table_name}"`,
so the divergence produced a view referencing a relation that never existed —
silently, for every budget mart built off `presupuesto_abierto::*` identities.

The guard is structural: both the write and the registration must read the
same constant, so they cannot drift apart again.
"""

from __future__ import annotations

import ast
from pathlib import Path

# Read the source directly instead of importing: the module pulls in celery
# and the whole task graph, which this purely structural check does not need.
_MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "app"
    / "infrastructure"
    / "celery"
    / "tasks"
    / "presupuesto_tasks.py"
)
_SOURCE = _MODULE_PATH.read_text(encoding="utf-8")


def _keyword_values(func_name: str, kwarg: str) -> list[str]:
    """Collect the source text of `kwarg` for every call to `func_name`."""
    tree = ast.parse(_SOURCE)
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        name = target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
        if name != func_name:
            continue
        for kw in node.keywords:
            if kw.arg == kwarg:
                found.append(ast.unparse(kw.value))
    return found


class TestSchemaRegistration:
    def test_target_schema_is_raw(self) -> None:
        tree = ast.parse(_SOURCE)
        values = [
            ast.literal_eval(node.value)
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "_TARGET_SCHEMA" for t in node.targets)
        ]
        assert values == ["raw"], f"expected _TARGET_SCHEMA = 'raw', got {values}"

    def test_registration_uses_the_same_constant_as_the_write(self) -> None:
        """`register_via_b_table(schema_name=)` must not diverge from `to_sql(schema=)`."""
        registered = _keyword_values("register_via_b_table", "schema_name")
        written = _keyword_values("to_sql", "schema")

        assert registered, "no register_via_b_table(schema_name=...) call found"
        assert written, "no to_sql(schema=...) call found"

        # Both sides must reference the constant, never a literal — a literal
        # is exactly how the two drifted apart in the first place.
        assert set(registered) == {"_TARGET_SCHEMA"}, (
            f"register_via_b_table must pass _TARGET_SCHEMA, got: {sorted(set(registered))}"
        )
        assert set(written) == {"_TARGET_SCHEMA"}, (
            f"to_sql must pass _TARGET_SCHEMA, got: {sorted(set(written))}"
        )

    def test_no_hardcoded_public_schema_remains(self) -> None:
        assert 'schema_name="public"' not in _SOURCE
        assert "schema_name='public'" not in _SOURCE
