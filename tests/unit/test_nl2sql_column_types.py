"""The NL2SQL prompt can only respect column types if it is shown them.

Measured on staging 2026-07-26. `nl2sql.txt` tells the model not to wrap
numeric columns in `replace()` — correct advice, since `replace(numeric,
...)` errors outright. It then tells the model to determine the type from
`tables_context`. Two independent gaps made that impossible:

1. `tables_context` sourced types from `table_catalog.column_types`, which
   `catalog_enrichment.txt` fills with an LLM-written "descripción breve"
   — prose, not types. And `table_catalog` had zero rows for `mart.*`
   anyway, so marts rendered as a bare `Columns: a, b, c` list.
2. The one code path that did ask Postgres for real types queried
   `information_schema.columns`, which does not include materialized
   views. Every mart is a matview, so it returned nothing for exactly the
   tables that expose pre-cast `numeric` columns.

Net effect: the model wrapped `mart.presupuesto_consolidado.credito_devengado`
(numeric) in `replace()`, every SQL attempt errored, and the budget
question deflected with confidence 0.9 — a no-answer on data we hold.
"""

from __future__ import annotations

from pathlib import Path

from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

_PROMPT = (
    Path(__file__).resolve().parents[2] / "src" / "app" / "prompts" / "nl2sql.txt"
).read_text(encoding="utf-8")


class TestColumnTypeLookupCoversMatviews:
    def test_query_targets_pg_catalog_not_information_schema(self) -> None:
        """information_schema.columns omits matviews; pg_attribute does not."""
        captured: dict = {}

        class _Result:
            @staticmethod
            def fetchall() -> list:
                return []

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *_a) -> bool:
                return False

            def execute(self, statement, params=None):
                captured["sql"] = str(statement)
                captured["params"] = params
                return _Result()

            def rollback(self) -> None:
                pass

        class _Engine:
            @staticmethod
            def connect() -> _Conn:
                return _Conn()

        adapter = PgSandboxAdapter()
        adapter._get_engine = lambda: _Engine()  # type: ignore[method-assign]
        adapter._get_column_types_sync(["mart.presupuesto_consolidado"])

        sql = captured["sql"].lower()
        assert "information_schema" not in sql, (
            "information_schema.columns silently excludes materialized views"
        )
        assert "pg_attribute" in sql and "pg_class" in sql
        assert "'m'" in sql, "relkind 'm' (matview) must be included"

    def test_schema_qualified_name_is_split(self) -> None:
        """`mart.x` must be looked up in schema `mart`, not `public`."""
        captured: dict = {}

        class _Result:
            @staticmethod
            def fetchall() -> list:
                return []

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *_a) -> bool:
                return False

            def execute(self, statement, params=None):
                captured["params"] = params
                return _Result()

            def rollback(self) -> None:
                pass

        class _Engine:
            @staticmethod
            def connect() -> _Conn:
                return _Conn()

        adapter = PgSandboxAdapter()
        adapter._get_engine = lambda: _Engine()  # type: ignore[method-assign]
        adapter._get_column_types_sync(["mart.presupuesto_consolidado"])

        assert "mart" in captured["params"]["schemas"]
        assert "presupuesto_consolidado" in captured["params"]["tables"]


class TestPromptTypeGuidance:
    def test_prompt_does_not_point_at_a_nonexistent_label(self) -> None:
        """The rule used to cite `Columnas (tipo):` lines that were never emitted."""
        assert "Columnas (tipo)" not in _PROMPT

    def test_prompt_forbids_casting_numerics_to_text(self) -> None:
        """`replace(1234.56::text, '.', '')` -> `123456`: a silent x100 error.

        This is the failure the guidance must name explicitly, because it
        is the "obvious" way to make the normalization expression compile
        against a numeric column.
        """
        assert "::text" in _PROMPT
        assert "123456" in _PROMPT

    def test_prompt_carries_a_numeric_mart_example(self) -> None:
        """Every worked example used to be a TEXT `cache_*` table."""
        assert "mart.presupuesto_consolidado" in _PROMPT
        assert "credito_vigente (numeric)" in _PROMPT

    def test_prompt_warns_about_cross_type_joins(self) -> None:
        """`integer = text` was the second error the failed run produced."""
        assert "integer = text" in _PROMPT
