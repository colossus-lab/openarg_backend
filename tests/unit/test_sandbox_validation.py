from __future__ import annotations

from unittest.mock import MagicMock

from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter, _validate_sql


class TestSQLValidation:
    def test_select_allowed(self):
        assert _validate_sql("SELECT * FROM cache_table") is None

    def test_select_with_where(self):
        assert _validate_sql("SELECT col1, col2 FROM cache_datos WHERE col1 > 10") is None

    def test_with_cte_allowed(self):
        assert _validate_sql("WITH cache_cte AS (SELECT 1 AS x) SELECT * FROM cache_cte") is None

    def test_empty_rejected(self):
        assert _validate_sql("") is not None
        assert _validate_sql("   ") is not None

    def test_insert_rejected(self):
        result = _validate_sql("INSERT INTO table1 VALUES (1, 2)")
        assert result is not None
        assert "Forbidden" in result or "Only SELECT" in result

    def test_update_rejected(self):
        result = _validate_sql("UPDATE table1 SET col1 = 1")
        assert result is not None

    def test_delete_rejected(self):
        result = _validate_sql("DELETE FROM table1")
        assert result is not None

    def test_drop_rejected(self):
        result = _validate_sql("DROP TABLE table1")
        assert result is not None

    def test_alter_rejected(self):
        result = _validate_sql("ALTER TABLE table1 ADD COLUMN x INT")
        assert result is not None

    def test_truncate_rejected(self):
        result = _validate_sql("TRUNCATE table1")
        assert result is not None

    def test_create_rejected(self):
        result = _validate_sql("CREATE TABLE evil (id INT)")
        assert result is not None

    def test_comment_bypass_rejected(self):
        # Semicolon creates a multi-statement query which sqlglot's AST validation
        # correctly rejects, even though the INSERT is behind a comment
        result = _validate_sql("SELECT 1; -- INSERT INTO evil VALUES (1)")
        assert result is not None  # multi-statement rejected by AST validator

    def test_select_starting_with_spaces(self):
        assert _validate_sql("  SELECT 1") is None

    def test_case_insensitive_select(self):
        assert _validate_sql("select * from cache_t") is None
        assert _validate_sql("SELECT * FROM cache_t") is None

    def test_grant_rejected(self):
        result = _validate_sql("GRANT ALL ON table1 TO public")
        assert result is not None

    def test_vacuum_rejected(self):
        result = _validate_sql("VACUUM table1")
        assert result is not None

    def test_internal_table_rejected_in_public(self):
        result = _validate_sql("SELECT * FROM query_analytics")
        assert result is not None
        assert "internal table" in result

    def test_internal_table_rejected_in_raw_schema(self):
        # raw is prefix-free, so without the blocklist this would slip through.
        result = _validate_sql("SELECT * FROM raw.catalog_resources")
        assert result is not None
        assert "internal table" in result

    def test_internal_table_rejected_via_join(self):
        result = _validate_sql(
            "SELECT * FROM cache_datos d JOIN raw.raw_table_versions v ON d.id = v.id"
        )
        assert result is not None
        assert "internal table" in result

    def test_internal_table_rejected_ast_only(self):
        # Comma-separated FROM lists are caught by the AST validator, not the regex.
        result = _validate_sql("SELECT * FROM cache_datos, api_keys")
        assert result is not None

    def test_cache_data_table_still_allowed(self):
        # Legitimate cache_* data tables must NOT be blocked by the new rule.
        assert _validate_sql("SELECT * FROM cache_delitos_caba_v3") is None


class _FetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class TestSandboxTableDiscovery:
    def test_list_tables_includes_live_raw_tables_without_cached_dataset_rows(self):
        adapter = PgSandboxAdapter()
        conn = MagicMock()
        conn.execute.side_effect = [
            # 1) cached_datasets SELECT — registered legacy/public row
            _FetchAllResult(
                [
                    MagicMock(
                        dataset_id="ds-1",
                        table_name="cache_budget_sample",
                        row_count=42,
                        columns_json='["a","b"]',
                    )
                ]
            ),
            # 2) raw_table_versions SELECT — one extra live raw table
            _FetchAllResult(
                [
                    MagicMock(
                        schema_name="raw",
                        table_name="energia_pozos_v2",
                        row_count=100,
                    ),
                ]
            ),
            # 3) columns SELECT for the unregistered live raw table
            _FetchAllResult(
                [
                    MagicMock(column_name="a"),
                    MagicMock(column_name="b"),
                    MagicMock(column_name="_source_dataset_id"),
                ]
            ),
        ]
        engine = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        adapter._engine = engine

        tables = adapter._list_tables_sync()

        assert [t.table_name for t in tables] == [
            "cache_budget_sample",
            "raw.energia_pozos_v2",
        ]
        assert tables[1].dataset_id == ""
        assert tables[1].row_count == 100
        assert tables[1].columns == ["a", "b", "_source_dataset_id"]


class TestSandboxColumnTypes:
    def test_get_column_types_supports_qualified_raw_and_mart_names(self):
        adapter = PgSandboxAdapter()
        conn = MagicMock()
        conn.execute.side_effect = [
            _FetchAllResult(
                [
                    MagicMock(
                        table_schema="public",
                        table_name="cache_budget_sample",
                        column_name="monto",
                        data_type="numeric",
                    ),
                    MagicMock(
                        table_schema="raw",
                        table_name="energia_pozos_v2",
                        column_name="anio",
                        data_type="text",
                    ),
                    MagicMock(
                        table_schema="mart",
                        table_name="series_economicas",
                        column_name="valor",
                        data_type="double precision",
                    ),
                ]
            )
        ]
        engine = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        adapter._engine = engine

        types = adapter._get_column_types_sync(
            [
                "cache_budget_sample",
                "raw.energia_pozos_v2",
                "mart.series_economicas",
            ]
        )

        execute_call = conn.execute.call_args
        assert execute_call is not None
        params = execute_call.args[1]
        assert set(params["schemas"]) == {"public", "raw", "mart"}
        assert set(params["tables"]) == {
            "cache_budget_sample",
            "energia_pozos_v2",
            "series_economicas",
        }
        assert types == {
            "cache_budget_sample": [("monto", "numeric")],
            "raw.energia_pozos_v2": [("anio", "text")],
            "mart.series_economicas": [("valor", "double precision")],
        }
