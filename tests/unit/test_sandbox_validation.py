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


class _FetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class TestSandboxTableDiscovery:
    def test_list_tables_includes_physical_cache_tables_without_cached_dataset_rows(self):
        adapter = PgSandboxAdapter()
        conn = MagicMock()
        conn.execute.side_effect = [
            # 1) cached_datasets SELECT — registered legacy public.cache_*
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
            # 2) physical public.cache_* SELECT
            _FetchAllResult(
                [
                    MagicMock(table_name="cache_budget_sample", row_count=42),
                    MagicMock(table_name="cache_budget_sample_gaaaaaaaa", row_count=100),
                ]
            ),
            # 3) columns SELECT for the unregistered physical cache table
            _FetchAllResult(
                [
                    MagicMock(column_name="a"),
                    MagicMock(column_name="b"),
                    MagicMock(column_name="_source_dataset_id"),
                ]
            ),
            # 4) NEW: raw_table_versions live SELECT — empty in this test
            #    (no raw landings configured). Without this stub the adapter
            #    would raise StopIteration when it tries to enumerate raw.
            _FetchAllResult([]),
        ]
        engine = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        adapter._engine = engine

        tables = adapter._list_tables_sync()

        assert [t.table_name for t in tables] == [
            "cache_budget_sample",
            "cache_budget_sample_gaaaaaaaa",
        ]
        assert tables[1].dataset_id == ""
        assert tables[1].row_count == 100
        assert tables[1].columns == ["a", "b", "_source_dataset_id"]
