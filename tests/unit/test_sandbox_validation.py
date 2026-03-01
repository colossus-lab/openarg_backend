from __future__ import annotations

import pytest

from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import _validate_sql


class TestSQLValidation:
    def test_select_allowed(self):
        assert _validate_sql("SELECT * FROM cached_table") is None

    def test_select_with_where(self):
        assert _validate_sql("SELECT col1, col2 FROM t WHERE col1 > 10") is None

    def test_with_cte_allowed(self):
        assert _validate_sql("WITH cte AS (SELECT 1) SELECT * FROM cte") is None

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
        assert _validate_sql("select * from t") is None
        assert _validate_sql("SELECT * FROM t") is None

    def test_grant_rejected(self):
        result = _validate_sql("GRANT ALL ON table1 TO public")
        assert result is not None

    def test_vacuum_rejected(self):
        result = _validate_sql("VACUUM table1")
        assert result is not None
