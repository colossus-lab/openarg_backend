"""Tests for `application/common/sql_safety.is_pure_select`.

Covers the cases the previous `_WRITE_KEYWORDS` regex got wrong:
  - false positive: `SELECT update_at FROM foo` (col named after a keyword)
  - false negative: `SELECT 1 -- insert` (keyword inside a comment)
And the structural cases the AST check exists to catch:
  - DML/DDL embedded in a CTE
  - stacked statements
  - non-SELECT top-level statements
"""

from __future__ import annotations

import pytest

from app.application.common.sql_safety import is_pure_select


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "SELECT * FROM foo",
        "SELECT update_at FROM foo",
        "SELECT 1 -- insert",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "SELECT a FROM foo UNION SELECT b FROM bar",
    ],
)
def test_accepts_pure_selects(sql: str) -> None:
    ok, reason = is_pure_select(sql)
    assert ok, f"expected accepted, got reason={reason!r}"
    assert reason is None


@pytest.mark.parametrize(
    ("sql", "expect_substring"),
    [
        ("UPDATE foo SET a = 1", "only SELECT"),
        ("DELETE FROM foo", "only SELECT"),
        ("DROP TABLE foo", "only SELECT"),
        ("INSERT INTO foo VALUES (1)", "only SELECT"),
        (
            "WITH x AS (INSERT INTO foo VALUES (1) RETURNING *) SELECT * FROM x",
            "Insert",
        ),
        ("SELECT 1; DROP TABLE foo", "single SELECT"),
        # sqlglot raises on empty input → caught and surfaced as parse error.
        ("", "could not parse"),
    ],
)
def test_rejects_writes_and_malformed(sql: str, expect_substring: str) -> None:
    ok, reason = is_pure_select(sql)
    assert not ok
    assert reason is not None
    assert expect_substring in reason
