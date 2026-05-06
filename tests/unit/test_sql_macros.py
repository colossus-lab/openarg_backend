"""Unit tests for `app.application.marts.sql_macros.resolve_macros`.

The resolver mocks the engine so tests don't touch the database. The
critical scenarios:
  - live_table('rid') → `raw."<table>"` for the live row.
  - live_table on missing rid → `(SELECT WHERE FALSE)` placeholder.
  - live_tables_by_portal → UNION ALL or empty placeholder.
  - live_tables_by_pattern with `*` glob → matches by SQL LIKE.
  - Bad arg charset → MacroResolutionError.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.application.marts.sql_macros import (
    MacroResolutionError,
    resolve_macros,
)


def _mock_engine_with_lives(rows: list[tuple[str, str, str]]) -> MagicMock:
    """Build an engine mock whose connect()→execute().fetchall() returns
    `rows` shaped as (resource_identity, schema_name, table_name).
    """
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    fake_rows = []
    for rid, schema, table in rows:
        r = MagicMock()
        r.resource_identity = rid
        r.schema_name = schema
        r.table_name = table
        fake_rows.append(r)

    cursor = MagicMock()
    cursor.fetchall.return_value = fake_rows
    conn.execute.return_value = cursor
    return engine


def test_live_table_resolves_to_qualified_name() -> None:
    engine = _mock_engine_with_lives(
        [("bcra::tasa-x", "raw", "bcra__tasa_x__abcd1234__v3")]
    )
    sql = "SELECT * FROM {{ live_table('bcra::tasa-x') }}"
    out = resolve_macros(sql, engine)
    assert out == 'SELECT * FROM raw."bcra__tasa_x__abcd1234__v3"'


def test_live_table_missing_emits_empty_placeholder() -> None:
    engine = _mock_engine_with_lives([])
    sql = "SELECT * FROM {{ live_table('absent::none') }}"
    out = resolve_macros(sql, engine)
    assert "WHERE FALSE" in out  # placeholder shape may evolve
    assert "absent::none" in out  # comment carries the missing rid


def test_live_tables_by_portal_unions_all_matches() -> None:
    engine = _mock_engine_with_lives(
        [
            ("bcra::a", "raw", "bcra__a__h1__v1"),
            ("bcra::b", "raw", "bcra__b__h2__v2"),
            ("indec::z", "raw", "indec__z__h3__v1"),
        ]
    )
    sql = "SELECT * FROM {{ live_tables_by_portal('bcra') }} t"
    out = resolve_macros(sql, engine)
    assert "UNION ALL" in out
    assert 'bcra__a__h1__v1' in out
    assert 'bcra__b__h2__v2' in out
    assert 'indec__z' not in out  # other portal must not appear


def test_live_tables_by_portal_empty_returns_placeholder() -> None:
    engine = _mock_engine_with_lives([("bcra::a", "raw", "bcra__a__h1__v1")])
    sql = "SELECT * FROM {{ live_tables_by_portal('nonexistent') }} t"
    out = resolve_macros(sql, engine)
    assert "WHERE FALSE" in out  # placeholder shape may evolve


def test_live_tables_by_pattern_glob_match() -> None:
    engine = _mock_engine_with_lives(
        [
            ("bcra::tasa_badlar", "raw", "t1"),
            ("bcra::tasa_pasiva", "raw", "t2"),
            ("bcra::dolar", "raw", "t3"),
        ]
    )
    sql = "SELECT * FROM {{ live_tables_by_pattern('bcra::tasa*') }} t"
    out = resolve_macros(sql, engine)
    assert "t1" in out
    assert "t2" in out
    assert "t3" not in out  # dolar doesn't match tasa*


def test_invalid_arg_charset_raises() -> None:
    engine = _mock_engine_with_lives([])
    with pytest.raises(MacroResolutionError):
        resolve_macros("{{ live_table('bad`arg') }}", engine)


def test_unknown_macro_passes_through() -> None:
    """Macros not in the closed list shouldn't match the regex; they
    pass through unchanged. We *intentionally* don't error on unknown
    macros so we don't break SQL that legitimately contains `{{ ... }}`
    in string literals (rare, but possible)."""
    engine = _mock_engine_with_lives([])
    sql = "SELECT '{{ foo() }}' AS literal"
    out = resolve_macros(sql, engine)
    assert out == sql  # untouched


def test_no_macros_passes_through() -> None:
    engine = _mock_engine_with_lives([])
    sql = "SELECT 1"
    out = resolve_macros(sql, engine)
    assert out == "SELECT 1"


def test_multiple_macros_in_one_sql() -> None:
    engine = _mock_engine_with_lives(
        [
            ("bcra::a", "raw", "bcra_a"),
            ("bcra::b", "raw", "bcra_b"),
        ]
    )
    sql = """
    SELECT * FROM {{ live_table('bcra::a') }} a
    JOIN {{ live_table('bcra::b') }} b ON a.id = b.id
    """
    out = resolve_macros(sql, engine)
    assert 'raw."bcra_a"' in out
    assert 'raw."bcra_b"' in out
