"""Tests for the table → marts index.

Two things matter more than the happy path. The index is used as a **brake** on
an unattended repair, so a missed reference is the dangerous direction: a mart
absent from the consumer list is a mart the sweep is free to break. And the
column check has to be crude in that same direction — better to postpone a
repair to a person than to rename a column a view names by hand.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.application.marts.consumers import (
    ConsumerIndex,
    build_consumer_index,
    source_references,
)


class _Row:
    def __init__(self, mart_id, sql_definition):
        self.mart_id = mart_id
        self.sql_definition = sql_definition


def _engine(rows):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = rows
    return engine


# ── extracción de referencias ──────────────────────────────────


def test_reads_a_schema_qualified_quoted_reference():
    assert source_references('SELECT * FROM raw."cache_x"') == {("raw", "cache_x")}


def test_reads_public_as_well_as_raw():
    # `escuelas_padron_nacional` reads a `public.cache_*` table in production;
    # an index that only looked at `raw` would call it unconsumed.
    sql = 'SELECT * FROM public."cache_datos_gob_ar_padron"'
    assert source_references(sql) == {("public", "cache_datos_gob_ar_padron")}


def test_reads_a_join_even_though_production_has_none_today():
    sql = 'SELECT * FROM raw."a" JOIN raw."b" ON true'
    assert source_references(sql) == {("raw", "a"), ("raw", "b")}


def test_is_case_insensitive_on_the_keyword():
    assert source_references('select * from raw."a"') == {("raw", "a")}


def test_normalises_the_schema_but_not_the_table():
    # Postgres folds the unquoted schema and preserves the quoted table.
    assert source_references('FROM RAW."Cache_X"') == {("raw", "Cache_X")}


def test_ignores_an_unqualified_reference():
    # A bare CTE name is not a table, and treating it as one would invent
    # consumers for tables that do not exist.
    assert source_references("FROM src") == set()


def test_survives_null_sql():
    assert source_references("") == set()


# ── índice ─────────────────────────────────────────────────────


def test_inverts_marts_into_table_to_marts():
    index = build_consumer_index(
        _engine(
            [
                _Row("m1", 'FROM raw."a" UNION ALL SELECT * FROM raw."b"'),
                _Row("m2", 'FROM raw."b"'),
            ]
        )
    )
    assert index.marts_for("raw", "a") == ("m1",)
    assert index.marts_for("raw", "b") == ("m1", "m2")
    assert index.marts == 2
    assert index.tables == 2


def test_a_table_no_mart_reads_has_no_consumers():
    index = build_consumer_index(_engine([_Row("m1", 'FROM raw."a"')]))
    assert index.marts_for("raw", "zzz") == ()


def test_one_mart_naming_a_table_twice_is_counted_once():
    index = build_consumer_index(
        _engine([_Row("m1", 'FROM raw."a" UNION ALL SELECT * FROM raw."a"')])
    )
    assert index.marts_for("raw", "a") == ("m1",)


def test_a_mart_with_unresolved_macros_contributes_nothing():
    # `staff_estado` is in this state in production: its `live_table()` never
    # resolved. It must not be reported as consuming anything.
    index = build_consumer_index(_engine([_Row("staff_estado", "FROM {{ live_table('x') }}")]))
    assert index.tables == 0
    assert index.marts == 1


# ── el freno ───────────────────────────────────────────────────


def _index(sql, mart_id="m1"):
    return build_consumer_index(_engine([_Row(mart_id, sql)]))


def test_a_column_named_bare_by_a_mart_blocks_the_rename():
    index = _index('SELECT col_1 AS monto FROM raw."a"')
    assert index.marts_referencing_column("raw", "a", "col_1") == ("m1",)


def test_a_column_named_in_quotes_blocks_it_too():
    index = _index('SELECT "Tasa Cruda*10 mil h" FROM raw."a"')
    assert index.marts_referencing_column("raw", "a", "Tasa Cruda*10 mil h") == ("m1",)


def test_a_column_nobody_names_does_not_block():
    index = _index('SELECT col_1 FROM raw."a"')
    assert index.marts_referencing_column("raw", "a", "col_9") == ()


def test_a_substring_of_another_column_does_not_block():
    # `col_1` must not match `col_12`, or every rename in a wide table would be
    # refused and the ladder would never apply anything.
    index = _index('SELECT col_12 FROM raw."a"')
    assert index.marts_referencing_column("raw", "a", "col_1") == ()


def test_a_non_identifier_name_is_only_matched_quoted():
    # An unquoted search for `a-b` would match the expression `a - b`.
    index = _index('SELECT a - b FROM raw."t"')
    assert index.marts_referencing_column("raw", "t", "a-b") == ()


def test_a_mart_that_does_not_read_the_table_is_never_blocking():
    # The crude match is scoped to the table's own consumers, so a different
    # mart mentioning `col_1` about a different table is not a reason to refuse.
    index = build_consumer_index(
        _engine([_Row("m1", 'SELECT col_1 FROM raw."other"'), _Row("m2", 'SELECT x FROM raw."a"')])
    )
    assert index.marts_referencing_column("raw", "a", "col_1") == ()


def test_an_empty_index_blocks_nothing():
    assert ConsumerIndex().marts_referencing_column("raw", "a", "col_1") == ()
