"""Tests for `parsers.column_normalization`.

Covers the byte-aware dedup that prevents `DuplicateColumnError` at
`df.to_sql`, plus the garbage-column detectors used by header recovery and
the in-place repair scripts (specs/021-parser-hardening Phase 1).
"""

from __future__ import annotations

import pytest

from app.application.pipeline.parsers.column_normalization import (
    PG_NAME_LIMIT_BYTES,
    dedupe_column_names,
    garbage_column_ratio,
    is_garbage_column,
    is_placeholder_column,
    is_title_row_column,
    is_url_column,
    truncate_utf8_bytes,
)


def test_truncate_utf8_short_string_passes_through():
    assert truncate_utf8_bytes("hello", 10) == "hello"


def test_truncate_utf8_ascii_clip():
    assert truncate_utf8_bytes("a" * 100, 5) == "aaaaa"


def test_truncate_utf8_does_not_split_multibyte_char():
    """`á` is 2 bytes in UTF-8. Truncating mid-char must drop the partial."""
    s = "Población"
    truncated = truncate_utf8_bytes(s, 4)
    assert truncated.encode("utf-8") and len(truncated.encode("utf-8")) <= 4
    assert "\\x" not in repr(truncated)


def test_dedupe_simple_collision():
    out = dedupe_column_names(["x", "x", "y"])
    assert out == ["x", "x_2", "y"]


def test_dedupe_chain_collision():
    """If `x_2` already exists when we try to dedupe a second `x`, the dedup
    must walk forward (incrementing the suffix counter), not collide again."""
    out = dedupe_column_names(["x", "x_2", "x"])
    assert out == ["x", "x_2", "x_3"]
    # All distinct.
    assert len(set(out)) == len(out)


def test_dedupe_byte_truncation_collision():
    """Two distinct names whose first 63 bytes coincide collide on Postgres
    side. The dedup has to detect this in byte space."""
    long_a = "Cuadro 3.1 Población por condición de pobreza — A_" + "x" * 100
    long_b = "Cuadro 3.1 Población por condición de pobreza — A_" + "y" * 100
    out = dedupe_column_names([long_a, long_b])
    # Both must be ≤63 bytes, distinct.
    assert all(len(c.encode("utf-8")) <= PG_NAME_LIMIT_BYTES for c in out)
    assert out[0] != out[1]


@pytest.mark.parametrize(
    "name,expected",
    [
        ("col_1", True),
        ("col_999", True),
        ("Unnamed: 4", True),
        ("unnamed:0", True),
        ("provincia", False),
        ("col_X", False),  # not strictly a digit suffix
    ],
)
def test_is_placeholder_column(name, expected):
    assert is_placeholder_column(name) is expected


def test_is_title_row_column():
    assert is_title_row_column("Cuadro 3.1 Población") is True
    assert is_title_row_column("CUADRO 3.1") is True
    assert is_title_row_column("Población") is False


def test_is_url_column():
    assert is_url_column("http://datos.pami.org.ar/dataset/abc") is True
    assert is_url_column("https://example.com") is True
    assert is_url_column("normal_col") is False


def test_garbage_column_ratio_clean():
    assert garbage_column_ratio(["provincia", "anio", "valor"]) == 0.0


def test_garbage_column_ratio_mixed():
    cols = ["provincia", "col_1", "col_2", "Cuadro 3.1"]
    # 3 garbage out of 4
    assert garbage_column_ratio(cols) == pytest.approx(0.75)


def test_garbage_column_ratio_empty():
    assert garbage_column_ratio([]) == 0.0


def test_is_garbage_column_paginas():
    assert is_garbage_column("Páginas:") is True
    assert is_garbage_column("Páginas:_2") is True
    assert is_garbage_column("paginas:") is True
