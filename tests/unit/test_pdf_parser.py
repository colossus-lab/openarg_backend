"""Tests for `parsers.pdf` (specs/021-parser-hardening Phase 3).

Most tests use synthetic PDF input (built with reportlab if available, or
just unit-test the merge/normalize functions directly). Real-PDF
end-to-end coverage happens in integration against staging.
"""

from __future__ import annotations

import pandas as pd
import pytest

from app.application.pipeline.parsers.pdf import (
    PdfParserError,
    _normalize_cell,
    _table_to_dataframe,
    extract_tables_from_pdf,
    merge_consecutive_tables,
)


def test_normalize_cell_none():
    assert _normalize_cell(None) == ""


def test_normalize_cell_collapses_whitespace():
    assert _normalize_cell("  hello\n\tworld  ") == "hello world"


def test_normalize_cell_handles_nbsp():
    """pdfplumber emits non-breaking spaces; treat them like normal spaces."""
    assert _normalize_cell("hello\xa0world") == "hello world"


def test_table_to_dataframe_simple():
    raw = [
        ["Provincia", "Año", "Valor"],
        ["BA", "2020", "100"],
        ["Córdoba", "2020", "80"],
    ]
    df = _table_to_dataframe(raw)
    assert df is not None
    assert list(df.columns) == ["Provincia", "Año", "Valor"]
    assert len(df) == 2
    assert df.iloc[0]["Provincia"] == "BA"


def test_table_to_dataframe_pads_short_rows():
    """Some PDF cells go missing; later rows might have fewer cells than the
    header. Pad with empty strings instead of failing."""
    raw = [
        ["A", "B", "C"],
        ["1", "2"],  # missing third cell
        ["x", "y", "z"],
    ]
    df = _table_to_dataframe(raw)
    assert df is not None
    assert df.iloc[0]["C"] == ""
    assert df.iloc[1]["C"] == "z"


def test_table_to_dataframe_rejects_below_min_columns():
    raw = [["solo_col"], ["a"], ["b"]]
    assert _table_to_dataframe(raw) is None


def test_table_to_dataframe_rejects_no_data_rows():
    raw = [["A", "B", "C"]]  # only header
    assert _table_to_dataframe(raw) is None


def test_table_to_dataframe_drops_empty_rows():
    raw = [
        ["A", "B"],
        ["1", "2"],
        ["", ""],  # empty data row
        ["3", "4"],
    ]
    df = _table_to_dataframe(raw)
    assert df is not None
    assert len(df) == 2  # empty row dropped


def test_table_to_dataframe_synthesizes_missing_headers():
    """If first row has empty cells, fill with col_N placeholder."""
    raw = [
        ["", "Año", ""],
        ["1", "2020", "x"],
    ]
    df = _table_to_dataframe(raw)
    assert df is not None
    cols = list(df.columns)
    assert "col_0" in cols
    assert "Año" in cols
    assert "col_2" in cols


def test_merge_consecutive_tables_continuation():
    """Two tables with identical column names → concat."""
    t1 = pd.DataFrame({"A": ["1", "2"], "B": ["x", "y"]})
    t2 = pd.DataFrame({"A": ["3", "4"], "B": ["z", "w"]})
    merged = merge_consecutive_tables([t1, t2])
    assert len(merged) == 1
    assert len(merged[0]) == 4
    assert list(merged[0]["A"]) == ["1", "2", "3", "4"]


def test_merge_consecutive_tables_different_shape():
    """Different column count → kept separate."""
    t1 = pd.DataFrame({"A": ["1"], "B": ["x"]})
    t2 = pd.DataFrame({"A": ["2"], "B": ["y"], "C": ["z"]})
    merged = merge_consecutive_tables([t1, t2])
    assert len(merged) == 2


def test_merge_consecutive_tables_drops_repeated_header_in_continuation():
    """Some PDFs include the header row in every page → if next table's
    first row matches the previous columns, drop it."""
    t1 = pd.DataFrame({"A": ["1", "2"], "B": ["x", "y"]})
    # t2 has the header row again as data row 0
    t2 = pd.DataFrame({"A": ["A", "3"], "B": ["B", "z"]})
    merged = merge_consecutive_tables([t1, t2])
    assert len(merged) == 1
    # Row "A,B" should NOT appear in data
    assert not ((merged[0]["A"] == "A") & (merged[0]["B"] == "B")).any()
    assert len(merged[0]) == 3


def test_merge_consecutive_tables_empty_input():
    assert merge_consecutive_tables([]) == []


def test_extract_tables_raises_on_missing_file():
    """A non-existent path should produce PdfParserError, not a generic
    exception."""
    with pytest.raises(PdfParserError):
        extract_tables_from_pdf("/tmp/this-path-does-not-exist-xyzzy.pdf")
