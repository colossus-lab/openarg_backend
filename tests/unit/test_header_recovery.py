"""Tests for `parsers.header_recovery`.

Covers the buried-header recovery that runs when pandas mistakes a TITLE row
for the header (specs/021-parser-hardening Phase 1).
"""

from __future__ import annotations

import pandas as pd

from app.application.pipeline.parsers.header_recovery import (
    find_data_start_row,
    promote_buried_headers,
)


def test_find_data_start_row_simple():
    """Row 0 = header text, row 1+ = numeric data → data starts at row 1."""
    df = pd.DataFrame(
        [
            ["provincia", "anio", "poblacion"],
            ["Buenos Aires", "2020", "1234567"],
            ["Córdoba", "2020", "987654"],
        ]
    )
    assert find_data_start_row(df) == 1


def test_find_data_start_row_with_title():
    """Row 0 = title, row 1 = sub-headers, row 2+ = data."""
    df = pd.DataFrame(
        [
            ["Cuadro 3.1 Población por condición", None, None],
            ["Provincia", "Año", "Población"],
            ["Buenos Aires", "2020", "1234567"],
            ["Córdoba", "2020", "987654"],
        ]
    )
    # data starts at row 2 (numeric backbone there)
    assert find_data_start_row(df) == 2


def test_find_data_start_row_text_only_returns_none():
    """No numeric backbone anywhere → can't anchor data, return None."""
    df = pd.DataFrame(
        [
            ["Some title", None, None],
            ["Author", "Year", "Notes"],
            ["A", "B", "C"],
        ]
    )
    assert find_data_start_row(df) is None


def test_find_data_start_row_skips_year_header_row():
    """Year-only row (merged-cell propagation) must NOT count as data start.

    Real-world: cordoba_modelo_insumo_producto layout —
      row 0: title text
      row 1: years repeated across cols (2003, 2003, 2003, 2004, 2004, 2004)
      row 2: sub-categories ("Total", "Hombres", "Mujeres", ...)
      row 3+: actual numeric data (varied magnitudes)

    The detector should skip row 1 (looks numeric but is a year header) and
    return row 3.
    """
    df = pd.DataFrame(
        [
            ["Cuadro 2.1.1.1: Matriz", None, None, None, None, None, None],
            [None, "2003", "2003", "2003", "2004", "2004", "2004"],
            [None, "Total", "Hombres", "Mujeres", "Total", "Hombres", "Mujeres"],
            ["Población", "12345", "5678", "6789", "13000", "6000", "7000"],
            ["Empleo", "8000", "4000", "4000", "9000", "4500", "4500"],
        ]
    )
    # Row 1 looks numeric (6 of 6 are numeric) but they're all years 2003-2004.
    # Row 3 is the real data start.
    assert find_data_start_row(df) == 3


def test_find_data_start_row_does_not_skip_real_numeric_data():
    """Negative control: numeric data with varied values must NOT be skipped."""
    df = pd.DataFrame(
        [
            ["provincia", "anio", "valor", "categoria"],
            ["BA", "2020", "12345.67", "A"],
            ["Córdoba", "2020", "9876.54", "B"],
        ]
    )
    # Row 1 has 1 year + 1 decimal + 1 text = mixed but still data.
    assert find_data_start_row(df) == 1


def test_promote_buried_headers_idempotent_on_clean_input():
    """Already-good column names → no-op."""
    df = pd.DataFrame({"provincia": ["A", "B"], "anio": [2020, 2021]})
    out = promote_buried_headers(df)
    assert list(out.columns) == ["provincia", "anio"]
    assert len(out) == 2


def test_promote_buried_headers_rescues_col_n():
    """Cols are col_0..col_2 (placeholder), real header is row 0, data starts row 1."""
    # Construct: cols=col_0, col_1, col_2; row 0 = real header text;
    # row 1+ = numeric data.
    df = pd.DataFrame(
        {
            "col_0": ["provincia", "Buenos Aires", "Córdoba"],
            "col_1": ["anio", "2020", "2021"],
            "col_2": ["poblacion", "1234567", "987654"],
        }
    )
    out = promote_buried_headers(df)
    # New cols should come from row 0 of original frame
    assert "provincia" in out.columns
    assert "anio" in out.columns
    assert "poblacion" in out.columns
    # The original row-0 header must be consumed — only data rows remain.
    assert len(out) == 2


def test_promote_buried_headers_with_merged_parent():
    """Two-row header where row 0 has parent label and row 1 has children;
    forward-fill propagates parent across all child columns."""
    df = pd.DataFrame(
        {
            "col_0": ["", "provincia", "Buenos Aires", "Córdoba"],
            "col_1": ["Visitantes", "var_pct", "1.5", "2.3"],
            "col_2": ["", "absoluto", "1000", "2000"],
            "col_3": ["Hospedados", "var_pct", "0.8", "1.1"],
            "col_4": ["", "absoluto", "500", "750"],
        }
    )
    out = promote_buried_headers(df)
    cols = list(out.columns)
    # First col gets just "provincia" (no parent above)
    assert any("provincia" in c for c in cols)
    # Visitantes parent should propagate to its children (cols 1 and 2)
    assert any("Visitantes" in c and "var_pct" in c for c in cols)
    assert any("Visitantes" in c and "absoluto" in c for c in cols)
    # Hospedados parent should propagate
    assert any("Hospedados" in c and "var_pct" in c for c in cols)
    # Data preserved
    assert len(out) == 2
