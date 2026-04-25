"""Tests for the INDEC hierarchical header parser (WS5)."""

from __future__ import annotations

import pandas as pd
import pytest

from app.application.pipeline.parsers import (
    NORMALIZATION_VERSION,
    PARSER_VERSION,
    parse_hierarchical_headers,
)


def test_versions_exist():
    assert PARSER_VERSION
    assert NORMALIZATION_VERSION


def test_year_over_quarter_layout():
    """Mirrors INDEC EPH layout: row0=year, row1=quarter, row2+=data."""
    df = pd.DataFrame(
        [
            ["", "2003", "", "", "", "2004", "", "", ""],
            ["", "1° trimestre", "2° trimestre", "3° trimestre", "4° trimestre",
             "1° trimestre", "2° trimestre", "3° trimestre", "4° trimestre"],
            ["Tasa", 35.0, 36.0, 37.0, 38.0, 35.5, 36.5, 37.5, 38.5],
        ]
    )
    result = parse_hierarchical_headers(df)
    cols = result.columns
    assert "2003_T1" in cols
    assert "2003_T4" in cols
    assert "2004_T1" in cols
    assert result.period_kind == "quarter"
    assert result.skipped_rows == 2


def test_year_over_month_layout():
    df = pd.DataFrame(
        [
            ["", "2024", "", "", "2025", "", ""],
            ["categoria", "enero", "febrero", "marzo", "enero", "febrero", "marzo"],
            ["A", 1, 2, 3, 4, 5, 6],
        ]
    )
    result = parse_hierarchical_headers(df)
    cols = result.columns
    assert "2024_01" in cols
    assert "2024_02" in cols
    assert "2025_03" in cols
    assert result.period_kind == "month"


def test_numeric_only_quarter_columns_under_year_row():
    """When the period row is bare numbers `1 2 3 4` interpreted as quarters."""
    df = pd.DataFrame(
        [
            ["", "2010", "", "", "", "2011", "", "", ""],
            ["concepto", 1, 2, 3, 4, 1, 2, 3, 4],
            ["x", 10, 20, 30, 40, 11, 21, 31, 41],
        ]
    )
    result = parse_hierarchical_headers(df)
    cols = result.columns
    # The 1-4 row should be detected as quarter (4 unique values match)
    assert "2010_T1" in cols
    assert "2011_T4" in cols


def test_no_temporal_falls_back_to_first_row():
    df = pd.DataFrame(
        [
            ["region", "indicador", "valor"],
            ["pais", "PBI", 100],
        ]
    )
    result = parse_hierarchical_headers(df)
    assert result.columns == ["region", "indicador", "valor"]
    assert result.period_kind is None


def test_dedup_collisions():
    df = pd.DataFrame(
        [
            ["", "2003", "", "2003", ""],
            ["concepto", "1° trimestre", "2° trimestre", "1° trimestre", "2° trimestre"],
            ["x", 1, 2, 3, 4],
        ]
    )
    cols = parse_hierarchical_headers(df).columns
    # second occurrence of 2003_T1 must be deduped, not silently overwritten
    assert cols.count("2003_T1") == 1
    assert any(c.startswith("2003_T1_") for c in cols)


def test_classify_handles_empty_dataframe_gracefully():
    df = pd.DataFrame()
    result = parse_hierarchical_headers(df)
    # Empty DF should not crash
    assert result is not None


@pytest.mark.parametrize(
    "label,expected_quarter",
    [
        ("1° trimestre", 1),
        ("2° trimestre", 2),
        ("T3", 3),
        ("4", 4),
    ],
)
def test_quarter_synonyms(label: str, expected_quarter: int):
    df = pd.DataFrame(
        [
            ["", "2024"],
            ["x", label],
            ["v", 1],
        ]
    )
    result = parse_hierarchical_headers(df)
    assert f"2024_T{expected_quarter}" in result.columns
