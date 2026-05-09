"""Tests for `parsers.time_pivot`.

Covers the time-pivot detection + unpivot that turns wide stats-bureau layouts
into long format `(concepto, periodo, valor)`. Specs/021-parser-hardening
Phase 1.
"""

from __future__ import annotations

import pandas as pd
import pytest

from app.application.pipeline.parsers.time_pivot import (
    is_time_column,
    time_column_ratio,
    unpivot_if_time_pivoted,
)


@pytest.mark.parametrize(
    "name,expected",
    [
        ("2024", True),
        ("2024-01", True),
        ("2024-01-15", True),
        ("2024-01-15 00:00:00", True),
        ("2024_2", True),  # pandas dup-suffix
        ("2024 (1)", True),  # INDEC footnote
        ("Ene-2024", True),
        ("Enero 2024", True),
        ("2024 ENERO", True),
        ("T1 2024", True),
        ("Q1-2024", True),
        ("provincia", False),
        ("var_pct", False),
        ("Población", False),
        ("", False),
    ],
)
def test_is_time_column(name, expected):
    assert is_time_column(name) is expected


def test_is_time_column_too_long_rejects():
    """Anything past 32 chars can't be a single period token."""
    assert is_time_column("a" * 40) is False


def test_time_column_ratio_clean():
    assert time_column_ratio(["provincia", "anio", "valor"]) == 0.0


def test_time_column_ratio_full_pivot():
    assert time_column_ratio(["2020", "2021", "2022", "2023"]) == 1.0


def test_time_column_ratio_mixed():
    cols = ["provincia", "2020", "2021", "2022"]
    assert time_column_ratio(cols) == pytest.approx(0.75)


def test_unpivot_idempotent_on_long_layout():
    """Already-long input → returns unchanged."""
    df = pd.DataFrame(
        {
            "provincia": ["BA", "BA", "Córdoba"],
            "periodo": ["2020", "2021", "2020"],
            "valor": [100.0, 110.0, 80.0],
        }
    )
    out = unpivot_if_time_pivoted(df)
    assert list(out.columns) == ["provincia", "periodo", "valor"]
    assert len(out) == 3


def test_unpivot_wide_to_long():
    """Classic INDEC IPC layout: id col + 3 period cols → long with 9 rows."""
    df = pd.DataFrame(
        {
            "Región GBA": ["Nivel general", "Alimentos", "Vestimenta"],
            "2024-01": [1.5, 2.0, 0.5],
            "2024-02": [2.0, 2.5, 0.7],
            "2024-03": [1.8, 2.2, 0.6],
        }
    )
    out = unpivot_if_time_pivoted(df)
    assert set(out.columns) == {"Región GBA", "periodo", "valor"}
    assert len(out) == 9
    # Check that a known cell survives the melt
    cell = out[(out["Región GBA"] == "Alimentos") & (out["periodo"] == "2024-02")]
    assert cell["valor"].iloc[0] == 2.5


def test_unpivot_drops_nan_observations():
    """Empty cell in wide layout = "no observation", not a row in long.

    Use 4 cols to clear the `len(cols) < 4` defensive minimum.
    """
    df = pd.DataFrame(
        {
            "Región GBA": ["Nivel general", "Alimentos"],
            "2024-01": [1.5, 2.0],
            "2024-02": [None, 2.5],  # missing observation
            "2024-03": [1.8, 2.2],
        }
    )
    out = unpivot_if_time_pivoted(df)
    # 2 ids × 3 periods = 6, minus 1 NaN = 5
    assert len(out) == 5
    assert not out["valor"].isna().any()


def test_unpivot_no_id_col_returns_unchanged():
    """If every col is a period, melting would erase identity → return as-is."""
    df = pd.DataFrame({"2020": [100], "2021": [110], "2022": [120], "2023": [130]})
    out = unpivot_if_time_pivoted(df)
    assert list(out.columns) == ["2020", "2021", "2022", "2023"]


def test_unpivot_below_threshold_returns_unchanged():
    """Only 1/4 cols are periods → not a pivot, leave alone."""
    df = pd.DataFrame(
        {
            "provincia": ["BA"],
            "ciudad": ["CABA"],
            "categoria": ["X"],
            "2024": [100],
        }
    )
    out = unpivot_if_time_pivoted(df)
    assert list(out.columns) == ["provincia", "ciudad", "categoria", "2024"]
