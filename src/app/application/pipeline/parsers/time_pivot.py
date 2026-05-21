"""Time-pivot detection and unpivot.

Extracted from `infrastructure/celery/tasks/indec_tasks.py` (Phase 1 of
specs/021-parser-hardening). Wide layouts where each column is a period
(year/month/quarter) are common in stats-bureau publishing — pleasant for
humans, awful for SQL. This module turns them into long format.

Used both at parse time (collector path) and at repair time (in-place rewrite
of an existing wide table to its long equivalent).
"""

from __future__ import annotations

import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

# Pandas auto-suffixes duplicate column headers with `_2`, `_3`, ... — strip
# the suffix before checking the period regexes so `2004_2` still counts as
# a period column (it almost always means "second quarter of 2004" or
# similar after a merged-cell flatten).
_PIVOT_TAIL_RE = re.compile(r"_\d+$")

# Strip trailing footnote markers like `2022 (1)` or `2018 (provisional)` so
# the year token underneath still matches the date patterns.
_FOOTNOTE_RE = re.compile(r"\s*\([^)]*\)\s*$")

_DATE_COL_PATTERNS: tuple[re.Pattern[str], ...] = (
    # ISO-like: 2017, 2017-01, 2017-01-01, 2017-01-01 00:00, 2017-01-01 00:00:00
    re.compile(r"^\d{4}(?:-\d{2}(?:-\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?)?)?$"),
    # Word-month + year: "Ene-2017", "Enero 2017", "ene_2017"
    re.compile(r"^[A-Za-z]{3,}[\s\-_/]+\d{4}$"),
    # Year + word-month: "2017 ENERO", "2024 - enero"
    re.compile(r"^\d{4}[\s\-_/]+[A-Za-z]{3,}$"),
    # Quarterly / semestral labels: "T1 2017", "Q1-2017", "I 2017"
    re.compile(r"^[QqTtSsIiVv]+\d?[\s\-_/]+\d{4}$"),
    re.compile(r"^\d{4}[\s\-_/]+[QqTtSsIiVv]+\d?$"),
)


def is_time_column(name: str) -> bool:
    """True iff `name` looks like a single date/period token.

    Strips pandas' duplicate-suffix `_N` and INDEC-style trailing footnotes
    `(N)` before matching, so `2004_2` and `2022 (1)` both qualify as
    period columns.
    """
    s = str(name).strip()
    if not s or len(s) > 32:
        return False
    s_stripped = _PIVOT_TAIL_RE.sub("", s)
    s_stripped = _FOOTNOTE_RE.sub("", s_stripped).strip()
    return any(p.match(s_stripped) for p in _DATE_COL_PATTERNS)


def time_column_ratio(cols: list[str]) -> float:
    """Fraction of columns that look like period tokens. 0.0 = no time
    columns, 1.0 = all columns are periods (degenerate, missing the id col).
    """
    if not cols:
        return 0.0
    return sum(1 for c in cols if is_time_column(c)) / len(cols)


def unpivot_if_time_pivoted(
    df: pd.DataFrame, *, pivot_threshold: float = 0.50
) -> pd.DataFrame:
    """Detect and unpivot a wide time-pivoted layout to long format.

    Wide shape: one row per concept (region, indicator, etc.) and one column
    per period — `(C, p1, p2, p3)`. SQL queries against this shape are
    awful. The platform downstream assumes long format
    `(concepto, periodo, valor)` which trivially answers
    `WHERE periodo = '2024-03' AND concepto LIKE '%alimentos%'`.

    Detection:
      * `pivot_threshold` of columns match a period token AND
      * at least one column does NOT (the id/concept column).

    Without an id column we'd melt away the entire table identity.

    Effect on a single sheet: `(C, p1, p2, p3)` with N rows → `(C, periodo,
    valor)` with N×3 rows. NaN values are dropped after melt because empty
    cells in the wide layout mean "no observation for that period", not a
    legitimate row.

    Idempotent: a sheet that's already long passes through unchanged.
    """
    cols = [str(c) for c in df.columns]
    if len(cols) < 4:
        return df
    time_cols = [c for c in cols if is_time_column(c)]
    id_cols = [c for c in cols if c not in time_cols]
    if not time_cols or not id_cols:
        return df
    if len(time_cols) < len(cols) * pivot_threshold:
        return df

    df = df.rename(columns={c: str(c) for c in df.columns})
    melted = df.melt(
        id_vars=id_cols,
        value_vars=time_cols,
        var_name="periodo",
        value_name="valor",
    )
    melted = melted.dropna(subset=["valor"]).reset_index(drop=True)
    if melted.empty:
        return df
    logger.info(
        "time_pivot.unpivot: %d wide rows × %d time cols → %d long rows",
        len(df),
        len(time_cols),
        len(melted),
    )
    return melted


__all__ = [
    "is_time_column",
    "time_column_ratio",
    "unpivot_if_time_pivoted",
]
