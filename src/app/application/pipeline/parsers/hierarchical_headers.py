"""Hierarchical header parser for INDEC-style spreadsheets (WS5).

Targets the 127 INDEC tables in production with malformed headers (87 with
`col_*` generics, 40 with `.1/.2/.3` pandas suffixes, 22 with bare `trimestre`).

Detects:
  - rows of years (e.g., `2003`, `2004`, ..., merged across columns)
  - rows of quarter labels (`1° trimestre`, `T1`, `1`)
  - rows of month labels (`enero` ... `12`)
  - forward-fills merged cells so the year propagates to its child columns
  - reconstructs canonical column names like `2003_T1`, `2024_01`

Designed to operate on a *raw* `pd.DataFrame` produced by `pd.read_excel`
with `header=None` (so we get every cell verbatim, including the merged
header rows).
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import pandas as pd

PARSER_VERSION = "1"
NORMALIZATION_VERSION = "1"

_YEAR_RE = re.compile(r"^(?:19|20)\d{2}$")
_QUARTER_NUM_RE = re.compile(r"^(?:T|t)?[1-4](?:°|\.|\s)?(?:\s*trimestre)?$", re.IGNORECASE)
_MONTH_NAMES_ES: dict[str, int] = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}
_MONTH_NAMES_ABBR: dict[str, int] = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "set": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}


def _strip(s) -> str:
    if s is None:
        return ""
    if isinstance(s, float) and pd.isna(s):
        return ""
    return unicodedata.normalize("NFKC", str(s)).strip()


def _is_year(value: str) -> bool:
    return bool(_YEAR_RE.match(value))


def _quarter_index(value: str) -> int | None:
    v = value.strip().lower()
    if not v:
        return None
    if _QUARTER_NUM_RE.match(v):
        # Pull the digit
        digits = re.findall(r"[1-4]", v)
        if digits:
            return int(digits[0])
    return None


def _month_index(value: str) -> int | None:
    v = value.strip().lower()
    if not v:
        return None
    if v in _MONTH_NAMES_ES:
        return _MONTH_NAMES_ES[v]
    abbr = v[:3]
    if abbr in _MONTH_NAMES_ABBR:
        return _MONTH_NAMES_ABBR[abbr]
    if v.isdigit() and 1 <= int(v) <= 12:
        return int(v)
    return None


@dataclass
class HeaderClassification:
    row_index: int
    kind: str  # "year" | "quarter" | "month" | "label"
    coverage: float  # share of non-null cells matching the kind


def classify_row(row: list[str]) -> HeaderClassification | None:
    """Classify a header row by what kind of temporal label it holds."""
    cells = [_strip(c) for c in row]
    non_empty = [c for c in cells if c]
    if not non_empty:
        return None
    years = sum(1 for c in non_empty if _is_year(c))
    quarters = sum(1 for c in non_empty if _quarter_index(c) is not None)
    months = sum(1 for c in non_empty if _month_index(c) is not None)

    # Disambiguate bare-numeric rows: if all numeric cells are within 1-4,
    # prefer quarter over month (the 22 INDEC tables with `trimestre` crudo
    # land here). Month-only labels (`enero`, `febrero`...) are unambiguous.
    numeric_cells = [int(c) for c in non_empty if c.isdigit()]
    if numeric_cells and max(numeric_cells) <= 4 and quarters >= months:
        months = 0  # collapse to quarter

    total = len(non_empty)
    candidates = (
        ("year", years),
        ("month", months),
        ("quarter", quarters),
    )
    kind, count = max(candidates, key=lambda kv: kv[1])
    coverage = count / total
    if coverage < 0.5:
        return HeaderClassification(row_index=-1, kind="label", coverage=coverage)
    return HeaderClassification(row_index=-1, kind=kind, coverage=coverage)


def _forward_fill(values: list[str]) -> list[str]:
    out: list[str] = []
    last = ""
    for v in values:
        if v:
            last = v
            out.append(v)
        else:
            out.append(last)
    return out


def _compose_temporal(year: str, period: str, kind: str) -> str:
    """Compose 2003_T1 / 2024_01 / fall back to year alone."""
    if not year:
        return period
    if kind == "quarter":
        idx = _quarter_index(period)
        if idx is not None:
            return f"{year}_T{idx}"
        return f"{year}_{period}".strip("_")
    if kind == "month":
        idx = _month_index(period)
        if idx is not None:
            return f"{year}_{idx:02d}"
        return f"{year}_{period}".strip("_")
    return year


@dataclass
class ParsedHeaders:
    columns: list[str]
    year_row_idx: int | None
    period_row_idx: int | None
    period_kind: str | None  # 'quarter' | 'month' | None
    skipped_rows: int


class HierarchicalHeaderParser:
    """Parse multi-row INDEC-style headers into a flat column list."""

    def __init__(self, *, max_header_rows: int = 8) -> None:
        self._max_header_rows = max_header_rows

    def detect(self, df: pd.DataFrame) -> ParsedHeaders:
        """Inspect the first N rows of `df` and return the canonical columns.

        `df` must come from `pd.read_excel(..., header=None)` so that the
        original header rows are preserved as data.
        """
        n_rows = min(self._max_header_rows, len(df))
        classifications: list[HeaderClassification] = []
        for i in range(n_rows):
            row = df.iloc[i].tolist()
            cls = classify_row(row)
            if cls is None:
                continue
            cls.row_index = i
            classifications.append(cls)

        year_cls = next((c for c in classifications if c.kind == "year"), None)
        period_cls = next(
            (c for c in classifications if c.kind in {"quarter", "month"}), None
        )

        if year_cls is None and period_cls is None:
            # No temporal hierarchy detected — fall back to first non-empty row
            return self._flat_first_row(df)

        max_used = max(
            (c.row_index for c in (year_cls, period_cls) if c is not None), default=0
        )
        n_cols = df.shape[1]
        year_row = (
            _forward_fill([_strip(df.iat[year_cls.row_index, j]) for j in range(n_cols)])
            if year_cls is not None
            else [""] * n_cols
        )
        period_row = (
            [_strip(df.iat[period_cls.row_index, j]) for j in range(n_cols)]
            if period_cls is not None
            else [""] * n_cols
        )
        kind = period_cls.kind if period_cls is not None else "year"

        # Optional descriptive label row — first non-classified row above the data
        label_row: list[str] = [""] * n_cols
        for c in classifications:
            if c.kind == "label" and c.row_index < max_used:
                label_row = [_strip(df.iat[c.row_index, j]) for j in range(n_cols)]
                break

        cols: list[str] = []
        for j in range(n_cols):
            year = year_row[j]
            period = period_row[j]
            label = label_row[j]
            if year or period:
                composed = _compose_temporal(year, period, kind)
                cols.append(composed if not label else f"{label}__{composed}".strip("_"))
            elif label:
                cols.append(label)
            else:
                cols.append(f"col_{j}")
        cols = self._dedupe(cols)
        return ParsedHeaders(
            columns=cols,
            year_row_idx=year_cls.row_index if year_cls else None,
            period_row_idx=period_cls.row_index if period_cls else None,
            period_kind=period_cls.kind if period_cls else None,
            skipped_rows=max_used + 1,
        )

    def _flat_first_row(self, df: pd.DataFrame) -> ParsedHeaders:
        first_row = [
            _strip(df.iat[0, j]) if 0 < df.shape[0] else f"col_{j}" for j in range(df.shape[1])
        ]
        first_row = [v if v else f"col_{i}" for i, v in enumerate(first_row)]
        return ParsedHeaders(
            columns=self._dedupe(first_row),
            year_row_idx=None,
            period_row_idx=None,
            period_kind=None,
            skipped_rows=1,
        )

    @staticmethod
    def _dedupe(cols: list[str]) -> list[str]:
        seen: dict[str, int] = {}
        out: list[str] = []
        for c in cols:
            if c not in seen:
                seen[c] = 1
                out.append(c)
            else:
                seen[c] += 1
                out.append(f"{c}_{seen[c]}")
        return out


_default_parser = HierarchicalHeaderParser()


def parse_hierarchical_headers(df: pd.DataFrame) -> ParsedHeaders:
    """Convenience wrapper: detect headers and return the parsed result.

    Caller is responsible for slicing data rows: `df.iloc[result.skipped_rows:]`.
    """
    return _default_parser.detect(df)
