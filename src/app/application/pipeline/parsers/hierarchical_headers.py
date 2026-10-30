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

import math
import re
import unicodedata
from dataclasses import dataclass, field

import pandas as pd

PARSER_VERSION = "1"
NORMALIZATION_VERSION = "1"

# `xlrd` hands numeric cells back as floats, so a year typed as a number reads
# `2016.0`; INDEC also writes `Año 2016` and hangs notes off it, `2016 (1)` or
# `2016*`. All of them are the year 2016.
_YEAR_CELL_RE = re.compile(r"^(?:año|ano)?\s*((?:19|20)\d{2})(?:\.0+)?$")
# Footnote markers that trail a header cell: `(1)`, `(2)`, `*`, `**`.
_NOTE_RE = re.compile(r"(?:\s*\(\d+\)|\s*\*+)+$")
# `1° trimestre`, `1º trimestre` (NFKC turns `º` into `o`), `1er trimestre`,
# `2do trim.`, `T3`, `Q4`, bare `4`.
_QUARTER_NUM_RE = re.compile(
    r"^(?:t|q)?([1-4])\s*(?:°|o|er|ero|do|ro|to|\.)?\s*(?:trim(?:estre)?\.?)?$", re.IGNORECASE
)
_QUARTER_WORDS: dict[str, int] = {"primer": 1, "segundo": 2, "tercer": 3, "cuarto": 4}
_QUARTER_WORD_RE = re.compile(r"^(primer|segundo|tercer|cuarto)o?\s+trim(?:estre)?\.?$")
_DATE_CELL_RE = re.compile(r"^(?:19|20)\d{2}-(0[1-9]|1[0-2])-\d{2}(?:[ t]00:00:00)?$")
# A header cell this long with nothing beside it is the sheet's title.
_TITLE_MIN_CHARS = 40
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


def _without_notes(value: str) -> str:
    return _NOTE_RE.sub("", value.strip()).strip()


def _year_of(value: str) -> str | None:
    """The year a header cell names, as four digits, or None."""
    m = _YEAR_CELL_RE.match(_without_notes(value).lower())
    return m.group(1) if m else None


def _is_year(value: str) -> bool:
    return _year_of(value) is not None


def _quarter_index(value: str) -> int | None:
    v = _without_notes(value).lower()
    if not v:
        return None
    m = _QUARTER_NUM_RE.match(v)
    if m:
        return int(m.group(1))
    m = _QUARTER_WORD_RE.match(v)
    if m:
        return _QUARTER_WORDS[m.group(1)]
    return None


def _month_index(value: str) -> int | None:
    v = _without_notes(value).lower()
    if not v:
        return None
    m = _DATE_CELL_RE.match(v)
    if m:
        # A month header typed as a date (`2019-03-01 00:00:00`): the CVS
        # sheets mix them with month names.
        return int(m.group(1))
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
    if kind == "year" and not _years_in_order(non_empty):
        # Now that `2016.0` counts, a data row of values between 1900 and 2099
        # could pass for a year axis. A header lists its years in order; data
        # almost never does.
        return HeaderClassification(row_index=-1, kind="label", coverage=coverage)
    return HeaderClassification(row_index=-1, kind=kind, coverage=coverage)


def _years_in_order(cells: list[str]) -> bool:
    """Ascending or descending — once, or as a block repeated per group
    (`2024 2025 | 2024 2025` under "Exportaciones" / "Importaciones")."""
    years = [int(y) for y in (_year_of(c) for c in cells) if y]

    def monotonic(seq: list[int]) -> bool:
        pairs = list(zip(seq, seq[1:]))
        return all(a <= b for a, b in pairs) or all(a >= b for a, b in pairs)

    for size in range(len(years), 1, -1):
        if len(years) % size == 0 and years == years[:size] * (len(years) // size):
            if monotonic(years[:size]):
                return True
    return monotonic(years)


def _number(value: str) -> float | None:
    try:
        return float(value.replace(",", "."))
    except ValueError:
        return None


def _has_fraction(row: list) -> bool:
    for v in row:
        n = _number(_strip(v))
        if n is not None and math.isfinite(n) and not n.is_integer():
            return True
    return False


def _looks_like_data(row: list) -> bool:
    """A row of figures: where the header ends, whatever comes after."""
    cells = [c for c in (_strip(v) for v in row) if c]
    numeric = sum(1 for c in cells if _number(c) is not None)
    return numeric >= 3 and numeric >= len(cells) / 2


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
    # Spacer columns inside the period block: no header of any kind, sitting
    # between one year's periods and the next. INDEC leaves one after every
    # year; the caller drops them instead of storing a `col_N`.
    separator_columns: list[int] = field(default_factory=list)


class HierarchicalHeaderParser:
    """Parse multi-row INDEC-style headers into a flat column list."""

    # Twenty, not eight: a sheet with a long preamble (title, subtitle, unit,
    # notes) puts its year row lower than the old window looked. Scanning stops
    # at the first row of figures anyway, so the wider window cannot reach
    # into the data.
    def __init__(self, *, max_header_rows: int = 20) -> None:
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
            if _has_fraction(row):
                # No header cell is `1.8`. A row like `2017-02-01 | 1.8 | 3`
                # would otherwise pass for a month row (a date and a `3`).
                break
            cls = classify_row(row)
            if cls is None:
                continue
            if cls.kind == "label" and _looks_like_data(row):
                break
            cls.row_index = i
            classifications.append(cls)

        period_cls = next((c for c in classifications if c.kind in {"quarter", "month"}), None)
        years = [c for c in classifications if c.kind == "year"]
        if period_cls is not None:
            # The year row that heads the periods is the nearest one above them.
            above = [c for c in years if c.row_index < period_cls.row_index]
            year_cls = above[-1] if above else None
        else:
            year_cls = years[0] if years else None

        if year_cls is None and period_cls is None:
            # No temporal hierarchy detected — fall back to first non-empty row
            return self._flat_first_row(df)

        temporal_rows = [c.row_index for c in (year_cls, period_cls) if c is not None]
        n_cols = df.shape[1]

        def cells(row_index: int | None) -> list[str]:
            if row_index is None:
                return [""] * n_cols
            return [_strip(df.iat[row_index, j]) for j in range(n_cols)]

        year_cells = cells(year_cls.row_index if year_cls else None)
        raw_years = [_year_of(c) or "" for c in year_cells]
        # Text in the year row that is not a year names its own column only;
        # forward-filling it would date the next block with it.
        year_row = _forward_fill(raw_years)
        year_text = [c if not y else "" for c, y in zip(year_cells, raw_years)]
        period_row = cells(period_cls.row_index if period_cls else None)
        kind = period_cls.kind if period_cls is not None else "year"

        def period_of(value: str) -> int | None:
            if kind == "quarter":
                return _quarter_index(value)
            if kind == "month":
                return _month_index(value)
            return None

        if period_cls is None:
            # Years alone: a merged year cell dates every column under it.
            temporal = [bool(y) for y in year_row]
        else:
            temporal = [
                bool(raw_years[j]) or period_of(period_row[j]) is not None for j in range(n_cols)
            ]
        if not any(temporal):
            return self._flat_first_row(df)
        first_t = temporal.index(True)
        last_t = n_cols - 1 - temporal[::-1].index(True)

        label_row = cells(self._label_row(df, classifications, min(temporal_rows)))
        # A label spanning the whole period block ("Total 31 aglomerados
        # urbanos") is a caption. Only when it splits the block into groups
        # ("Exportaciones" / "Importaciones") does it belong in the names, and
        # then it covers every column of its group, not just the first.
        group_labels = {label_row[j] for j in range(first_t, last_t + 1) if label_row[j]}
        grouped = len(group_labels) > 1
        group_row = _forward_fill(
            [label_row[j] if first_t <= j <= last_t else "" for j in range(n_cols)]
        )

        cols: list[str] = []
        separators: list[int] = []
        for j in range(n_cols):
            year = year_row[j]
            period = period_row[j]
            if temporal[j]:
                composed = _compose_temporal(year, period, kind)
                cols.append(f"{group_row[j]}__{composed}" if grouped else composed)
            elif year and period:
                cols.append(f"{year}_{period}")
            elif period or year_text[j] or label_row[j]:
                cols.append(period or year_text[j] or label_row[j])
            elif period_cls is not None and first_t < j < last_t:
                # Under a period row every column of data has a period. One
                # with no header at all, inside the block, is the spacer.
                separators.append(j)
                cols.append(f"col_{j}")
            elif j < first_t and not any(label_row[:first_t]):
                # The concept column of a sheet whose header names only the
                # periods (Cuadro 1.2 of the EPH).
                cols.append("concepto")
            else:
                cols.append(f"col_{j}")
        cols = self._dedupe(cols)
        return ParsedHeaders(
            columns=cols,
            year_row_idx=year_cls.row_index if year_cls else None,
            period_row_idx=period_cls.row_index if period_cls else None,
            period_kind=period_cls.kind if period_cls else None,
            skipped_rows=max(temporal_rows) + 1,
            separator_columns=separators,
        )

    @staticmethod
    def _label_row(
        df: pd.DataFrame, classifications: list[HeaderClassification], first_temporal: int
    ) -> int | None:
        """The descriptive row nearest above the year/period rows, if any.

        Nearest, not first: the first is usually the sheet's title, which used
        to end up as the name of the concept column.
        """
        for c in reversed(classifications):
            if c.kind != "label" or c.row_index >= first_temporal:
                continue
            filled = [v for v in (_strip(x) for x in df.iloc[c.row_index].tolist()) if v]
            if len(filled) == 1 and len(filled[0]) >= _TITLE_MIN_CHARS:
                continue
            return c.row_index
        return None

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
