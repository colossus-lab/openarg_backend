"""Header recovery for tables where the parser landed with placeholder columns.

Extracted from `infrastructure/celery/tasks/indec_tasks.py` (Phase 1 of
specs/021-parser-hardening). This module's job is to take a dataframe whose
columns are garbage (`col_N`, `Unnamed:`, title-row-as-header) and try to
recover the real header from the first few data rows.

Used both at parse time (collector_tasks → call this after the default read)
and at repair time (repair_in_place orchestrator → run against an existing
DB table by reading row 0 and ALTER COLUMN RENAME).
"""

from __future__ import annotations

import re

import pandas as pd

from app.application.pipeline.parsers.column_normalization import (
    PG_NAME_LIMIT_BYTES,
    garbage_column_ratio,
    is_garbage_column,
    truncate_utf8_bytes,
)


def _is_numeric_str(v: str) -> bool:
    """Loose numeric check tolerant to thousand separators (`.` Argentine /
    `,` US), decimals, percent signs and leading +/-.

    Used to distinguish data rows (numeric backbone) from header rows (mostly
    text) when scanning for the data-start anchor.

    Strips ALL dots and replaces commas with dots — assumes Spanish/Argentine
    locale (`.` = thousand sep, `,` = decimal). For US-format numbers like
    `12345.67` the dots get removed (value parses as 1234567) but the row
    still counts as numeric, which is what the caller actually needs.
    """
    s = v.strip().replace("%", "").replace(".", "").replace(",", ".")
    s = s.lstrip("-+")
    if not s:
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False


_MONTHS_AR_FULL = {
    "ENERO",
    "FEBRERO",
    "MARZO",
    "ABRIL",
    "MAYO",
    "JUNIO",
    "JULIO",
    "AGOSTO",
    "SEPTIEMBRE",
    "OCTUBRE",
    "NOVIEMBRE",
    "DICIEMBRE",
}
_MONTHS_AR_SHORT = {
    "ENE",
    "FEB",
    "MAR",
    "ABR",
    "MAY",
    "JUN",
    "JUL",
    "AGO",
    "SEP",
    "OCT",
    "NOV",
    "DIC",
}


def _looks_like_month_header_row(values: list[str]) -> bool:
    """True iff the row is essentially a list of month-name labels.

    Real-world: `ciudad_mendoza__pauta_publicitaria` puts months as
    column sub-headers right under the title row:
      `["ENERO", "", "FEBRERO", "", "MARZO", "", ...]`
    Same shape as the year-axis pattern (year_header_row) but with
    Spanish month names. The detector should treat these rows as
    sub-headers, not data, so `find_data_start_row` keeps walking.
    """
    populated = [v for v in values if v]
    if len(populated) < 3:
        return False
    upper = [v.strip().upper() for v in populated]
    month_count = sum(1 for v in upper if v in _MONTHS_AR_FULL or v in _MONTHS_AR_SHORT)
    if month_count < 3:
        return False
    # >=70% of populated cells are month names → header, not data.
    return month_count >= len(populated) * 0.7


def _looks_like_year_header_row(values: list[str]) -> bool:
    """True iff the row is essentially a list of year-labels — either
    repeated across merged-cell sub-columns or a flat row of distinct years.

    Two shapes covered:

    1. Merged-cell sub-headers (INDEC / CABA cuadros) emit
       `["", "2003", "2003", "2003", "", "2004", "2004", "2004", ""]`
       between the title and the data. Many numeric cells, all years,
       repeated — distinct count low.

    2. Flat year-axis (caba__femicidios pattern) emits
       `["Año", "2023", "2022", "2021", ..., "2015"]` — all distinct
       years, one row of column headers. Distinct count equals
       numeric_count, but year_ratio over populated cells is ~100 %.

    Both look "numeric" to the data-start detector and need to be
    classified as headers, not data.
    """
    populated = [v for v in values if v]
    if len(populated) < 3:
        return False
    numeric_vals = [v for v in populated if _is_numeric_str(v)]
    if len(numeric_vals) < 3:
        return False
    year_count = 0
    for v in numeric_vals:
        try:
            raw = v.strip().replace(",", "")
            # pandas reads a year column as float, so a header row arrives as
            # `2018.0` and `int()` refuses it. Measured 2026-08-23: this is why
            # `['DEFUNCIONES MATERNAS', '2017', '2018.0', '2019.0']` was not
            # recognised while the same row without the decimals was. A whole
            # family of statistical tables turns on that `.0`.
            if raw.endswith(".0"):
                raw = raw[:-2]
            n = int(raw)
            if 1900 <= n <= 2100:
                year_count += 1
        except (ValueError, AttributeError):
            continue
    if year_count < len(numeric_vals) * 0.7:
        return False
    # Shape 2: row dominated by years end-to-end (year_count >= 80 % of
    # populated cells) — a flat year axis. Doesn't matter if distinct.
    if year_count >= len(populated) * 0.8:
        return True
    # Shape 1: years repeated across merged sub-columns (distinct count
    # very low relative to numeric count).
    distinct = len(set(numeric_vals))
    return distinct <= max(3, len(numeric_vals) // 4)


def find_data_start_row(
    df: pd.DataFrame,
    *,
    max_search: int = 8,
    numeric_threshold: float = 0.30,
) -> int | None:
    """Walk down rows, return the index of the first row that looks like data.

    Heuristic: a row is "data" if at least `numeric_threshold` of its
    populated cells parse as numbers (with a floor of 2 numeric cells, so a
    title row containing one stray year like "2024" doesn't trip the
    detector). Year-only header rows (lots of repeated 4-digit values from
    merged-cell propagation, or a flat row of distinct years) are explicitly
    skipped via `_looks_like_year_header_row`.

    Metadata columns whose name starts with `_` (collector-injected:
    `_source_dataset_id`, `_source_url`, `_parser_version`, ...) are
    excluded from the row-shape analysis. Their values reflect ingest
    bookkeeping, not the dataset's natural structure, and would dilute the
    ratios that drive year-header detection.

    Returns None if no row in the window crosses the threshold (e.g. a pure
    text-only sheet) — caller should leave the dataframe alone.
    """
    data_col_idx = [idx for idx, c in enumerate(df.columns) if not str(c).startswith("_")]
    if not data_col_idx:
        return None
    for i in range(min(max_search, len(df))):
        row = df.iloc[i]
        populated = [
            str(row.iloc[idx]).strip()
            for idx in data_col_idx
            if pd.notna(row.iloc[idx]) and str(row.iloc[idx]).strip() != ""
        ]
        if not populated:
            continue
        if _looks_like_year_header_row(populated):
            continue
        if _looks_like_month_header_row(populated):
            continue
        numeric_count = sum(1 for v in populated if _is_numeric_str(v))
        if numeric_count >= max(2, int(len(populated) * numeric_threshold)):
            return i
    return None


def _forward_fill(values: list[str]) -> list[str]:
    """Fill blank cells in `values` with the most recent non-blank to the
    left.

    Excel publishes merged cells as a value in the leftmost cell and blanks
    in the rest. Forward-filling reconstructs the parent label across all
    children of the merge.
    """
    out: list[str] = []
    last = ""
    for v in values:
        if v:
            last = v
            out.append(v)
        else:
            out.append(last)
    return out


def promote_buried_headers(
    df: pd.DataFrame,
    *,
    garbage_threshold: float = 0.50,
    max_search: int = 8,
    name_byte_limit: int = 120,
) -> pd.DataFrame:
    """Recover a multi-row header buried inside the first data rows.

    Some sheets open with a TITLE row (e.g. "Cuadro 3.1 Población por
    condición de pobreza...") that pandas mistakes for the header row,
    leaving every other column with a placeholder name (`Unnamed:N` →
    renamed to `col_N` in the legacy fallback path). The actual column
    labels are spread across the next 1-3 rows of the dataframe, laid out
    with merged cells.

    Detection: the fraction of garbage column names (`col_N`, `Unnamed:`,
    `Cuadro X.Y`, URL) is at least `garbage_threshold`.

    Recovery:
      1. Walk down rows until the first numeric-dominant row → data starts there.
      2. Forward-fill blank cells horizontally in each header row above.
      3. Compose the final column name as `parent1_parent2_...` joined
         with `_`, deduping repeats.
      4. Drop the consumed header rows from the data.

    Idempotent: cleaner-than-threshold input passes through unchanged.

    `name_byte_limit` bounds composite names *before* the
    `dedupe_column_names` pass that brings them to Postgres's 63-byte limit.
    Keeping it loose here (120) lets dedupe see distinct prefixes even when
    titles repeat.
    """
    cols = [str(c) for c in df.columns]
    if not cols:
        return df
    if garbage_column_ratio(cols) < garbage_threshold:
        return df
    if len(df) < 2:
        return df

    data_start = find_data_start_row(df, max_search=max_search)
    if data_start is None or data_start < 1:
        return df

    # Build composite headers from rows [0, data_start). The header stack is
    # taken from data rows above the anchor — those rows hold the buried
    # header.
    #
    # Original `df.columns` is forward-filled horizontally so that a valid
    # parent label like `Total Provincia` propagates through the
    # `col_2, col_3, col_4` garbage that came from merged-cell sub-columns.
    # Merged cells in Excel typically produce ONE valid name (the
    # leftmost child of the merge) followed by N placeholders — without
    # the forward-fill we'd lose the parent label for those children.
    n_cols = len(cols)
    filled_originals: list[str] = []
    last_valid = ""
    for c in cols:
        cleaned = re.sub(r"\s+", " ", str(c)).strip()
        if not is_garbage_column(cleaned):
            last_valid = cleaned
        filled_originals.append(last_valid)

    header_stack: list[list[str]] = []
    for i in range(data_start):
        row_vals = [
            re.sub(r"\s+", " ", str(v)).strip() if pd.notna(v) else "" for v in df.iloc[i].tolist()
        ]
        header_stack.append(_forward_fill(row_vals))

    new_cols: list[str] = []
    for col_idx in range(n_cols):
        original_clean = re.sub(r"\s+", " ", str(cols[col_idx])).strip()
        # Metadata cols (collector convention `_source_*`, `_parser_*`,
        # `_collector_*`, `_ingest_*`) are injected per-row at ingest time,
        # not part of the source schema. They are by definition correct
        # and must NOT be composed with row-0 data (that data is the
        # metadata's first value, not a sub-header).
        if original_clean.startswith("_"):
            new_cols.append(truncate_utf8_bytes(original_clean, name_byte_limit))
            continue
        parts: list[str] = []
        # Forward-filled original (if anything was valid), then buried header rows.
        if filled_originals[col_idx]:
            parts.append(filled_originals[col_idx])
        for row in header_stack:
            if col_idx < len(row):
                v = row[col_idx]
                if v and v not in parts:
                    parts.append(v)
        if not parts:
            parts.append(f"col_{col_idx}")
        composed = "_".join(parts)
        new_cols.append(truncate_utf8_bytes(composed, name_byte_limit))

    df_recovered = df.iloc[data_start:].reset_index(drop=True).copy()
    df_recovered.columns = new_cols
    return df_recovered


__all__ = [
    "PG_NAME_LIMIT_BYTES",
    "find_data_start_row",
    "promote_buried_headers",
]
