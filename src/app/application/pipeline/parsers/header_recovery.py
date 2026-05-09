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


def _looks_like_year_header_row(values: list[str]) -> bool:
    """True iff most numeric cells in the row look like years (4-digit, 1900-2100)
    AND the row has very few unique numeric values (because year headers are
    repeated across merged sub-columns).

    Concretely, INDEC/CABA tables emit rows like
    `["", "2003", "2003", "2003", "", "2004", "2004", "2004", ""]` between
    the title and the data — this looks numeric but it's actually a header
    level. The data-start detector has to skip past it.
    """
    numeric_vals = [v for v in values if _is_numeric_str(v)]
    if len(numeric_vals) < 3:
        return False
    year_count = 0
    for v in numeric_vals:
        try:
            n = int(v.strip().replace(",", ""))
            if 1900 <= n <= 2100:
                year_count += 1
        except (ValueError, AttributeError):
            continue
    if year_count < len(numeric_vals) * 0.7:
        return False
    # Year-headers repeat across merged columns; real data won't.
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
    merged-cell propagation) are explicitly skipped via
    `_looks_like_year_header_row`.

    Returns None if no row in the window crosses the threshold (e.g. a pure
    text-only sheet) — caller should leave the dataframe alone.
    """
    for i in range(min(max_search, len(df))):
        row = df.iloc[i]
        populated = [
            str(v).strip()
            for v in row.tolist()
            if pd.notna(v) and str(v).strip() != ""
        ]
        if not populated:
            continue
        # A row of nothing but repeated years (between title and data) is a
        # header level, not data — keep walking.
        if _looks_like_year_header_row(populated):
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
            re.sub(r"\s+", " ", str(v)).strip() if pd.notna(v) else ""
            for v in df.iloc[i].tolist()
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
