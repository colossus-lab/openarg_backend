"""PDF table extraction (Phase 3 of specs/021-parser-hardening).

Uses `pdfplumber` (pure Python, no Java) to extract tabular content from
PDFs. Targets the data-publishing pattern where the same dataset gets
published as a PDF instead of CSV/Excel — common at PAMI, datos.gob.ar,
and various provincial open-data portals.

The 132 staging tables with `http://...` as column names are the symptom:
the previous code path routed PDFs to a generic pandas fallback that
scraped `<a href>` links and used URL strings as headers.

Strategy:
  - Open with pdfplumber.
  - For each page, run `extract_tables()` (default settings = good for
    bordered tables; cell-merging is auto-detected).
  - Keep only tables that have at least 2 columns AND ≥1 row of content.
  - If consecutive pages produce tables with identical column counts and
    matching headers, concatenate them (multi-page tables are very common).
  - Return a list of DataFrames; the caller decides whether to ingest one
    big concatenated frame or N child resources (multi-file expander).
"""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Soft-dependency: pdfplumber is optional at import time so the rest of the
# parser package keeps loading even on hosts without the dep installed.
# The first call raises a clear error.
try:
    import pdfplumber

    _HAS_PDFPLUMBER = True
except ImportError:
    pdfplumber = None  # type: ignore[assignment]
    _HAS_PDFPLUMBER = False


_MIN_COLUMNS = 2
_MIN_ROWS = 1
_MAX_TABLES_PER_PDF = 100  # safety cap; very large PDFs risk OOM


class PdfParserError(RuntimeError):
    """Raised when the PDF can't be opened or has no extractable tables."""


def _normalize_cell(value: Any) -> str:
    """Trim, collapse whitespace, and replace pdfplumber's None with empty."""
    if value is None:
        return ""
    s = str(value).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _table_to_dataframe(
    raw_table: list[list[str | None]], *, header_row_index: int = 0
) -> pd.DataFrame | None:
    """Convert a pdfplumber table (list of rows) into a DataFrame.

    Treats the first row as the header. Returns None if the table fails
    minimum quality bars (≥ `_MIN_COLUMNS` cols and ≥ `_MIN_ROWS` data
    rows).
    """
    if not raw_table or len(raw_table) <= header_row_index:
        return None
    cleaned = [[_normalize_cell(c) for c in row] for row in raw_table]
    # Pad short rows so columns align.
    max_cols = max(len(r) for r in cleaned) if cleaned else 0
    if max_cols < _MIN_COLUMNS:
        return None
    cleaned = [r + [""] * (max_cols - len(r)) for r in cleaned]

    headers = cleaned[header_row_index]
    # Replace empty headers with col_N placeholder; downstream
    # `promote_buried_headers` can recover them if needed.
    headers = [h if h else f"col_{idx}" for idx, h in enumerate(headers)]
    body = cleaned[header_row_index + 1 :]
    if len(body) < _MIN_ROWS:
        return None
    df = pd.DataFrame(body, columns=headers)
    # Drop rows that are entirely empty (table-rule artifacts).
    df = df[~df.apply(lambda row: all(_normalize_cell(c) == "" for c in row), axis=1)]
    if len(df) < _MIN_ROWS:
        return None
    return df.reset_index(drop=True)


def extract_tables_from_pdf(path: str) -> list[pd.DataFrame]:
    """Return all extractable tables in `path` as DataFrames.

    Empty list if the PDF has no detectable tables (text-only or graphics-only
    document). Raises `PdfParserError` if the PDF can't be opened.
    """
    if not _HAS_PDFPLUMBER:
        raise PdfParserError("pdfplumber not installed — add to dependencies before using")
    tables: list[pd.DataFrame] = []
    try:
        with pdfplumber.open(path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                if len(tables) >= _MAX_TABLES_PER_PDF:
                    logger.warning(
                        "PDF %s reached cap of %d tables, stopping early",
                        path,
                        _MAX_TABLES_PER_PDF,
                    )
                    break
                try:
                    raw_tables = page.extract_tables()
                except Exception:
                    logger.warning(
                        "extract_tables failed on page %d of %s; skipping",
                        page_num,
                        path,
                        exc_info=True,
                    )
                    continue
                for raw in raw_tables:
                    df = _table_to_dataframe(raw)
                    if df is not None:
                        tables.append(df)
    except Exception as exc:
        raise PdfParserError(f"failed to open {path}: {exc}") from exc
    return tables


def merge_consecutive_tables(tables: list[pd.DataFrame]) -> list[pd.DataFrame]:
    """Concatenate tables that look like continuations (same columns).

    Two tables are continuations when they have the same number of columns
    AND the same column names (after normalization). The first table's
    header wins; subsequent tables contribute only their data rows.

    This handles the common multi-page table where each page is reported
    by `extract_tables` separately but they're semantically the same table.
    """
    if not tables:
        return []
    out: list[pd.DataFrame] = [tables[0].copy()]
    for nxt in tables[1:]:
        prev = out[-1]
        prev_cols = [str(c) for c in prev.columns]
        nxt_cols = [str(c) for c in nxt.columns]
        if len(prev_cols) == len(nxt_cols) and prev_cols == nxt_cols:
            # Continuation: concat with prev, drop the duplicated header
            # row if it was carried into nxt's first row.
            data = nxt
            if len(data) > 0:
                first_row = [_normalize_cell(v) for v in data.iloc[0].tolist()]
                if first_row == prev_cols:
                    data = data.iloc[1:].reset_index(drop=True)
            out[-1] = pd.concat([prev, data], ignore_index=True)
        else:
            out.append(nxt.copy())
    return out


def parse_pdf_file(path: str) -> pd.DataFrame | None:
    """Top-level entry point: parse a PDF, returning a single DataFrame.

    Strategy:
      1. Extract all tables.
      2. Merge consecutive same-shape tables (multi-page continuations).
      3. If exactly one table remains, return it.
      4. If multiple distinct tables remain, return the largest (by rows).
         (The collector's multi-file expander would emit each as a child;
         this single-DF return is for the legacy single-table ingest path.)
      5. Return None if no tables were extractable.

    The single-DataFrame contract matches `_read_excel_frame` and
    `_read_csv_preview` so the wire-up at the call site is symmetric.
    """
    tables = extract_tables_from_pdf(path)
    if not tables:
        return None
    merged = merge_consecutive_tables(tables)
    if not merged:
        return None
    if len(merged) == 1:
        return merged[0]
    # Pick the largest by row count.
    merged.sort(key=len, reverse=True)
    logger.info(
        "PDF %s yielded %d distinct tables, returning the largest (%d rows)",
        path,
        len(merged),
        len(merged[0]),
    )
    return merged[0]


__all__ = [
    "PdfParserError",
    "extract_tables_from_pdf",
    "merge_consecutive_tables",
    "parse_pdf_file",
]
