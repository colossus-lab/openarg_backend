"""Column-name normalization primitives shared across collector parsers.

Extracted from `infrastructure/celery/tasks/indec_tasks.py` (Phase 1 of
specs/021-parser-hardening). The goal is to reuse these from the generic
collector path (`collector_tasks.py`) and from any future parser, instead of
duplicating the logic.

Two responsibilities:

1. Byte-aware identifier dedup. Postgres caps identifiers at 63 BYTES (not
   chars). Two distinct unicode names whose first 63 bytes coincide collide
   silently at CREATE TABLE time. `dedupe_column_names` truncates first, then
   dedupes in the same byte space Postgres uses.

2. Recognising "garbage" column names produced by upstream parser failures —
   `col_N` placeholders, URL-as-column from PDFs, title-rows-as-header. Used by
   higher-level recovery passes (header_recovery, repair_in_place) to decide
   when to apply a recovery strategy.
"""

from __future__ import annotations

import re

# Postgres identifier byte limit (NAMEDATALEN - 1). Hard-coded because the
# server's NAMEDATALEN is the default 64 — if that ever changes, override
# at the call site, don't bump this constant.
PG_NAME_LIMIT_BYTES = 63

# Regexes for known garbage patterns from upstream parsers.
_PLACEHOLDER_COL_RE = re.compile(r"^col_\d+$")
_UNNAMED_COL_RE = re.compile(r"^unnamed:\s*\d+$", re.IGNORECASE)
_TITLE_COL_RE = re.compile(r"^cuadro\s", re.IGNORECASE)
_URL_COL_RE = re.compile(r"^https?://", re.IGNORECASE)
_PAGINAS_COL_RE = re.compile(r"^p[áa]ginas?:", re.IGNORECASE)


def truncate_utf8_bytes(s: str, byte_limit: int = PG_NAME_LIMIT_BYTES) -> str:
    """Truncate `s` so its UTF-8 encoding fits in `byte_limit`.

    Postgres caps identifiers at 63 bytes, NOT chars. A column name like
    `"Cuadro 3.1. Población..."` is 70+ bytes once accents (`á`, `ó`) expand
    to 2 bytes each — Postgres silently truncates it on CREATE TABLE, and two
    such cols truncated to the same byte prefix cause `DuplicateColumnError`
    even when the Python strings differ.
    """
    encoded = s.encode("utf-8")
    if len(encoded) <= byte_limit:
        return s
    return encoded[:byte_limit].decode("utf-8", errors="ignore")


def dedupe_column_names(
    cols: list[str], *, byte_limit: int = PG_NAME_LIMIT_BYTES
) -> list[str]:
    """Truncate each name to `byte_limit` AND dedupe collisions in one pass.

    Why both at once: two long names that differ in their tail share their
    first 63 bytes after truncation. Detecting the collision in Python's
    char space misses them because Python sees them as distinct. The
    collision check has to happen in the same byte space Postgres uses.

    First occurrence keeps its truncated name; subsequent occurrences get
    `_2`, `_3`, ... — the suffix is reserved within the byte budget so the
    final names still fit.
    """
    seen: dict[str, int] = {}
    out: list[str] = []
    for raw in cols:
        name = truncate_utf8_bytes(str(raw), byte_limit)
        if name not in seen:
            seen[name] = 1
            out.append(name)
        else:
            seen[name] += 1
            suffix = f"_{seen[name]}"
            base = truncate_utf8_bytes(name, byte_limit - len(suffix))
            candidate = base + suffix
            # Edge: even with suffix, the truncated base might collide with
            # an earlier different value. Walk forward until unique.
            while candidate in seen:
                seen[name] += 1
                suffix = f"_{seen[name]}"
                base = truncate_utf8_bytes(name, byte_limit - len(suffix))
                candidate = base + suffix
            seen[candidate] = 1
            out.append(candidate)
    return out


def is_placeholder_column(name: str) -> bool:
    """True iff the column name was synthesised by a parser fallback (`col_N`,
    `Unnamed:N`).

    These signal that header detection failed and the row of real names is
    somewhere else in the dataframe.
    """
    s = str(name).strip()
    return bool(_PLACEHOLDER_COL_RE.match(s) or _UNNAMED_COL_RE.match(s))


def is_title_row_column(name: str) -> bool:
    """True iff the column name looks like a title row promoted by mistake
    (e.g. `"Cuadro 3.1 Población..."`).

    INDEC and other Excel-publishing portals open sheets with a section title
    in row 0; pandas' default header=0 then captures it.
    """
    return bool(_TITLE_COL_RE.match(str(name).strip()))


def is_url_column(name: str) -> bool:
    """True iff the column name is a URL (PDF parser fallback artifact).

    Happens when a PDF gets routed to the generic table fallback that scrapes
    `<a href>` links and uses the link text/URL as headers.
    """
    return bool(_URL_COL_RE.match(str(name).strip()))


def is_garbage_column(name: str) -> bool:
    """Convenience: any of the three garbage patterns above plus `Páginas:_N`.

    Used by repair scripts to count affected tables and decide whether to
    trigger header recovery.
    """
    s = str(name).strip()
    return bool(
        _PLACEHOLDER_COL_RE.match(s)
        or _UNNAMED_COL_RE.match(s)
        or _TITLE_COL_RE.match(s)
        or _URL_COL_RE.match(s)
        or _PAGINAS_COL_RE.match(s)
    )


def garbage_column_ratio(cols: list[str]) -> float:
    """Fraction of `cols` that look garbage. 0.0 = clean, 1.0 = all garbage.

    Threshold callers typically use:
      ≥ 0.40 → trigger header recovery
      ≥ 0.20 → log + flag for review (don't auto-recover yet)
    """
    if not cols:
        return 0.0
    return sum(1 for c in cols if is_garbage_column(c)) / len(cols)
