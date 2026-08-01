"""Repair-in-place for tables that landed with placeholder columns.

The collector path was producing tables with `col_0, col_1, col_2, ...`
when pandas mistook a TITLE row for the header. After the collector fix
(specs/021-parser-hardening Phase 2), new ingests are clean — but ~500
already-landed tables still carry the bug.

This module provides the inverse: read the first few rows of an existing
table, run the same `promote_buried_headers` logic against them, and emit
DDL (`ALTER TABLE RENAME COLUMN`) + DML (`DELETE FROM` to drop the consumed
header row) to fix the table in place.

Every operation is recorded in `parse_repair_audit` so a bad rename can be
reverted by reading the inverse from the audit row.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.application.pipeline.parsers import (
    dedupe_column_names,
    garbage_column_ratio,
    is_garbage_column,
    promote_buried_headers,
)

logger = logging.getLogger(__name__)


@dataclass
class RepairOutcome:
    """Result of a single table repair attempt.

    `ok=False` with `reason` set means we declined to repair (no change to
    the table). `ok=False` with `error_message` set means we tried and
    failed (audit row written with the error).
    """

    table_schema: str
    table_name: str
    ok: bool
    reason: str = ""
    old_columns: list[str] = field(default_factory=list)
    new_columns: list[str] = field(default_factory=list)
    rows_deleted: int = 0
    error_message: str = ""
    dry_run: bool = False

    def as_log_dict(self) -> dict[str, Any]:
        return {
            "table": f"{self.table_schema}.{self.table_name}",
            "ok": self.ok,
            "reason": self.reason or self.error_message,
            "old_cols": len(self.old_columns),
            "new_cols": len(self.new_columns),
            "rows_deleted": self.rows_deleted,
            "dry_run": self.dry_run,
        }


def list_col_n_candidates(
    engine: Engine,
    *,
    schemas: tuple[str, ...] = ("raw", "public"),
    limit: int = 100,
    min_garbage_ratio: float = 0.40,
) -> list[tuple[str, str, list[str]]]:
    """Return tables whose `garbage_column_ratio` is at least `min_garbage_ratio`.

    Returns a list of `(schema, table_name, columns)`, ordered by ratio
    descending so worst offenders surface first. The actual ratio is
    computed in Python (against the same primitives the parser uses) — the
    SQL just narrows the candidate set.

    Implementation: two-query approach because psycopg3 returns
    `ARRAY_AGG(text)` as a Postgres-array-literal string by default, not a
    Python list. We first list the tables, then fetch columns per table.
    """
    list_sql = """
        SELECT table_schema, table_name
          FROM information_schema.columns
         WHERE table_schema = ANY(:schemas)
         GROUP BY table_schema, table_name
        HAVING SUM(CASE
                   WHEN column_name ~ '^col_[0-9]+$' THEN 1
                   WHEN column_name ~* '^unnamed:' THEN 1
                   ELSE 0
                 END) >= 1
        ORDER BY 1, 2
    """
    cols_sql = """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = :s AND table_name = :t
         ORDER BY ordinal_position
    """
    out: list[tuple[str, str, list[str], float]] = []
    with engine.connect() as conn:
        candidate_rows = conn.execute(text(list_sql), {"schemas": list(schemas)}).fetchall()
        for sch, tname in candidate_rows:
            col_rows = conn.execute(text(cols_sql), {"s": sch, "t": tname}).fetchall()
            cols = [r[0] for r in col_rows]
            ratio = garbage_column_ratio(cols)
            if ratio >= min_garbage_ratio:
                out.append((sch, tname, cols, ratio))
    out.sort(key=lambda x: x[3], reverse=True)
    return [(s, t, cols) for s, t, cols, _ in out[:limit]]


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier safely (Postgres-style double quotes, escape
    embedded quotes by doubling).

    All caller paths read identifiers from `information_schema` so they're
    already trusted, but defence-in-depth: never interpolate raw."""
    return '"' + name.replace('"', '""') + '"'


def _audit(
    engine: Engine,
    *,
    run_id: uuid.UUID,
    outcome: RepairOutcome,
    operation: str,
    phase: str = "col_n",
) -> None:
    """Write one row to `parse_repair_audit`. Best-effort: never raises so
    audit failures don't block repair runs."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO parse_repair_audit
                        (run_id, phase, table_schema, table_name, operation,
                         old_columns, new_columns, rows_deleted, ok,
                         error_message, dry_run)
                    VALUES
                        (CAST(:run_id AS uuid), :phase, :schema, :tname, :operation,
                         CAST(:old_cols AS jsonb), CAST(:new_cols AS jsonb),
                         :rows_deleted, :ok, :err, :dry)
                    """
                ),
                {
                    "run_id": str(run_id),
                    "phase": phase,
                    "schema": outcome.table_schema,
                    "tname": outcome.table_name,
                    "operation": operation,
                    "old_cols": json.dumps(outcome.old_columns),
                    "new_cols": json.dumps(outcome.new_columns),
                    "rows_deleted": outcome.rows_deleted,
                    "ok": outcome.ok,
                    "err": outcome.error_message or None,
                    "dry": outcome.dry_run,
                },
            )
    except Exception:
        logger.warning("parse_repair_audit insert failed (non-fatal)", exc_info=True)


# Characters that legitimately appear in Argentine public-data column names.
# Anything outside this set is almost always a decoding artefact: the header row
# the repair recovers came out of the same badly-decoded file as the names it is
# replacing, so a "recovered" name can be worse than what it replaces.
# Observed on staging 2026-07-31: `Periodo: Ańo 2003`, `AÒo`, `Utilizaci—n`,
# `socioeconůmicos`, `C¾rdoba`, `b·sic`, `1¤ Sem.` — each one an accented vowel
# that arrived through the wrong codec.
_SPANISH_OK = set(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "áéíóúüñÁÉÍÓÚÜÑ"
    " \t.,;:()[]{}/%-_+°ºª'\"$#&@!¡?¿<>=*|\\~^\n"
)

# `Periodo: Ańo 2003`, `_2`, `_3` … ×148. A proposal where most names share one
# base is the defect it was meant to fix, one row further down.
_MAX_REPEATED_BASE_SHARE = 0.50

# Consuming header rows is irreversible — the audit log stores column names, not
# deleted rows. `caba__fecundidad` has 9 rows and the proposal ate 2 of them
# (22%), one of which was the TOTAL data row. On a 100k-row table the same 2
# rows are free; the cost only matters relative to what is left.
_MAX_HEADER_ROW_SHARE = 0.15

# Above this many rows the header-row cost is irrelevant, so the probe stops
# counting. Keeps the check off the critical path of a 52-million-row relation.
_ROW_COUNT_PROBE_CAP = 1000

_SUFFIX_RE = re.compile(r"^(?P<base>.*?)(?:_\d+)?$")


# Compare a bounded prefix, not the whole base. Postgres truncates identifiers
# at 63 bytes, so appending `_2` pushes the cut one character earlier and two
# copies of the same title stop matching exactly:
#     'Provincia de Córdoba … Proyecciones de Pob'
#     'Provincia de Córdoba … Proyecciones de P_2'
# That pair slipped the check and would have been applied. Any prefix shorter
# than the truncation point sees them as identical, while names that genuinely
# differ (`1998_Total` vs `1999_Total`, `Año_2023_Sexo_Mujer` vs `…_Varón`)
# diverge in their first few characters.
_BASE_COMPARISON_PREFIX = 40


def _repeated_base_share(cols: list[str]) -> float:
    """Largest share of names collapsing to the same base once `_N` is stripped."""
    bases = [
        (_SUFFIX_RE.match(str(c) or "").group("base") or "")
        .strip()
        .lower()[:_BASE_COMPARISON_PREFIX]
        for c in cols
    ]
    bases = [b for b in bases if b]
    if not bases:
        return 0.0
    return max(bases.count(b) for b in set(bases)) / len(bases)


def _has_decoding_artefacts(cols: list[str]) -> bool:
    """True when an unexpected character sits where a letter should be.

    Position is what separates the two cases, not the character. An em dash is
    ordinary punctuation in `Cuadro 3.1 Población — Total`, and a mis-decoded
    `ó` in `Utilizaci—n`. Same codepoint, and a flat blocklist has to be wrong
    about one of them. Mojibake lands *inside* a word, glued to the letters
    around it; punctuation is separated by whitespace or ends the string.
    """
    for name in cols:
        text_value = str(name or "")
        for i, ch in enumerate(text_value):
            if ch in _SPANISH_OK:
                continue
            before = text_value[i - 1] if i else ""
            after = text_value[i + 1] if i + 1 < len(text_value) else ""
            if before.isalnum() or after.isalnum():
                return True
    return False


def assess_rename_proposal(
    proposed: list[str],
    *,
    rows_to_delete: int,
    total_rows: int | None,
) -> str | None:
    """Reject a proposal that is not actually an improvement. None means accept.

    `propose_col_n_rename` already refuses when the garbage ratio does not drop,
    which catches `col_N` surviving the rename. It does not catch a rename into
    names that are *differently* useless, and on staging that was 3 of every 5
    viable proposals. Each check below is one observed failure, not a guess.
    """
    if _repeated_base_share(proposed) > _MAX_REPEATED_BASE_SHARE:
        return "proposal_is_one_repeated_title"
    if _has_decoding_artefacts(proposed):
        return "proposal_has_decoding_artefacts"
    if total_rows and rows_to_delete and rows_to_delete / total_rows > _MAX_HEADER_ROW_SHARE:
        return f"header_cost_too_high:{rows_to_delete}/{total_rows}"
    return None


def propose_col_n_rename(
    old_cols: list[str],
    sample_rows_data: list[list[Any]],
    *,
    min_garbage_ratio: float = 0.40,
    total_rows: int | None = None,
) -> tuple[list[str], int, str]:
    """Pure-function core of the repair: given the current column names and
    a few sample rows of data, propose new column names and how many header
    rows to delete from the data.

    Returns `(new_cols, rows_to_delete, reason)`. If no improvement is
    proposed, `new_cols == old_cols` and `reason` documents why (used by
    the orchestrating function to decide whether to apply or skip).
    """
    if garbage_column_ratio(old_cols) < min_garbage_ratio:
        return old_cols, 0, "garbage_ratio_below_threshold"
    if not sample_rows_data:
        return old_cols, 0, "table_empty"

    df = pd.DataFrame(sample_rows_data, columns=old_cols)
    promoted = promote_buried_headers(df)
    proposed = list(promoted.columns)
    proposed = dedupe_column_names(proposed)
    rows_to_delete = len(df) - len(promoted)

    if garbage_column_ratio(proposed) >= garbage_column_ratio(old_cols):
        return old_cols, 0, "no_improvement"
    if len(proposed) != len(old_cols):
        return (
            old_cols,
            0,
            f"column_count_changed:{len(old_cols)}->{len(proposed)}",
        )
    if proposed == old_cols:
        return old_cols, 0, "no_renames_needed"
    rejected = assess_rename_proposal(
        proposed, rows_to_delete=rows_to_delete, total_rows=total_rows
    )
    if rejected:
        return old_cols, 0, rejected
    return proposed, rows_to_delete, "applied"


def repair_trailing_garbage_cols(
    engine: Engine,
    *,
    table_schema: str,
    table_name: str,
    run_id: uuid.UUID | None = None,
    dry_run: bool = False,
    sample_size: int = 5000,
    max_populated_ratio: float = 0.01,
) -> RepairOutcome:
    """Drop columns whose name is garbage AND whose contents are >99 % empty.

    Targets the common parser failure where the source file had a few
    dozen real columns plus dozens-to-hundreds of cells with stray
    whitespace that pandas interpreted as columns. Those cols carry no
    information AND their names are placeholders (`col_N`, `Unnamed:N`).
    Dropping them cleans up the schema for NL2SQL without losing data.

    Safety:
      - Only drops a col if BOTH the name is garbage (`is_garbage_column`)
        AND the populated ratio over `sample_size` rows is below
        `max_populated_ratio` (default 1 %).
      - Each drop is recorded in `parse_repair_audit` with the column name.
      - `dry_run=True` skips DDL.
    """
    run_id = run_id or uuid.uuid4()
    outcome = RepairOutcome(
        table_schema=table_schema,
        table_name=table_name,
        ok=False,
        dry_run=dry_run,
    )

    with engine.connect() as conn:
        cols_rows = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t "
                "ORDER BY ordinal_position"
            ),
            {"s": table_schema, "t": table_name},
        ).fetchall()
    old_cols = [r[0] for r in cols_rows]
    outcome.old_columns = old_cols

    if not old_cols:
        outcome.reason = "table_not_found_or_no_columns"
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip", phase="trailing_garbage")
        return outcome

    garbage_cols = [c for c in old_cols if is_garbage_column(c)]
    if not garbage_cols:
        outcome.reason = "no_garbage_cols"
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip", phase="trailing_garbage")
        return outcome

    # Build a single SQL that counts populated rows per garbage col over a
    # bounded sample. For very wide tables this query is large but bounded
    # (one SUM per garbage col).
    qident_table = f"{_quote_ident(table_schema)}.{_quote_ident(table_name)}"
    # Population check excludes string-form NaN sentinels (`'None'`,
    # `'nan'`, `'NULL'`, etc.) that pandas reads AS-IS from CSV/Excel
    # without converting to actual NULL. Without this list, repair
    # treats those cols as populated and skips them.
    agg_parts = [
        (
            f"SUM(CASE WHEN {_quote_ident(c)} IS NOT NULL "
            f"AND LOWER(COALESCE(TRIM({_quote_ident(c)}::text), '')) "
            f"NOT IN ('', 'none', 'nan', 'null', 'n/a', 'na', '<na>', "
            f"'-', '--', 's/d', 's.d.', '.') "
            f"THEN 1 ELSE 0 END) AS pop_{idx}"
        )
        for idx, c in enumerate(garbage_cols)
    ]
    populated_sql = (
        f"SELECT COUNT(*) AS total_rows, {', '.join(agg_parts)} "
        f"FROM (SELECT * FROM {qident_table} LIMIT :n) sample"
    )
    try:
        with engine.connect() as conn:
            row = conn.execute(text(populated_sql), {"n": sample_size}).fetchone()
    except Exception as exc:
        outcome.reason = "sample_query_failed"
        outcome.error_message = f"{type(exc).__name__}: {exc!s}"[:500]
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip", phase="trailing_garbage")
        return outcome

    if not row or row[0] == 0:
        outcome.reason = "table_empty"
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip", phase="trailing_garbage")
        return outcome

    total_rows = row[0]
    drops: list[str] = []
    for idx, c in enumerate(garbage_cols):
        populated = row[idx + 1] or 0
        if populated / total_rows <= max_populated_ratio:
            drops.append(c)

    if not drops:
        outcome.reason = "no_drops_needed"
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip", phase="trailing_garbage")
        return outcome

    outcome.new_columns = [c for c in old_cols if c not in drops]

    if dry_run:
        outcome.ok = True
        outcome.reason = "dry_run_proposal"
        _audit(
            engine, run_id=run_id, outcome=outcome, operation="dry_run", phase="trailing_garbage"
        )
        return outcome

    try:
        with engine.begin() as conn:
            for c in drops:
                conn.execute(text(f"ALTER TABLE {qident_table} DROP COLUMN {_quote_ident(c)}"))
        outcome.ok = True
        outcome.reason = "applied"
        _audit(engine, run_id=run_id, outcome=outcome, operation="apply", phase="trailing_garbage")
        logger.info(
            "parse_repair (trim): %s.%s dropped %d empty garbage cols",
            table_schema,
            table_name,
            len(drops),
        )
    except Exception as exc:
        outcome.ok = False
        outcome.error_message = f"{type(exc).__name__}: {exc!s}"[:500]
        _audit(engine, run_id=run_id, outcome=outcome, operation="apply", phase="trailing_garbage")
        logger.exception("parse_repair (trim) failed for %s.%s", table_schema, table_name)
    return outcome


def list_trailing_garbage_candidates(
    engine: Engine,
    *,
    schemas: tuple[str, ...] = ("raw", "public"),
    limit: int = 100,
    min_garbage_count: int = 5,
) -> list[tuple[str, str, list[str]]]:
    """Tables with at least `min_garbage_count` garbage cols.

    Like `list_col_n_candidates` but the threshold is absolute count of
    garbage cols (not ratio) — this catches wide tables where most cols
    are valid but trailing 50+ cols are placeholders.
    """
    list_sql = """
        SELECT table_schema, table_name
          FROM information_schema.columns
         WHERE table_schema = ANY(:schemas)
         GROUP BY table_schema, table_name
        HAVING SUM(CASE
                   WHEN column_name ~ '^col_[0-9]+$' THEN 1
                   WHEN column_name ~* '^unnamed:' THEN 1
                   ELSE 0
                 END) >= :min_g
        ORDER BY 1, 2
    """
    cols_sql = """
        SELECT column_name FROM information_schema.columns
         WHERE table_schema = :s AND table_name = :t
         ORDER BY ordinal_position
    """
    with engine.connect() as conn:
        candidate_rows = conn.execute(
            text(list_sql),
            {"schemas": list(schemas), "min_g": min_garbage_count},
        ).fetchall()
        out = []
        for sch, tname in candidate_rows[:limit]:
            col_rows = conn.execute(text(cols_sql), {"s": sch, "t": tname}).fetchall()
            cols = [r[0] for r in col_rows]
            out.append((sch, tname, cols))
    return out


def repair_col_n_table(
    engine: Engine,
    *,
    table_schema: str,
    table_name: str,
    run_id: uuid.UUID | None = None,
    dry_run: bool = False,
    sample_rows: int = 5,
    min_garbage_ratio: float = 0.40,
) -> RepairOutcome:
    """Repair a single table whose columns include `col_N` placeholders.

    Strategy:
      1. Read first `sample_rows` rows + current column names.
      2. Run `promote_buried_headers` on a synthetic dataframe with those
         rows. If the result has materially fewer garbage cols, accept
         the proposed rename.
      3. For each (old → new) pair where the name actually changes,
         emit `ALTER TABLE … RENAME COLUMN`. After dedup pass.
      4. Delete the consumed header row(s) from the data.
      5. Audit.

    `dry_run=True` skips DDL/DML; only computes & audits the proposal.
    """
    run_id = run_id or uuid.uuid4()
    outcome = RepairOutcome(
        table_schema=table_schema,
        table_name=table_name,
        ok=False,
        dry_run=dry_run,
    )

    with engine.connect() as conn:
        cols_rows = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t "
                "ORDER BY ordinal_position"
            ),
            {"s": table_schema, "t": table_name},
        ).fetchall()
    old_cols = [r[0] for r in cols_rows]
    outcome.old_columns = old_cols

    if not old_cols:
        outcome.reason = "table_not_found_or_no_columns"
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip")
        return outcome

    # Read sample rows from the actual table.
    qident_table = f"{_quote_ident(table_schema)}.{_quote_ident(table_name)}"
    select_cols = ", ".join(_quote_ident(c) for c in old_cols)
    with engine.connect() as conn:
        sample = conn.execute(
            text(f"SELECT {select_cols} FROM {qident_table} LIMIT :n"),
            {"n": sample_rows},
        ).fetchall()
        # Bounded count, not `COUNT(*)`. The quality gate only cares whether
        # deleting the header rows costs a meaningful share of the table, and
        # that is only ever true for a small one — so "exactly n, or at least
        # the cap" is the whole answer, and it never scans a large relation.
        total_rows = conn.execute(
            text(f"SELECT count(*) FROM (SELECT 1 FROM {qident_table} LIMIT :cap) s"),
            {"cap": _ROW_COUNT_PROBE_CAP},
        ).scalar()

    proposed_cols, rows_to_delete, reason = propose_col_n_rename(
        old_cols,
        [list(r) for r in sample],
        min_garbage_ratio=min_garbage_ratio,
        total_rows=int(total_rows or 0),
    )

    if reason != "applied":
        outcome.reason = reason
        outcome.new_columns = proposed_cols
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip")
        return outcome

    rename_pairs = [(old, new) for old, new in zip(old_cols, proposed_cols) if old != new]
    outcome.new_columns = proposed_cols
    outcome.rows_deleted = rows_to_delete

    if dry_run:
        outcome.ok = True
        outcome.reason = "dry_run_proposal"
        _audit(engine, run_id=run_id, outcome=outcome, operation="dry_run")
        return outcome

    # Apply: in a single transaction, run all RENAMEs + the DELETE for the
    # consumed header row(s). DDL takes AccessExclusiveLock briefly; do NOT
    # batch many tables in one transaction (each table = one tx).
    try:
        with engine.begin() as conn:
            for old, new in rename_pairs:
                conn.execute(
                    text(
                        f"ALTER TABLE {qident_table} "
                        f"RENAME COLUMN {_quote_ident(old)} TO {_quote_ident(new)}"
                    )
                )
            if rows_to_delete > 0:
                # Delete the first N rows by ctid — only works because we
                # just read those exact rows in `sample`. Safer than
                # filtering by content.
                conn.execute(
                    text(
                        f"DELETE FROM {qident_table} WHERE ctid IN ("
                        f"SELECT ctid FROM {qident_table} "
                        f"ORDER BY ctid LIMIT :n)"
                    ),
                    {"n": rows_to_delete},
                )
        outcome.ok = True
        outcome.reason = "applied"
        _audit(engine, run_id=run_id, outcome=outcome, operation="apply")
        logger.info(
            "parse_repair: %s.%s renamed %d cols, deleted %d rows",
            table_schema,
            table_name,
            len(rename_pairs),
            rows_to_delete,
        )
    except Exception as exc:
        outcome.ok = False
        outcome.error_message = f"{type(exc).__name__}: {exc!s}"[:500]
        _audit(engine, run_id=run_id, outcome=outcome, operation="apply")
        logger.exception("parse_repair failed for %s.%s", table_schema, table_name)

    return outcome


# ---------- title_as_columns ----------
#
# Distinct from `col_N`: when the source Excel had a merged-cell TITLE row
# (e.g. "LISTADO DE LLAMADOS DE LICITACIONES PUBLICAS - AÑO 2017"), pandas
# took it as the header and dedupeed across columns with `_2.._N` suffixes.
# The real headers (`N° L.P`, `EXPEDIENTE`, ...) sit in data row 1, with
# row 0 typically all-NULL (separator).
#
# `propose_col_n_rename` does NOT detect this — it rejects on
# `garbage_ratio_below_threshold` because the cols carry text (the title).
# Hence this dedicated detector.


_DIGIT_PREFIX_RE = re.compile(r"^\d")
_ARROW_BAD_TOKENS_RE = re.compile(r"[°ª]")
_ASCII_ALPHA_RE = re.compile(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]")


def _normalize_header_to_identifier(raw: object) -> str:
    """Turn a header value (any string-coercible) into a snake_case ident.

    Strips accents, drops non-alphanumeric, collapses runs of `_`. Caps at
    50 chars. Prefixes with `col_` if the result starts with a digit
    (Postgres allows leading-digit identifiers only when quoted, and we
    don't want to force every consumer to quote).
    """
    if raw is None:
        return ""
    s = str(raw).strip().lower()
    s = _ARROW_BAD_TOKENS_RE.sub("", s)
    table = str.maketrans("áàäéèëíìïóòöúùüñ", "aaaeeeiiioooouun")
    s = s.translate(table)
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    if s and _DIGIT_PREFIX_RE.match(s):
        s = "col_" + s
    return s[:50]


def _dedupe_identifiers(names: Iterable[str]) -> list[str]:
    """Append `_2`, `_3`, ... to repeated names, preserving order. Empty
    names become `col` (which then gets the dedup suffix if there are
    multiple)."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        base = n if n else "col"
        if base in seen:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
        else:
            seen[base] = 1
            out.append(base)
    return out


def _common_prefix(strings: list[str]) -> str:
    """Return the longest common prefix across `strings`, then trimmed of
    trailing space/punctuation/digits. Used to detect the dedup-suffix
    fingerprint of a title-as-columns table."""
    if not strings:
        return ""
    pref = strings[0]
    for s in strings[1:]:
        common = 0
        for a, b in zip(pref, s):
            if a == b:
                common += 1
            else:
                break
        pref = pref[:common]
        if not pref:
            return ""
    return pref.rstrip(" /-_0123456789")


def propose_title_as_columns_rename(
    old_cols: list[str],
    sample_rows_data: list[list[Any]],
    *,
    min_cols: int = 30,
    min_common_prefix_len: int = 20,
    min_alpha_cells_in_header_row: int = 5,
    null_ratio_separator: float = 0.7,
) -> tuple[list[str], int, str]:
    """Pure-function detector + proposer for the title-as-columns bug.

    Returns `(new_cols, rows_to_delete, reason)`. `reason='applied'` means
    the proposal is ready to apply; any other reason means skip with no
    change.

    Heuristic:
      1. ≥80 % of the first 80 % of cols share a common prefix of length
         ≥`min_common_prefix_len` (tail cols often hold URLs/UUIDs without
         the prefix).
      2. Sample row 0 is ≥`null_ratio_separator` NULL/empty.
      3. Sample row 1 has ≥`min_alpha_cells_in_header_row` alpha-bearing
         cells (the real headers).

    All three must hold. The repair takes row 1 as the header source and
    deletes 2 rows (separator + header).
    """
    n_cols = len(old_cols)
    if n_cols < min_cols:
        return old_cols, 0, "too_few_cols"
    if not sample_rows_data or len(sample_rows_data) < 2:
        return old_cols, 0, "not_enough_sample_rows"

    business_cols = old_cols[: max(1, int(n_cols * 0.8))]
    prefix = _common_prefix(business_cols)
    if len(prefix) < min_common_prefix_len:
        return old_cols, 0, "no_common_prefix"

    row0 = sample_rows_data[0]
    null_count = sum(1 for v in row0 if v is None or not str(v).strip())
    if null_count < n_cols * null_ratio_separator:
        return old_cols, 0, "row0_not_separator"

    row1 = sample_rows_data[1]
    alpha_count = sum(1 for v in row1 if v is not None and _ASCII_ALPHA_RE.search(str(v)))
    if alpha_count < min_alpha_cells_in_header_row:
        return old_cols, 0, "row1_not_header_like"

    # Build the proposal: row 1 → header. Tail cols whose row 1 cell is
    # empty fall back to `metadata_<i>` if the original looked like a URL
    # or UUID, else `col_<i>`.
    new_cols: list[str] = []
    for i, (old, hdr_val) in enumerate(zip(old_cols, row1)):
        new = _normalize_header_to_identifier(hdr_val) if hdr_val is not None else ""
        if not new:
            if "://" in old or (len(old) > 20 and "-" in old):
                new = f"metadata_{i + 1}"
            else:
                new = f"col_{i + 1}"
        new_cols.append(new)
    new_cols = _dedupe_identifiers(new_cols)

    if new_cols == old_cols:
        return old_cols, 0, "no_renames_needed"

    return new_cols, 2, "applied"


def repair_title_as_columns_table(
    engine: Engine,
    *,
    table_schema: str,
    table_name: str,
    run_id: uuid.UUID | None = None,
    dry_run: bool = False,
    sample_rows: int = 5,
    min_cols: int = 30,
    min_common_prefix_len: int = 20,
) -> RepairOutcome:
    """Repair a table that landed with the title-as-columns bug.

    Strategy mirrors `repair_col_n_table` but uses
    `propose_title_as_columns_rename` so the detector matches the
    distinct fingerprint (long common prefix + NULL row 0 + header row 1).
    Audited under `phase='title_as_columns'`.

    `dry_run=True` records the proposal without DDL/DML.
    """
    run_id = run_id or uuid.uuid4()
    outcome = RepairOutcome(
        table_schema=table_schema,
        table_name=table_name,
        ok=False,
        dry_run=dry_run,
    )

    with engine.connect() as conn:
        cols_rows = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t "
                "ORDER BY ordinal_position"
            ),
            {"s": table_schema, "t": table_name},
        ).fetchall()
    old_cols = [r[0] for r in cols_rows]
    outcome.old_columns = old_cols

    if not old_cols:
        outcome.reason = "table_not_found_or_no_columns"
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip", phase="title_as_columns")
        return outcome

    qident_table = f"{_quote_ident(table_schema)}.{_quote_ident(table_name)}"
    select_cols = ", ".join(_quote_ident(c) for c in old_cols)
    with engine.connect() as conn:
        sample = [
            list(r)
            for r in conn.execute(
                text(f"SELECT {select_cols} FROM {qident_table} LIMIT :n"),
                {"n": sample_rows},
            ).fetchall()
        ]

    proposed_cols, rows_to_delete, reason = propose_title_as_columns_rename(
        old_cols,
        sample,
        min_cols=min_cols,
        min_common_prefix_len=min_common_prefix_len,
    )
    outcome.new_columns = proposed_cols
    outcome.rows_deleted = rows_to_delete

    if reason != "applied":
        outcome.reason = reason
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip", phase="title_as_columns")
        return outcome

    if dry_run:
        outcome.ok = True
        outcome.reason = "dry_run_proposal"
        _audit(
            engine, run_id=run_id, outcome=outcome, operation="dry_run", phase="title_as_columns"
        )
        return outcome

    rename_pairs = [(old, new) for old, new in zip(old_cols, proposed_cols) if old != new]
    try:
        with engine.begin() as conn:
            for old, new in rename_pairs:
                conn.execute(
                    text(
                        f"ALTER TABLE {qident_table} "
                        f"RENAME COLUMN {_quote_ident(old)} TO {_quote_ident(new)}"
                    )
                )
            if rows_to_delete > 0:
                conn.execute(
                    text(
                        f"DELETE FROM {qident_table} WHERE ctid IN ("
                        f"SELECT ctid FROM {qident_table} "
                        f"ORDER BY ctid LIMIT :n)"
                    ),
                    {"n": rows_to_delete},
                )
        outcome.ok = True
        outcome.reason = "applied"
        _audit(engine, run_id=run_id, outcome=outcome, operation="apply", phase="title_as_columns")
        logger.info(
            "parse_repair (title_as_columns): %s.%s renamed %d cols, deleted %d rows",
            table_schema,
            table_name,
            len(rename_pairs),
            rows_to_delete,
        )
    except Exception as exc:
        outcome.ok = False
        outcome.error_message = f"{type(exc).__name__}: {exc!s}"[:500]
        _audit(engine, run_id=run_id, outcome=outcome, operation="apply", phase="title_as_columns")
        logger.exception(
            "parse_repair (title_as_columns) failed for %s.%s",
            table_schema,
            table_name,
        )

    return outcome


# ---------- LLM-assisted repair (DEBT-021-002) ----------
#
# The heuristic-based repairs above (`propose_col_n_rename`,
# `propose_title_as_columns_rename`) cover the common patterns: pandas
# misread a header row, deduped a title across cols, etc. ~76 % of
# candidates that fail those heuristics ship with `no_improvement`.
# Their structures are genuinely heterogeneous: data transposed
# (rows-as-cols), multi-level NaN-hierarchical headers, sparse text
# without an identifiable header row at all.
#
# This LLM-assisted path is a fallback ONLY for tables where:
#   - A heuristic detector returned `no_improvement` or
#     `garbage_ratio_below_threshold`
#   - The cols still look semantically suspicious
#
# We send a few sample rows + the current col names to a cheap LLM and
# ask it to propose semantic identifiers. The LLM response is treated as
# a *proposal*, NOT as ground truth: we still validate length, reject
# duplicates, normalize via `_normalize_header_to_identifier`, and audit.
#
# COST: one LLM call per table. Default to dry_run=True so operators
# can review proposals before applying. Hard-coded `max_tokens=512` and
# truncated samples keep per-call cost to ~$0.001 on Claude Haiku.


_LLM_REPAIR_PROMPT_SYSTEM = (
    "Eres un experto en datos abiertos argentinos. Recibirás los nombres "
    "actuales de las columnas de una tabla de Postgres (que probablemente "
    "estén corruptos: `col_0`, `Unnamed: 1`, títulos repetidos, fechas, "
    "URLs, UUIDs, etc.) y 3-5 filas de datos reales. Tu tarea: proponer "
    "nombres semánticos en snake_case para cada columna basándote en el "
    "CONTENIDO de las filas, no en los nombres viejos.\n\n"
    "REGLAS:\n"
    "1. Devuelve EXACTAMENTE la misma cantidad de nombres que columnas "
    "originales.\n"
    "2. Solo a-z, 0-9 y _. No empieces con dígito.\n"
    "3. Cada nombre debe ser único.\n"
    "4. Si una columna parece metadata sin valor semántico (URL, UUID, "
    "fecha de scrape), llamala `metadata_<i>`.\n"
    "5. Si no puedes inferir un nombre con razonable confianza, dejá el "
    "nombre original (la heurística aplicará dedup).\n"
    "6. Responde SOLO el JSON. Nada más."
)


async def propose_llm_assisted_rename(
    old_cols: list[str],
    sample_rows_data: list[list[Any]],
    *,
    llm: Any,
    max_sample_rows: int = 5,
    max_cell_chars: int = 80,
) -> tuple[list[str], int, str]:
    """LLM-backed proposer. Returns `(new_cols, rows_to_delete, reason)`.

    Conservative on `rows_to_delete`: we don't ask the LLM to identify a
    "buried header row" because that's higher-stakes (deleting data). The
    LLM only renames cols. The heuristic handles row deletion.

    `reason='applied'` → use new_cols. Anything else → keep old_cols.
    """
    if not old_cols:
        return old_cols, 0, "no_columns"
    if len(old_cols) > 100:
        # LLM context cost grows with col count; cap at a reasonable
        # ceiling. Tables with 100+ cols are usually mega-wide pivots
        # that need a different strategy (unpivot) anyway.
        return old_cols, 0, "too_many_cols"

    # Build a compact prompt: cols list + sample rows truncated to
    # max_cell_chars per cell. JSON-encoded for the LLM.
    samples = []
    for row in sample_rows_data[:max_sample_rows]:
        truncated = [(str(v)[:max_cell_chars] if v is not None else None) for v in row]
        samples.append(truncated)

    user_prompt = json.dumps(
        {"current_columns": old_cols, "sample_rows": samples},
        ensure_ascii=False,
    )

    from app.domain.ports.llm.llm_provider import LLMMessage

    messages = [
        LLMMessage(role="system", content=_LLM_REPAIR_PROMPT_SYSTEM),
        LLMMessage(role="user", content=user_prompt),
    ]
    schema = {
        "type": "object",
        "properties": {"proposed_columns": {"type": "array", "items": {"type": "string"}}},
        "required": ["proposed_columns"],
    }

    try:
        response = await llm.chat_json(
            messages=messages, json_schema=schema, temperature=0.0, max_tokens=2048
        )
    except Exception as exc:
        logger.warning("llm-assisted repair: chat failed: %s", exc)
        return old_cols, 0, f"llm_error:{type(exc).__name__}"

    # Extract JSON robustly: some Bedrock responses prepend code fences
    # or explanation text despite the system prompt. Slice to the first
    # `{` / `[` and the matching last `}` / `]`.
    raw_text = (response.content or "").strip()
    if raw_text.startswith("```"):
        # Strip ```json fences if present
        raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
        raw_text = re.sub(r"\s*```$", "", raw_text)
    first_brace = raw_text.find("{")
    last_brace = raw_text.rfind("}")
    if first_brace >= 0 and last_brace > first_brace:
        raw_text = raw_text[first_brace : last_brace + 1]
    try:
        parsed = json.loads(raw_text)
        proposed_raw = parsed.get("proposed_columns", [])
    except (json.JSONDecodeError, AttributeError) as exc:
        logger.warning("llm-assisted repair: bad JSON: %s | content=%s", exc, raw_text[:200])
        return old_cols, 0, "llm_bad_json"

    if not isinstance(proposed_raw, list) or len(proposed_raw) != len(old_cols):
        return old_cols, 0, (f"length_mismatch:{len(proposed_raw)}_vs_{len(old_cols)}")

    # Defense-in-depth: re-normalize what the LLM returned and dedupe.
    proposed_normalized = [
        _normalize_header_to_identifier(name) or f"col_{i + 1}"
        for i, name in enumerate(proposed_raw)
    ]
    proposed_final = _dedupe_identifiers(proposed_normalized)

    if proposed_final == old_cols:
        return old_cols, 0, "llm_no_change"

    return proposed_final, 0, "applied"


async def repair_with_llm_assist(
    engine: Engine,
    *,
    llm: Any,
    table_schema: str,
    table_name: str,
    run_id: uuid.UUID | None = None,
    dry_run: bool = True,
    sample_rows: int = 5,
) -> RepairOutcome:
    """Repair a table using LLM proposal when heuristics couldn't.

    Default `dry_run=True` because LLM proposals are best reviewed before
    being applied. Audited under `phase='llm_assisted'`.
    """
    run_id = run_id or uuid.uuid4()
    outcome = RepairOutcome(
        table_schema=table_schema,
        table_name=table_name,
        ok=False,
        dry_run=dry_run,
    )

    with engine.connect() as conn:
        cols_rows = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t "
                "ORDER BY ordinal_position"
            ),
            {"s": table_schema, "t": table_name},
        ).fetchall()
    old_cols = [r[0] for r in cols_rows]
    outcome.old_columns = old_cols

    if not old_cols:
        outcome.reason = "table_not_found_or_no_columns"
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip", phase="llm_assisted")
        return outcome

    qident_table = f"{_quote_ident(table_schema)}.{_quote_ident(table_name)}"
    select_cols = ", ".join(_quote_ident(c) for c in old_cols)
    with engine.connect() as conn:
        sample = [
            list(r)
            for r in conn.execute(
                text(f"SELECT {select_cols} FROM {qident_table} LIMIT :n"),
                {"n": sample_rows},
            ).fetchall()
        ]

    proposed_cols, _rows_to_delete, reason = await propose_llm_assisted_rename(
        old_cols, sample, llm=llm
    )
    outcome.new_columns = proposed_cols

    if reason != "applied":
        outcome.reason = reason
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip", phase="llm_assisted")
        return outcome

    if dry_run:
        outcome.ok = True
        outcome.reason = "dry_run_proposal"
        _audit(engine, run_id=run_id, outcome=outcome, operation="dry_run", phase="llm_assisted")
        return outcome

    rename_pairs = [(old, new) for old, new in zip(old_cols, proposed_cols) if old != new]
    try:
        with engine.begin() as conn:
            for old, new in rename_pairs:
                conn.execute(
                    text(
                        f"ALTER TABLE {qident_table} "
                        f"RENAME COLUMN {_quote_ident(old)} TO {_quote_ident(new)}"
                    )
                )
        outcome.ok = True
        outcome.reason = "applied"
        _audit(engine, run_id=run_id, outcome=outcome, operation="apply", phase="llm_assisted")
        logger.info(
            "parse_repair (llm_assisted): %s.%s renamed %d cols",
            table_schema,
            table_name,
            len(rename_pairs),
        )
    except Exception as exc:
        outcome.ok = False
        outcome.error_message = f"{type(exc).__name__}: {exc!s}"[:500]
        _audit(engine, run_id=run_id, outcome=outcome, operation="apply", phase="llm_assisted")
        logger.exception(
            "parse_repair (llm_assisted) failed for %s.%s",
            table_schema,
            table_name,
        )

    return outcome


# Re-exported for callers (admin endpoint, future Celery task).
__all__ = [
    "RepairOutcome",
    "list_col_n_candidates",
    "repair_col_n_table",
    "repair_trailing_garbage_cols",
    "repair_title_as_columns_table",
    "repair_with_llm_assist",
    "propose_col_n_rename",
    "propose_title_as_columns_rename",
    "propose_llm_assisted_rename",
    "is_garbage_column",
]
