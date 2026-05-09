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
import uuid
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
        candidate_rows = conn.execute(
            text(list_sql), {"schemas": list(schemas)}
        ).fetchall()
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
        logger.warning(
            "parse_repair_audit insert failed (non-fatal)", exc_info=True
        )


def propose_col_n_rename(
    old_cols: list[str],
    sample_rows_data: list[list[Any]],
    *,
    min_garbage_ratio: float = 0.40,
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
    agg_parts = [
        (
            f"SUM(CASE WHEN {_quote_ident(c)} IS NOT NULL "
            f"AND COALESCE(TRIM({_quote_ident(c)}::text), '') <> '' "
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
        _audit(engine, run_id=run_id, outcome=outcome, operation="dry_run", phase="trailing_garbage")
        return outcome

    try:
        with engine.begin() as conn:
            for c in drops:
                conn.execute(
                    text(
                        f"ALTER TABLE {qident_table} DROP COLUMN {_quote_ident(c)}"
                    )
                )
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
        logger.exception(
            "parse_repair (trim) failed for %s.%s", table_schema, table_name
        )
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
            col_rows = conn.execute(
                text(cols_sql), {"s": sch, "t": tname}
            ).fetchall()
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

    proposed_cols, rows_to_delete, reason = propose_col_n_rename(
        old_cols, [list(r) for r in sample]
    )

    if reason != "applied":
        outcome.reason = reason
        outcome.new_columns = proposed_cols
        _audit(engine, run_id=run_id, outcome=outcome, operation="skip")
        return outcome

    rename_pairs = [
        (old, new) for old, new in zip(old_cols, proposed_cols) if old != new
    ]
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
        logger.exception(
            "parse_repair failed for %s.%s", table_schema, table_name
        )

    return outcome


# Re-exported for callers (admin endpoint, future Celery task).
__all__ = [
    "RepairOutcome",
    "list_col_n_candidates",
    "repair_col_n_table",
    "is_garbage_column",
]
