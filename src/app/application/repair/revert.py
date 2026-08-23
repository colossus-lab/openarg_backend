"""Undo a repair, so that "reversible" is a property of the system.

`parse_repair_audit` has recorded `old_columns` for every repair since May, and
the project has described those repairs as reversible ever since. Verified
2026-08-21: nothing matching `revert`, `rollback` or `undo` exists anywhere in
the repair package or the admin router. Reversal was possible *in principle* —
the information is there — which made reversibility a property of the data and
not of the system. "Someone could reconstruct it by hand from a JSONB column" is
not a rollback.

That distinction is load-bearing rather than pedantic. Every argument in
[025-self-repair](../../../specs/025-self-repair/spec.md) for letting the data
lane *act* rather than only propose rests on its failures being cheap to undo:
one table, recoverable. Until this module existed that premise was false, so no
automatic repair could ship.

**What a revert is here.** A repair renames columns and may delete rows. Only
the rename is reversible: `rows_deleted` counts rows the repair removed as a
buried header, and those bytes are gone. A revert therefore restores the column
names and reports honestly that any deleted rows are not coming back, rather
than claiming a completeness it cannot deliver.

**It refuses when the world moved on.** If the table's current columns no longer
match what the repair produced, something else has edited it since — another
repair, a re-ingest, a hand-run migration. Renaming back from an unknown state
would corrupt rather than restore, so the revert declines and says why.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_AUDIT_ROW_SQL = text(
    """
    SELECT id, run_id, phase, table_schema, table_name, operation,
           old_columns, new_columns, rows_deleted, ok, dry_run, applied_at
    FROM parse_repair_audit
    WHERE id = :audit_id
    """
)

_CURRENT_COLUMNS_SQL = text(
    """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = :sch AND table_name = :tbl
    ORDER BY ordinal_position
    """
)

_RECORD_REVERSAL_SQL = text(
    """
    INSERT INTO parse_repair_audit
        (run_id, phase, table_schema, table_name, operation,
         old_columns, new_columns, rows_deleted, ok, error_message, dry_run)
    VALUES
        (CAST(:run_id AS uuid), :phase, :sch, :tbl, 'revert',
         CAST(:old_cols AS jsonb), CAST(:new_cols AS jsonb),
         0, :ok, :err, :dry)
    """
)


@dataclass
class RevertOutcome:
    """What a revert did, or why it declined.

    `ok=False` with `reason` means it declined and the table is untouched —
    which is the desired outcome whenever the table's state is not the one the
    repair left behind.
    """

    audit_id: int
    table_schema: str
    table_name: str
    ok: bool
    reason: str = ""
    restored_columns: list[str] = field(default_factory=list)
    rows_not_recoverable: int = 0
    dry_run: bool = False

    def as_log_dict(self) -> dict[str, Any]:
        return {
            "audit_id": self.audit_id,
            "table": f"{self.table_schema}.{self.table_name}",
            "ok": self.ok,
            "reason": self.reason,
            "rows_not_recoverable": self.rows_not_recoverable,
            "dry_run": self.dry_run,
        }


# Audited acts that are not column renames. `revert_repair` restores column
# names and has no meaning for these.
_NOT_COLUMN_REPAIRS = frozenset({"registry_location", "registry_phantom"})


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = json.loads(value)
    return [str(v) for v in value]


def revert_repair(
    engine: Engine,
    *,
    audit_id: int,
    dry_run: bool = True,
    run_id: uuid.UUID | None = None,
) -> RevertOutcome:
    """Restore the column names one audited repair changed.

    Defaults to `dry_run=True`: a revert is itself a mutation, and the caller
    should be able to see what it would do before it does it.
    """
    run_id = run_id or uuid.uuid4()

    with engine.connect() as conn:
        row = conn.execute(_AUDIT_ROW_SQL, {"audit_id": audit_id}).fetchone()
        conn.rollback()

    if row is None:
        return RevertOutcome(
            audit_id=audit_id,
            table_schema="",
            table_name="",
            ok=False,
            reason="audit_row_not_found",
            dry_run=dry_run,
        )

    outcome = RevertOutcome(
        audit_id=audit_id,
        table_schema=row.table_schema,
        table_name=row.table_name,
        ok=False,
        dry_run=dry_run,
    )

    # A revert here means: put the old column names back. Not every audited act
    # is a rename — `registry_location` moves a table between schemas and
    # `registry_phantom` retires a registry row, and neither touches a column.
    # Those rows carry NULL column lists, so this would already refuse them as
    # incomplete; refusing by phase says why, and keeps the refusal correct if a
    # future act records columns for context.
    if row.phase in _NOT_COLUMN_REPAIRS:
        outcome.reason = f"phase_not_revertible_here:{row.phase}"
        return outcome

    if row.dry_run or not row.ok or row.operation != "apply":
        # Nothing was changed, so there is nothing to undo. Worth distinguishing
        # from a failure: the corpus is mostly `skip` rows, and a caller
        # sweeping it should not read those as reverts that went wrong.
        outcome.reason = "nothing_was_applied"
        return outcome

    old_columns = _as_list(row.old_columns)
    new_columns = _as_list(row.new_columns)
    if not old_columns or len(old_columns) != len(new_columns):
        outcome.reason = "audit_row_incomplete"
        return outcome

    with engine.connect() as conn:
        current = [
            r.column_name
            for r in conn.execute(
                _CURRENT_COLUMNS_SQL, {"sch": row.table_schema, "tbl": row.table_name}
            ).fetchall()
        ]
        conn.rollback()

    if not current:
        outcome.reason = "table_no_longer_exists"
        return outcome

    # The repair's own metadata columns are not part of what it renamed, and a
    # later ingest may have added more. Compare only the positions the repair
    # claims to have produced.
    if current[: len(new_columns)] != new_columns:
        # Something edited this table after the repair. Renaming back from a
        # state we do not recognise would corrupt rather than restore.
        outcome.reason = "table_changed_since_repair"
        return outcome

    outcome.restored_columns = old_columns
    outcome.rows_not_recoverable = int(row.rows_deleted or 0)

    if dry_run:
        outcome.ok = True
        outcome.reason = "dry_run"
        return outcome

    qualified = f'{row.table_schema}."{row.table_name}"'
    try:
        with engine.begin() as conn:
            for current_name, original_name in zip(new_columns, old_columns, strict=True):
                if current_name == original_name:
                    continue
                conn.execute(
                    text(
                        f'ALTER TABLE {qualified} RENAME COLUMN "{current_name}" '
                        f'TO "{original_name}"'
                    )
                )
        outcome.ok = True
        outcome.reason = "reverted"
    except Exception as exc:  # noqa: BLE001 — recorded, then reported
        outcome.reason = "revert_failed"
        _record(engine, outcome, run_id, error=str(exc)[:500])
        logger.warning("revert of audit row %s failed", audit_id, exc_info=True)
        return outcome

    _record(engine, outcome, run_id)
    if outcome.rows_not_recoverable:
        # Said plainly rather than buried: a repair that deleted a buried header
        # row cannot give it back, and a caller that believes otherwise will
        # make a worse decision than one that knows.
        logger.warning(
            "reverted audit row %s, but %d deleted row(s) are not recoverable",
            audit_id,
            outcome.rows_not_recoverable,
        )
    return outcome


def _record(
    engine: Engine, outcome: RevertOutcome, run_id: uuid.UUID, *, error: str = ""
) -> None:
    """Write the reversal as its own audit row.

    A revert is a mutation like any other. Leaving it out of the log would make
    the table's history unreadable — the next reader would see a repair that
    apparently still stands.
    """
    try:
        with engine.begin() as conn:
            conn.execute(
                _RECORD_REVERSAL_SQL,
                {
                    "run_id": str(run_id),
                    "phase": "revert",
                    "sch": outcome.table_schema,
                    "tbl": outcome.table_name,
                    # From the reversal's point of view the repaired names are
                    # what it found and the original names are what it left.
                    "old_cols": json.dumps(
                        outcome.restored_columns and list(outcome.restored_columns)
                    ),
                    "new_cols": json.dumps(list(outcome.restored_columns)),
                    "ok": outcome.ok,
                    "err": error,
                    "dry": outcome.dry_run,
                },
            )
    except Exception:
        # The revert already happened. Failing to log it is bad, but raising
        # here would report a failure that did not occur.
        logger.warning("could not record reversal of audit row %s", outcome.audit_id, exc_info=True)
