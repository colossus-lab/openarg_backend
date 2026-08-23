"""Make `raw_table_versions` agree with where the tables actually are.

The registry is consulted by `live_table()`, by the Serving Port, and by every
mart. Nothing consults the tables themselves, so when the two disagree the
registry wins and the data loses: the macro resolves to a schema the table is
not in, and the mart fails on a column that exists.

That is not hypothetical. Three marts in production were down for exactly this
shape, and the cause took weeks to find because a registry that lies does not
contradict itself — every query it answers is internally consistent.

Two disagreements exist, measured in production on 2026-08-23:

- **82 rows locate a table in `raw` that lives in `public`.** Eighty-one carry a
  legacy `cache_*` name, and they hold 14 million rows between them. They are
  the residue of the collector's legacy path, which wrote to `public` while the
  registration said `raw`.
- **111 rows locate a table that exists nowhere.** No `cached_datasets` row
  refers to any of them, so nothing downstream depends on them; they are the
  registry remembering tables that were dropped without it being told.

The repair is deliberately asymmetric, because the two cases are not equally
knowable. A misplaced table can be *moved* to where the registry already says it
is, which makes the registry true without deciding anything. A phantom row
cannot be repaired at all — the table is gone — so it is retired rather than
deleted, leaving the history intact and the claim false no longer.

Neither direction invents a location. This module never edits `schema_name` to
match a table it found; it moves the table to the schema the registry names, or
it stops claiming the row is live. Rewriting the registry to match reality would
be the other defensible choice, and it is the wrong one here: the direction of
travel is `raw`, and a registry edited to say `public` would have to be edited
back.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Belt and braces. Nothing in this module can reach these — it iterates rows of
# `raw_table_versions`, which only ever names dataset tables — but a sweep that
# issues `ALTER TABLE ... SET SCHEMA` should carry an explicit statement of what
# it will not touch, so that the guarantee survives someone widening the query.
_NEVER_TOUCH = frozenset(
    {
        "users",
        "user_queries",
        "conversations",
        "conversation_messages",
        "api_keys",
        "api_usage",
        "alembic_version",
        "raw_table_versions",
        "cached_datasets",
        "datasets",
        "dataset_chunks",
    }
)


class RegistryUnavailable(RuntimeError):
    """The registry cannot be trusted, so no reconciliation may run."""


# A reconciliation that ran against a truncated registry would move or retire
# whatever happened to be left. Same floor as the deletion sweeps, same reason.
_REGISTRY_MIN_ROWS = 1000


@dataclass
class ReconcileOutcome:
    """What one reconciliation run did, per row it considered."""

    run_id: uuid.UUID
    dry_run: bool
    moved: list[str] = field(default_factory=list)
    retired: list[str] = field(default_factory=list)
    by_reason: dict[str, int] = field(default_factory=dict)

    def note(self, reason: str) -> None:
        self.by_reason[reason] = self.by_reason.get(reason, 0) + 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id),
            "dry_run": self.dry_run,
            "moved": len(self.moved),
            "retired": len(self.retired),
            "by_reason": self.by_reason,
            "samples_moved": self.moved[:5],
            "samples_retired": self.retired[:5],
        }


def require_registry(engine: Engine, *, task: str) -> None:
    """Refuse to reconcile when the registry looks missing or truncated."""
    with engine.connect() as conn:
        present = conn.execute(text("SELECT to_regclass('public.raw_table_versions')")).scalar()
        if not present:
            conn.rollback()
            raise RegistryUnavailable(f"{task}: public.raw_table_versions does not exist")
        rows = conn.execute(text("SELECT count(*) FROM public.raw_table_versions")).scalar() or 0
        conn.rollback()
    if rows < _REGISTRY_MIN_ROWS:
        raise RegistryUnavailable(
            f"{task}: registry holds {rows} rows, below the {_REGISTRY_MIN_ROWS} "
            "floor; refusing to move or retire anything"
        )


# Live rows whose table is not in the schema they name, but is in the other one.
# `table_type = 'BASE TABLE'` throughout: a view cannot be moved by this path and
# listing one would produce a failure that reads like a missing table.
_MISPLACED_SQL = text(
    """
    SELECT v.resource_identity, v.version, v.table_name,
           v.schema_name AS declared,
           t.table_schema AS actual
    FROM public.raw_table_versions v
    JOIN information_schema.tables t
      ON t.table_name = v.table_name
     AND t.table_type = 'BASE TABLE'
     AND t.table_schema IN ('raw', 'public')
     AND t.table_schema <> v.schema_name
    WHERE v.superseded_at IS NULL
      -- Not misplaced if it also exists where the registry says: that is the
      -- shadow-schema case, where both copies are real and choosing between
      -- them is a different decision than this one.
      AND NOT EXISTS (
          SELECT 1 FROM information_schema.tables s
          WHERE s.table_name = v.table_name
            AND s.table_schema = v.schema_name
            AND s.table_type = 'BASE TABLE'
      )
    ORDER BY v.table_name
    LIMIT :limit
    """
)

# Live rows naming a table that is in neither schema.
_PHANTOM_SQL = text(
    """
    SELECT v.resource_identity, v.version, v.table_name, v.schema_name
    FROM public.raw_table_versions v
    WHERE v.superseded_at IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM information_schema.tables t
          WHERE t.table_name = v.table_name
            AND t.table_schema IN ('raw', 'public')
            AND t.table_type = 'BASE TABLE'
      )
      -- A row something still points at is not a phantom to retire quietly; it
      -- is a broken reference someone needs to see.
      AND NOT EXISTS (
          SELECT 1 FROM raw.cached_datasets cd WHERE cd.table_name = v.table_name
      )
    ORDER BY v.table_name
    LIMIT :limit
    """
)

_AUDIT_SQL = text(
    """
    INSERT INTO parse_repair_audit
        (run_id, phase, table_schema, table_name, operation, ok, error_message, dry_run)
    VALUES
        (CAST(:run_id AS uuid), :phase, :schema, :tname, :operation, :ok, :err, :dry)
    """
)


def _audit(
    engine: Engine,
    *,
    run_id: uuid.UUID,
    phase: str,
    schema: str,
    table: str,
    operation: str,
    ok: bool,
    err: str | None,
    dry_run: bool,
) -> None:
    """Record one reconciliation act. Best-effort, like every audit here.

    `old_columns` and `new_columns` are deliberately left NULL. Neither act
    changes a column, and `revert_repair` reads those two lists to undo a rename
    — handing it a populated row for a schema move would invite it to rename
    columns that were never renamed.
    """
    try:
        with engine.begin() as conn:
            conn.execute(
                _AUDIT_SQL,
                {
                    "run_id": str(run_id),
                    "phase": phase,
                    "schema": schema,
                    "tname": table,
                    "operation": operation,
                    "ok": ok,
                    "err": err,
                    "dry": dry_run,
                },
            )
    except Exception:
        logger.warning("registry reconcile: could not audit %s.%s", schema, table, exc_info=True)


# Resources the catalogue serves whose table exists but was never registered.
# Everything the registry needs is already known: the table, its schema, its row
# count, and the identity from `datasets`.
_UNREGISTERED_SQL = text(
    r"""
    SELECT cd.table_name,
           t.table_schema,
           cd.row_count,
           d.portal || '::' || d.source_id AS resource_identity
    FROM raw.cached_datasets cd
    JOIN datasets d ON d.id = cd.dataset_id
    JOIN information_schema.tables t
      ON t.table_name = cd.table_name
     AND t.table_type = 'BASE TABLE'
     AND t.table_schema IN ('raw', 'public')
    WHERE cd.status = 'ready'
      -- Registering a table with no rows is worse than leaving a gap:
      -- `live_table()` would resolve to it and serve nothing.
      AND cd.row_count > 0
      AND d.portal IS NOT NULL
      AND d.source_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM public.raw_table_versions v
          WHERE v.table_name = cd.table_name
      )
      -- An identity already in the registry needs a decision this sweep is not
      -- equipped to make: is the unregistered table a newer version of that
      -- resource, or a duplicate to drop? Answering needs both tables looked
      -- at. They are counted and skipped rather than guessed.
      AND NOT EXISTS (
          SELECT 1 FROM public.raw_table_versions v2
          WHERE v2.resource_identity = d.portal || '::' || d.source_id
      )
    ORDER BY cd.table_name
    LIMIT :limit
    """
)

_INSERT_SQL = text(
    """
    INSERT INTO public.raw_table_versions
        (resource_identity, version, schema_name, table_name, row_count,
         parser_version, created_at)
    VALUES
        (:ri, 1, :schema, :tbl, :rows, 'legacy:unknown', now())
    ON CONFLICT DO NOTHING
    """
)


def backfill_legacy_registry(
    engine: Engine,
    *,
    run_id: uuid.UUID | None = None,
    dry_run: bool = True,
    limit: int = 5000,
) -> ReconcileOutcome:
    """Register tables the catalogue serves that the registry never learned about.

    Measured in production on 2026-08-23: 4,019 `ready` resources whose table
    exists, holds rows, and is identifiable — but has no registry row. All of
    them carry legacy `cache_*` names and live in `public`. They are sediment
    from before the raw layer, not a collection failure, and they are the whole
    of the gap between 86.4 % coverage and the 90 % the plan asks for.

    **Registered in the schema they are actually in, which is `public`.**
    Writing `raw` because that is where new tables go is exactly the defect that
    had three marts failing on columns that existed, and that
    `reconcile_locations` above exists to repair. This sweep must not
    manufacture more of them.

    **Provenance is `legacy:unknown`, deliberately.** `is_real_provenance`
    rejects that value, so these rows raise coverage without becoming eligible
    for the drift cascade. We do not know which parser read them, and writing a
    fingerprint we did not measure would feed the cascade false evidence — a
    worse outcome than the gap.

    Version 1 for every row: there is no earlier version to be second to.
    """
    run_id = run_id or uuid.uuid4()
    require_registry(engine, task="backfill_legacy_registry")
    outcome = ReconcileOutcome(run_id=run_id, dry_run=dry_run)

    with engine.connect() as conn:
        rows = conn.execute(_UNREGISTERED_SQL, {"limit": limit}).fetchall()
        conn.rollback()

    for row in rows:
        table = str(row.table_name)
        if table in _NEVER_TOUCH:
            outcome.note("protected_table")
            continue
        if dry_run:
            outcome.note("would_register")
            outcome.moved.append(f"{row.table_schema}.{table}")
            continue
        try:
            with engine.begin() as conn:
                conn.execute(
                    _INSERT_SQL,
                    {
                        "ri": row.resource_identity,
                        "schema": row.table_schema,
                        "tbl": table,
                        "rows": int(row.row_count or 0),
                    },
                )
        except Exception as exc:
            outcome.note("register_failed")
            _audit(
                engine, run_id=run_id, phase="registry_backfill",
                schema=str(row.table_schema), table=table, operation="apply",
                ok=False, err=str(exc)[:500], dry_run=False,
            )
            continue
        outcome.note("registered")
        outcome.moved.append(f"{row.table_schema}.{table}")

    # One audit row for the run rather than 4,019 — the reversal is "delete the
    # rows this run inserted", which is a single statement keyed on run_id, not
    # a per-table undo.
    if not dry_run:
        _audit(
            engine, run_id=run_id, phase="registry_backfill", schema="public",
            table=f"<{len(outcome.moved)} tables>", operation="apply",
            ok=True, err=None, dry_run=False,
        )
    return outcome


def reconcile_locations(
    engine: Engine,
    *,
    run_id: uuid.UUID | None = None,
    dry_run: bool = True,
    limit: int = 500,
) -> ReconcileOutcome:
    """Move each misplaced table to the schema its registry row names.

    Moves the table rather than editing the row, so the registry's answer to
    `live_table()` becomes true without this function deciding where anything
    ought to live. It only ever agrees with what the registry already said.
    """
    run_id = run_id or uuid.uuid4()
    require_registry(engine, task="reconcile_locations")
    outcome = ReconcileOutcome(run_id=run_id, dry_run=dry_run)

    with engine.connect() as conn:
        rows = conn.execute(_MISPLACED_SQL, {"limit": limit}).fetchall()
        conn.rollback()

    for row in rows:
        table = str(row.table_name)
        if table in _NEVER_TOUCH:
            outcome.note("protected_table")
            continue

        # A name already taken in the destination cannot be moved into it. The
        # measurement said zero such collisions, which is exactly why the check
        # belongs here rather than in the reasoning that preceded the run.
        with engine.connect() as conn:
            taken = conn.execute(
                text(
                    """
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = :t AND table_schema = :s
                    """
                ),
                {"t": table, "s": row.declared},
            ).fetchone()
            conn.rollback()
        if taken:
            outcome.note("name_taken_in_destination")
            continue

        if dry_run:
            outcome.note("would_move")
            outcome.moved.append(f"{row.actual}.{table} -> {row.declared}")
            _audit(
                engine, run_id=run_id, phase="registry_location", schema=str(row.actual),
                table=table, operation="dry_run", ok=True, err=None, dry_run=True,
            )
            continue

        try:
            with engine.begin() as conn:
                conn.execute(
                    text(f'ALTER TABLE "{row.actual}"."{table}" SET SCHEMA "{row.declared}"')
                )
        except Exception as exc:
            outcome.note("move_failed")
            _audit(
                engine, run_id=run_id, phase="registry_location", schema=str(row.actual),
                table=table, operation="apply", ok=False, err=str(exc)[:500], dry_run=False,
            )
            logger.warning("registry reconcile: could not move %s", table, exc_info=True)
            continue

        outcome.note("moved")
        outcome.moved.append(f"{row.actual}.{table} -> {row.declared}")
        _audit(
            engine, run_id=run_id, phase="registry_location", schema=str(row.actual),
            table=table, operation="apply", ok=True, err=None, dry_run=False,
        )

    return outcome


def retire_phantom_rows(
    engine: Engine,
    *,
    run_id: uuid.UUID | None = None,
    dry_run: bool = True,
    limit: int = 500,
) -> ReconcileOutcome:
    """Stop claiming a row is live when its table no longer exists.

    Sets `superseded_at` rather than deleting. The row is the only surviving
    evidence that the table ever existed, including its provenance and row
    count, and the drift work depends on exactly that kind of evidence. Deleting
    it would buy nothing and cost the history.
    """
    run_id = run_id or uuid.uuid4()
    require_registry(engine, task="retire_phantom_rows")
    outcome = ReconcileOutcome(run_id=run_id, dry_run=dry_run)

    with engine.connect() as conn:
        rows = conn.execute(_PHANTOM_SQL, {"limit": limit}).fetchall()
        conn.rollback()

    for row in rows:
        table = str(row.table_name)
        if table in _NEVER_TOUCH:
            outcome.note("protected_table")
            continue

        if dry_run:
            outcome.note("would_retire")
            outcome.retired.append(f"{row.schema_name}.{table}")
            _audit(
                engine, run_id=run_id, phase="registry_phantom", schema=str(row.schema_name),
                table=table, operation="dry_run", ok=True, err=None, dry_run=True,
            )
            continue

        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE public.raw_table_versions
                           SET superseded_at = now()
                         WHERE resource_identity = :ri
                           AND version = :v
                           AND superseded_at IS NULL
                        """
                    ),
                    {"ri": row.resource_identity, "v": row.version},
                )
        except Exception as exc:
            outcome.note("retire_failed")
            _audit(
                engine, run_id=run_id, phase="registry_phantom", schema=str(row.schema_name),
                table=table, operation="apply", ok=False, err=str(exc)[:500], dry_run=False,
            )
            continue

        outcome.note("retired")
        outcome.retired.append(f"{row.schema_name}.{table}")
        _audit(
            engine, run_id=run_id, phase="registry_phantom", schema=str(row.schema_name),
            table=table, operation="apply", ok=True, err=None, dry_run=False,
        )

    return outcome
