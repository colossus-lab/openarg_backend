"""Try to fix it before telling anybody — and say which rung did it.

The repair machinery grew as five independent sweeps, each scheduled on its own
day, each seeing one shape and declining everything else. A table carrying a
defect no single sweep recognises is refused five times and stays broken, and
nothing anywhere records that all five refused. The drift report, separately,
tells a person a table changed shape without ever having tried to do anything
about it.

This is the ladder those two halves imply: **every rung is attempted before a
person is told, and the message says which rung worked, or that none did.**

    heurísticas (5, deterministas, gratis)  →  LLM (1 llamada, con canario)  →  persona

Order is specificity, not preference. Each heuristic declines cheaply when it
does not recognise the shape — `garbage_ratio_below_threshold`, `too_few_cols`,
`inconsistent_field_count` — so running all five costs five refusals and the
first one that recognises the table wins. The model is asked only about what
survives all five, which is the population it was measured on.

**The brake is the point of the ladder, not the rungs.** Renaming `col_1` to
`monto` fixes a table and breaks every mart whose SQL says `col_1`; unattended,
that trades one visible defect for a silent one. So a repair that would touch a
column some mart names is refused and handed to a person, with the marts listed.
`marts.consumers` supplies that fact — the caller passes a `guard` and this
module never repairs past it.

Two rungs, two ways of asking the guard, for a reason:

- **Heuristics** are deterministic, so the proposal is computed first with
  `dry_run=True`, the guard sees exactly the columns that would change, and the
  repair is applied only if none of them is spoken for. Precise.
- **The model** sees every column and may rename any of them, and a second call
  need not repeat the first. Asking the guard about the specific proposal would
  be checking one answer and applying another, so the model rung is guarded
  against **every** column instead. Blunter, and blunt in the safe direction.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# What the guard is asked. Returns the marts that would break.
Guard = Callable[[Sequence[str]], Sequence[str]]

# The collector's own columns. Never proposed, never guarded, never changed.
_PROTECTED_PREFIX = "_"

_COLUMNS_SQL = text(
    """
    SELECT a.attname AS name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE n.nspname = :schema AND c.relname = :table
      AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
    """
)


@dataclass(frozen=True)
class Attempt:
    """One rung, and what it said."""

    tier: str
    ok: bool
    reason: str
    changed_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class Escalation:
    """What happened to one table, all the way up the ladder."""

    table_schema: str
    table_name: str
    fixed: bool
    tier: str | None = None
    reason: str = ""
    attempts: tuple[Attempt, ...] = ()
    blocked_by_marts: tuple[str, ...] = ()
    changed_columns: tuple[str, ...] = ()

    @property
    def table(self) -> str:
        return f"{self.table_schema}.{self.table_name}"

    @property
    def needs_a_person(self) -> bool:
        """Nothing fixed it, so somebody has to look."""
        return not self.fixed

    def as_log_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "fixed": self.fixed,
            "tier": self.tier,
            "reason": self.reason,
            "tried": [a.tier for a in self.attempts],
            "blocked_by_marts": list(self.blocked_by_marts),
        }


def heuristic_tiers() -> tuple[tuple[str, Any], ...]:
    """The deterministic rungs, most specific first.

    Imported lazily: `parse_repair` pulls in pandas, and the escalation module
    is imported by the Celery task registry at worker boot.
    """
    from app.application.repair import parse_repair as pr

    return (
        # An unsplit CSV carries its own proof — the header names N fields and
        # every row splits into exactly N — which is why it is also the one
        # rung already trusted to run unattended.
        ("unsplit_csv", pr.repair_unsplit_csv_table),
        # Drops columns that are garbage *and* >99 % empty. Before the renames,
        # so they see the real shape rather than the padding.
        ("trailing_garbage", pr.repair_trailing_garbage_cols),
        ("title_as_columns", pr.repair_title_as_columns_table),
        ("smeared_title", pr.repair_smeared_title_table),
        # Last because it is the most general: promote a buried header row.
        ("col_n", pr.repair_col_n_table),
    )


def current_columns(engine: Engine, schema: str, table: str) -> list[str]:
    """The table's columns as Postgres has them, in order."""
    with engine.connect() as conn:
        rows = conn.execute(_COLUMNS_SQL, {"schema": schema, "table": table}).fetchall()
        conn.rollback()
    return [r.name for r in rows]


def changed_columns(old: Sequence[str], new: Sequence[str]) -> tuple[str, ...]:
    """Which of the old names stop meaning what they meant.

    Positional when the arity is unchanged — that is a rename, and position is
    the only thing tying old to new. When columns were dropped instead, set
    difference: a name that survived is untouched wherever it ended up.
    """
    if len(old) == len(new):
        return tuple(o for o, n in zip(old, new, strict=True) if o != n)
    return tuple(o for o in old if o not in set(new))


def _guarded(guard: Guard | None, columns: Sequence[str]) -> tuple[str, ...]:
    """Ask the guard, and treat its failure as a refusal.

    A guard that raises means we could not establish whether a repair is safe,
    and "unknown" has to read as "no" — the whole reason the guard exists is
    that the unsafe case is invisible after the fact.
    """
    if guard is None or not columns:
        return ()
    try:
        return tuple(guard(columns))
    except Exception:
        logger.warning("escalation: guard failed, refusing the repair", exc_info=True)
        return ("<guard-no-disponible>",)


def escalate_table(
    engine: Engine,
    *,
    table_schema: str,
    table_name: str,
    guard: Guard | None = None,
    llm: Any | None = None,
    run_id: uuid.UUID | None = None,
    dry_run: bool = True,
) -> Escalation:
    """Walk the ladder for one table and report which rung fixed it, if any.

    `guard` is asked before anything is written and can stop any rung.
    `llm` is the last rung and is skipped when it is `None` — the caller runs
    the canary once for the batch rather than once per table, so passing an
    adapter here is also the statement that it answered correctly.

    `dry_run=True` walks the whole ladder and writes nothing, which is what
    makes the result readable before it is trusted.
    """
    run_id = run_id or uuid.uuid4()
    attempts: list[Attempt] = []

    for tier, fn in heuristic_tiers():
        try:
            proposal = fn(
                engine,
                table_schema=table_schema,
                table_name=table_name,
                run_id=run_id,
                dry_run=True,
            )
        except Exception as exc:
            # One rung failing is not the ladder failing. Record it and climb.
            logger.warning("escalation: %s raised on %s", tier, table_name, exc_info=True)
            attempts.append(Attempt(tier=tier, ok=False, reason=f"raised:{type(exc).__name__}"))
            continue

        if not proposal.ok:
            attempts.append(
                Attempt(tier=tier, ok=False, reason=proposal.reason or proposal.error_message)
            )
            continue

        touched = changed_columns(proposal.old_columns, proposal.new_columns)
        blocking = _guarded(guard, touched)
        if blocking:
            attempts.append(
                Attempt(tier=tier, ok=False, reason="would_break_marts", changed_columns=touched)
            )
            return Escalation(
                table_schema=table_schema,
                table_name=table_name,
                fixed=False,
                reason="would_break_marts",
                attempts=tuple(attempts),
                blocked_by_marts=blocking,
                changed_columns=touched,
            )

        if dry_run:
            attempts.append(Attempt(tier=tier, ok=True, reason="dry_run", changed_columns=touched))
            return Escalation(
                table_schema=table_schema,
                table_name=table_name,
                fixed=True,
                tier=tier,
                reason="dry_run",
                attempts=tuple(attempts),
                changed_columns=touched,
            )

        applied = fn(
            engine,
            table_schema=table_schema,
            table_name=table_name,
            run_id=run_id,
            dry_run=False,
        )
        attempts.append(
            Attempt(
                tier=tier,
                ok=applied.ok,
                reason=applied.reason or applied.error_message,
                changed_columns=touched,
            )
        )
        if applied.ok:
            return Escalation(
                table_schema=table_schema,
                table_name=table_name,
                fixed=True,
                tier=tier,
                reason=applied.reason or "applied",
                attempts=tuple(attempts),
                changed_columns=touched,
            )
        # The dry run said yes and the apply said no. Rare, and worth climbing
        # rather than stopping: another rung may still recognise the table.

    if llm is None:
        return Escalation(
            table_schema=table_schema,
            table_name=table_name,
            fixed=False,
            reason="heuristics_declined_and_no_model",
            attempts=tuple(attempts),
        )

    return _llm_rung(
        engine,
        table_schema=table_schema,
        table_name=table_name,
        guard=guard,
        llm=llm,
        run_id=run_id,
        dry_run=dry_run,
        attempts=attempts,
    )


def _llm_rung(
    engine: Engine,
    *,
    table_schema: str,
    table_name: str,
    guard: Guard | None,
    llm: Any,
    run_id: uuid.UUID,
    dry_run: bool,
    attempts: list[Attempt],
) -> Escalation:
    """The last rung. Guarded against every column, for the reason in the module docstring."""
    import asyncio

    from app.application.repair.parse_repair import repair_with_llm_assist

    columns = [c for c in current_columns(engine, table_schema, table_name)]
    candidates = [c for c in columns if not c.startswith(_PROTECTED_PREFIX)]
    blocking = _guarded(guard, candidates)
    if blocking:
        attempts.append(Attempt(tier="llm", ok=False, reason="would_break_marts"))
        return Escalation(
            table_schema=table_schema,
            table_name=table_name,
            fixed=False,
            reason="would_break_marts",
            attempts=tuple(attempts),
            blocked_by_marts=blocking,
            changed_columns=tuple(candidates),
        )

    try:
        outcome = asyncio.run(
            repair_with_llm_assist(
                engine,
                llm=llm,
                table_schema=table_schema,
                table_name=table_name,
                run_id=run_id,
                dry_run=dry_run,
            )
        )
    except Exception as exc:
        # Throttling, a timeout, a credential blip: the table stays as it is.
        logger.warning("escalation: llm rung raised on %s", table_name, exc_info=True)
        attempts.append(Attempt(tier="llm", ok=False, reason=f"raised:{type(exc).__name__}"))
        return Escalation(
            table_schema=table_schema,
            table_name=table_name,
            fixed=False,
            reason=f"raised:{type(exc).__name__}",
            attempts=tuple(attempts),
        )

    touched = changed_columns(outcome.old_columns, outcome.new_columns)
    attempts.append(
        Attempt(
            tier="llm",
            ok=outcome.ok,
            reason=outcome.reason or outcome.error_message,
            changed_columns=touched,
        )
    )
    return Escalation(
        table_schema=table_schema,
        table_name=table_name,
        fixed=outcome.ok,
        tier="llm" if outcome.ok else None,
        reason=outcome.reason or outcome.error_message or "llm_declined",
        attempts=tuple(attempts),
        changed_columns=touched,
    )
