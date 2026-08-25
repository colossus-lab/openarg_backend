"""Some repairs may not run because a model approved of them.

The ladder gates on *difficulty*: heuristics first, model last. Every reviewed
production system gates on **reversibility** instead, and the distinction is not
academic — in-place DDL is high-risk whoever proposed it, and a canary passing
says something about the model, not about the blast radius of the next
statement.

The line that matters here is what the audit can undo. `parse_repair_audit`
records the old and new column names, so a rename is a mistake somebody can walk
back. **Deleted rows do not come back.** A repair that drops a buried header row
is usually right and occasionally catastrophic, and no amount of confidence
upstream changes which of those two it was.

So repairs that only rename apply themselves, and repairs that destroy data
become proposals: recorded with their full diff, reported to a person, and
applied later by an explicit act. That is the ARIR pattern — patches as
reviewable artifacts rather than direct calls — in the smallest form that is not
theatre.

**And the target set is checked before any of it.** The recurring finding across
every automation incident reviewed — Azure's remediation job hitting the wrong
scope, Google's Diskerase reading the empty set as "everything" — is that the
action was correct and the scope was wrong. A sweep that suddenly has ten times
its usual work to do has a bug, not a busy day.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Classes the audit can walk back, so they need no second signature.
REVERSIBLE = frozenset({"rename", "drop_empty_columns", "none"})

# Above this multiple of the run's own cap, the candidate list is a bug rather
# than a busy day, and the sweep refuses rather than working through it.
IMPLAUSIBLE_MULTIPLE = 10


@dataclass(frozen=True)
class ActionClass:
    """What a repair would actually do to the table."""

    name: str
    reversible: bool
    detail: str = ""


def classify(outcome: Any) -> ActionClass:
    """Read the proposal, not the tier that produced it.

    A tier is a statement about how the answer was reached; this is a statement
    about what the answer would do, which is the thing worth gating on.
    """
    borradas = int(getattr(outcome, "rows_deleted", 0) or 0)
    viejas = list(getattr(outcome, "old_columns", []) or [])
    nuevas = list(getattr(outcome, "new_columns", []) or [])

    if borradas > 0:
        return ActionClass(
            "delete_rows",
            reversible=False,
            detail=f"borraría {borradas} fila(s), y el audit no las puede devolver",
        )
    if len(nuevas) < len(viejas):
        # The heuristic that drops columns proves they are >99 % empty before
        # doing it, which is why this stays on the reversible side: the proof
        # travels with the repair.
        return ActionClass(
            "drop_empty_columns",
            reversible=True,
            detail=f"quita {len(viejas) - len(nuevas)} columna(s) verificadas como vacías",
        )
    if viejas != nuevas:
        return ActionClass("rename", reversible=True, detail=f"renombra {len(nuevas)} columna(s)")
    return ActionClass("none", reversible=True)


def target_set_is_plausible(found: int, cap: int) -> tuple[bool, str]:
    """Is this a normal amount of work, or a bug in the candidate query?

    Checked before anything executes. Both incidents that shaped this — the one
    that took out a region and the one that wiped a fleet — were correct actions
    against a wrong target set.
    """
    if found <= 0:
        return True, ""
    if found > cap * IMPLAUSIBLE_MULTIPLE:
        return False, (
            f"{found} candidatas contra un tope de {cap}: "
            f"más de {IMPLAUSIBLE_MULTIPLE}× lo esperado, la consulta cambió de significado"
        )
    return True, ""


_ENSURE_SQL = text(
    """
    CREATE TABLE IF NOT EXISTS public.repair_proposals (
        id            UUID PRIMARY KEY,
        table_schema  TEXT NOT NULL,
        table_name    TEXT NOT NULL,
        tier          TEXT NOT NULL,
        action_class  TEXT NOT NULL,
        detail        TEXT,
        old_columns   JSONB,
        new_columns   JSONB,
        status        TEXT NOT NULL DEFAULT 'pending',
        proposed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        decided_at    TIMESTAMPTZ,
        decided_by    TEXT
    )
    """
)

_PROPOSE_SQL = text(
    """
    INSERT INTO public.repair_proposals
        (id, table_schema, table_name, tier, action_class, detail, old_columns, new_columns)
    SELECT :id, :schema, :table, :tier, :cls, :detail,
           CAST(:old AS jsonb), CAST(:new AS jsonb)
    WHERE NOT EXISTS (
        SELECT 1 FROM public.repair_proposals
        WHERE table_schema = :schema AND table_name = :table AND status = 'pending'
    )
    RETURNING id
    """
)

_DECIDE_SQL = text(
    """
    UPDATE public.repair_proposals
    SET status = :status, decided_at = now(), decided_by = :who
    WHERE id = CAST(:id AS uuid) AND status = 'pending'
    RETURNING table_schema, table_name, tier
    """
)

_PENDING_SQL = text(
    """
    SELECT id, table_schema, table_name, tier, action_class, detail,
           old_columns, new_columns, proposed_at
    FROM public.repair_proposals
    WHERE status = 'pending'
    ORDER BY proposed_at
    LIMIT :limit
    """
)

_APPROVED_SQL = text(
    """
    SELECT id, table_schema, table_name, tier
    FROM public.repair_proposals
    WHERE status = 'approved'
    ORDER BY decided_at
    LIMIT :limit
    """
)


def propose(
    engine: Any, *, table_schema: str, table_name: str, tier: str, outcome: Any
) -> str | None:
    """Record a repair that needs a person, without applying it.

    One pending proposal per table: a sweep that runs daily would otherwise
    build a queue of identical rows nobody could read.
    """
    import json

    cls = classify(outcome)
    pid = uuid.uuid4()
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_SQL)
            row = conn.execute(
                _PROPOSE_SQL,
                {
                    "id": pid,
                    "schema": table_schema,
                    "table": table_name,
                    "tier": tier,
                    "cls": cls.name,
                    "detail": cls.detail,
                    "old": json.dumps(list(getattr(outcome, "old_columns", []) or [])),
                    "new": json.dumps(list(getattr(outcome, "new_columns", []) or [])),
                },
            ).fetchone()
    except Exception:
        logger.warning("approval: could not record the proposal", exc_info=True)
        return None
    if row is None:
        return None
    logger.info("approval: %s.%s espera aprobación (%s)", table_schema, table_name, cls.name)
    return str(pid)


def decide(engine: Any, proposal_id: str, *, approved: bool, who: str) -> bool:
    """Approve or reject one proposal. Only a pending one can be decided."""
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_SQL)
            row = conn.execute(
                _DECIDE_SQL,
                {
                    "id": proposal_id,
                    "status": "approved" if approved else "rejected",
                    "who": who,
                },
            ).fetchone()
    except Exception:
        logger.warning("approval: could not decide %s", proposal_id, exc_info=True)
        return False
    return row is not None


def approved(engine: Any, *, limit: int = 25) -> list[dict[str, Any]]:
    """Proposals a person signed off on, waiting to be applied."""
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_SQL)
            rows = conn.execute(_APPROVED_SQL, {"limit": limit}).fetchall()
    except Exception:
        logger.warning("approval: could not read the queue", exc_info=True)
        return []
    return [
        {
            "id": str(r.id),
            "table_schema": r.table_schema,
            "table_name": r.table_name,
            "tier": r.tier,
        }
        for r in rows
    ]


def pending(engine: Any, *, limit: int = 50) -> list[dict[str, Any]]:
    """Proposals waiting for a signature, with the diff that would be applied.

    Ensures the table like every other entry point here. The queue is created
    lazily by the first proposal, so a reader that assumed it existed answered
    "unavailable" on a system where nothing had needed approval yet — which is
    the most likely state and the one where the answer should be "nothing
    pending".
    """
    with engine.connect() as conn:
        conn.execute(_ENSURE_SQL)
        conn.commit()
        rows = conn.execute(_PENDING_SQL, {"limit": limit}).fetchall()
        conn.rollback()
    return [
        {
            "id": str(r.id),
            "tabla": f"{r.table_schema}.{r.table_name}",
            "via": r.tier,
            "clase": r.action_class,
            "detalle": r.detail,
            "columnas_antes": r.old_columns,
            "columnas_despues": r.new_columns,
            "propuesta_el": r.proposed_at.isoformat() if r.proposed_at else None,
        }
        for r in rows
    ]
