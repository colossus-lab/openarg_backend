"""Where a person signs off on a repair the system would not apply by itself.

The ladder applies what the audit can walk back and stops at what it cannot.
Everything it stops at is recorded as a proposal with its full diff, and this is
the surface where somebody reads one and decides.

Two endpoints and nothing else on purpose. A proposal queue that needs a UI
before anyone can use it is a queue that stays full, and the decision here is
small: this rename would delete three rows, yes or no.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from app.infrastructure.celery.tasks._db import get_sync_engine
from app.presentation.http.controllers.admin.tasks_router import verify_admin_key

router = APIRouter(prefix="/admin", tags=["admin-repair-approval"])


@router.get("/repair-proposals", dependencies=[Depends(verify_admin_key)])
def list_proposals(limit: int = 50) -> dict[str, Any]:
    """Repairs waiting for a signature, with the diff that would be applied."""
    from app.application.repair.approval import pending

    try:
        return {"pendientes": pending(get_sync_engine(), limit=limit)}
    except Exception:
        # An empty queue and an unreadable one must not look the same.
        raise HTTPException(status_code=503, detail="no se pudo leer la cola") from None


@router.post("/repair-proposals/{proposal_id}/decide", dependencies=[Depends(verify_admin_key)])
def decide_proposal(proposal_id: str, approve: bool, who: str = "admin") -> dict[str, Any]:
    """Approve or reject one proposal.

    Approving does not apply it here: `openarg.apply_approved_repairs` does that
    on its own schedule. Deciding and executing are separate acts, so a decision
    made in a hurry is not also a write made in a hurry.
    """
    from app.application.repair.approval import decide

    # Validated here rather than let through to `CAST(:id AS uuid)`. The
    # parameter binding makes it safe either way, but a malformed id would come
    # back as a 500 with a database error in it — an unnecessarily talkative
    # answer on an admin surface, and one that reads like a bug rather than a
    # bad request.
    try:
        uuid.UUID(proposal_id)
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(status_code=404, detail="no existe o ya fue decidida") from None

    # Bounded because it is stored and shown back. Nothing here is trusted for
    # anything, but an unbounded field on a write path is a field somebody fills.
    ok = decide(get_sync_engine(), proposal_id, approved=approve, who=who[:64])
    if not ok:
        raise HTTPException(status_code=404, detail="no existe o ya fue decidida")
    return {"id": proposal_id, "estado": "approved" if approve else "rejected"}
