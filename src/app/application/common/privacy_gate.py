"""Server-side privacy gate.

Until 2026-04-10 the privacy-notice acceptance was enforced only by the
frontend (``UserSyncProvider`` redirects to ``/privacy`` if
``privacy_accepted_at`` is null). A user with a valid JWT or API key
could bypass the gate by calling the backend endpoints directly. This
module provides a shared helper that looks up the user by email and
raises 403 if privacy is not accepted. Endpoints that handle user
content (query, conversations) call it before processing the request.

The check is a no-op for:
- anonymous users (``email == "anonymous"`` or empty) — the pipeline
  has its own anonymous fallback and those callers never hit the gate
- users that don't exist yet — they get rejected downstream when the
  handler tries to load them, with a clearer error
"""

from __future__ import annotations

import logging

from fastapi import HTTPException

from app.domain.ports.user.user_repository import IUserRepository

logger = logging.getLogger(__name__)

_ANONYMOUS_MARKERS = frozenset({"", "anonymous", "unknown"})


async def ensure_privacy_accepted(
    email: str | None,
    user_repo: IUserRepository,
) -> None:
    """Raise 403 if the given user has not accepted the privacy notice.

    Silent no-op when ``email`` is anonymous or the user record does not
    exist yet — those callers are either service-level or brand-new
    accounts that will be upserted by ``/users/sync`` on first load.
    """
    if not email or email.lower() in _ANONYMOUS_MARKERS:
        return

    try:
        user = await user_repo.get_by_email(email)
    except Exception:
        logger.debug("privacy_gate: user lookup failed for %s", email, exc_info=True)
        return

    if user is None:
        return

    if user.privacy_accepted_at is None:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "PRIVACY_NOT_ACCEPTED",
                "message": (
                    "You must accept the privacy notice before using this "
                    "endpoint. Visit /privacy in the web app to accept."
                ),
            },
        )
