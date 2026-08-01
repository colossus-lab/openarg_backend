"""Rate limit key function — identifies callers by user, key, or IP.

Priority (most specific first):

  1. **Per-user** via `request.state.user_email` — set by
     `GoogleJwtAuthMiddleware` from the verified `email` claim. Used for
     every endpoint that passes through the Google JWT middleware
     (the SPA, `/smart`, `/sandbox/*`, `/query/*`, etc.). This closes
     round v46 H6 + M21: pre-fix every SPA user shared a single
     BACKEND_API_KEY bucket, so one noisy tenant exhausted the cap
     for everybody.

  2. **Per-API-key** via `request.state.api_key_id` — set by
     `APIKeyMiddleware`. Still applies to server-to-server callers
     (the `/api/v1/data/*` service token, plus `/api/v1/ask` callers
     that present a personal `oarg_sk_*` bearer — those buckets are
     genuinely per-key because each developer holds their own key).

  3. **Per-IP** fallback for unauthenticated traffic (health checks,
     anonymous public surfaces).
"""

from __future__ import annotations

from slowapi.util import get_remote_address
from starlette.requests import Request


def get_rate_limit_identifier(request: Request) -> str:
    user_email: str | None = getattr(request.state, "user_email", None)
    if user_email:
        return f"user:{user_email.lower()}"
    api_key_id: str | None = getattr(request.state, "api_key_id", None)
    if api_key_id:
        return f"key:{api_key_id}"
    return get_remote_address(request)
