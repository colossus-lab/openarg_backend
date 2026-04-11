from __future__ import annotations

import logging
import os

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.infrastructure.auth import GoogleJwtValidator, InvalidGoogleToken

logger = logging.getLogger(__name__)

_ALWAYS_PUBLIC = frozenset({"/health", "/health/ready", "/api/v1/ask"})
_SERVICE_PREFIXES = ("/api/v1/data/", "/api/v1/admin/")
_DEV_PUBLIC = frozenset({"/docs", "/openapi.json", "/redoc"})

# FR-007a: admin-gated endpoints are exempt from Google JWT validation.
# They already require ``X-API-Key`` (APIKeyMiddleware) plus ``X-Admin-Key``
# (``_verify_admin_key`` dependency inside each handler), so layering a
# user JWT on top would only block ops tasks — there is no real user
# driving these calls. See ``specs/003-auth/spec.md`` FR-007a.
_ADMIN_ONLY_PATHS = frozenset(
    {
        "/api/v1/transparency/rescore",
        "/api/v1/transparency/rescrape",
        "/api/v1/transparency/snapshot-staff",
        "/api/v1/transparency/flush-cache",
    }
)

_env = os.getenv("APP_ENV", "local").lower()
_PUBLIC_PATHS = _ALWAYS_PUBLIC | _DEV_PUBLIC if _env != "prod" else _ALWAYS_PUBLIC


def get_request_user_email(request: Request) -> str:
    """Return the caller's email as validated by ``GoogleJwtAuthMiddleware``.

    The value comes from ``request.state.user_email``, which the middleware
    populates from the verified ``email`` claim of the Google OAuth ID token.
    Returns an empty string only for requests that bypass the middleware
    (public paths like ``/health``).
    """
    return getattr(request.state, "user_email", "") or ""


def _extract_bearer(auth_header: str) -> str:
    if not auth_header:
        return ""
    parts = auth_header.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return ""
    return parts[1].strip()


class GoogleJwtAuthMiddleware(BaseHTTPMiddleware):
    """Validates the Google OAuth ID token on every authenticated request.

    Sets ``request.state.user_email`` from the verified ``email`` claim.
    Rejects any non-public request that lacks a valid
    ``Authorization: Bearer <jwt>`` with HTTP 401.
    """

    def __init__(
        self,
        app,  # type: ignore[no-untyped-def]
        *,
        validator: GoogleJwtValidator,
    ) -> None:
        super().__init__(app)
        self._validator = validator

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path
        if (
            path in _PUBLIC_PATHS
            or path in _ADMIN_ONLY_PATHS
            or any(path.startswith(p) for p in _SERVICE_PREFIXES)
        ):
            return await call_next(request)

        bearer_token = _extract_bearer(request.headers.get("Authorization", ""))
        if not bearer_token:
            return JSONResponse(
                status_code=401,
                content={"detail": "Authorization: Bearer <jwt> required"},
            )

        try:
            email = await self._validator.validate(bearer_token)
        except InvalidGoogleToken as exc:
            logger.warning("Rejected invalid Google JWT: %s", exc)
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
            )

        request.state.user_email = email
        return await call_next(request)
