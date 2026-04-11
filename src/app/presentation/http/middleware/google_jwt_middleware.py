from __future__ import annotations

import logging
import os
from typing import Literal

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.infrastructure.auth import GoogleJwtValidator, InvalidGoogleToken

logger = logging.getLogger(__name__)

Mode = Literal["disabled", "dual", "enforced"]

_ALWAYS_PUBLIC = frozenset({"/health", "/health/ready", "/api/v1/ask"})
_SERVICE_PREFIXES = ("/api/v1/data/",)
_DEV_PUBLIC = frozenset({"/docs", "/openapi.json", "/redoc"})

_env = os.getenv("APP_ENV", "local").lower()
_PUBLIC_PATHS = _ALWAYS_PUBLIC | _DEV_PUBLIC if _env != "prod" else _ALWAYS_PUBLIC


def get_request_user_email(request: Request) -> str:
    """Return the caller's email, preferring the middleware-validated value.

    Priority:
        1. ``request.state.user_email`` — set by ``GoogleJwtAuthMiddleware``
           after validating the Google ID token (enforced/dual mode) or
           accepting the deprecated header (dual mode only).
        2. ``X-User-Email`` header — only reached when the middleware is in
           ``disabled`` mode and therefore never populated state. Kept so the
           new middleware can ship as a no-op until ops flips the flag.
    """
    email: str = getattr(request.state, "user_email", "") or ""
    if email:
        return email
    return request.headers.get("X-User-Email", "").strip().lower()


def _extract_bearer(auth_header: str) -> str:
    if not auth_header:
        return ""
    parts = auth_header.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return ""
    return parts[1].strip()


class GoogleJwtAuthMiddleware(BaseHTTPMiddleware):
    """Populates ``request.state.user_email`` from the Google OAuth ID token.

    Modes:
        * ``disabled`` — no-op. The legacy ``X-User-Email`` path is untouched.
        * ``dual`` — if ``Authorization: Bearer <jwt>`` is present and valid,
          use the claim email. Otherwise fall back to ``X-User-Email`` with a
          warning log. Invalid tokens still return 401.
        * ``enforced`` — require a valid ``Authorization: Bearer`` on every
          non-public request. The ``X-User-Email`` header is ignored.
    """

    def __init__(
        self,
        app,  # type: ignore[no-untyped-def]
        *,
        validator: GoogleJwtValidator | None,
        mode: Mode = "disabled",
    ) -> None:
        super().__init__(app)
        self._validator = validator
        self._mode: Mode = mode

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        if self._mode == "disabled":
            return await call_next(request)

        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path
        if path in _PUBLIC_PATHS or any(path.startswith(p) for p in _SERVICE_PREFIXES):
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        bearer_token = _extract_bearer(auth_header)

        if bearer_token:
            if self._validator is None:
                logger.error(
                    "GoogleJwtAuthMiddleware received a bearer token but no validator is configured"
                )
                return JSONResponse(
                    status_code=500,
                    content={"detail": "Auth validator not configured"},
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

        if self._mode == "enforced":
            return JSONResponse(
                status_code=401,
                content={"detail": "Authorization: Bearer <jwt> required"},
            )

        header_email = request.headers.get("X-User-Email", "").strip().lower()
        if header_email:
            logger.warning(
                "Accepting deprecated X-User-Email header for %s (path=%s)",
                header_email,
                path,
            )
            request.state.user_email = header_email
        return await call_next(request)
