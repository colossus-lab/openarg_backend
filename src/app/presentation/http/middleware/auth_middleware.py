from __future__ import annotations

import hashlib
import logging
import secrets

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.presentation.http.middleware.public_paths import PUBLIC_PATHS, SERVICE_PREFIXES

logger = logging.getLogger(__name__)


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Validates X-API-Key header for service-to-service auth."""

    def __init__(self, app, api_key: str) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app)
        self._api_key = api_key

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path
        if path in PUBLIC_PATHS or any(path.startswith(p) for p in SERVICE_PREFIXES):
            return await call_next(request)

        provided_key = request.headers.get("X-API-Key", "")
        if not provided_key or not secrets.compare_digest(provided_key, self._api_key):
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing API key"},
            )

        # Store API key identifier for rate limiting
        request.state.api_key_id = hashlib.sha256(provided_key.encode()).hexdigest()[:12]

        return await call_next(request)
