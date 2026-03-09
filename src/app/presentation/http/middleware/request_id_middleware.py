"""Middleware that binds a unique request_id to structlog context vars for log correlation."""
from __future__ import annotations

import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Assign a unique ``request_id`` to every HTTP request.

    - Reuses the incoming ``X-Request-ID`` header when present.
    - Binds the value into :mod:`structlog.contextvars` so all log messages
      emitted during the request carry the same id.
    - Echoes the id back in the ``X-Request-ID`` response header.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id

        return response
