from __future__ import annotations

import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from app.infrastructure.monitoring.metrics import MetricsCollector


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start = time.monotonic()
        response = await call_next(request)
        duration_ms = round((time.monotonic() - start) * 1000, 1)

        metrics = MetricsCollector()
        is_error = response.status_code >= 400
        metrics.record_request(error=is_error)

        # Add timing header
        response.headers["X-Response-Time-Ms"] = str(duration_ms)

        return response
