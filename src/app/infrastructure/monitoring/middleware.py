from __future__ import annotations

import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from app.infrastructure.monitoring.metrics import MetricsCollector
from app.infrastructure.monitoring.prometheus_metrics import REQUEST_COUNT, REQUEST_LATENCY


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start = time.monotonic()
        response = await call_next(request)
        duration_s = time.monotonic() - start
        duration_ms = round(duration_s * 1000, 1)

        # In-memory metrics (backward compat)
        metrics = MetricsCollector()
        is_error = response.status_code >= 400
        metrics.record_request(error=is_error)

        # Prometheus metrics
        endpoint = request.url.path
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=endpoint,
            status=str(response.status_code),
        ).inc()
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(duration_s)

        # Add timing header
        response.headers["X-Response-Time-Ms"] = str(duration_ms)

        return response
