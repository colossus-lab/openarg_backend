from __future__ import annotations

import asyncio
import functools
import logging
import random
from collections.abc import Callable
from typing import Any

import httpx

from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.infrastructure.resilience.circuit_breaker import get_circuit_breaker

logger = logging.getLogger(__name__)

# Default status codes that are safe to retry
DEFAULT_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# Exception types that are safe to retry
RETRYABLE_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.ConnectTimeout,
    ConnectionError,
    TimeoutError,
)


def with_retry(
    max_retries: int = 2,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
    retryable_statuses: set[int] | None = None,
    service_name: str | None = None,
) -> Callable:
    """Decorator that adds exponential backoff retry + circuit breaker to async methods.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Initial delay in seconds (before jitter).
        max_delay: Maximum delay cap in seconds.
        retryable_statuses: HTTP status codes that trigger a retry.
        service_name: Circuit breaker service key. If None, uses the function's qualname.
    """
    if retryable_statuses is None:
        retryable_statuses = DEFAULT_RETRYABLE_STATUSES

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            cb_name = service_name or func.__qualname__
            cb = get_circuit_breaker(cb_name)

            if cb.is_open:
                logger.warning("Circuit OPEN for %s — skipping call", cb_name)
                raise ConnectorError(
                    error_code=ErrorCode.CN_CIRCUIT_OPEN,
                    details={"service": cb_name},
                )

            last_exc: Exception | None = None
            for attempt in range(max_retries + 1):
                try:
                    result = await func(*args, **kwargs)
                    cb.record_success()
                    return result
                except httpx.HTTPStatusError as exc:
                    last_exc = exc
                    status = exc.response.status_code
                    if status not in retryable_statuses or attempt == max_retries:
                        cb.record_failure()
                        raise
                    delay = _backoff_delay(attempt, base_delay, max_delay)
                    logger.warning(
                        "%s attempt %d/%d failed (HTTP %d), retrying in %.1fs",
                        cb_name,
                        attempt + 1,
                        max_retries + 1,
                        status,
                        delay,
                    )
                    await asyncio.sleep(delay)
                except RETRYABLE_EXCEPTIONS as exc:
                    last_exc = exc
                    if attempt == max_retries:
                        cb.record_failure()
                        raise
                    delay = _backoff_delay(attempt, base_delay, max_delay)
                    logger.warning(
                        "%s attempt %d/%d failed (%s), retrying in %.1fs",
                        cb_name,
                        attempt + 1,
                        max_retries + 1,
                        type(exc).__name__,
                        delay,
                    )
                    await asyncio.sleep(delay)

            # Should not reach here, but just in case
            if last_exc:
                raise last_exc
            return None

        return wrapper

    return decorator


def _backoff_delay(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter."""
    exp_delay = base * (2**attempt)
    capped = min(exp_delay, cap)
    return random.uniform(0, capped)  # noqa: S311
