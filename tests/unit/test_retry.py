"""Tests for retry decorator with exponential backoff."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.infrastructure.resilience.retry import with_retry


@pytest.fixture(autouse=True)
def _clear_circuit_breakers():
    from app.infrastructure.resilience.circuit_breaker import circuit_breakers
    circuit_breakers.clear()
    yield
    circuit_breakers.clear()


class TestWithRetry:
    @pytest.mark.anyio
    async def test_success_no_retry(self):
        call_count = 0

        @with_retry(max_retries=2, service_name="test_ok")
        async def fn():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = await fn()
        assert result == "ok"
        assert call_count == 1

    @pytest.mark.anyio
    async def test_retries_on_retryable_status(self):
        call_count = 0
        mock_resp = AsyncMock()
        mock_resp.status_code = 503

        @with_retry(max_retries=2, base_delay=0.01, service_name="test_retry_status")
        async def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.HTTPStatusError("503", request=AsyncMock(), response=mock_resp)
            return "recovered"

        result = await fn()
        assert result == "recovered"
        assert call_count == 3

    @pytest.mark.anyio
    async def test_no_retry_on_non_retryable_status(self):
        mock_resp = AsyncMock()
        mock_resp.status_code = 404

        @with_retry(max_retries=2, base_delay=0.01, service_name="test_no_retry")
        async def fn():
            raise httpx.HTTPStatusError("404", request=AsyncMock(), response=mock_resp)

        with pytest.raises(httpx.HTTPStatusError):
            await fn()

    @pytest.mark.anyio
    async def test_retries_on_connect_error(self):
        call_count = 0

        @with_retry(max_retries=1, base_delay=0.01, service_name="test_connect")
        async def fn():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectError("connection refused")
            return "ok"

        result = await fn()
        assert result == "ok"
        assert call_count == 2

    @pytest.mark.anyio
    async def test_exhausted_retries_raises(self):
        @with_retry(max_retries=1, base_delay=0.01, service_name="test_exhaust")
        async def fn():
            raise httpx.ReadTimeout("timeout")

        with pytest.raises(httpx.ReadTimeout):
            await fn()

    @pytest.mark.anyio
    async def test_circuit_breaker_skips_when_open(self):
        from app.infrastructure.resilience.circuit_breaker import get_circuit_breaker, CircuitState

        cb = get_circuit_breaker("test_open_cb")
        cb.state = CircuitState.OPEN
        cb.last_failure_time = float("inf")  # Never recover

        @with_retry(max_retries=2, service_name="test_open_cb")
        async def fn():
            return "should not reach"

        result = await fn()
        assert result is None  # Skipped due to open circuit
