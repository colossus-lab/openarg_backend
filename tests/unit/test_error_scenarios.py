"""Error scenario tests for resilience infrastructure.

Tests retry decorator behavior, circuit breaker state transitions,
dispatch step retry logic, and Celery SoftTimeLimitExceeded handling.
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.domain.entities.connectors.data_result import DataResult, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.infrastructure.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    circuit_breakers,
    get_circuit_breaker,
)
from app.infrastructure.resilience.retry import with_retry


@pytest.fixture(autouse=True)
def _clear_circuit_breakers():
    """Ensure circuit breaker state is clean for each test."""
    circuit_breakers.clear()
    yield
    circuit_breakers.clear()


# ── Retry decorator tests ─────────────────────────────────────


class TestRetryDecoratorRetriesOnTransientError:
    async def test_retries_on_transient_error_then_succeeds(self):
        """Function fails twice with a retryable error, then succeeds on the 3rd try."""
        call_count = 0

        @with_retry(max_retries=2, base_delay=0.01, service_name="test_transient_ok")
        async def flaky_fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.ConnectError("connection refused")
            return "success"

        result = await flaky_fn()
        assert result == "success"
        assert call_count == 3  # 1 initial + 2 retries

    async def test_retries_on_503_then_succeeds(self):
        """Function fails with HTTP 503 twice, then succeeds."""
        call_count = 0
        mock_resp = AsyncMock()
        mock_resp.status_code = 503

        @with_retry(max_retries=2, base_delay=0.01, service_name="test_503_ok")
        async def flaky_fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.HTTPStatusError(
                    "503", request=AsyncMock(), response=mock_resp,
                )
            return "recovered"

        result = await flaky_fn()
        assert result == "recovered"
        assert call_count == 3

    async def test_retries_on_timeout(self):
        """Function fails with ReadTimeout once, then succeeds."""
        call_count = 0

        @with_retry(max_retries=1, base_delay=0.01, service_name="test_timeout_ok")
        async def flaky_fn():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ReadTimeout("read timed out")
            return "ok"

        result = await flaky_fn()
        assert result == "ok"
        assert call_count == 2


class TestRetryDecoratorGivesUpAfterMaxRetries:
    async def test_raises_after_max_retries_connection_error(self):
        """After max_retries, the original exception is raised."""
        call_count = 0

        @with_retry(max_retries=2, base_delay=0.01, service_name="test_exhaust_conn")
        async def always_fails():
            nonlocal call_count
            call_count += 1
            raise httpx.ConnectError("connection refused")

        with pytest.raises(httpx.ConnectError):
            await always_fails()

        # Should have tried 1 + 2 = 3 times total
        assert call_count == 3

    async def test_raises_after_max_retries_http_status(self):
        """After exhausting retries on retryable HTTP status, raises the status error."""
        call_count = 0
        mock_resp = AsyncMock()
        mock_resp.status_code = 502

        @with_retry(max_retries=1, base_delay=0.01, service_name="test_exhaust_502")
        async def always_fails():
            nonlocal call_count
            call_count += 1
            raise httpx.HTTPStatusError("502", request=AsyncMock(), response=mock_resp)

        with pytest.raises(httpx.HTTPStatusError):
            await always_fails()

        assert call_count == 2  # 1 initial + 1 retry

    async def test_circuit_breaker_records_failure_on_exhaust(self):
        """When retries are exhausted, the circuit breaker should record a failure."""
        @with_retry(max_retries=1, base_delay=0.01, service_name="test_cb_failure")
        async def always_fails():
            raise httpx.ReadTimeout("timeout")

        with pytest.raises(httpx.ReadTimeout):
            await always_fails()

        cb = get_circuit_breaker("test_cb_failure")
        assert cb.failure_count >= 1


# ── Circuit breaker tests ─────────────────────────────────────


class TestCircuitBreakerOpensAfterFailures:
    def test_opens_after_failure_threshold(self):
        """Circuit breaker transitions to OPEN after hitting failure_threshold."""
        cb = CircuitBreaker("test_open", failure_threshold=3)
        assert cb.state == CircuitState.CLOSED

        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_is_open_returns_true_when_open(self):
        """is_open property returns True when state is OPEN and timeout not elapsed."""
        cb = CircuitBreaker("test_is_open", failure_threshold=1, recovery_timeout=60.0)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.is_open is True

    def test_retry_decorator_raises_connector_error_when_open(self):
        """with_retry raises ConnectorError when circuit breaker is open."""
        cb = get_circuit_breaker("test_retry_open", failure_threshold=1, recovery_timeout=999)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        @with_retry(max_retries=2, service_name="test_retry_open")
        async def should_not_run():
            return "should not reach"

        async def run_test():
            with pytest.raises(ConnectorError) as exc_info:
                await should_not_run()
            assert exc_info.value.error_code == ErrorCode.CN_CIRCUIT_OPEN

        asyncio.get_event_loop().run_until_complete(run_test())


class TestCircuitBreakerHalfOpenRecovery:
    def test_transitions_to_half_open_after_timeout(self):
        """After recovery_timeout, circuit moves from OPEN to HALF_OPEN."""
        cb = CircuitBreaker("test_half_open", failure_threshold=1, recovery_timeout=0.01)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        time.sleep(0.02)
        assert cb.is_open is False  # Triggers transition to HALF_OPEN
        assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_recovers_on_success(self):
        """In HALF_OPEN, enough successes transition back to CLOSED."""
        cb = CircuitBreaker(
            "test_recover", failure_threshold=1,
            recovery_timeout=0.01, success_threshold=2,
        )
        cb.record_failure()
        time.sleep(0.02)
        _ = cb.is_open  # Trigger HALF_OPEN

        assert cb.state == CircuitState.HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitState.HALF_OPEN  # Needs 2 successes
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_half_open_failure_reopens(self):
        """A failure during HALF_OPEN transitions back to OPEN."""
        cb = CircuitBreaker("test_reopen", failure_threshold=1, recovery_timeout=0.01)
        cb.record_failure()
        time.sleep(0.02)
        _ = cb.is_open  # Trigger HALF_OPEN

        assert cb.state == CircuitState.HALF_OPEN
        cb.record_failure()
        assert cb.state == CircuitState.OPEN


# ── Dispatch step retry tests ──────────────────────────────────


def _make_service():
    """Build a SmartQueryService with all-mocked dependencies."""
    from app.application.smart_query_service import SmartQueryService

    llm = AsyncMock()
    llm.chat.return_value = MagicMock(
        content="test response", tokens_used=10, model="test",
    )

    return SmartQueryService(
        llm=llm,
        embedding=AsyncMock(),
        vector_search=AsyncMock(),
        cache=AsyncMock(),
        series=AsyncMock(),
        arg_datos=AsyncMock(),
        georef=AsyncMock(),
        ckan=AsyncMock(),
        sesiones=AsyncMock(),
        ddjj=MagicMock(),
        semantic_cache=AsyncMock(),
    )


class TestDispatchStepRetryOnTimeout:
    async def test_dispatch_step_retries_on_timeout_error(self):
        """_dispatch_step_with_retry retries on timeout-like errors."""
        service = _make_service()
        call_count = 0

        async def flaky_dispatch(step, nl_query=""):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TimeoutError("connection timed out")
            return [DataResult(
                source="test", portal_name="Test", portal_url="",
                dataset_title="Test", format="json", records=[{"data": 1}],
                metadata={},
            )]

        step = PlanStep(
            id="step_1", action="query_series",
            description="test", params={},
        )

        with patch.object(service, "_dispatch_step", side_effect=flaky_dispatch):
            results = await service._dispatch_step_with_retry(step, max_retries=2)

        assert len(results) == 1
        assert call_count == 3  # 1 initial + 2 retries

    async def test_dispatch_step_retries_on_503_in_message(self):
        """_dispatch_step_with_retry retries when exception message contains '503'."""
        service = _make_service()
        call_count = 0

        async def flaky_dispatch(step, nl_query=""):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("HTTP 503 Service Unavailable")
            return []

        step = PlanStep(
            id="step_1", action="query_series",
            description="test", params={},
        )

        with patch.object(service, "_dispatch_step", side_effect=flaky_dispatch):
            await service._dispatch_step_with_retry(step, max_retries=2)

        assert call_count == 2


class TestDispatchStepNoRetryOn400:
    async def test_dispatch_step_no_retry_on_client_error(self):
        """400-level errors should NOT be retried by _dispatch_step_with_retry."""
        service = _make_service()
        call_count = 0

        async def bad_request_dispatch(step, nl_query=""):
            nonlocal call_count
            call_count += 1
            raise ValueError("Bad request: invalid parameter")

        step = PlanStep(
            id="step_1", action="query_series",
            description="test", params={},
        )

        with (
            patch.object(service, "_dispatch_step", side_effect=bad_request_dispatch),
            pytest.raises(ValueError, match="Bad request"),
        ):
                await service._dispatch_step_with_retry(step, max_retries=2)

        # Should have been called only once -- no retries for non-transient errors
        assert call_count == 1

    async def test_is_retryable_returns_false_for_non_transient(self):
        """_is_retryable returns False for non-transient error messages."""
        service = _make_service()
        assert service._is_retryable(ValueError("invalid parameter")) is False
        assert service._is_retryable(KeyError("missing key")) is False
        assert service._is_retryable(RuntimeError("unexpected error")) is False

    async def test_is_retryable_returns_true_for_transient(self):
        """_is_retryable returns True for transient error messages."""
        service = _make_service()
        assert service._is_retryable(RuntimeError("connection refused")) is True
        assert service._is_retryable(RuntimeError("HTTP 503")) is True
        assert service._is_retryable(RuntimeError("read timeout")) is True
        assert service._is_retryable(RuntimeError("connection reset by peer")) is True
        assert service._is_retryable(TimeoutError("timed out")) is True


# ── SoftTimeLimitExceeded handling in collector tasks ──────────


class TestSoftTimeLimitHandling:
    def test_soft_time_limit_exception_is_importable(self):
        """Verify SoftTimeLimitExceeded can be imported from celery.exceptions."""
        from celery.exceptions import SoftTimeLimitExceeded

        exc = SoftTimeLimitExceeded("Task timed out")
        assert "Task timed out" in str(exc)
        # Verify it is an exception subclass
        assert isinstance(exc, Exception)

    def test_soft_time_limit_updates_retry_count(self):
        """Simulate the SoftTimeLimitExceeded handler logic from collector_tasks.

        The handler should increment retry_count and mark as permanently_failed
        when retry_count + 1 >= MAX_TOTAL_ATTEMPTS.
        """
        from app.infrastructure.celery.tasks.collector_tasks import MAX_TOTAL_ATTEMPTS

        # Simulate the DB state: retry_count starts at 0
        retry_count = 0

        # Simulate the handler logic (from collector_tasks.py)
        new_retry_count = retry_count + 1
        new_status = "permanently_failed" if new_retry_count >= MAX_TOTAL_ATTEMPTS else "error"

        assert new_status == "error"
        assert new_retry_count == 1

    def test_permanently_failed_after_max_attempts(self):
        """After MAX_TOTAL_ATTEMPTS, the dataset should be marked permanently_failed."""
        from app.infrastructure.celery.tasks.collector_tasks import MAX_TOTAL_ATTEMPTS

        # Simulate retry_count at MAX_TOTAL_ATTEMPTS - 1 (last attempt)
        retry_count = MAX_TOTAL_ATTEMPTS - 1

        new_retry_count = retry_count + 1
        new_status = "permanently_failed" if new_retry_count >= MAX_TOTAL_ATTEMPTS else "error"

        assert new_status == "permanently_failed"
        assert new_retry_count == MAX_TOTAL_ATTEMPTS

    def test_max_total_attempts_is_five(self):
        """Verify the configured MAX_TOTAL_ATTEMPTS value."""
        from app.infrastructure.celery.tasks.collector_tasks import MAX_TOTAL_ATTEMPTS

        assert MAX_TOTAL_ATTEMPTS == 5
