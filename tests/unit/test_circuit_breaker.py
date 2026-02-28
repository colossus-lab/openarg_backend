"""Tests for circuit breaker state machine."""
from __future__ import annotations

import time

import pytest

from app.infrastructure.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    get_circuit_breaker,
    circuit_breakers,
)


@pytest.fixture(autouse=True)
def _clear():
    circuit_breakers.clear()
    yield
    circuit_breakers.clear()


class TestCircuitBreaker:
    def test_starts_closed(self):
        cb = CircuitBreaker("test")
        assert cb.state == CircuitState.CLOSED
        assert not cb.is_open

    def test_opens_after_threshold(self):
        cb = CircuitBreaker("test", failure_threshold=3)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.is_open

    def test_stays_closed_below_threshold(self):
        cb = CircuitBreaker("test", failure_threshold=5)
        for _ in range(4):
            cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        assert not cb.is_open

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker("test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.failure_count == 0
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED

    def test_transitions_to_half_open_after_recovery_timeout(self):
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.01)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        time.sleep(0.02)
        assert not cb.is_open  # Triggers transition
        assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_success_closes(self):
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.01, success_threshold=2)
        cb.record_failure()
        time.sleep(0.02)
        _ = cb.is_open  # Trigger half_open

        cb.record_success()
        assert cb.state == CircuitState.HALF_OPEN  # needs 2 successes
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_half_open_failure_reopens(self):
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.01)
        cb.record_failure()
        time.sleep(0.02)
        _ = cb.is_open  # Trigger half_open

        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_to_dict(self):
        cb = CircuitBreaker("my_service")
        cb.record_failure()
        d = cb.to_dict()
        assert d["service"] == "my_service"
        assert d["state"] == "closed"
        assert d["failure_count"] == 1

    def test_get_circuit_breaker_singleton(self):
        cb1 = get_circuit_breaker("svc_a")
        cb2 = get_circuit_breaker("svc_a")
        assert cb1 is cb2

    def test_get_circuit_breaker_different_names(self):
        cb1 = get_circuit_breaker("svc_a")
        cb2 = get_circuit_breaker("svc_b")
        assert cb1 is not cb2
