"""Tests for in-memory MetricsCollector."""
from __future__ import annotations

import pytest

from app.infrastructure.monitoring.metrics import MetricsCollector


@pytest.fixture
def metrics():
    # Reset singleton for test isolation
    MetricsCollector._instance = None
    m = MetricsCollector()
    return m


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    MetricsCollector._instance = None


class TestMetricsCollector:
    def test_singleton(self, metrics):
        m2 = MetricsCollector()
        assert m2 is metrics

    def test_record_request(self, metrics):
        metrics.record_request()
        metrics.record_request(error=True)
        data = metrics.get_metrics()
        assert data["requests"]["total"] == 2
        assert data["requests"]["errors"] == 1

    def test_record_connector_call(self, metrics):
        metrics.record_connector_call("series_tiempo", 150.0)
        metrics.record_connector_call("series_tiempo", 200.0, error=True)
        data = metrics.get_metrics()
        assert data["connectors"]["series_tiempo"]["calls"] == 2
        assert data["connectors"]["series_tiempo"]["errors"] == 1
        assert data["connectors"]["series_tiempo"]["avg_latency_ms"] == 175.0

    def test_cache_metrics(self, metrics):
        metrics.record_cache_hit()
        metrics.record_cache_hit()
        metrics.record_cache_miss()
        data = metrics.get_metrics()
        assert data["cache"]["hits"] == 2
        assert data["cache"]["misses"] == 1
        assert data["cache"]["hit_rate_percent"] == pytest.approx(66.7, abs=0.1)

    def test_token_tracking(self, metrics):
        metrics.record_tokens_used(500)
        metrics.record_tokens_used(300)
        data = metrics.get_metrics()
        assert data["tokens"]["total_used"] == 800

    def test_uptime(self, metrics):
        data = metrics.get_metrics()
        assert data["uptime_seconds"] >= 0

    def test_empty_cache_rate(self, metrics):
        data = metrics.get_metrics()
        assert data["cache"]["hit_rate_percent"] == 0

    def test_connector_latency_buffer(self, metrics):
        # Add 150 entries — should only keep last 100
        for i in range(150):
            metrics.record_connector_call("test_svc", float(i))
        data = metrics.get_metrics()
        # Avg should be of last 100 entries (50..149), not all 150
        assert data["connectors"]["test_svc"]["calls"] == 150
