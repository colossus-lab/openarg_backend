from __future__ import annotations

import threading
import time
from typing import Any


class MetricsCollector:
    """In-memory metrics collector (singleton)."""

    _instance: MetricsCollector | None = None
    _lock = threading.Lock()

    def __new__(cls) -> MetricsCollector:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._start_time = time.monotonic()
        self._request_count = 0
        self._request_errors = 0
        self._connector_calls: dict[str, int] = {}
        self._connector_errors: dict[str, int] = {}
        self._connector_latencies: dict[str, list[float]] = {}
        self._cache_hits = 0
        self._cache_misses = 0
        self._tokens_used = 0
        self._lock_data = threading.Lock()

    def record_request(self, error: bool = False) -> None:
        with self._lock_data:
            self._request_count += 1
            if error:
                self._request_errors += 1

    def record_connector_call(
        self, connector: str, latency_ms: float, error: bool = False
    ) -> None:
        with self._lock_data:
            self._connector_calls[connector] = self._connector_calls.get(connector, 0) + 1
            if error:
                self._connector_errors[connector] = self._connector_errors.get(connector, 0) + 1
            if connector not in self._connector_latencies:
                self._connector_latencies[connector] = []
            # Keep last 100 latencies per connector
            lats = self._connector_latencies[connector]
            lats.append(latency_ms)
            if len(lats) > 100:
                self._connector_latencies[connector] = lats[-100:]

    def record_cache_hit(self) -> None:
        with self._lock_data:
            self._cache_hits += 1

    def record_cache_miss(self) -> None:
        with self._lock_data:
            self._cache_misses += 1

    def record_tokens_used(self, tokens: int) -> None:
        with self._lock_data:
            self._tokens_used += tokens

    def get_metrics(self) -> dict[str, Any]:
        with self._lock_data:
            uptime_s = round(time.monotonic() - self._start_time, 1)

            connectors = {}
            for name in set(list(self._connector_calls) + list(self._connector_errors)):
                calls = self._connector_calls.get(name, 0)
                errors = self._connector_errors.get(name, 0)
                lats = self._connector_latencies.get(name, [])
                avg_lat = round(sum(lats) / len(lats), 1) if lats else 0
                connectors[name] = {
                    "calls": calls,
                    "errors": errors,
                    "avg_latency_ms": avg_lat,
                }

            total_cache = self._cache_hits + self._cache_misses
            cache_rate = round(self._cache_hits / total_cache * 100, 1) if total_cache else 0

            return {
                "uptime_seconds": uptime_s,
                "requests": {
                    "total": self._request_count,
                    "errors": self._request_errors,
                },
                "connectors": connectors,
                "cache": {
                    "hits": self._cache_hits,
                    "misses": self._cache_misses,
                    "hit_rate_percent": cache_rate,
                },
                "tokens": {
                    "total_used": self._tokens_used,
                },
            }
