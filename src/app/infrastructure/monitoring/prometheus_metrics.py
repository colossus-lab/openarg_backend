"""Prometheus metrics definitions for OpenArg."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ── Request metrics ─────────────────────────────────────────

REQUEST_COUNT = Counter(
    "openarg_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

REQUEST_LATENCY = Histogram(
    "openarg_request_duration_seconds",
    "HTTP request latency in seconds",
    ["endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

# ── Connector metrics ───────────────────────────────────────

CONNECTOR_CALLS = Counter(
    "openarg_connector_calls_total",
    "Total data connector calls",
    ["connector", "status"],
)

CONNECTOR_LATENCY = Histogram(
    "openarg_connector_duration_seconds",
    "Data connector call latency in seconds",
    ["connector"],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

# ── Cache metrics ───────────────────────────────────────────

CACHE_OPS = Counter(
    "openarg_cache_operations_total",
    "Cache operations (get/set)",
    ["operation", "result"],
)

# ── LLM metrics ────────────────────────────────────────────

LLM_TOKENS = Counter(
    "openarg_llm_tokens_total",
    "Total LLM tokens consumed",
    ["model"],
)

LLM_CALLS = Counter(
    "openarg_llm_calls_total",
    "Total LLM API calls",
    ["provider", "status"],
)

# ── WebSocket metrics ──────────────────────────────────────

ACTIVE_WS_CONNECTIONS = Gauge(
    "openarg_active_ws_connections",
    "Currently active WebSocket connections",
)
