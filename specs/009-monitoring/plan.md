# Plan: Monitoring (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Infrastructure | `HealthCheckService` | `infrastructure/monitoring/health.py` |
| Infrastructure | `MetricsCollector` | `infrastructure/monitoring/metrics.py` |
| Infrastructure | `MetricsMiddleware` | `infrastructure/monitoring/middleware.py` |
| Presentation | `health_router.py` | `presentation/http/controllers/health/health_router.py` |
| Presentation | `metrics_router.py` | `presentation/http/controllers/monitoring/metrics_router.py` |

## 2. HealthCheckService

```python
class HealthCheckService:
    async def check_all(self) -> dict:
        # Run in parallel
        results = await asyncio.gather(
            self._check_postgres(),
            self._check_redis(),
            self._check_ddjj(),
            self._check_sesion_chunks(),
            self._check_circuit_breakers(),
            self._check_stuck_tasks(),
            return_exceptions=True,
        )
        components = {...}
        status = "healthy" if all_ok else "degraded"
        return {"status": status, "components": components}

    async def _check_postgres(self) -> dict:
        # SELECT 1, measure latency
        ...

    async def _check_redis(self) -> dict:
        # PING, measure latency
        ...

    async def _check_ddjj(self) -> dict:
        # DDJJAdapter.record_count > 0
        ...

    async def _check_stuck_tasks(self) -> dict:
        # Count downloads stuck > 30min
        # Count queries in intermediate state > 30min
        # Count errors in last 24h
        ...
```

## 3. MetricsCollector

```python
class MetricsCollector:  # Singleton
    _instance = None
    _lock = threading.Lock()

    def record_request(self, endpoint: str, method: str, status: int, duration_ms: float): ...
    def record_connector_call(self, connector: str, success: bool, duration_ms: float): ...
    def record_cache_event(self, hit: bool): ...
    def record_tokens(self, count: int, model: str): ...

    def get_metrics(self) -> dict:
        return {
            "uptime_seconds": ...,
            "requests": {
                "total": ...,
                "per_endpoint": {...},
                "avg_latency_ms": ...,
            },
            "connectors": {
                "BCRA": {"calls": N, "errors": M, "avg_latency_ms": L},
                ...
            },
            "cache": {
                "hits": ...,
                "misses": ...,
                "hit_rate": ...,
            },
            "tokens": {
                "total": ...,
                "per_model": {...},
            },
        }
```

## 4. MetricsMiddleware (ASGI)

```python
class MetricsMiddleware:
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        start = time.monotonic()
        status = [500]
        async def send_wrapper(msg):
            if msg["type"] == "http.response.start":
                status[0] = msg["status"]
            await send(msg)
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration_ms = (time.monotonic() - start) * 1000
            MetricsCollector().record_request(
                scope["path"], scope["method"], status[0], duration_ms
            )
```

## 5. Endpoints

| Method | Path | Auth | Behavior |
|---|---|---|---|
| GET | `/health` | Optional X-API-Key | Full component status |
| GET | `/health/ready` | None | `{"status": "ready"}` always |
| GET | `/api/v1/metrics` | None | In-memory metrics JSON |
| GET | `/metrics/prometheus` | None | Prometheus format |

## 6. Source Files

- `infrastructure/monitoring/{health,metrics,middleware}.py`
- `presentation/http/controllers/health/health_router.py`
- `presentation/http/controllers/monitoring/metrics_router.py`

## 7. Deviations from Constitution

- **Principle VII (Observability)**: partial implementation (in-memory metrics, not exported, no Sentry, no traces).

---

**End of plan.md**
