# Spec: Monitoring & Health Checks

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Infrastructure + Presentation
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

**Operational observability** module: exposes component-level health checks (`GET /health`), in-memory metrics (`GET /api/v1/metrics`, `GET /metrics/prometheus`), and captures metrics via ASGI middleware. Enables outage detection, per-endpoint/connector performance tracking, and exposure for Prometheus scraping.

It is the module that **does not depend on Sentry** (not yet configured — see open debt in `MEMORY.md`).

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Health check** | Synthetic query to each component (PG, Redis, DDJJ, Sesiones, Pipeline) with latency measurement. |
| **Component status** | `healthy` | `degraded` | `unhealthy` per component. |
| **MetricsCollector** | In-memory singleton with thread-safe counters (requests, connectors, cache, tokens). |
| **Prometheus endpoint** | Export in `prometheus_client` format for scraping. |
| **Stuck task** | Dataset in `downloading` state for >30min or user_query in an intermediate state for >30min. |

## 3. User Stories

### US-001 (P1) — Health check for the load balancer
**As** Caddy / a load balancer, **I want** a simple `/health/ready` endpoint that returns 200 if the service is ready.

### US-002 (P1) — Detailed health check
**As** an operator, **I want** to see the detailed state of PG, Redis, DDJJ, Sesiones, stuck tasks, circuit breakers, pipeline.

### US-003 (P1) — Runtime metrics
**As** an operator, **I want** to see how many requests per endpoint, how many calls per connector, cache hit rate, tokens consumed.

### US-004 (P2) — Prometheus scraping
**As** an external observability system, **I want** to scrape `/metrics/prometheus` every 30s.

## 4. Functional Requirements

- **FR-001**: MUST expose `GET /health` with parallel checks: postgres, redis, ddjj, sesion_chunks, circuit_breakers, stuck_tasks.
- **FR-002**: `/health` MUST return `{status: "healthy"|"degraded"|"unhealthy", components: {...}}` with latency per component.
- **FR-003**: MUST expose a simple `GET /health/ready` (always 200) for liveness probes.
- **FR-004**: MUST expose `GET /api/v1/metrics` with in-memory metrics: uptime, requests per endpoint, connector stats, cache hit rate, tokens.
- **FR-005**: MUST expose `GET /metrics/prometheus` in Prometheus format.
- **FR-006**: The `MetricsCollector` MUST be thread-safe (threading.Lock).
- **FR-007**: The `MetricsMiddleware` ASGI MUST capture latency for each request.
- **FR-008**: MUST support optional auth with `X-API-Key` on `/health` (if `BACKEND_API_KEY` is set, returns minimal info without the key).
- **FR-009**: Stuck task detection: `cached_datasets.status='downloading' AND updated_at < now() - 30min`.
- **FR-010**: Recent errors count: 24h window.

## 5. Success Criteria

- **SC-001**: `/health/ready` responds in **<50ms**.
- **SC-002**: `/health` full check responds in **<500ms (p95)**.
- **SC-003**: Metrics have no drift (thread-safety).
- **SC-004**: Prometheus scrape does not block normal requests.

## 6. Assumptions & Out of Scope

### Assumptions
- In-memory metrics are lost on restart (accepted).
- We do not need distributed observability in the short term.
- Prometheus scrape is optional (not configured upstream today).

### Out of scope
- **Sentry** (error monitoring) — known debt, not configured.
- **Distributed traces** (OpenTelemetry) — not implemented.
- **Alerting** — no alert rules defined.
- **Dashboard** (Grafana) — responsibility of the operational layer, not the code.
- **Log aggregation** — infra responsibility (CloudWatch, ELK, etc.).

## 7. Open Questions

- **[NEEDS CLARIFICATION CL-001]** — Is anyone currently scraping `/metrics/prometheus`? The endpoint exists but it is unknown whether it has a consumer.
- **[RESOLVED CL-002]** — Sentry **accepted debt, not a priority**. No configuration plan in the short term. The project still has no external error monitoring; it depends on local logs and the in-memory `MetricsCollector`.
- **[RESOLVED CL-003]** — **Hardcoded**. In `health.py:116` the query uses the literal SQL `INTERVAL '30 minutes'`. No env var or settings. To change the threshold, edit the SQL.
- **[RESOLVED CL-004]** — **Symbolic**. `/health/ready` returns `{"status": "ready"}` unconditionally without real checks (see `health_router.py:32-34`). Useful as a simple liveness probe (the process is alive), not as real readiness. For effective readiness use `/health` which does perform component checks via `HealthCheckService.check_all()`.

## 8. Tech Debt Discovered

- **[DEBT-001]** — **`MetricsCollector` is an in-memory singleton** — it resets on every restart, does not persist.
- **[DEBT-002]** — **Latency samples keep only the last 100** per connector (FIFO, not a real histogram).
- **[DEBT-003]** — **Sentry not configured** — no external error monitoring.
- **[DEBT-004]** — **No distributed traces** — limited cross-service debugging.
- **[DEBT-005]** — **`/health/ready` performs no real checks** — always 200.

---

**End of spec.md**
