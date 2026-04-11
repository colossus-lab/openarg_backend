# Spec: Connectors (Generic Pattern)

**Type**: Reverse-engineered (abstraction extracted from multiple specific connectors)
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Application + Infrastructure
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

A **Connector** is the component that encapsulates a heterogeneous external data source and transforms it into a common entity the rest of the system understands (`DataResult`). It is the key abstraction that lets the query pipeline, scheduled ingestion, the SQL sandbox, and the dataset catalog speak the same language even though the sources are: REST APIs (BCRA, Argentina Datos), CKAN portals (datos.gob.ar, CABA, provincial CKANs), DKAN portals (Rosario, Jujuy), flat files (CSV/Excel from Senado, HCDN), statistical APIs (Series de Tiempo, INDEC), or legislative datasets.

Each specific connector (see `002a-bcra/`, `002b-series-tiempo/`, etc.) is a **concrete instance** of this pattern. This spec documents what is **common**.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Connector** | OpenArg component that reads data from an external source and returns `DataResult`. |
| **DataResult** | Domain DTO with normalized fields: `source`, `portal_name`, `portal_url`, `dataset_title`, `format`, `records`, `metadata`. It is the common language between all connectors and the pipeline. |
| **PlanStep** | Unit of work in the query pipeline; it has an `action` (e.g. `query_bcra`, `search_ckan`, `query_series`) and free-form `params`. The planner decides which action to dispatch. |
| **Snapshot** | Scheduled execution that downloads data from a source and persists it in a cached PostgreSQL table for ad-hoc analytical queries. |
| **Ingest vs Query** | Two usage modes: **Ingest** runs in Celery beat, populating the DB. **Query** runs on the pipeline request path and returns `DataResult` on the fly. |

## 3. User Stories (generic level)

### US-001 (P1) — The pipeline queries a connector on-demand
**As** the query pipeline, **I want** to invoke any connector with a `PlanStep` and receive a `DataResult`, **so** that it can feed the Analyst's analysis.

- **Trigger**: the planner emits a `PlanStep` with an `action` that matches a connector
- **Happy path**: the pipeline calls `execute_<connector>_step(step, adapter)` and receives `list[DataResult]`
- **Edge case**: the connector fails → returns `[]` and the pipeline continues with the other sources

### US-002 (P1) — Scheduled snapshot refreshes data
**As** the system, **I need** every connector that supports snapshots to refresh its data at the configured frequency (daily/weekly/monthly), **so** the cached tables stay alive.

- **Trigger**: Celery beat
- **Happy path**: task runs, downloads data, writes to `cache_<connector>_*`, registers in `datasets` + `cached_datasets`, triggers embedding indexing
- **Edge case**: upstream source down → retry with backoff; on definitive failure, keep the previous snapshot

### US-003 (P1) — Manual trigger from admin
**As** an administrator, **I want** to trigger a specific snapshot outside the schedule, **so** I can force an ad-hoc update.

- **Trigger**: POST to admin tasks_router
- **Happy path**: enqueues the connector task on the corresponding queue

### US-004 (P2) — Connector health check
**As** an operator, **I want** to know whether a connector is healthy (endpoint reachable, last execution successful), **so** I can detect problems proactively.

- **Trigger**: `GET /health` with per-component check
- **Current state**: only DDJJ and sesiones have health checks. The rest are missing (see debt).

## 4. Functional Requirements

- **FR-001**: Every connector MUST normalize its output to `DataResult`.
- **FR-002**: Every connector MUST expose a port in `domain/ports/connectors/` with an abstract interface (ABC). *[BCRA is the only known exception — `[002a-bcra/DEBT-001]`]*
- **FR-003**: Every connector MUST use `@with_retry` for transient HTTP failures.
- **FR-004**: Every connector MUST handle graceful degradation: a failing source does not take down the pipeline.
- **FR-005**: Every connector supporting snapshots MUST register in `datasets` and `cached_datasets` upon completion.
- **FR-006**: Every connector supporting snapshots MUST trigger embedding indexing (`index_dataset_embedding.delay(dataset_id)`) upon completion.
- **FR-007**: Every connector used in the query pipeline MUST have a corresponding module in `application/pipeline/connectors/` with an `execute_<name>_step(step, adapter) -> list[DataResult]` function.
- **FR-008**: Every connector MUST have an entry in `orchestrator_tasks.py` if it supports a manual trigger from admin.
- **FR-009**: Every connector MUST have an entry in `celery/app.py::beat_schedule` if it supports periodic snapshots.
- **FR-010**: Every connector MUST use `ConnectorError` with a specific `ErrorCode.CN_<NAME>_*` for upstream errors.
- **FR-011**: Every connector MUST honor the timeouts and rate limits of the external source (no spamming).
- **FR-012**: Every connector MUST have unit tests for the adapter (mocking HTTP) and for its pipeline step (with an adapter double).

## 5. Success Criteria

- **SC-001**: **Architectural consistency**: 100% of connectors follow the 3-layer pattern (port + adapter + pipeline step). *[Currently: BCRA breaks this]*
- **SC-002**: **Snapshot success rate** ≥95% per connector over the last 30 days (no metric today, target).
- **SC-003**: Every connector responds in `<5 seconds (p95)` to a pipeline query under normal conditions.
- **SC-004**: A downed connector **does not prevent** other connectors from responding in the same query.
- **SC-005**: Data cached by snapshot is refreshed **within 24 hours** of its schedule (on days when the source is available).

## 6. Assumptions & Out of Scope

### Assumptions
- External sources remain exposed via APIs or files accessible without paid commercial authentication.
- The combined pipeline latency tolerates HTTP calls to several connectors in parallel (not all sources are queried on every query).
- The source catalog grows slowly (a new one every few weeks, not hundreds per day).

### Out of scope (at the pattern level)
- **Change streaming** (CDC) from sources — all connectors are pull, not push.
- **Cross-connector deduplication** — if two connectors bring the same datum, there is no merge.
- **Complex semantic transformations** — each connector normalizes but does not interpret; the pipeline does the interpretation.
- **Upstream authentication** — no current connector uses real credentials; all sources are public.

## 7. Open Questions (generic pattern)

- **[NEEDS CLARIFICATION CL-001]** — When should a connector have a scheduled snapshot and when only query-on-demand? Today the decision appears ad-hoc. There should be explicit criteria (e.g. frequency of upstream data change, HTTP cost, expected sandbox usage).
- **[NEEDS CLARIFICATION CL-002]** — What is the naming convention for planner actions? Some are `query_X` (query_bcra, query_series), others are `search_X` (search_ckan). Is there a semantic difference or is it historical inconsistency?
- **[NEEDS CLARIFICATION CL-003]** — All connectors should honor the global circuit breaker, but today only some integrate it. Remediation plan?
- **[RESOLVED CL-004]** — 10 downed CKAN portals: **keep them in the active catalog with a circuit breaker**. Architectural decision: the cost of keeping entries blocked by CB is lower than the risk of removing portals that may come back online in the future. The circuit breaker protects them without affecting performance.
- **[NEEDS CLARIFICATION CL-005]** — Is there an SLO per connector? Today the only health check is binary (UP/DOWN) for 2 connectors; the rest are missing.

## 8. Tech Debt Discovered (generic pattern)

- **[DEBT-001]** — **Inconsistency in ports**: BCRA has no abstract port while the rest do (`series_tiempo`, `argentina_datos`, `ckan_search`, `sesiones`, `staff`, `georef`). See `002a-bcra/[DEBT-001]`.
- **[DEBT-002]** — **Pipeline steps coupled to concrete adapters**: several pipeline steps type the parameter as the adapter instead of the port (revealed in BCRA, likely in others). See individual debt per connector.
- **[DEBT-003]** — **Celery tasks bypass Dishka DI**: recurring pattern — tasks import adapters inline and do not go through the container. Specific symptoms in each connector with snapshots.
- **[DEBT-004]** — **No connector observability in `MetricsCollector`**: calls, times, and errors per connector are not recorded. Invisible in dashboards.
- **[DEBT-005]** — **Circuit breaker not uniformly applied**: it exists in `infrastructure/resilience/circuit_breaker.py` but is not wired into all connectors.
- **[DEBT-006]** — **Raw SQL directly to `datasets`/`cached_datasets` in each snapshot**: duplication of the `_register_dataset()` pattern across connectors. There should be a shared helper or a use case in `application/`.
- **[DEBT-007]** — **Async/sync mixing via `asyncio.run()`** in every Celery task that consumes an async adapter. Fragile pattern, not DRY.
- **[DEBT-008]** — **Unversioned `cache_*` table schemas** (no Alembic migrations). Created with `df.to_sql(..., if_exists="replace")`. Impact: silent schema drift; the SQL sandbox can break on upstream changes.
- **[DEBT-009]** — **`DataResult.metadata` is an untyped `dict`**: each connector puts different keys without a contract. See `domain/entities/connectors/data_result.py:17` (`# metadata keys: total_records, fetched_at, description, units, last_updated`).
- **[DEBT-010]** — **`PlanStep.params` is an untyped `dict`**: each connector defines its keys independently without a schema.

## 9. Connector Inventory (index)

The individual specs live in subdirectories. This is the index:

| Connector | Spec | Type | Port | Snapshot | Query |
|---|---|---|---|---|---|
| BCRA | `002a-bcra/` | REST API | ❌ *[DEBT]* | ✅ daily | ✅ |
| Series de Tiempo | `002b-series-tiempo/` | REST API | ✅ | ✅ weekly | ✅ |
| Argentina Datos | `002c-argentina-datos/` | REST API | ✅ | — | ✅ |
| CKAN Search | `002d-ckan-search/` | CKAN | ✅ | — | ✅ |
| Sesiones (legislative) | `002e-sesiones/` | JSON | ✅ | ✅ weekly | ✅ |
| Staff (HCDN + Senado) | `002f-staff/` | JSON | ✅ | ✅ weekly | ✅ |
| Georef | `002g-georef/` | REST API | ✅ | — | ✅ |
| DDJJ | `002h-ddjj/` | local JSON | ❌? | partial | ✅ |
| Presupuesto | `002i-presupuesto/` | CSV/PEF | — | ✅ monthly | via sandbox |
| Senado (datos) | `002j-senado/` | Files | — | ✅ weekly | via sandbox |
| INDEC | `002k-indec/` | — | — | ✅ monthly | via sandbox |
| BAC | `002l-bac/` | — | — | ✅ weekly | via sandbox |
| DKAN Portals | `002m-dkan-portals/` | DKAN | via `IDataSource` | ✅ weekly | via CKAN search |
| CKAN portals (datos_gob_ar, CABA) | `002n-ckan-portals/` | CKAN | via `IDataSource` | ✅ various | via CKAN search |
| Gobernadores | `002o-gobernadores/` | — | — | — | — |
| Córdoba Legislativo | `002p-cordoba-leg/` | — | — | — | — |
| Mapa del Estado | `002q-mapa-estado/` | — | — | — | — |
| Catalog Enrichment | `002r-catalog-enrichment/` | meta-task | — | ✅ | — |

*[Note: Snapshot/Query/Port columns marked on a best-effort reverse-engineering basis; they are refined in each individual spec.]*

---

**End of spec.md** — See [./plan.md](./plan.md) for the as-built pattern.
