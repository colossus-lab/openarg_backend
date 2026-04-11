# Spec: Connector BCRA

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Application + Infrastructure
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The BCRA connector allows OpenArg to answer user questions about **Argentina's official exchange rates**, querying the Banco Central de la República Argentina as the authoritative source. It feeds two flows: (a) conversational responses in the query pipeline ("how much is the dollar today?"), and (b) a daily snapshot persisted for ad-hoc queries via the SQL sandbox / NL2SQL.

It is the **only financial-monetary data connector** in the system and the only one that maintains a cached table refreshed daily.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Cotización** | Buy/sell price of a foreign currency on a specific day, officially published by BCRA. |
| **Moneda** | Foreign currency identified by ISO code (USD, EUR, BRL, etc.). The system default is USD. |
| **Daily snapshot** | Copy persisted in PostgreSQL of the full set of exchange rates in effect each day. |
| **DataResult** | Normalized domain entity that all connectors return to the pipeline (see `domain/entities/connectors/data_result.py`). |
| **PlanStep / ExecutionPlan** | Unit of work in the query pipeline; the planner decides whether to invoke BCRA and with which parameters. |
| **Connector** | Generic abstraction in OpenArg that encapsulates an external data source. BCRA is an instance (see [DEBT-001]). |

## 3. User Stories

### US-001 (P1) — Point query for exchange rate
**As** a user of the OpenArg chat, **I want** to ask "how much is the official dollar today?", **so that** I can get the authoritative value from BCRA without having to navigate to their portal.

- **Trigger**: user message classified by the planner as `query_bcra` with `tipo=cotizaciones` and `moneda=USD`
- **Precondition**: BCRA API accessible
- **Happy path**:
  - **Given** the user asks about the official dollar
  - **When** the pipeline executes the BCRA step
  - **Then** the system returns the quote in effect for the day or the last business day
- **Edge cases**:
  - No quote for today (weekend / holiday) → return last available
  - BCRA API down → silent degradation, respond with cached data if it exists

### US-002 (P1) — Historical series query
**As** a data analyst, **I want** to request "official dollar between March 1 and 31", **so that** I can analyze temporal variations.

- **Trigger**: planner generates a step with `tipo=cotizaciones`, `fecha_desde`, `fecha_hasta`
- **Happy path**:
  - **Given** a valid date range
  - **When** the pipeline executes the step
  - **Then** it returns a DataResult with one record per business day in the range
- **Edge cases**:
  - Range with future dates → only returns up to today
  - Range longer than BCRA supports → returns what is available without error

### US-003 (P1) — Query for a currency other than USD
**As** a user, **I want** to ask about EUR, BRL or other currencies, **so that** I can cover cases beyond the dollar.

- **Happy path**: same flow as US-001 but with a different ISO code
- **Edge case**: currency not supported by BCRA → return empty list without error

### US-004 (P2) — Daily snapshot for SQL sandbox
**As** the NL2SQL system, **I want** the `cache_bcra_cotizaciones` table to be available and updated daily, **so that** I can answer ad-hoc analytical queries without making inline HTTP calls.

- **Trigger**: Celery beat scheduled daily
- **Happy path**:
  - **Given** the scheduled time arrives
  - **When** the worker runs the snapshot
  - **Then** the `cache_bcra_cotizaciones` table is updated, registered in `datasets` + `cached_datasets`, and the embedding indexing is dispatched
- **Edge cases**:
  - BCRA API down → retry with backoff; if it fails definitively, keep the previous snapshot
  - Worker timeout → retry up to 3 times

### US-005 (P2) — On-demand snapshot trigger
**As** an administrator, **I want** to manually trigger the BCRA snapshot, **so that** I can force an update outside the schedule.

- **Trigger**: admin orchestrator endpoint (`ingest_connectors_orchestrator`)
- **Happy path**: same logic as US-004, invoked imperatively

## 4. Functional Requirements

- **FR-001**: The system MUST fetch official quotes from BCRA on demand from the query pipeline.
- **FR-002**: The system MUST support filtering by currency code (ISO).
- **FR-003**: The system MUST support filtering by date range (`desde`/`hasta`).
- **FR-004**: The system MUST maintain a daily snapshot persisted in PostgreSQL, accessible via the SQL sandbox.
- **FR-005**: The system MUST be resilient to transient BCRA failures (retries with backoff).
- **FR-006**: The system MUST NOT expose raw BCRA errors to the end user; failures are handled with silent degradation.
- **FR-007**: The system MUST register the daily snapshot in the `datasets` and `cached_datasets` tables so that it appears in the unified catalog.
- **FR-008**: The snapshot MUST trigger the vector indexing (embeddings) of the resulting dataset.
- **FR-009**: The connector MUST expose a homogeneous interface with the other connectors (returning `DataResult`). *[Currently partial — see [DEBT-001]]*
- **FR-010**: The system MUST support on-demand snapshot triggering from the admin orchestrator.

## 5. Success Criteria

- **SC-001**: The daily snapshot completes successfully on **≥95% of business days**.
- **SC-002**: The "dollar today" query via the pipeline responds in **≤3 seconds** under normal conditions (BCRA online + warm cache).
- **SC-003**: Snapshot data is **no more than 24 hours old** on business days.
- **SC-004**: When BCRA is down, the system **does not fail catastrophically**: it returns an empty list or previously cached data and the pipeline continues.
- **SC-005**: The daily snapshot **does not exceed 5 minutes** of execution (current soft timeout: 300s).

## 6. Assumptions & Out of Scope

### Assumptions
- BCRA publishes official quotes for the dollar on business days before 04:00 ART of the following day.
- The public endpoint `estadisticascambiarias/v1.0/Cotizaciones` will remain available with the current format.
- BCRA does not impose severe rate limits for moderate institutional use. *[See CL-007]*
- The user accepts a delay of ≤24h in snapshot data (real-time quotes are not required).

### Out of scope
- **Monetary statistical variables** (monetary base, M2/M3, reserves, monetary policy rate). The v2 endpoint that served them was deprecated by BCRA; the adapter methods that promised them are broken *[see [DEBT-002]]*.
- **Parallel market quotes** (dólar blue, MEP, CCL). We only handle the official one published by BCRA.
- **Alerts or notifications** of sharp changes in quotes.
- **Forecasts or econometric analysis** — OpenArg only returns raw data.

## 7. Open Questions

- **[RESOLVED CL-001]** — Duplicate methods `get_principales_variables` and `get_variable_historica` both call the same `/Cotizaciones` endpoint. **Decision**: remove both, leave only `get_cotizaciones`. The v2 PrincipalesVariables endpoint was deprecated by BCRA and there is no migration to another source. See [`FIX_BACKLOG.md#fix-002-bcra-deduplicate-broken-methods`](../../FIX_BACKLOG.md).
- **[RESOLVED CL-002]** — **Dead code**. Grep confirms that **there are no callers** to `BCRAAdapter.search()` in the codebase. The pipeline step `execute_bcra_step` calls `get_cotizaciones()` / `get_principales_variables()` / `get_variable_historica()` directly (`bcra.py:28-46`). **Action**: remove `search()` from the adapter along with FIX-002 (dedup). It has no consumers.
- **[RESOLVED CL-003]** — Schedule `crontab(hour=4, minute=0)`: **will be interpreted as ART (America/Argentina/Buenos_Aires)**. Requires explicitly setting `celery_app.conf.timezone` (today it is not configured, interprets UTC by default). See [`FIX_BACKLOG.md#fix-003-bcra-schedule-timezone--art`](../../FIX_BACKLOG.md).
- **[NEEDS CLARIFICATION CL-004]** — What happens with currencies other than USD? The endpoint returns them all and filtering is client-side. Do we need to register separate snapshots per currency?
- **[RESOLVED CL-005]** — Silent degradation **is not acceptable**. When BCRA fails, the system MUST surface the error to the user: add an entry to `step_warnings`, increment connector metrics, integrate the circuit breaker, and let the analyst explicitly mention that data was missing. No more silent `return []`. See [`FIX_BACKLOG.md#fix-001-bcra-silent-degradation--surface-errors`](../../FIX_BACKLOG.md).
- **[PARTIAL CL-006]** — **No explanatory comment in code**. It is a hardcoded literal in `bcra_adapter.py:32`, treated as a static User-Agent. It has not been verified against official BCRA docs whether the public API **requires** this exact token, accepts any value, or does not need it at all. **Suggested action**: test manually with curl without the header against `https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones` to verify. Low priority — the current value works in production.
- **[NEEDS CLARIFICATION CL-007]** — BCRA API rate limits are not documented in the code. Do we know them? Are we getting close to them with the current schedule?

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Missing port in `domain/ports/connectors/bcra.py`**. The homologous connectors (`series_tiempo`, `argentina_datos`, `ckan_search`, `sesiones`, `staff`, `georef`) do have an abstract interface. BCRA breaks the pattern: the concrete `BCRAAdapter` class lives only in `infrastructure/adapters/connectors/`. Impact: the pipeline step depends directly on the implementation, blocking testing with doubles and making source swapping harder. **Priority: medium**.

- **[DEBT-002]** — **Methods with misleading names**. `get_principales_variables()` and `get_variable_historica(id_variable, ...)` both end up calling the same `/Cotizaciones` endpoint. The comment mentions that v2 was deprecated, but the name still promises something that is not delivered. Impact: confused calling code, "dead" features that appear alive. **Priority: high** (risk of incorrect use).

- **[DEBT-003]** — **Pipeline step depends on the concrete adapter, not on an interface**. `application/pipeline/connectors/bcra.py:20` types the parameter as `BCRAAdapter | None` directly (although it uses `TYPE_CHECKING`). It should depend on the port from [DEBT-001]. **Priority: medium** (tied to DEBT-001).

- **[DEBT-004]** — **Celery task bypasses Dishka DI**. `bcra_tasks.py:89` does `from ...BCRAAdapter` inline inside the helper function. It does not go through the DI container. Impact: possible configuration divergence between request-path and worker-path. **Priority: medium**.

- **[DEBT-005]** — **Non-ergonomic async/sync mix**. `_fetch_bcra_data()` in `bcra_tasks.py:87-96` wraps an async call with `asyncio.run()` inside a sync Celery worker. Functional but fragile (e.g., new event loop on each call, does not reuse HTTP client). **Priority: low**.

- **[DEBT-006]** — **Direct coupling to `datasets` / `cached_datasets` schema**. `_register_dataset()` in `bcra_tasks.py:25-84` uses raw SQL with detailed knowledge of the schema. It should use the `IDatasetRepository` port from the domain. **Priority: medium**.

- **[DEBT-007]** — **Silent degradation without observability**. `execute_bcra_step` in `application/pipeline/connectors/bcra.py:48-50` catches *any* exception, logs it and returns `[]`. It does not emit failure metrics nor integrate with the circuit breaker. The end user does not know that the data did not come from BCRA. **Priority: high**.

- **[DEBT-008]** — **No integration with the `infrastructure/resilience/` circuit breaker**. Other connectors do use it. BCRA only has `@with_retry` at the method level. **Priority: low-medium**.

- **[DEBT-009]** — **No connector metrics**. The system's `MetricsCollector` does not record calls to BCRA, times, errors. Invisible in observability. **Priority: medium**.

---

**End of spec.md** — See [./plan.md](./plan.md) for the current implementation.
