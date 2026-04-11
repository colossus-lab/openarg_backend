# Plan: Connector BCRA (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File | Note |
|---|---|---|---|
| **Domain / Entities** | `DataResult`, `PlanStep`, `ExecutionPlan` | `domain/entities/connectors/data_result.py` | Shared with all connectors |
| **Domain / Ports** | Absent | `domain/ports/connectors/bcra.py` | **[DEBT-001]** — An `IBCRAConnector(ABC)` analogous to `ISeriesTiempoConnector` should exist |
| **Application / Pipeline** | `execute_bcra_step` | `application/pipeline/connectors/bcra.py` | Orchestrates the call from a plan step in the query pipeline |
| **Infrastructure / Adapter** | `BCRAAdapter` | `infrastructure/adapters/connectors/bcra_adapter.py` | Async HTTP client against BCRA API |
| **Infrastructure / Celery** | `snapshot_bcra` task + `_register_dataset` + `_fetch_bcra_data` | `infrastructure/celery/tasks/bcra_tasks.py` | Daily snapshot task |
| **Infrastructure / Resilience** | `@with_retry(max_retries=2)` | `infrastructure/resilience/retry.py` | Decorator applied to the 3 async adapter methods |
| **Presentation** | — | — | No direct HTTP endpoint; it is invoked only from the pipeline and from the admin orchestrator |

## 2. As-Built Flow

### 2.1 Query flow (query pipeline)

```
User (chat)
    ↓ HTTP POST /api/v1/query/smart
FastAPI router (smart_query_v2_router)
    ↓ dispatch to the LangGraph pipeline
Planner (Bedrock Claude Haiku)
    ↓ generates ExecutionPlan with PlanStep(action="query_bcra")
Collector / Gather node
    ↓ calls execute_bcra_step(step, bcra_adapter)
BCRAAdapter.get_cotizaciones(moneda, fecha_desde, fecha_hasta)
    ↓ HTTP GET https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones
    ↓ with @with_retry (up to 2 retries, exponential backoff)
BCRA API
    ↓ JSON {fecha, detalle:[...]}
BCRAAdapter parses and filters client-side by currency
    ↓ returns DataResult(source="bcra", records=[...])
Pipeline continues to the Analyst
    ↓
Response to the user
```

### 2.2 Daily snapshot flow

```
Celery Beat (daily 04:00)
    ↓ enqueues openarg.snapshot_bcra in the "ingest" queue
Worker (ingest queue, concurrency 2)
    ↓ snapshot_bcra() executes
_fetch_bcra_data()
    ↓ instantiates BCRAAdapter inline (bypass DI)
    ↓ asyncio.run(adapter.get_cotizaciones())
BCRAAdapter → BCRA API (with retry)
    ↓ DataResult.records
pd.DataFrame(records)
    ↓ df.to_sql("cache_bcra_cotizaciones", engine, if_exists="replace")
PostgreSQL
_register_dataset(engine, "bcra-cotizaciones", ..., df)
    ↓ UPSERT on `datasets` (source_id="bcra-cotizaciones", portal="bcra")
    ↓ UPSERT on `cached_datasets` (table_name="cache_bcra_cotizaciones", status="ready")
    ↓ obtains dataset_id
index_dataset_embedding.delay(dataset_id)
    ↓ dispatches to the embedding queue
End
```

## 3. Ports & Contracts

### 3.1 Consumed
- **[MISSING]** `IBCRAConnector` — does not exist as a domain port. The adapter is exposed directly. **[DEBT-001]**
- `get_sync_engine()` from `celery/tasks/_db.py` for a sync connection to PostgreSQL inside the task
- `index_dataset_embedding` task (from `scraper_tasks`) dispatched at the end of the snapshot

### 3.2 Exposed (de facto, by the concrete adapter)
Public methods of `BCRAAdapter`:

| Method | Params | Return | Actual behavior |
|---|---|---|---|
| `get_cotizaciones(moneda?, fecha_desde?, fecha_hasta?)` | Optional strings | `DataResult` | Works — calls `/Cotizaciones`, filters currency client-side |
| `get_principales_variables()` | — | `DataResult` | **Broken alias** — also calls `/Cotizaciones` **[DEBT-002]** |
| `get_variable_historica(id_variable, fecha_desde, fecha_hasta)` | int + strings | `DataResult` | **Broken alias** — ignores `id_variable`, calls `/Cotizaciones` **[DEBT-002]** |
| `search(query)` | string | `DataResult` | Shim — ignores `query`, delegates to `get_cotizaciones()` **[CL-002]** |

### 3.3 Internal contract used by the pipeline step
`execute_bcra_step(step: PlanStep, bcra: BCRAAdapter | None) -> list[DataResult]`

Expected `PlanStep.params`:
- `tipo: "cotizaciones" | "variables" | "historica"` (default: `"cotizaciones"`)
- `moneda: str` (default: `"USD"`)
- `fecha_desde: str` (optional, format `YYYY-MM-DD`)
- `fecha_hasta: str` (optional, format `YYYY-MM-DD`)
- `id_variable: int` (only for `tipo=historica`, currently ignored)

## 4. External Dependencies

| Attribute | Value |
|---|---|
| **Base URL** | `https://api.bcra.gob.ar` |
| **Main endpoint** | `/estadisticascambiarias/v1.0/Cotizaciones` |
| **HTTP Method** | `GET` |
| **Auth** | Header `Authorization: Bearer BCRA` (literal, not a real credential) **[CL-006]** |
| **User-Agent** | `OpenArg/1.0` |
| **HTTP Timeout** | 30s |
| **Retry policy (adapter)** | `@with_retry(max_retries=2)` — exponential backoff + jitter on 429/5xx |
| **Circuit breaker** | Not integrated **[DEBT-008]** |
| **Rate limits** | Unknown **[CL-007]** |
| **Official documentation** | https://www.bcra.gob.ar/Estadisticas/Datos_Abiertos.asp |

### Query parameters supported by the endpoint
- `fechaDesde`: `YYYY-MM-DD`
- `fechaHasta`: `YYYY-MM-DD`
- The `moneda` parameter **is no longer supported upstream** — filtering is done client-side.

### Response format
```json
{
  "results": {
    "fecha": "2026-04-09",
    "detalle": [
      {
        "codigoMoneda": "USD",
        "descripcion": "Dolar Estadounidense",
        "tipoCotizacion": 1234.56,
        ...
      },
      ...
    ]
  }
}
```

## 5. Persistence

### 5.1 Tables written by `snapshot_bcra`

#### `datasets` (UPSERT by `(source_id, portal)`)
| Field | Fixed value |
|---|---|
| `source_id` | `"bcra-cotizaciones"` |
| `portal` | `"bcra"` |
| `title` | `"Cotizaciones Cambiarias BCRA"` |
| `description` | `"Datos del BCRA: Cotizaciones Cambiarias BCRA"` |
| `organization` | `"Banco Central de la República Argentina"` |
| `url` | `"https://www.bcra.gob.ar/Estadisticas/Datos_Abiertos.asp"` |
| `download_url` | `""` (empty) |
| `format` | `"json"` |
| `columns` | JSON array of DataFrame columns |
| `tags` | `"bcra,monetario,cambiario,finanzas"` |
| `is_cached` | `true` |
| `row_count` | `len(df)` |
| `last_updated_at`, `updated_at` | Current UTC timestamp |

#### `cached_datasets` (UPSERT by `table_name`)
| Field | Value |
|---|---|
| `dataset_id` | FK to the id obtained from the previous UPSERT |
| `table_name` | `"cache_bcra_cotizaciones"` |
| `status` | `"ready"` |
| `row_count` | `len(df)` |
| `columns_json` | JSON of columns |
| `updated_at` | UTC Timestamp |

#### `cache_bcra_cotizaciones` (raw data table)
Created with `df.to_sql(..., if_exists="replace")`. Schema dynamically derived from the DataFrame. **It has no Alembic migration** — the task creates it on the fly.

### 5.2 Tables read
None directly by BCRA. Downstream, the SQL sandbox reads `cache_bcra_cotizaciones` and the embedding indexer reads `datasets` / `dataset_chunks`.

### 5.3 Referenced Alembic migrations
None of its own. `datasets` and `cached_datasets` are part of the core schema (base project migrations).

## 6. Async / Scheduled Work

### Task: `openarg.snapshot_bcra`

| Attribute | Value |
|---|---|
| **Module** | `app.infrastructure.celery.tasks.bcra_tasks` |
| **Registered name** | `openarg.snapshot_bcra` |
| **Queue** | `ingest` (concurrency 2) |
| **Schedule** | `crontab(hour=4, minute=0)` — daily 04:00 (worker timezone **[CL-003]**) |
| **max_retries** | 3 (at Celery level) |
| **soft_time_limit** | 300s (5 min) |
| **time_limit** | 360s (6 min, hard kill) |
| **Retry on error** | `self.retry(exc=exc, countdown=60)` — waits 60s and retries |
| **Idempotency** | Yes, via UPSERT `ON CONFLICT` on `datasets` and `cached_datasets`, and `if_exists="replace"` on the data table |
| **Observability** | Only `logger.info` / `logger.exception` — no metrics **[DEBT-009]** |
| **Downstream dispatch** | `index_dataset_embedding.delay(dataset_id)` on successful completion |

### Manual trigger
`orchestrator_tasks.py:75` includes `{"name": "openarg.snapshot_bcra"}` in the admin orchestrator's list of connectors. This allows triggering it via the admin endpoint.

## 7. Observability

| Signal | Status | Location |
|---|---|---|
| **Structured logs** | Standard Python logger (`logging.getLogger`) — not structlog | `bcra_adapter.py:15`, `bcra_tasks.py:22` |
| **Metrics (MetricsCollector)** | Absent | **[DEBT-009]** |
| **Distributed traces** | Absent | — |
| **Audit trail** | Absent | — |
| **Health check** | BCRA is not part of `HealthCheckService` (only postgres/redis/ddjj/sesiones) | `infrastructure/monitoring/health.py` |
| **Circuit breaker state** | Not applied | **[DEBT-008]** |

## 8. Source Files

| File | Role |
|---|---|
| `src/app/infrastructure/adapters/connectors/bcra_adapter.py` | Async HTTP adapter (`BCRAAdapter`, 196 lines) |
| `src/app/application/pipeline/connectors/bcra.py` | Pipeline step (`execute_bcra_step`, 50 lines) |
| `src/app/infrastructure/celery/tasks/bcra_tasks.py` | Daily snapshot task (145 lines) |
| `src/app/infrastructure/celery/app.py:237-241` | Beat schedule entry `"snapshot-bcra"` |
| `src/app/infrastructure/celery/app.py:107` | Routing entry `"openarg.snapshot_bcra": {"queue": "ingest"}` |
| `src/app/infrastructure/celery/tasks/orchestrator_tasks.py:75` | Entry in the admin orchestrator list |
| `src/app/infrastructure/celery/tasks/transparency_tasks.py:219` | Reference to string literal `"bcra"` — review usage |
| `src/app/domain/entities/connectors/data_result.py` | Entities `DataResult`, `PlanStep`, `ExecutionPlan` |
| `src/app/infrastructure/resilience/retry.py` | `@with_retry` decorator |
| `src/app/domain/exceptions/error_codes.py` | `ErrorCode.CN_BCRA_UNAVAILABLE` |
| `src/app/domain/exceptions/connector_errors.py` | Base `ConnectorError` class |
| `src/app/domain/ports/connectors/bcra.py` | **[MISSING — DEBT-001]** |

## 9. Deviations from Constitution

> _Note: `constitution.md` does not yet exist. This section assumes the principles that will be extracted from `CLAUDE.md`. It will be updated once an official constitution exists._

- **Hexagonal principle "domain ports (ABC) → infrastructure adapters"** — **violated by [DEBT-001]** (BCRA has no port). Justification: none documented. Debt ticket: open.
- **Principle "All DI wiring in setup/ioc/provider_registry.py via Dishka"** — **violated by [DEBT-004]** (the task instantiates the adapter inline). Justification: working with the Celery sync engine requires bypassing the request scope. Solution: create a dedicated provider for the worker scope.
- **Principle "Async-first: all I/O uses async/await"** — **partially violated by [DEBT-005]**. Celery is not async-native; the workaround with `asyncio.run()` is acceptable but fragile.
- **Principle "Pydantic models for API schemas, dataclasses for domain entities"** — Complied with. `DataResult` is a dataclass.
- **Principle "Spanish comments in domain docstrings, English in infrastructure"** — Mostly complied with (`bcra_adapter.py:19` has a Spanish comment).

---

**End of plan.md** — See [./spec.md](./spec.md) for the "what" and success criteria.
