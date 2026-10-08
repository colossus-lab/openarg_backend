# OpenArg Backend

Backend service for OpenArg — AI-powered analysis of Argentine government open data. It scrapes public data portals into PostgreSQL, embeds the catalogue, keeps curated marts and live connectors, and answers natural-language questions for the web chat (`/api/v1/query/*`), the public API (`/api/v1/ask`) and the public MCP (`mcp_publico/`, mcp.openarg.org). See `README.md` for the full picture.

## Stack

- **Framework:** FastAPI 0.115 + Uvicorn (async, UVLoop)
- **Database:** PostgreSQL 16 + pgvector (HNSW indexing, 1024-dim embeddings) + PostGIS (migration 0025)
- **ORM:** SQLAlchemy 2.0 (async) + Alembic migrations
- **DI:** Dishka 1.6 (IoC container)
- **Workers:** Celery 5.4 + Redis 7 (broker + cache + results)
- **Answer engines** (chosen per turn with `ANSWERS_ENGINE`, code default `legacy`; `application/answers/engine.py`):
  - `agent`: Claude Sonnet 4.6 on AWS Bedrock (`BEDROCK_AGENT_MODEL`) in a tool loop, through the Anthropic SDK (`AsyncAnthropicBedrock`). The agent has answered in prod since 2026-10-03.
  - `legacy`: the LangGraph graph on Claude Haiku 4.5 on Bedrock (`BEDROCK_LLM_MODEL`) with Google Gemini (`GEMINI_MODEL`, default 2.5 Flash) as fallback. Haiku 4.5 is also the default model of the workers' LLM tasks.
- **Figure verifier:** `ANSWERS_VERIFY_MODE` = `off` / `shadow` (default) / `correct` (`application/answers/verification.py`)
- **Embeddings:** AWS Bedrock Cohere Embed Multilingual v3 (1024-dim)
- **AWS credentials:** boto3's default chain (instance role on the servers); no keys required in `.env`
- **HTTP:** HTTPX (async client)
- **Auth:** shared service key (`X-API-Key`) + Google ID token for the frontend; `oarg_sk_` user keys (Bearer) for the public API/MCP; `X-Admin-Key` for `/admin`
- **Rate Limiting:** SlowAPI + Redis counters (monthly quotas in `application/public_quota.py` and `application/web_quota.py`)
- **Config:** TOML files + Pydantic settings
- **Logging:** structlog

## Architecture (Hexagonal / Ports & Adapters)

```
src/app/
├── domain/                                      # Domain layer
│   ├── entities/                                # Dataclass entities, one package per area
│   │   ├── base.py                              # BaseEntity (id, created_at, updated_at)
│   │   ├── dataset/                             # Dataset, cached data, catalog resources
│   │   ├── chat/ user/ api_key/ credits/        # Conversations, users, keys, credits
│   │   ├── connectors/data_result.py            # DataResult (what every connector/tool returns)
│   │   └── agent/ query/ serving/ staff/
│   ├── ports/                                   # Abstract interfaces (ABC / Protocol)
│   │   ├── llm/llm_provider.py                  # ILLMProvider, IEmbeddingProvider
│   │   ├── llm/agent_llm.py                     # IAgentLLM (tool-calling turns, streaming)
│   │   ├── search/vector_search.py              # IVectorSearch
│   │   ├── sandbox/sql_sandbox.py               # ISQLSandbox: execute_readonly
│   │   ├── cache/cache_port.py                  # ICacheService
│   │   ├── connectors/*.py                      # ISeriesTiempoConnector, IBCRAConnector, ...
│   │   └── source/ dataset/ serving/ chat/ user/ api_key/ credits/
│   └── exceptions/                              # Domain exceptions
│
├── application/                                 # Application layer (see "Application layer modules" below)
│   ├── answers/                                 # AnswerEngine contract, agent, legacy wrapper, EngineRunner,
│   │                                            #   agent tools (tools/), figure verifier (verification.py)
│   ├── consultas/                               # SQL builders shared by the agent and the MCP data mode
│   ├── catalog/ quality/ marts/                 # Catalogue integrity, data-quality signals, curated marts
│   ├── pipeline/                                # Legacy LangGraph pipeline + collector parsers
│   └── public_catalog.py public_quota.py web_quota.py api_key_service.py ask_dedupe.py
│
├── infrastructure/                              # Infrastructure layer
│   ├── adapters/
│   │   ├── source/                              # IDataSource → datos.gob.ar / CABA CKAN
│   │   ├── llm/
│   │   │   ├── anthropic_bedrock_agent_adapter.py # IAgentLLM → Sonnet 4.6 on Bedrock (agent engine)
│   │   │   ├── bedrock_llm_adapter.py           # ILLMProvider → Haiku 4.5 on Bedrock (Converse; legacy + workers)
│   │   │   ├── gemini_adapter.py                # ILLMProvider → Gemini (fallback of the Bedrock adapter)
│   │   │   ├── fallback_llm_adapter.py          # primary → fallback wrapper
│   │   │   ├── bedrock_embedding_adapter.py     # IEmbeddingProvider → Cohere Embed Multilingual v3 (1024-dim)
│   │   │   └── anthropic_adapter.py             # Anthropic API adapter — not wired in provider_registry
│   │   ├── search/pgvector_search_adapter.py    # IVectorSearch → HNSW walk (400 / 1000 candidates) + capped exact search
│   │   ├── sandbox/pg_sandbox_adapter.py        # ISQLSandbox → read-only PG queries (SANDBOX_DATABASE_URL)
│   │   ├── connectors/                          # Series de Tiempo, BCRA, ArgentinaDatos/DolarApi, Georef, DDJJ, sesiones, staff, CKAN
│   │   └── dataset/ cache/ serving/ chat/ user/ api_key/ credits/
│   ├── resilience/                              # Fault tolerance
│   │   ├── retry.py                             # @with_retry decorator (exponential backoff + jitter)
│   │   └── circuit_breaker.py                   # In-memory circuit breaker (CLOSED→OPEN→HALF_OPEN)
│   ├── monitoring/                              # Observability
│   │   ├── health.py                            # HealthCheckService (postgres, redis, ddjj, sesion chunks, stuck tasks)
│   │   ├── metrics.py, prometheus_metrics.py    # MetricsCollector + Prometheus exposition
│   │   └── middleware.py                        # MetricsMiddleware (ASGI)
│   ├── persistence_sqla/
│   │   ├── mappings/                            # SQLAlchemy table ↔ entity mappings
│   │   ├── alembic/versions/                    # Migration files
│   │   └── provider.py                          # DB session provider
│   └── celery/
│       ├── app.py                               # Celery app: task routing, beat_schedule, OPENARG_BEAT_DESACTIVADAS
│       ├── heartbeat_signals.py                 # Records that each scheduled task ran
│       └── tasks/                               # Task modules: scraper, collector, embedding, series ETL,
│                                                #   BCRA snapshot, marts, sweeps, repairs, canaries, alerts
│
├── presentation/http/
│   ├── controllers/
│   │   ├── root_router.py                       # Composes all routers under /api/v1
│   │   ├── health/health_router.py              # GET /health, /health/ready
│   │   ├── query/smart_query_v2_router.py       # Chat: POST /query/smart + WS /query/ws/smart; picks the engine
│   │   ├── public_api/ask_router.py             # POST /ask (Bearer oarg_sk_, monthly quota)
│   │   ├── public_api/catalogo_router.py        # /catalogo/buscar|tabla|datos|agregar (MCP data mode, no LLM)
│   │   ├── public_api/fuentes_router.py         # GET /fuentes
│   │   ├── developers/developers_router.py      # API key CRUD (create/list/revoke) + usage
│   │   ├── conversations/ users/                # Chat history, users, privacy, feedback
│   │   ├── datasets/datasets_router.py          # List, stats, download
│   │   ├── sandbox/sandbox_router.py            # SQL sandbox + NL2SQL
│   │   ├── data/data_router.py                  # Internal service API (DATA_SERVICE_TOKEN)
│   │   ├── skills/ taxonomy/ transparency/ monitoring/
│   │   └── admin/                               # Tasks, analytics, MCP analytics, data health, repairs, supporters
│   └── middleware/                              # APIKeyMiddleware, GoogleJwtAuthMiddleware, request id, security headers
│
└── setup/
    ├── ioc/provider_registry.py                 # Dishka providers (all DI wiring)
    ├── config/
    │   ├── settings.py                          # Pydantic settings classes
    │   └── loader.py                            # TOML config loader
    └── app_factory.py                           # Middleware, CORS, startup checks (app/run.py: make_app)
```

The old `presentation/http/controllers/query/query_router.py` (`POST /query/`, `/quick`, `/{id}`, `/cache`, `WS /ws/stream`) was removed on 2026-05-05.

### Worker Pipeline (Celery queues)

Answers run in the API process, not in Celery. Queues, routes and the beat
schedule live in `infrastructure/celery/app.py`; consumers and concurrency in
`docker-compose.prod.yml` and `docker/*.Dockerfile`:

```
scraper       (worker-scraper, -c 2)      catalogue scrapes (29 portals daily, 03:00–05:50 ART), DKAN, Senate, staff
collector     (worker-collector, -c 8)    collect_data: download, parse with pandas, store in raw.*
collector-heavy / collector-heavy-retry   large files (one worker each, -c 1)
ingest + orchestrator (worker-ingest, -c 4)
              series ETL, BCRA snapshot, presupuesto, INDEC, marts, sweeps, cleanups, canaries,
              quality alerts; bulk_collect_all / refresh_stale_datasets on `orchestrator`
embedding     (worker-embedding, -c 8)    dataset + session embeddings (Bedrock Cohere, 1024-dim), enrichment
analyst       (worker-analyst, -c 2)      repair_columns_with_llm, repair_mart_sources
              (`analyze_query` is still registered but nothing dispatches it)
transparency  (worker-transparency, -c 2) portal health scores, session topics
s3            (worker-s3, -c 2)           uploads to S3
beat                                      94 entries, timezone America/Argentina/Buenos_Aires
```

No worker consumes a `default` queue (`tests/unit/test_celery_queues_have_consumers.py`).
An entry's `options.queue` in `beat_schedule` overrides `task_routes`.
`OPENARG_BEAT_DESACTIVADAS` (comma-separated `beat_schedule` keys, not task
names) removes entries when the Celery app is created.

### Resilience

- `@with_retry` decorator on all connector HTTP calls (exponential backoff + jitter, max 2 retries)
- In-memory circuit breaker per connector (failure_threshold=5, recovery_timeout=60s)
- Retryable HTTP statuses: 429, 500, 502, 503, 504

### API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Health check (components — postgres, redis, ddjj, sesion chunks, stuck tasks — only with `X-API-Key`) |
| GET | `/health/ready` | Readiness probe |
| POST | `/api/v1/query/smart` | Chat answer over HTTP (service key + Google ID token; 10/min, 50/day) |
| WS | `/api/v1/query/ws/smart` | Chat answer streamed over WebSocket (`X-API-Key` in the handshake) |
| POST | `/api/v1/ask` | **Public API** (Bearer `oarg_sk_`, monthly quota; what the MCP's `consultar_datos_publicos` calls) |
| GET | `/api/v1/fuentes` | Public data mode: portals and dataset counts |
| GET | `/api/v1/catalogo/buscar` | Public data mode: catalogue search |
| GET | `/api/v1/catalogo/tabla` | Public data mode: describe a table |
| POST | `/api/v1/catalogo/datos` | Public data mode: read rows |
| POST | `/api/v1/catalogo/agregar` | Public data mode: aggregates |
| POST/GET/DELETE | `/api/v1/developers/keys`, `/api/v1/developers/usage` | API key CRUD (one active key per user, shown once) and usage |
| * | `/api/v1/conversations/*`, `/api/v1/users/*` | Chat history, users, privacy, feedback |
| GET | `/api/v1/datasets/`, `/stats`, `/{id}/download` | List, counts per portal, download (presigned S3 URL or redirect to the portal) |
| POST/GET | `/api/v1/sandbox/query`, `/tables`, `/ask` | Read-only SQL, cached tables, NL2SQL |
| GET | `/api/v1/skills`, `/api/v1/taxonomy`, `/api/v1/taxonomy/hints` | Skills and taxonomy |
| GET/POST | `/api/v1/transparency/*` | Portal health, ghost datasets, session topics; admin POSTs |
| GET | `/api/v1/metrics`, `/api/v1/metrics/prometheus` | In-memory and Prometheus metrics |
| * | `/api/v1/admin/*` | Admin (`X-Admin-Key`): tasks, analytics, MCP analytics, data health, findings, repairs, supporters/credits |
| * | `/api/v1/data/*` | Internal service API (`DATA_SERVICE_TOKEN`) |

Caddy blocks `/api/v1/admin/*`, `/api/v1/data/*`, `/docs` and `/openapi.json` on the public domain.

### Database Tables

| Table | Purpose |
|-------|---------|
| `datasets` | Indexed dataset metadata (source_id + portal UNIQUE) |
| `dataset_chunks` | Vector-embedded chunks (pgvector 1024-dim, HNSW index) |
| `raw.cached_datasets` | References to cached data tables (status: pending/downloading/ready/error) |
| `user_queries` | Query history with plan, analysis, sources, token usage |
| `query_dataset_links` | Query ↔ dataset many-to-many with relevance score |
| `agent_tasks` | Individual agent task execution logs |
| `query_cache` | Semantic cache (pgvector 1024-dim, HNSW index, TTL-based expiry) |
| `table_catalog` | Cached table metadata with vector(1024) + HNSW for NL2SQL matching |
| `successful_queries` | Log of successfully answered queries for analytics |
| `api_keys` | API keys for public access (SHA-256 hash, unique, per-user) |
| `api_usage` | Append-only API request log (endpoint, tokens, duration) |
| `public.query_analytics` | Per-attempt log (question, served_table, mart_used, success, duration_ms) — feeds `/admin/analytics/*`. Always qualify it with `public.` (migration 0065) |
| `public.api_supporters` | Fundadores (larger monthly quota while `hasta` is NULL or in the future) |
| `public.api_credit_balances` / `public.api_credit_movements` | Extra credits, spent only after the monthly quota, and every grant/spend |
| `parse_repair_audit` | Reversibility log for in-place table repairs (rename col / drop col / drop superseded). Filterable by `run_id`. |
| `mart_definitions` | Mart catalog with embedding (1024-dim Cohere). Drives mart routing. |
| `mart_sample_queries` | Sample queries per mart for the gated +0.17 routing boost. |
| `raw_table_versions` | Tracks live vs superseded `raw.<table>__<hash>__vN` per resource_identity. |
| `raw_schema_snapshots` | Shape + `pg_stats` value profile of a table, captured immediately before any audited `DROP`. The only record of what a format looked like before it changed. |

### Application layer modules (worth knowing)

`src/app/application/` is the inner ring of hexagonal architecture. The modules
that a contributor most often touches:

- **`answers/`** — How a question gets answered, for every channel.
  - `engine.py` — `AnswerEngine` contract, events, `EngineResult`, and
    `selected_engine_name()` (`ANSWERS_ENGINE`; unknown values fall back to
    `legacy`).
  - `runner.py` — `EngineRunner`. For every engine: turn deadline and the
    `query_analytics` row of a turn that did not finish. For an engine with
    `handles_cross_cutting = False` (today the agent), also greetings/injection
    filter, semantic cache, history, stale-data notice at the top of the text
    (`quality/data_age.py`), `query_analytics`, metrics, audit. The legacy graph
    (`handles_cross_cutting = True`) does those in its own nodes and gets no
    notice in the text: `finalize` only adds the catalogue-table read date
    (`staleness_warning`) to `warnings`.
  - `agent_engine.py` — the tool loop (≤ 10 tool calls, 35 s soft budget,
    25 s per tool), streaming, corrective round with `ANSWERS_VERIFY_MODE=correct`.
  - `tools/` — `buscar_series`, `series_tiempo`, `variables_bcra`,
    `buscar_datos`, `describir_tabla`, `obtener_datos`, `calcular`,
    `cotizaciones`, `declaraciones_juradas`, `sesiones`,
    `personal_legislativo`, `ubicar_lugar`, `pedir_aclaracion`
    (`build_tools` drops a tool whose dependency is missing).
  - `verification.py` — every figure in the answer checked against the tool
    results; modes `off` / `shadow` (default) / `correct`.
  - `legacy_engine.py` — the LangGraph graph (`pipeline/graph.py`) behind the
    same contract.

- **`consultas/`** — SQL builders shared by the agent (`obtener_datos`,
  `calcular`) and the MCP data mode (`public_catalog.py`, `catalogo_router.py`):
  number formats stored as text, date columns and period filters, the filter
  grammar, suggestions when a filter matches nothing, aggregates. Values are
  always bound parameters and the query still goes through the sandbox validator.

- **`catalog/collapse.py`, `catalog/search_canary.py`** — one entry per file in
  search results; nightly recall@10 of the HNSW index against the exact search.

- **`quality/`** — `data_age.py` (stale-data notices), `expectations.py`
  (mart expectations), `heartbeat.py`, `portal_canary.py`, `model_canary.py`,
  `alerting.py` (Telegram).

- **`pipeline/parsers/`** — Pure-Python primitives reused by every collector path.
  - `column_normalization.py` — byte-aware dedup (`dedupe_column_names`) and
    garbage-name detectors (`is_garbage_column` / `is_url_column` /
    `is_title_row_column`).
  - `header_recovery.py` — `promote_buried_headers` recovers real headers when
    pandas mistook a TITLE row for the header. Year-row aware,
    Argentine-number-format aware, preserves valid original col names.
  - `time_pivot.py` — `unpivot_if_time_pivoted` melts wide year/month layouts
    to long format `(id, periodo, valor)`.
  - `pdf.py` — pdfplumber wrapper for PDF table extraction.
  - `hierarchical_headers.py` — multi-row header parser (existing).

- **`repair/`** — In-place DDL/DML fixes for tables already in `raw.*` /
  `public.*` that match a known parser bug. Pair-with-parser pattern: every
  parser fix has a `repair_<phase>_table()` companion that rewrites existing
  rows without re-ingesting from upstream. Audited via `parse_repair_audit`.

- **`marts/sql_macros.py`** — `live_table` / `live_tables_by_*` macros expanded
  by `build_mart` / `refresh_mart` Celery tasks. Supports `expected_columns`
  (schema-intersection projection) and `require_all_columns=True`
  (filters out tables missing any expected col, useful for fact-vs-dim
  clusters). Cap of 200 unions is configurable via
  `OPENARG_MART_MAX_UNION_TABLES`.

- **`expander/`** — Multi-file expansion (Excel multi-sheet + ZIP children).

- **`validation/collector_hooks.py`** — WS0/WS0.5 ingest validators
  (`placeholder_headers`, `row_count`, `html_as_data`).

## Conventions

- Hexagonal architecture: domain ports (ABC) → infrastructure adapters
- All DI wiring in `setup/ioc/provider_registry.py` via Dishka providers
- Scope.APP for singletons (settings, engine), Scope.REQUEST for per-request (session)
- Async-first: all I/O uses async/await
- Spanish comments in domain docstrings, English in infrastructure
- Pydantic models for API schemas, dataclasses for domain entities
- Config hierarchy: `config/{env}/config.toml` + `.secrets.toml`

## Git

- Do NOT add `Co-Authored-By` lines to commit messages.

## Dev Commands

```bash
make install                # Install dependencies (uv pip)
make dev                    # Dev server with reload
make db.up                  # Start PostgreSQL + Redis (docker)
make db.migrate             # Run Alembic migrations
make db.revision msg="..."  # Create new migration
make workers.scraper        # Run scraper worker
make workers.collector      # Run collector worker
make workers.embedding      # Run embedding worker
make workers.analyst        # Run analyst worker
make workers.transparency   # Run transparency worker
make workers.ingest         # Run ingest worker
make workers.s3             # Run S3 worker
make flower                 # Celery monitoring UI
make docker.up              # Start all services (API + workers)
make docker.down            # Stop all services
make docker.prod            # Start production stack
make code.format            # Ruff format
make code.lint              # Ruff check + mypy
make code.test              # Pytest with coverage
make code.check             # Lint + tests
```

## Environment Variables

Names only; never commit values. Local ports match `docker-compose.yaml`
(Postgres on 5435 via `config/local/config.toml`, Redis on 6381):

```
APP_ENV=local                        # local | dev | prod | test
DATABASE_URL=...                     # overrides config/{env}/config.toml
SANDBOX_DATABASE_URL=...             # read-only role used by the sandbox
CELERY_BROKER_URL=redis://localhost:6381/0
CELERY_RESULT_BACKEND=redis://localhost:6381/1
REDIS_CACHE_URL=redis://localhost:6381/2
AWS_REGION=us-east-1                 # credentials come from boto3's default chain
ANSWERS_ENGINE=legacy                # legacy (code default) | agent
ANSWERS_VERIFY_MODE=shadow           # off | shadow (default) | correct
BEDROCK_AGENT_MODEL=...              # default us.anthropic.claude-sonnet-4-6
BEDROCK_LLM_MODEL=...                # default us.anthropic.claude-haiku-4-5-20251001-v1:0
GEMINI_API_KEY=...                   # fallback of the legacy LLM
BACKEND_API_KEY=... ADMIN_API_KEY=... GOOGLE_OAUTH_CLIENT_ID=... DATA_SERVICE_TOKEN=...
OPENARG_BEAT_DESACTIVADAS=           # beat_schedule keys to skip, comma-separated
```

These are the names the app reads. On the servers `docker-compose.prod.yml`
builds some of them in `environment:`, which wins over the `.env`:
`GOOGLE_OAUTH_CLIENT_ID` from `GOOGLE_CLIENT_ID`, and `DATABASE_URL`,
`SANDBOX_DATABASE_URL` and the Redis URLs from `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST` and `REDIS_PASSWORD`. For
those, the server `.env` carries the compose's names, not the app's
(`docs/configuration.md` § On the servers).

Public API / web quotas (`PUBLIC_API_*`, `PUBLIC_WEB_*`), the MCP (`BACKEND_URL`,
`MCP_*`) and the rest are listed in `README.md` § Configuration and
`docs/configuration.md`.

## CI/CD

- **`.github/workflows/test.yml`** — ruff, unit tests, integration tests against a migrated Postgres built with pgvector + PostGIS, public MCP tests with `mcp_publico/requirements.txt`, mypy (informational)
- **`.github/workflows/build.yml`** — Build & push 10 images (api, beat, 7 workers, openarg-mcp) to GHCR; `:latest` only from `main`, `:staging` from `staging`, `:sha-<7>` always
- **`.github/workflows/e2e.yml`** — E2E suite against the staging database (pushes to `main`, manual runs)
- Deploy, verification gate (`scripts/verify_deploy.sh`) and rollback: `docs/deploy-produccion.md`
