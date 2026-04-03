# OpenArg Backend

Backend service for OpenArg — AI-powered analysis of Argentine government open data. Implements a pipeline that scrapes public data portals, generates vector embeddings, caches datasets, and answers natural-language queries using LLMs.

## Stack

- **Framework:** FastAPI 0.115 + Uvicorn (async, UVLoop)
- **Database:** PostgreSQL 16 + pgvector (HNSW indexing, 1024-dim embeddings)
- **ORM:** SQLAlchemy 2.0 (async) + Alembic migrations
- **DI:** Dishka 1.6 (IoC container)
- **Workers:** Celery 5.4 + Redis 7 (broker + cache + results)
- **AI:** AWS Bedrock Claude Haiku 4.5 (primary LLM) + Google Gemini 2.5 Flash (fallback)
- **Embeddings:** AWS Bedrock Cohere Embed Multilingual v3 (1024-dim)
- **Pipeline:** LangGraph (stateful graph with checkpointing)
- **HTTP:** HTTPX (async client)
- **Auth:** PyJWT + bcrypt
- **Rate Limiting:** SlowAPI
- **Config:** TOML files + Pydantic settings
- **Logging:** structlog

## Architecture (Hexagonal / Ports & Adapters)

```
src/app/
├── domain/                                      # Domain layer
│   ├── entities/                                # Dataclass entities
│   │   ├── base.py                              # BaseEntity (id, created_at, updated_at)
│   │   ├── dataset.py                           # Dataset, DatasetChunk
│   │   ├── user_query.py                        # UserQuery, AgentTask
│   │   └── query_dataset_link.py
│   ├── ports/                                   # Abstract interfaces (ABC)
│   │   ├── source/data_source.py                # IDataSource: fetch_catalog, download_dataset
│   │   ├── dataset/dataset_repository.py        # IDatasetRepository: save, get_by_id, upsert
│   │   ├── llm/llm_provider.py                  # ILLMProvider, IEmbeddingProvider
│   │   ├── search/vector_search.py              # IVectorSearch: search_datasets, index_dataset
│   │   ├── sandbox/sql_sandbox.py               # ISQLSandbox: execute_readonly
│   │   └── cache/cache_port.py                  # ICacheService: get, set, delete
│   └── exceptions/                              # Domain exceptions
│
├── infrastructure/                              # Infrastructure layer
│   ├── adapters/
│   │   ├── source/
│   │   │   ├── datos_gob_ar_adapter.py          # IDataSource → datos.gob.ar CKAN
│   │   │   └── caba_adapter.py                  # IDataSource → CABA CKAN
│   │   ├── llm/
│   │   │   ├── bedrock_llm_adapter.py           # ILLMProvider → Claude Haiku 4.5 (AWS Bedrock, primary)
│   │   │   ├── bedrock_embedding_adapter.py     # IEmbeddingProvider → Cohere Embed Multilingual v3 (1024-dim)
│   │   │   ├── gemini_adapter.py                # ILLMProvider → Gemini 2.5 Flash (Google, fallback)
│   │   │   ├── anthropic_adapter.py             # ILLMProvider → Claude Sonnet 4 (Anthropic API)
│   │   ├── search/
│   │   │   └── pgvector_search_adapter.py       # IVectorSearch → pgvector
│   │   ├── sandbox/
│   │   │   └── pg_sandbox_adapter.py            # ISQLSandbox → read-only PG queries
│   │   ├── dataset/
│   │   │   └── dataset_repository_sqla.py       # IDatasetRepository → SQLAlchemy
│   │   └── cache/
│   │       └── redis_cache_adapter.py           # ICacheService → Redis
│   ├── resilience/                              # Fault tolerance
│   │   ├── retry.py                             # @with_retry decorator (exponential backoff + jitter)
│   │   └── circuit_breaker.py                   # In-memory circuit breaker (CLOSED→OPEN→HALF_OPEN)
│   ├── monitoring/                              # Observability
│   │   ├── health.py                            # HealthCheckService (postgres, redis, ddjj, sesiones)
│   │   ├── metrics.py                           # MetricsCollector singleton (requests, connectors, cache, tokens)
│   │   └── middleware.py                        # MetricsMiddleware (ASGI)
│   ├── persistence_sqla/
│   │   ├── mappings/                            # SQLAlchemy table ↔ entity mappings
│   │   ├── alembic/versions/                    # Migration files
│   │   └── provider.py                          # DB session provider
│   └── celery/
│       ├── app.py                               # Celery app config + task routing
│       └── tasks/
│           ├── scraper_tasks.py                 # scrape_catalog, index_dataset_embedding
│           ├── collector_tasks.py               # collect_dataset (download + cache in PG)
│           ├── embedding_tasks.py               # reindex_all_embeddings
│           └── analyst_tasks.py                 # analyze_query (plan → search → gather → analyze)
│
├── presentation/http/controllers/               # API layer
│   ├── root_router.py                           # Composes all routers under /api/v1
│   ├── health/health_router.py                  # GET /health, /health/ready
│   ├── datasets/datasets_router.py              # CRUD + scrape trigger
│   ├── query/query_router.py                    # Query submission + WebSocket stream
│   ├── query/smart_query_v2_router.py           # LangGraph pipeline (POST /smart + WS /ws/smart)
│   ├── public_api/ask_router.py                 # POST /ask (public API with Bearer token auth)
│   ├── developers/developers_router.py          # API key CRUD (create/list/revoke keys)
│   ├── skills/skills_router.py                  # GET /skills (list auto-detected skills)
│   ├── sandbox/sandbox_router.py                # SQL sandbox + NL2SQL
│   ├── taxonomy/taxonomy_router.py              # Taxonomy management
│   ├── transparency/transparency_router.py      # Transparency data
│   ├── admin/tasks_router.py                    # Admin task management
│   └── monitoring/metrics_router.py             # GET /api/v1/metrics
│
└── setup/
    ├── ioc/provider_registry.py                 # Dishka providers (all DI wiring)
    ├── config/
    │   ├── settings.py                          # Pydantic settings classes
    │   └── loader.py                            # TOML config loader
    └── run.py                                   # App factory (make_app)
```

### Worker Pipeline (Celery queues)

```
scrape_catalog → scraper queue (concurrency 2)
    ↓ dispatches per dataset
index_dataset_embedding → embedding queue (concurrency 8)
    → 3 chunks per dataset (main, columns, contextual) with 1024-dim embeddings (Bedrock Cohere)

collect_dataset → collector queue (concurrency 4)
    → downloads file, parses with pandas, caches in PG table

analyze_query → analyst queue (concurrency 2)
    → 4 steps: plan (Bedrock Claude Haiku) → vector search → gather sample rows → analyze

transparency_tasks → transparency queue (concurrency 2)
    → presupuesto, DDJJ processing

ingest_tasks → ingest queue (concurrency 2)
    → senado, staff, series tiempo structured data

s3_tasks → s3 queue (concurrency 2)
    → large dataset storage on S3
```

### Resilience

- `@with_retry` decorator on all connector HTTP calls (exponential backoff + jitter, max 2 retries)
- In-memory circuit breaker per connector (failure_threshold=5, recovery_timeout=60s)
- Retryable HTTP statuses: 429, 500, 502, 503, 504

### API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Health check (component-level: postgres, redis, ddjj, sesiones) |
| GET | `/health/ready` | Readiness probe |
| GET | `/api/v1/datasets/` | List indexed datasets |
| GET | `/api/v1/datasets/stats` | Dataset counts per portal |
| POST | `/api/v1/datasets/scrape/{portal}` | Trigger catalog scrape |
| POST | `/api/v1/query/` | Submit async query |
| GET | `/api/v1/query/{query_id}` | Check query status |
| POST | `/api/v1/query/quick` | Synchronous query (rate limited) |
| WS | `/api/v1/query/ws/stream` | Stream query responses |
| POST | `/api/v1/query/smart` | LangGraph pipeline (frontend, X-API-Key auth) |
| WS | `/api/v1/query/ws/smart` | LangGraph pipeline with WebSocket streaming |
| POST | `/api/v1/ask` | **Public API** (Bearer token auth, rate limited per plan) |
| GET | `/api/v1/skills` | List auto-detected skills |
| POST | `/api/v1/developers/keys` | Create API key (1 per user, returns key once) |
| GET | `/api/v1/developers/keys` | List user's API keys (masked) |
| DELETE | `/api/v1/developers/keys/{id}` | Revoke API key |
| GET | `/api/v1/developers/usage` | API usage stats |
| POST | `/api/v1/sandbox/query` | Execute raw SQL (read-only) |
| GET | `/api/v1/sandbox/tables` | List cached tables |
| POST | `/api/v1/sandbox/ask` | NL2SQL query |
| GET | `/api/v1/metrics` | In-memory metrics |

### Database Tables

| Table | Purpose |
|-------|---------|
| `datasets` | Indexed dataset metadata (source_id + portal UNIQUE) |
| `dataset_chunks` | Vector-embedded chunks (pgvector 1024-dim, HNSW index) |
| `cached_datasets` | References to cached data tables (status: pending/downloading/ready/error) |
| `user_queries` | Query history with plan, analysis, sources, token usage |
| `query_dataset_links` | Query ↔ dataset many-to-many with relevance score |
| `agent_tasks` | Individual agent task execution logs |
| `query_cache` | Semantic cache (pgvector 1024-dim, HNSW index, TTL-based expiry) |
| `table_catalog` | Cached table metadata with vector(1024) + HNSW for NL2SQL matching |
| `successful_queries` | Log of successfully answered queries for analytics |
| `api_keys` | API keys for public access (SHA-256 hash, unique, per-user) |
| `api_usage` | Append-only API request log (endpoint, tokens, duration) |

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

```
APP_ENV=local
DATABASE_URL=postgresql+psycopg://openarg:openarg@localhost:5435/openarg
CELERY_BROKER_URL=redis://localhost:6381/0
CELERY_RESULT_BACKEND=redis://localhost:6381/1
REDIS_CACHE_URL=redis://localhost:6381/2
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
ANTHROPIC_API_KEY=...              # Fallback LLM
```

## CI/CD

- **`.github/workflows/test.yml`** — Unit tests, integration tests, type checking (pgvector:pg16 + redis:7 services)
- **`.github/workflows/build.yml`** — Build & push Docker images (API + workers + beat) to GHCR
