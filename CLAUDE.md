# OpenArg Backend

Backend service for OpenArg — AI-powered analysis of Argentine government open data. Implements a pipeline that scrapes public data portals, generates vector embeddings, caches datasets, and answers natural-language queries using LLMs.

## Stack

- **Framework:** FastAPI 0.115 + Uvicorn (async, UVLoop)
- **Database:** PostgreSQL 16 + pgvector (HNSW indexing, 1536-dim embeddings)
- **ORM:** SQLAlchemy 2.0 (async) + Alembic migrations
- **DI:** Dishka 1.6 (IoC container)
- **Workers:** Celery 5.4 + Redis 7 (broker + cache + results)
- **AI:** OpenAI SDK (GPT-4o, text-embedding-3-small) + Anthropic SDK (Claude Sonnet)
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
│   │   │   ├── openai_adapter.py                # ILLMProvider → GPT-4o
│   │   │   ├── anthropic_adapter.py             # ILLMProvider → Claude Sonnet
│   │   │   └── openai_embedding_adapter.py      # IEmbeddingProvider → text-embedding-3-small
│   │   ├── search/
│   │   │   └── pgvector_search_adapter.py       # IVectorSearch → pgvector
│   │   ├── sandbox/
│   │   │   └── pg_sandbox_adapter.py            # ISQLSandbox → read-only PG queries
│   │   ├── dataset/
│   │   │   └── dataset_repository_sqla.py       # IDatasetRepository → SQLAlchemy
│   │   └── cache/
│   │       └── redis_cache_adapter.py           # ICacheService → Redis
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
│   └── sandbox/sandbox_router.py                # SQL sandbox + NL2SQL
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
    → 3 chunks per dataset (main, columns, contextual) with 1536-dim embeddings

collect_dataset → collector queue (concurrency 4)
    → downloads file, parses with pandas, caches in PG table

analyze_query → analyst queue (concurrency 2)
    → 4 steps: plan (GPT-4o-mini) → vector search → gather sample rows → analyze (GPT-4o)
```

### API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Health check |
| GET | `/api/v1/datasets/` | List indexed datasets |
| GET | `/api/v1/datasets/stats` | Dataset counts per portal |
| POST | `/api/v1/datasets/scrape/{portal}` | Trigger catalog scrape |
| POST | `/api/v1/query/` | Submit async query |
| GET | `/api/v1/query/{query_id}` | Check query status |
| POST | `/api/v1/query/quick` | Synchronous query (rate limited) |
| WS | `/api/v1/query/ws/stream` | Stream query responses |
| POST | `/api/v1/sandbox/query` | Execute raw SQL (read-only) |
| GET | `/api/v1/sandbox/tables` | List cached tables |
| POST | `/api/v1/sandbox/ask` | NL2SQL query |

### Database Tables

| Table | Purpose |
|-------|---------|
| `datasets` | Indexed dataset metadata (source_id + portal UNIQUE) |
| `dataset_chunks` | Vector-embedded chunks (pgvector 1536-dim, HNSW index) |
| `cached_datasets` | References to cached data tables (status: pending/downloading/ready/error) |
| `user_queries` | Query history with plan, analysis, sources, token usage |
| `query_dataset_links` | Query ↔ dataset many-to-many with relevance score |
| `agent_tasks` | Individual agent task execution logs |

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
make flower                 # Celery monitoring UI
make docker.up              # Full stack (docker-compose)
make docker.down            # Stop all services
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
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=...
```
