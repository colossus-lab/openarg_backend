# Plan: Architecture (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

```
src/app/
├── domain/                                  # Ubiquitous language, pure
│   ├── entities/                            # Dataclasses
│   │   ├── base.py                          # BaseEntity (id, created_at, updated_at)
│   │   ├── dataset/                         # Dataset, DatasetChunk, CachedData
│   │   ├── query/                           # UserQuery, AgentTask
│   │   ├── connectors/data_result.py        # DataResult, PlanStep, ExecutionPlan, ChartData, MemoryContext
│   │   ├── chat/conversation.py             # Conversation, Message
│   │   ├── user/                            # User entity
│   │   ├── staff/staff.py                   # Staff entity (diputados/senadores)
│   │   └── agent/agent_task.py              # AgentTask
│   ├── ports/                               # Abstract interfaces (ABC)
│   │   ├── source/data_source.py            # IDataSource
│   │   ├── dataset/dataset_repository.py    # IDatasetRepository
│   │   ├── llm/llm_provider.py              # ILLMProvider, IEmbeddingProvider
│   │   ├── search/vector_search.py          # IVectorSearch
│   │   ├── sandbox/sql_sandbox.py           # ISQLSandbox
│   │   ├── cache/cache_port.py              # ICacheService
│   │   ├── user/                            # User repository port
│   │   ├── chat/                            # Chat repository port
│   │   └── connectors/                      # Specific connector ports
│   │       ├── series_tiempo.py             # ISeriesTiempoConnector
│   │       ├── argentina_datos.py           # IArgentinaDatosConnector
│   │       ├── ckan_search.py               # ICKANSearchConnector
│   │       ├── sesiones.py                  # ISesionesConnector
│   │       ├── staff.py                     # IStaffConnector
│   │       └── georef.py                    # IGeorefConnector
│   │       # [MISSING] bcra.py — see [002a-bcra/DEBT-001]
│   ├── value_objects/
│   ├── exceptions/
│   │   ├── base.py                          # DomainException base
│   │   ├── error_codes.py                   # ErrorCode enum
│   │   └── connector_errors.py              # ConnectorError
│   └── ...
│
├── application/                             # Use cases + orchestration
│   ├── common/                              # Shared utilities, exceptions
│   └── pipeline/                            # LangGraph pipeline
│       ├── connectors/                      # Pipeline steps per connector
│       │   ├── bcra.py
│       │   └── ...
│       ├── prompts/                         # Agent prompts
│       └── graph builders / nodes
│
├── infrastructure/                          # Implementations
│   ├── adapters/
│   │   ├── source/                          # Base CKAN portals
│   │   │   ├── datos_gob_ar_adapter.py
│   │   │   └── caba_adapter.py
│   │   ├── connectors/                      # Specific connectors
│   │   │   ├── bcra_adapter.py
│   │   │   ├── series_tiempo_adapter.py
│   │   │   ├── argentina_datos_adapter.py
│   │   │   ├── ckan_search_adapter.py
│   │   │   ├── sesiones_adapter.py
│   │   │   ├── staff_adapter.py
│   │   │   └── georef_adapter.py
│   │   ├── llm/
│   │   │   ├── bedrock_llm_adapter.py       # Claude Haiku 4.5 (primary)
│   │   │   ├── bedrock_embedding_adapter.py # Cohere Embed v3
│   │   │   ├── gemini_adapter.py            # Fallback
│   │   │   ├── gemini_embedding_adapter.py
│   │   │   └── anthropic_adapter.py         # Sonnet (direct Anthropic access)
│   │   ├── search/pgvector_search_adapter.py
│   │   ├── sandbox/
│   │   │   ├── pg_sandbox_adapter.py
│   │   │   └── table_validation.py
│   │   ├── dataset/dataset_repository_sqla.py
│   │   ├── cache/redis_cache_adapter.py
│   │   ├── user/                            # User repo sqla
│   │   └── chat/                            # Chat repo sqla
│   ├── persistence_sqla/
│   │   ├── config.py
│   │   ├── registry.py
│   │   ├── provider.py                      # DB session provider + pool config
│   │   ├── alembic/
│   │   │   ├── env.py
│   │   │   └── versions/                    # Numbered migrations
│   │   └── mappings/                        # SQLAlchemy table ↔ entity
│   ├── celery/
│   │   ├── app.py                           # Celery app + beat_schedule + routing
│   │   └── tasks/
│   │       ├── _db.py                       # get_sync_engine singleton
│   │       ├── scraper_tasks.py             # scrape_catalog, index_dataset_embedding
│   │       ├── collector_tasks.py           # collect_dataset
│   │       ├── embedding_tasks.py           # reindex_all_embeddings
│   │       ├── analyst_tasks.py             # analyze_query
│   │       ├── bcra_tasks.py
│   │       ├── transparency_tasks.py        # presupuesto, DDJJ
│   │       ├── orchestrator_tasks.py        # admin on-demand triggers
│   │       └── ...
│   ├── resilience/
│   │   ├── retry.py                         # @with_retry decorator
│   │   └── circuit_breaker.py
│   ├── monitoring/
│   │   ├── health.py                        # HealthCheckService
│   │   ├── metrics.py                       # MetricsCollector
│   │   └── middleware.py                    # MetricsMiddleware
│   ├── storage/                             # S3 adapter, local fs
│   └── audit/
│
├── presentation/
│   └── http/
│       ├── middleware/
│       ├── errors/                          # Error handlers
│       └── controllers/
│           ├── root_router.py               # Composition /api/v1
│           ├── health/health_router.py
│           ├── datasets/
│           ├── query/
│           │   ├── query_router.py
│           │   └── smart_query_v2_router.py
│           ├── public_api/ask_router.py     # POST /api/v1/ask (Bearer)
│           ├── developers/developers_router.py
│           ├── skills/
│           ├── sandbox/sandbox_router.py
│           ├── taxonomy/taxonomy_router.py
│           ├── transparency/
│           ├── admin/tasks_router.py
│           ├── monitoring/metrics_router.py
│           ├── users/
│           └── conversations/
│
└── setup/
    ├── ioc/provider_registry.py             # Dishka wiring (ONLY DI point)
    ├── config/
    │   ├── settings.py                      # Pydantic settings
    │   └── loader.py                        # TOML loader
    └── run.py                               # App factory
```

## 2. As-Built System Topology

```
                    ┌──────────────────────────┐
                    │   Next.js Frontend       │
                    │   (openarg_frontend)     │
                    └────────────┬─────────────┘
                                 │ HTTPS + WS
                                 ▼
                    ┌──────────────────────────┐
                    │   Caddy 2 (TLS + Proxy)  │
                    └────────────┬─────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │   FastAPI (Uvicorn)      │
                    │   /api/v1/*              │
                    └────┬─────────────┬───────┘
                         │             │
                         ▼             ▼
          ┌──────────────────┐  ┌─────────────┐
          │  Dishka DI       │  │ SlowAPI     │
          │  Container       │  │ Rate limit  │
          └───┬──────────┬───┘  └─────────────┘
              │          │
     ┌────────▼──┐  ┌────▼────────────┐
     │ PGBouncer │  │ LangGraph       │
     └─────┬─────┘  │ Pipeline        │
           │        └──┬──────────┬───┘
           ▼           │          │
    ┌───────────────┐  ▼          ▼
    │ PostgreSQL 16 │  Bedrock   Redis
    │  + pgvector   │  Claude    Cache/
    │   (RDS)       │  Haiku     Broker
    └───────────────┘              │
                                   ▼
                         ┌──────────────────┐
                         │ Celery Workers   │
                         │ (7 queues)       │
                         └──┬───────────┬───┘
                            ▼           ▼
                    External APIs    S3 (storage)
                    (BCRA, CKAN,
                     datos.gob.ar,
                     senado, etc.)
```

## 3. Worker Pipeline (Celery queues)

| Queue | Concurrency | Main tasks |
|---|---|---|
| `scraper` | 2 | `scrape_catalog`, `index_dataset_embedding` |
| `embedding` | 8 | Chunking + Cohere embeddings (main/columns/contextual, 1024-dim) |
| `collector` | 4 | `collect_dataset` — download + parse + cache in PG |
| `analyst` | 2 | `analyze_query` — offline pipeline (plan → search → gather → analyze) |
| `transparency` | 2 | Presupuesto, DDJJ |
| `ingest` | 2 | Senado, staff, series tiempo, BCRA, INDEC, BAC |
| `s3` | 2 | Storage of large datasets |

## 4. Beat Schedule (summary)

See `infrastructure/celery/app.py:181-300` for the complete source. Highlights:

| Task | Schedule | Queue |
|---|---|---|
| `snapshot_bcra` | Daily 04:00 | ingest |
| `ingest_bac` | Sunday 01:00 | ingest |
| `ingest_indec` | Day 15 of the month, 01:00 | ingest |
| `scrape-dkan-rosario` | Saturday 00:30 | scraper |
| `scrape-dkan-jujuy` | Saturday 01:00 | scraper |
| `ingest_presupuesto_dimensiones` | Day 5 of the month, 00:30 | ingest |
| `ingest_senado` | Sunday 02:00 | ingest |
| `ingest_staff_hcdn` | Monday 02:30 | ingest |
| `ingest_staff_senado` | Monday 01:30 | ingest |
| `scrape-datos-gob-ar` | Per-portal, configured times | scraper |

## 5. API Endpoints (as-built)

See `presentation/http/controllers/root_router.py` and `CLAUDE.md` section "API Endpoints" for the complete table. Categories:

1. **Health / Ops**: `/health`, `/health/ready`, `/api/v1/metrics`
2. **Datasets**: `/api/v1/datasets/*` (CRUD, stats, scrape trigger)
3. **Query (user)**: `/api/v1/query/*` + WS streaming
4. **Smart (LangGraph)**: `/api/v1/query/smart` + WS
5. **Public API**: `/api/v1/ask` (Bearer token)
6. **Developers**: `/api/v1/developers/keys` (CRUD + usage)
7. **Sandbox**: `/api/v1/sandbox/{query,tables,ask}`
8. **Taxonomy**: `/api/v1/taxonomy/*`
9. **Transparency**: `/api/v1/transparency/*`
10. **Admin**: `/api/v1/admin/*`
11. **Users / Conversations / Skills**: various under `/api/v1/*`
12. **Data API (direct access)**: `/api/v1/data/query`, `/api/v1/data/tables`, `/api/v1/data/search` — internal endpoints for direct database access (read-only SQL over `cache_*` tables + table listing + semantic search of tables). **They do not go through the LLM pipeline** — they are designed for service-to-service consumers that need raw data without the cost or latency of Bedrock. Auth via `DATA_SERVICE_TOKEN` (Bearer header). Reuses the existing SQL sandbox validation.

### Auth mechanisms in the backend (full inventory)

| # | Mechanism | Usage | How it's passed | Where it's validated |
|---|---|---|---|---|
| 1 | **Google OAuth ID token** (obtained by NextAuth) | Frontend chat, browser users | `Authorization: Bearer <google_id_token>` injected by the Next.js server routes | `GoogleJwtAuthMiddleware` validates signature via Google JWKS + verifies `iss`/`aud`/`exp`/`email_verified` per FIX-005. Admin-gated endpoints are exempt under FR-007a (they use `X-Admin-Key` instead). |
| 2 | **User API keys** `oarg_sk_*` | Public API `/api/v1/ask` for developers | `Authorization: Bearer oarg_sk_...` | Backend validates SHA-256 hash in DB |
| 3 | **`BACKEND_API_KEY`** service token | Frontend↔backend (chat pipeline) | `X-API-Key` header in POST OR `?api_key=...` query param (only WS handshake — Node `ws` package workaround) | `smart_query_v2_router.py:97` (POST) and `:234-244` (WS) |
| 4 | **`ADMIN_EMAILS`** allowlist | Admin-gated endpoints (`/api/transparency`) | Email from verified JWT against env var | Frontend `requireAdmin()` — backend trusts header |
| 5 | **`DATA_SERVICE_TOKEN`** service token | `/api/v1/data/*` — direct DB access without LLM | `Authorization: Bearer svc_xxx` header | `data_router.py::verify_service_token()` — constant-time comparison |

## 6. Database Tables (canonical list)

| Table | Purpose |
|---|---|
| `datasets` | Metadata of indexed datasets (UNIQUE `source_id + portal`) |
| `dataset_chunks` | Chunks with `embedding vector(1024)` + HNSW index |
| `cached_datasets` | Reference to physical cached tables (`status: pending/downloading/ready/error`) |
| `cache_*` | Physical tables with raw data per connector (e.g.: `cache_bcra_cotizaciones`) — dynamic schema |
| `user_queries` | Query history with plan, analysis, sources, tokens |
| `query_dataset_links` | M:N between queries and datasets with relevance score |
| `agent_tasks` | Log of individual agent tasks in the pipeline |
| `query_cache` | Semantic cache (embedding + response + TTL) |
| `table_catalog` | Table metadata with `embedding vector(1024)` for NL2SQL matching |
| `successful_queries` | Log of well-answered queries (analytics) |
| `api_keys` | API keys (SHA-256 hash, UNIQUE, 1 per user by default) |
| `api_usage` | Append-only log of public API requests |
| `users` | User accounts |
| `conversations` | Chat sessions with conversational memory |
| `messages` | Messages of each conversation |
| `staff` | Diputados/senadores/legislative staff |
| `ddjj_*` | Sworn statements (partial) |

## 7. Configuration Layers

```
config/
├── local/
│   ├── config.toml       # Defaults for dev
│   └── .secrets.toml     # Local secrets (gitignored)
└── prod/
    ├── config.toml       # Defaults for prod
    └── .secrets.toml     # Prod secrets (gitignored, injected on deploy)
```

Loaded by `setup/config/loader.py`, validated against Pydantic classes in `setup/config/settings.py`. In deployed environments, runtime values come from a `.env` file injected by the deploy pipeline (never checked into the repo); local development uses a copy of `.env.example` with dev credentials.

## 8. Key External Dependencies

| Dependency | Usage | Critical |
|---|---|---|
| **AWS Bedrock** | Primary LLM (Claude Haiku) + embeddings (Cohere) | Yes — no coherent embeddings fallback |
| **Google Gemini** | Fallback LLM | No |
| **PostgreSQL RDS** | Main store | Yes |
| **Redis** | Celery broker + cache | Yes |
| **BCRA API** | Quotes | Medium — degrades gracefully |
| **datos.gob.ar CKAN** | Federal data | Medium |
| **Provincial CKAN portals** | Provincial/municipal data | Low — 10 down |
| **Senado / HCDN APIs** | Legislative | Low — weekly ingest |
| **INDEC** | Official statistics | Low — monthly |

## 9. Source Files (entry points)

| File | Role |
|---|---|
| `src/app/run.py` | uvicorn entry point, calls the app factory |
| `src/app/setup/run.py` | App factory (`make_app`) |
| `src/app/setup/ioc/provider_registry.py` | Dishka wiring (only DI location) |
| `src/app/infrastructure/celery/app.py` | Celery app + beat + routing |
| `src/app/presentation/http/controllers/root_router.py` | Router composer |
| `alembic.ini` | Migration config |
| `config/{env}/config.toml` | Runtime config |
| `pyproject.toml` | Dependencies + tooling (ruff, mypy, pytest) |
| `docker-compose.yaml` / `docker-compose.prod.yml` | Orchestration |
| `Makefile` | Dev commands |

## 10. Deviations from Constitution

- **Principle I (Hexagonal)**: violated by BCRA connector without port (`[002a-bcra/DEBT-001]`). Isolated.
- **Principle III (Single DI via Dishka)**: violated by Celery workers that instantiate adapters inline when they need a sync engine. Structural debt, not resolved.
- **Principle IV (Async-first)**: Celery workers are sync by nature of Celery 5 — accepted exception, not debt.
- **Principle VII (Observability)**: Sentry not configured. No distributed tracing. Metrics only in-memory (not exported to Prometheus/Grafana). Open debt.
- **Principle VIII (Migrations via Alembic)**: violated by dynamic `cache_*` tables created with `df.to_sql()` — accepted exception by the nature of the use case.

---

**End of plan.md**
