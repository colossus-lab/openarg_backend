# Plan: Architecture (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Status**: Draft — extended with WS0/WS0.5/WS2/WS3/WS4/WS5 modules (2026-04-25); medallion finalised raw → mart (2026-05-04); Sprint 0.1–0.7 polish (2026-05-05)
**Last synced with code**: 2026-05-05

## Medallion finalisation (2026-05-04)

The original WS3/WS4 plan envisioned a 3-tier medallion (`raw → staging → mart`). Mig 0042 dropped the `staging` schema and `staging_contract_state` table — see [spec 018](../018-contracts-staging/spec.md) marked DEPRECATED. The system now goes directly from raw landings to mart materialised views via `live_table()` macro resolution. References to staging in this plan that pre-date the deprecation are kept only as historical breadcrumbs; the schema does not exist in production.

## Sprint 0.1–0.7 polish (2026-05-05)

Operational hardening and bug fixes that landed AFTER the 2026-04-25 collector rewrite. Full breakdown in [spec.md §0](./spec.md#0-recent-material-changes-sprint-01--07-2026-05-04--2026-05-05). Highlights:

- **Atomic raw promotion** in `_apply_cached_outcome`: registry + catalog_resources update happens BEFORE the `cached_datasets='ready'` write; failure demotes outcome to `error`. Closes the dangling-ready window where the raw promotion could fail silently.
- **`raw_table_versions.is_truncated`** (mig 0046) flags rows that hit `MAX_TABLE_ROWS=500_000` so dashboards can tell apart full landings from sampled prefixes.
- **Mart embeddings** auto-generated on every build (Bedrock Cohere multilingual v3) and back-filled for all 6 existing marts.
- **Sandbox schema allowlist** widened from `('public',)` to `('public', 'mart', 'raw')` with `_PREFIX_FREE_SCHEMAS` so SELECT against curated views and raw landings is no longer rejected. `openarg_sandbox_ro` granted USAGE+SELECT on those schemas.
- **Advisory lock helpers** (`_try_advisory_lock`, `_try_backfill_lock`) now persist the acquiring connection in a module-level dict so the session-scoped lock survives between acquire and release. Previous version closed the connection between the two calls, releasing the lock instantly — the "mutex" was effectively a no-op.
- **Honest defaults** in `catalog_backfill`: `parser_version` → `legacy:unknown` (was lying as `phase4-v1`), `layout_profile` and `header_quality` → `None` for unknown rows (were fabricating `simple_tabular`/`good`).
- **Sweep retrospective** UNIONs `cached_datasets` with `raw_table_versions` so the ~7% of raw rows without a cd entry are still validated.
- **`/data/tables` includes raw** via `sandbox.list_cached_tables` JOIN with rtv; `/data/search` includes marts via embedding similarity.

## Collector rewrite addendum (2026-04-25)

Six new application-layer modules were added per [collector_plan.md](../../../collector_plan.md):

```
src/app/application/
├── validation/         # WS0  — IngestionValidator + 14 detectors + collector_hooks + findings_repository
├── state_machine/      # WS0.5 — Status enum + transitions + StateMachineEnforcer
├── catalog/            # WS4  — title_extractor (canonical) + physical_namer (deterministic table name)
├── discovery/          # WS3  — CatalogDiscovery (hybrid + catalog-only modes)
├── expander/           # WS5  — MultiFileExpander (per-file ZIP rules)
└── pipeline/parsers/   # WS5  — HierarchicalHeaderParser (INDEC year+quarter+month)
```

New domain entity:
- `app/domain/entities/dataset/catalog_resource.py` — `CatalogResource` (logical catalog row) with status/kind enum constants.

New persistence:
- `catalog_resources` (migration 0035) — vector(1024) HNSW + parent_resource_id + check constraints.
- `ingestion_findings` (migration 0033) — audit trail for the validator.
- `cached_datasets.error_category` (migration 0034) — closed taxonomy + retry-invariant trigger.
- `cache_drop_audit` (migration 0036) — pg_event_trigger logs every `DROP TABLE cache_*`.

New Celery tasks:
- `openarg.ws0_retrospective_sweep` (every 6h) — validator Modo 3.
- `openarg.ws0_5_state_invariants_sweep` (every 30 min) — state-machine enforcer.
- `openarg.catalog_backfill` — populates `catalog_resources` from existing `datasets`+`cached_datasets`.
- `openarg.seed_connector_endpoints` — inserts the logical `live_api` rows for connector-backed capabilities.
- `openarg.bulk_collect_all` / `openarg.reconcile_cache_coverage` — materialization + recovery phase that must run after a destructive staging reset.
- `openarg.refresh_curated_sources` (weekly) — loads `config/curated_sources.json` into `datasets`.
- `openarg.ingest_censo2022` — Censo 2022 cuadro-by-cuadro seeder.
- `openarg.ops_temp_dir_cleanup` (hourly) — `/tmp/tmp*` sweep.
- `openarg.ops_portal_health` (every 30 min) — pings each portal, marks dead ones in `portals`.

New scripts:
- `scripts/diagnostics/factual_map.py` — WS1 read-only diagnostic.
- `scripts/staging_reset.py` — destructive wipe + full rebuild dispatch chain (seed connectors, scrape, backfill, bulk collect, reconcile, final backfill; refuses in prod).
- `scripts/ci/validate_curated_sources.py` — CI check for `config/curated_sources.json`.

New specs:
- [013-ingestion-validation](../013-ingestion-validation/) — WS0.
- [014-state-machine](../014-state-machine/) — WS0.5.
- [015-catalog-resources](../015-catalog-resources/) — WS2 + WS3 + WS4.

Feature flags introduced:
- `OPENARG_DISABLE_INGESTION_VALIDATOR=1` — disable WS0 hooks (Modos 1+2).
- `OPENARG_SWEEP_AUTOFLIP=1` — let WS0 Modo 3 flip materialization_status.
- `OPENARG_WS0_5_AUTO_ENFORCE=1` — let WS0.5 enforcer auto-correct (default dry-run).
- `OPENARG_HYBRID_DISCOVERY=1` — append catalog_resources hits in the planner.
- `OPENARG_CATALOG_ONLY=1` — replace `table_catalog` entirely (staging cutover).

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
- **Principle VII (Observability)**: Sentry is wired conditionally rather than guaranteed; when `SENTRY_DSN` is unset, the backend falls back to logs + in-memory metrics only. No distributed tracing. Metrics remain primarily in-memory (plus Prometheus export where enabled). Open debt.
- **Principle VIII (Migrations via Alembic)**: violated by dynamic `cache_*` tables created with `df.to_sql()` — accepted exception by the nature of the use case.

---

**End of plan.md**
