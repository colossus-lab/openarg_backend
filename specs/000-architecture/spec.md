# Spec: Architecture (Macro)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-05-05 (Sprint 0.7 complete)
**Hexagonal scope**: Full-stack
**Related plan**: [./plan.md](./plan.md)

---

## 0. Recent material changes (Sprint 0.1 – 0.7, 2026-05-04 → 2026-05-05)

This block summarises decisions and bug fixes that landed AFTER the previous sync (2026-04-12) and that affect downstream specs. Read it before any spec further down — several of them still reflect pre-Sprint state in their wording.

| Sprint | Change | Spec impact |
|---|---|---|
| 0.1 | `raw_table_versions.is_truncated BOOLEAN DEFAULT FALSE` (alembic 0046). Collector marks the flag when `MAX_TABLE_ROWS=500_000` cuts off a CSV/Excel/JSON parse. `OPENARG_RAW_RETENTION_KEEP_LAST` env var now drives `retain_raw_versions` retention window (default 3, configurable without redeploy). | spec 017 (raw layer) — config + flag |
| 0.2 | `mart_definitions.embedding` populated for all 6 marts via `openarg.backfill_mart_embeddings` (Bedrock Cohere multilingual v3, 1024-dim). `_upsert_mart_definition` now generates the embedding on every successful build. | spec 019 (marts), spec 016 (serving port) |
| 0.3 | New invariant in `cleanup_invariants`: detects `mart_definitions.last_row_count = 0` while the matview has `reltuples > 0` and refreshes the metadata. Fixed `demo_energia_pozos` row_count=0 drift. | spec 014 (state machine) — extended sweep |
| 0.4 | `cleanup_invariants` also drops empty orphan raw tables (0 rows, no rtv entry, no `cached_datasets` row). Orphans WITH data still go through canonical/fallback registration. | spec 014 |
| 0.5 | Planner heuristic accepts `mart.foo` qualified names (was only `mart::foo` before). `parser_version=legacy:unknown` for backfilled rows (was lying as `phase4-v1`). `mart_id == mart_view_name` invariant enforced in `_upsert_mart_definition`. `/data/search` includes marts via embedding similarity. Advisory locks (`_try_advisory_lock`, `_try_backfill_lock`) now cache the acquiring connection in a module-level dict so the session-scoped lock survives between acquire and release — the previous helpers leaked the lock instantly. | spec 016, spec 019, spec 015 |
| 0.6 | **Atomicity**: raw promotion (`_register_raw_version` + `UPDATE catalog_resources`) now runs BEFORE the `cached_datasets='ready'` write. If it fails, the outcome demotes to `error` with `raw_promotion_failed:` so the dataset never sits as `ready` while the registry/catalog are out of sync. `register_via_b_table` reconciles `catalog_resources.materialized_table_name` to the canonical `{portal}::{source_id}` row so catalog and registry agree on the physical table even when their `resource_identity` strings differ. `sandbox.list_cached_tables` now includes raw layer tables (qualified as `raw.<bare>`). Qualified-name parsing helper applied to `ingestion_findings_sweep` and `catalog_enrichment_tasks`. | spec 014, spec 015, spec 017 |
| 0.7 | Sandbox `_ALLOWED_SCHEMAS = ("public", "mart", "raw")` with `_PREFIX_FREE_SCHEMAS = ("mart", "raw")` so SELECT against mart/raw is no longer rejected by the validator. `openarg_sandbox_ro` granted USAGE+SELECT on `mart` and `raw` schemas. `_derived_layout_profile` and `_derived_header_quality` in `catalog_backfill` return None for unknown rows (they used to fabricate `simple_tabular`/`good`, mis-tagging legacy rows as phase4-parsed). `ingestion_findings_sweep._load_batch` UNIONs `cached_datasets` with `raw_table_versions` so the ~7% of raw rows without a cd entry are still validated. `catalog_enrichment._resolve_resource_identity_for_table` resolves rtv-first for qualified names. | spec 010 (sandbox), spec 015, spec 014 |

### Architectural deletions

- **Staging schema removed** in mig 0042 (2026-05-04). The medallion is now raw → mart with no intermediate validation table. See [spec 018-contracts-staging](../018-contracts-staging/spec.md) marked DEPRECATED. Specs 016 and 014 still reference staging in places — those references are historical.

### Backlog (explicit deferred)

These bugs are real but the cost/benefit analysis says wait:

- `presupuesto_tasks._register_dimension` bypasses the canonical state machine (55 dimension tables don't get layout_profile/header_quality/error_category). MEDIUM, no runtime impact.
- `bac_tasks` pre-registers `cached_datasets='downloading'` and updates errors directly, bypassing `_apply_cached_outcome`. Same shape as the presupuesto bypass.
- vía-B writers (`bcra_tasks`, `presupuesto_tasks`, `senado_tasks`) use `{portal}::{curated}` resource identities while `catalog_resources` uses `{portal}::{datasets.source_id}`. Mitigated in Sprint 0.6 by syncing `materialized_table_name`; a full alignment requires a migration of historical rtv rows + updating mart SQLs that hardcode the curated identity.
- `register_via_b_table` triggers `_trigger_marts_for_portal` on every `ON CONFLICT DO UPDATE` even when no field changed. Mitigated by 110s debounce.

---

## 1. Context & Purpose

OpenArg is an **AI-powered platform for analyzing Argentine public data**. It ingests data from government portals (federal, provincial, municipal), structures and embeds it in a vector store, and answers natural-language questions by combining semantic retrieval and query execution over cached tables.

The system has two main audiences:

1. **End users** who interact via web chat to get answers to questions about public data (budget, legislation, officials, exchange rate statistics, sworn statements, etc.).
2. **External developers** who consume a public API (Bearer token) to integrate the data into their own applications.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Connector** | Component that encapsulates an external data source (CKAN portal, official API, flat file) and normalizes it to `DataResult`. |
| **Dataset** | Canonical unit of data cataloged in OpenArg; includes metadata, source, format, and possibly cached data. |
| **Dataset Chunk** | Semantic fragment of a dataset with a vector embedding, used for similarity search. |
| **Query Pipeline** | Stateful LangGraph graph that transforms a user question into a response, passing through planning, search, execution and analysis. |
| **Agent** | Pipeline stage with a specific role (Estratega, Investigador, Analista, Redactor). |
| **ExecutionPlan** | Structured plan produced by the planner that lists the steps to execute (which sources to query, with what parameters). |
| **PlanStep** | Atomic unit of pipeline work (e.g.: "search in CKAN", "query BCRA", "execute SQL sandbox"). |
| **Semantic Cache** | Vector-indexed response cache: similar questions return the same response without re-running the pipeline. |
| **Sandbox SQL** | Read-only environment where the system executes SQL generated by NL2SQL against cached tables (allowlist). |
| **Table Catalog** | Vector registry of available tables with enriched metadata for NL2SQL matching. |
| **DDJJ** | Sworn statements of public officials (assets, income). Source currently partial (195 diputados). |
| **Successful Queries** | Log of well-answered queries used for analytics and future training. |

## 3. User Stories

### US-001 (P1) — Conversational chat about public data
**As** a citizen, **I want** to ask in Spanish "how much did the Ministry of Health spend in 2025?" and receive an answer with cited sources.

### US-002 (P1) — Real-time progress stream
**As** a user, **I want** to see progress messages ("Investigating sources...", "Analyzing data...") while the pipeline works, so I don't sit waiting with a blank screen.

### US-003 (P1) — Persistent session with memory
**As** a user, **I want** the system to remember the context of previous turns in the same conversation.

### US-004 (P1) — Public API for integrators
**As** an external developer, **I want** a `/api/v1/ask` endpoint with Bearer token, **so that** I can integrate OpenArg into my application.

### US-005 (P2) — Developer portal
**As** a developer, **I want** to create and revoke API keys from a UI, **so that** I can manage my access.

### US-006 (P2) — Ad-hoc analytical query via NL2SQL
**As** an analyst, **I want** to ask "give me the 10 jurisdictions with the highest budget execution in March 2026", **so that** I can get a response generated from SQL executed over cached tables.

### US-007 (P2) — Automated daily/weekly ingestion
**As** the system, **I need** to refresh data from external sources on scheduled times (daily/weekly/monthly depending on source), **so that** datasets stay up to date.

### US-008 (P2) — Operational observability
**As** an operator, **I want** health check and metrics endpoints, **so that** I can monitor the state of the system.

### US-009 (P3) — Admin panel to trigger on-demand ingestions
**As** an administrator, **I want** to manually trigger ingestion of a specific connector, **so that** I can force updates outside the schedule.

### US-010 (P3) — Audit of sensitive actions
**As** a compliance officer, **I want** an audit log of actions such as API key creation and admin changes.

## 4. Functional Requirements

- **FR-001**: The system MUST ingest data from multiple heterogeneous sources (CKAN, DKAN, REST APIs, CSV/JSON files) and normalize them to a common entity (`DataResult`).
- **FR-002**: The system MUST index datasets with vector embeddings for semantic search.
- **FR-003**: The system MUST answer natural-language questions by combining vector retrieval, pipeline step execution, and LLM synthesis.
- **FR-004**: The system MUST support progress streaming via WebSocket/SSE.
- **FR-005**: The system MUST persist query history for authenticated users.
- **FR-006**: The system MUST cache semantically similar responses to reduce latency and cost.
- **FR-007**: The system MUST expose a public API with Bearer token and per-plan rate limiting.
- **FR-008**: The system MUST allow authenticated users to create, list and revoke their own API keys.
- **FR-009**: The system MUST run scheduled jobs (Celery beat) for dataset refresh.
- **FR-009a**: Heavy ingestion bootstrap (initial scrape, bulk collect, transparency backfills) MUST run from scheduled or explicitly triggered control paths. Worker process startup MUST NOT dispatch those jobs implicitly unless an operator enables an explicit bootstrap flag for one-off recovery.
- **FR-010**: The system MUST expose component-level health checks and in-memory metrics.
- **FR-011**: The system MUST support read-only SQL execution over a sandbox with a table allowlist.
- **FR-012**: The system MUST generate SQL from natural language (NL2SQL) using a vector catalog of tables.
- **FR-013**: The system MUST gracefully degrade when external sources fail (retry, circuit breaker, partial response).
- **FR-014**: The system MUST audit sensitive actions (API key creation, admin actions).
- **FR-015**: The system MUST support JWT authentication + Google OAuth + API keys.
- **FR-016**: The system MUST count and log tokens consumed per LLM request.
- **FR-017**: The system MUST serve an admin interface (tasks router) for on-demand triggers.

## 5. Success Criteria

- **SC-001**: The query pipeline answers P1 questions in **≤15 seconds (p95)** under normal conditions (cache miss, BCRA+CKAN available).
- **SC-002**: Semantic cache hit rate **≥30%** in production (target; not measured today).
- **SC-003**: Public endpoint availability **≥99%** monthly.
- **SC-004**: Scheduled connector snapshots complete successfully in **≥95% of executions**.
- **SC-005**: Zero outages due to individual external source failures (graceful degradation).
- **SC-006**: Unit + integration tests + mypy pass on every PR to `staging`.
- **SC-007**: Docker build time **≤10 minutes** in CI.
- **SC-008**: `GET /health` response time **≤200ms**.

## 6. Assumptions & Out of Scope

### Assumptions
- Argentine public data sources will remain exposed with the current endpoints (or will be migrated gradually).
- The "useful" data window for users is primarily the last 5 years (no need for full historical archive).
- Users have internet connectivity for real-time chat (there is no offline mode).
- AWS Bedrock is available in `us-east-1`.

### Out of scope (at the macro level)
- Econometric predictions / forecasting — only raw data and simple derivations.
- Collaborative dataset editing.
- Semantic versioning of datasets (multiple historical snapshots).
- Automatic translation to English or Portuguese.
- Access to data with restricted PII.

## 7. Open Questions

- **[RESOLVED CL-001]** — **No formal SLA for now**. Publish a **"Service Description"** with honest expectations: best-effort availability ~99% (not contractually committed), p95 response time <15s under normal conditions, graceful degradation when upstream sources fail, per-plan rate limits (committed — enforced by code). **Reasons**: single EC2 without HA, no Sentry, no monitoring, no paid customers yet. **Upgrade path**: (1) paid customers + Sentry + uptime monitoring → commit to 99.5%; (2) multi-AZ RDS + multiple replicas + tested DR → commit to 99.9%. **Hard rule**: never commit to numbers that aren't measured. Configure Sentry + uptime monitoring BEFORE publishing any formal SLA.
- **[RESOLVED CL-002]** — **Cache hit rate is effectively 0%** at current traffic levels. `query_cache` had a single entry during a historical inspection. The cache is not serving hits because the system has very low traffic — not a code bug. A sibling observation: queries reach the pipeline but only a fraction trigger `write_cache`, because `fast_reply`, `clarify`, and the NL2SQL inline path do not write to the cache, and the fire-and-forget write used to fail silently without retry (fixed in `001-query-pipeline/DEBT-003`). **Revisit when** there is real traffic volume (>100 queries/day) — today there is no signal to optimize.
- **[RESOLVED CL-003]** — The allowlist lives in the frontend (`openarg_frontend/src/lib/authOptions.ts` NextAuth signIn callback). Per-environment policy: **staging** runs a private alpha (allowlist active, `OPEN_BETA=false`); **production** opens to the public via `OPEN_BETA=true` or an empty allowlist. General opening plan: staging stays private, prod is public under operator policy.
- **[RESOLVED CL-004]** — **No formal retention strategy for now** (explicit decision not to define one yet). Current model: **the user can manually request deletion** of their own data via the existing ARCO endpoints (`DELETE /users/me` for full cancellation, `PATCH /users/me/settings` with `save_history=false` to disable saving + cascade delete of conversations). There is no scheduled automatic cleanup task for `user_queries`, `messages`, `api_usage`, `successful_queries`. **Review when**: there are more users, legal requirements (GDPR/Argentine data protection law 25.326), or storage cost signals.
- **[RESOLVED CL-005]** — Partial DDJJ (only 195 diputados) is **accepted out of scope for now**. No expansion plan to senadores/executive/judges in the short term. The current dataset is sufficient for current use cases.
- **[NEEDS CLARIFICATION CL-006]** — Is there a roadmap to replace Bedrock Haiku if prices change?

## 8. Tech Debt Discovered (macro)

- **[DEBT-001]** — **Sentry is only conditionally active**. The backend has `setup_sentry()` wiring in `setup/logging_config.py`, but if `SENTRY_DSN` is unset in a deploy, external error monitoring is effectively absent and the system falls back to logs + in-memory metrics.
- **[DEBT-002]** — **Missing threshold/reranking/dedup in vector search**. Results can be noisy.
- **[DEBT-003]** — **10 CKAN portals down** without removal from the active catalog (santa_fe, modernizacion, ambiente, rio_negro, jujuy, salta, la_plata, cordoba_prov, cultura, cordoba_muni).
- **[DEBT-004]** — **Backup folder `openarg_backend_backup/`** coexists in the workspace — cleanup pending.
- **[DEBT-005]** — **BCRA connector without port** (see `002-connectors/002a-bcra/[DEBT-001]`) — unique violation of the hexagonal pattern in the system.
- **[DEBT-006]** — **Mix of async/sync in Celery workers** via `asyncio.run()` — recurring pattern, fragile.
- **[DEBT-007]** — **No distributed tracing** (OpenTelemetry or similar). Limited observability.
- **[DEBT-008]** — **Partial audit trail** — structure exists but is not applied to all sensitive actions.
- **[DEBT-009]** — **Worker startup bootstrap storm**. Celery `on_after_finalize` can dispatch heavy recovery work from every worker container, creating duplicate `bulk_collect_all` and other startup-time spikes. This is operationally unsafe and should be replaced by beat/manual control paths.

---

**End of spec.md** — See [./plan.md](./plan.md) for the as-built architecture.
