# OpenArg Constitution

**Version**: 1.2.0
**Status**: Draft (reverse-engineered from codebase + `CLAUDE.md` + `MEMORY.md`)
**Last synced with code**: 2026-04-11
**Scope**: Backend (`openarg_backend`). Frontend has its own `constitution.md`.

---

## Purpose

This document codifies the **non-negotiable principles** that govern OpenArg development. Every `plan.md` of any feature must pass the "Constitution Check" declared here before moving forward. Known deviations are listed in section 9 of each `plan.md` with explicit justification and a debt ticket.

The constitution is **immutable by convention**: modifying it requires a semver version bump, written justification, and a review of the impact on existing specs (Sync Impact Report).

---

## 0. Keep It Simple (axiom)

**Simplicity beats cleverness, always.** This is the principal axiom — every other article in this constitution must be read in its light.

Concretely, when designing or reviewing a change:

1. **Prefer the obvious solution.** If a junior engineer can read the spec and plan and understand why the code does what it does, the design is good. If it takes a seasoned engineer 20 minutes to follow the dance, the design is wrong even if it is technically correct.
2. **Add abstractions only when they pay rent.** Never introduce an interface, a pattern, or a layer for a hypothetical second caller. Three similar lines beat a premature factory. A second caller earns the abstraction; a first caller does not.
3. **Small files beat small functions in big files.** Prefer splitting by responsibility over creating 30-line helpers inside 600-line modules.
4. **Data flow over control flow.** A pipeline of pure transforms is easier to reason about than a mesh of callbacks, events, and mutable shared state. Prefer the former even if it takes more keystrokes.
5. **One way to do something.** If two code paths converge on the same outcome, delete one. Duplicate code is cheaper than duplicate semantics.
6. **Delete before you add.** When a `spec.md` or `plan.md` grows, ask what can be removed. If a section can go without losing information, remove it.
7. **When in doubt, write the dumbest version that works, ship it, and let usage teach you what to generalize.**

A spec that violates this axiom for a good reason must cite the reason inline ("complexity justified because X") and open a debt item to revisit once the assumption is validated. Complexity without justification is a bug in the design.

---

## 0.5. Spec → Code → Verify (axiom)

**Every change follows a three-step cadence, in strict order.** This is the second principal axiom alongside §0 Keep It Simple.

1. **Modify the spec first.** Before writing any production code, update the relevant `spec.md` / `plan.md` under `specs/` to describe the new behavior in WHAT/WHY terms. Add or edit FRs, update the output contract, append or strike tech-debt entries. If no spec section needs to change, the code probably doesn't need to change either — or the spec is incomplete, in which case complete it first.

2. **Then write the code.** Make it match the spec as-written, not the other way around. If you discover during coding that the spec is wrong, STOP, fix the spec, then return to the code. Do not diverge and reconcile later.

3. **Then verify.** Run `make code.test`, run `ruff` and `mypy`, grep the diff against the spec's FRs to check for contradictions. Bump the spec's `Last synced with code` field. If verification reveals a mismatch, repair whichever side is wrong — usually it is the spec that under-specified a detail you had to invent while coding.

### Why the order matters

- Writing the spec first forces you to think in the **domain language** before you are tempted by implementation details. Implementation-first thinking produces specs that are thinly-disguised code commentary.
- The spec is the contract reviewers check the code against. If the spec arrives after the code, reviewers have nothing to check, and specs quietly rot into fiction.
- The first caller of a new abstraction is the spec itself: if you cannot defend the abstraction in FR form, you probably shouldn't add the code.
- Specs that move **after** code always lag. Specs that move **before** code stay current by construction.

### When you may bend this rule

- **Pure typo / rename / autoformat** — no spec change needed.
- **Test-only changes** that cover existing behavior — the spec already describes what's being tested.
- **Reverting a recent regression** to restore a previously-specified state — both sides can revert in the same commit.

### Never bend when

- You are adding a new `FR-NNN` or `SC-NNN`.
- You are changing an existing FR's acceptance criteria.
- You are closing a `[DEBT-NNN]` or `[CL-NNN]` entry — the strikethrough + `FIXED YYYY-MM-DD` marker lives in the spec and must land in the same commit as the code fix.
- You are touching a file that already has a sibling `spec.md` / `plan.md`.

### The bright line

Any PR that modifies `src/` without touching the corresponding `specs/` will be asked by reviewers to fix the drift before merging. The contributor contract in [`README.md#spec-driven-design-is-the-contract`](../README.md#spec-driven-design-is-the-contract) is the enforcement hook. This axiom exists because even the original author of these specs caught himself skipping the cadence twice in a single session: the cadence is cheap, the drift is expensive, and the only way not to skip it is to make it non-negotiable.

---

## I. Hexagonal Architecture

The code is organized strictly into the following layers:

```
src/app/
├── domain/              # Ubiquitous language, entities, ports (ABC), exceptions
├── application/         # Use cases, orchestration, pipeline steps
├── infrastructure/      # Adapters, persistence, celery, resilience, monitoring
├── presentation/        # HTTP controllers, routers, middleware
└── setup/               # IoC (Dishka), config, app factory
```

**Rules**:
1. **`domain/` does not import anything from `infrastructure/`**. Only from itself.
2. **`application/` imports from `domain/`** (entities, ports) but NOT from `infrastructure/` directly. It receives implementations via DI.
3. **`infrastructure/` implements `domain/ports/`**. Adapter classes type ports as dependencies.
4. **`presentation/` imports from `application/`** and from `domain/` for DTOs. Routers don't know SQL or external HTTP.
5. **`setup/ioc/provider_registry.py` is the ONLY place where implementations are wired to ports.**

**Known violations**: see individual specs. The most notable case is the BCRA connector which has no port (`[002a-bcra/DEBT-001]`).

---

## II. Pinned Stack

The tech stack is part of the constitution. Changes require a major bump.

| Layer | Technology | Version |
|---|---|---|
| Runtime | Python | 3.12 |
| Web framework | FastAPI | 0.115 |
| ASGI server | Uvicorn + UVLoop | — |
| ORM | SQLAlchemy async | 2.0 |
| Migrations | Alembic | — |
| Database | PostgreSQL + pgvector | 16 + HNSW |
| DI container | Dishka | 1.6 |
| Task queue | Celery + Redis | 5.4 + 7 |
| Primary AI | AWS Bedrock Claude Haiku | 4.5 |
| Fallback AI | Google Gemini Flash | 2.5 |
| Embeddings | AWS Bedrock Cohere Embed Multilingual | v3 (1024-dim) |
| Pipeline | LangGraph | — |
| HTTP client | HTTPX async | — |
| Auth | PyJWT + bcrypt | — |
| Rate limiting | SlowAPI | — |
| Config | TOML + Pydantic settings | — |
| Logging | structlog | — |
| Lint | Ruff (100 chars) | — |
| Type check | mypy strict | — |
| Test | pytest | — |

---

## III. Dependency Injection (Dishka)

1. **All singletons use `Scope.APP`** (settings, engines, AWS clients).
2. **All per-request objects use `Scope.REQUEST`** (DB sessions, user context).
3. **`Scope.SESSION` is not used** — there are no persistent sessions beyond the request.
4. **Celery workers use a dedicated provider** — workers are not async-native and request-scope does not apply.
5. **Instantiating adapters with inline `import` inside business code is forbidden.** Everything goes through the container.

**Known violation**: `bcra_tasks.py` instantiates `BCRAAdapter` directly (`[002a-bcra/DEBT-004]`).

---

## IV. Async-First

1. **All I/O uses `async`/`await`** in the HTTP and application layers.
2. **SQLAlchemy sessions are async** on the request-path.
3. **Celery workers are sync** because Celery 5 does not support native async. Inside the worker, `asyncio.run()` is allowed as a bridge, but it is considered structural debt and a ticket is opened every time it is used.
4. **HTTP clients are `httpx.AsyncClient`**. `requests` is not used.

---

## V. Models and Types

1. **Pydantic v2** for: HTTP API schemas, config validation, cross-process contracts.
2. **Dataclasses** for: domain entities.
3. **SQLAlchemy 2.0 mapped classes** in `infrastructure/persistence_sqla/mappings/` — explicit table ↔ entity mapping, never the other way around.
4. **Using Pydantic in `domain/` is forbidden**. It contaminates the domain with HTTP adapter knowledge.
5. **TypedDict or Protocol** for lightweight structural contracts that don't need a full class.

---

## VI. Resilience

All accesses to external services MUST implement:

1. **Retry with the `@with_retry` decorator** (`infrastructure/resilience/retry.py`)
   - Exponential backoff + jitter
   - Configurable max_retries (default: 2)
   - Retryable HTTP statuses: `429, 500, 502, 503, 504`
2. **Circuit breaker per connector** (`infrastructure/resilience/circuit_breaker.py`)
   - States: `CLOSED` → `OPEN` → `HALF_OPEN`
   - `failure_threshold=5`, `recovery_timeout=60s`
3. **Explicit timeouts** in all HTTP clients. Never `timeout=None`.
4. **Graceful degradation**: when an external source fails, the system responds with partial data or cache, never fails entirely.
5. **Errors are not silenced without structured logging and an incremented metric.**

**Recurring known violation**: silent degradation without metrics in several connectors. See debt items in each spec.

---

## VII. Observability

1. **Logging**: `structlog` in infrastructure and presentation. Standard logger accepted in domain/application for now.
2. **Metrics**: `MetricsCollector` singleton (`infrastructure/monitoring/metrics.py`) tracks requests, connectors, cache hits, tokens used. Exposed via `GET /api/v1/metrics`.
3. **Health checks**: `HealthCheckService` (`infrastructure/monitoring/health.py`) with per-component checks. Endpoints: `GET /health`, `GET /health/ready`.
4. **Audit trail**: `infrastructure/audit/` records sensitive actions (API key creation, admin actions).
5. **Sentry**: NOT yet configured. Open debt (`MEMORY.md: "Sentry DSN not configured"`).

---

## VIII. Data & Persistence

1. **All schema modifications go through Alembic**. Migrations in `infrastructure/persistence_sqla/alembic/versions/` named `YYYY_MM_DD_NNNN_description.py`.
2. **No tables are created on-the-fly in production** except for dynamic `cache_*` tables generated by connector snapshots (see violation `[002a-bcra/DEBT-006]`).
3. **The core schema** (`datasets`, `dataset_chunks`, `cached_datasets`, `user_queries`, `query_cache`, `api_keys`, `api_usage`, `table_catalog`, `successful_queries`, `agent_tasks`, `query_dataset_links`) is considered stable. Changes require justification in `plan.md`.
4. **pgvector with HNSW indexing**, 1024-dimensional embeddings (Cohere Embed Multilingual v3).
5. **Pool config**: `pool_pre_ping=True`, `pool_recycle` configured (see `persistence_sqla/provider.py`).
6. **Explicit transactions** with `async with session.begin()` in request-scope.

---

## IX. Testing

1. **`pytest` is the only test runner**.
2. **~697 maintained tests** (baseline Mar 2026).
3. **Levels**:
   - **Unit**: domain + application, without real DB.
   - **Integration**: with real PostgreSQL + Redis (Docker services in CI).
   - **The DB is not mocked in integration tests.** (Principle analogous to Spec Kit's advice: "Integration-First".)
4. **Coverage target**: no hard percentage established, but CI fails if it drops significantly.
5. **Type checking**: `mypy --strict` in CI. No function may remain untyped.
6. **Lint**: `ruff` with 100-character max line length.
7. **CI workflow**: `.github/workflows/test.yml` runs unit + integration + mypy on every PR.

---

## X. Language and Conventions

1. **Domain comments/docstrings in Spanish** (because the domain is Argentina-specific).
2. **Infrastructure code in English** (international standard).
3. **End-user messages in Spanish** (friendly pipeline status messages, HTTP errors).
4. **Logs in English** for compatibility with observability tools.
5. **Commits**: do not add `Co-Authored-By` at the end of the message (see `MEMORY.md: "Do NOT add Co-Authored-By to commits"`).
6. **PRs**: go to `staging`, not `main`/`master`.

---

## XI. AI / LLM

1. **Primary: AWS Bedrock Claude Haiku 4.5**. All LLM calls default to this model.
2. **Fallback: Google Gemini 2.5 Flash**. Activated when Bedrock fails or is rate-limited.
3. **Embeddings: AWS Bedrock Cohere Embed Multilingual v3** (1024-dim). OpenAI is not used.
4. **Prompts** live in `application/pipeline/prompts/` or similar — never hardcoded in business logic.
5. **Tokens counted and logged** per request (see `MetricsCollector`).
6. **Pipeline with LangGraph**: stateful graph with checkpointing. Do not use ad-hoc chains outside the graph.

---

## XII. Security

1. **Authentication**: JWT (PyJWT) + bcrypt for passwords. Google OAuth via NextAuth in the frontend.
2. **API Keys**: hashed with SHA-256 before persisting. Shown to the user **only once** when created.
3. **Rate limiting**: SlowAPI with per-plan policies (free, paid, admin).
4. **SQL sandbox**: read-only, statement timeout, table allowlist via `table_catalog`.
5. **Secrets**: `.secrets.toml` or environment variables. Never commit.
6. **Email allowlist** for alpha access (currently 25 emails).
7. **Auditing** of sensitive actions (see principle VII.4).

---

## XIII. Deployment

1. **Docker Compose** is the production runtime (13 services on EC2).
2. **Images on GHCR** (GitHub Container Registry), builds via GitHub Actions (`.github/workflows/build.yml`).
3. **Caddy 2** as reverse proxy / TLS terminator.
4. **PGBouncer** between the app and PostgreSQL RDS.
5. **Celery workers segmented** by queue (scraper, embedding, collector, analyst, transparency, ingest, s3) with configurable concurrencies.
6. **Celery beat** a single process.
7. **Per-environment configuration**: `config/local/`, `config/prod/` with TOML.

---

## Compliance

Every `plan.md` under `specs/` MUST have a "Deviations from Constitution" section that:

1. Explicitly lists which principles are violated.
2. For each violation: justification + debt ticket + `[DEBT-NNN]`.
3. Indicates whether the violation is **temporary** (fix plan) or **accepted** (conscious trade-off).

The `Constitution Check` is the gate prior to forward SDD: before implementing a new feature, the plan is validated to ensure it does not introduce new undocumented violations.

---

**End of constitution.md**
