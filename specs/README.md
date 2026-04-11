# OpenArg Backend Specs

Project specifications following **Spec-Driven Design** adapted to hexagonal architecture.

## Purpose

This directory contains the structural documentation of OpenArg **reverse-engineered** from the code: it describes what each component does, how it is built today, and what tech debt exists. It does not modify code; it only documents reality.

Going forward it serves as the basis for **forward SDD**: new features are specified before being coded, reusing the conventions established here.

**Contributor rule**: any PR that changes observable behavior must update the relevant `spec.md` / `plan.md` as part of the change. See the [README § Spec-Driven Design](../README.md#spec-driven-design) for the full contract.

## Module index

The top-level modules are ordered roughly by architectural depth. Modules with ≥3 distinct responsibilities are **decomposed into sub-modules** — in those cases the top-level `spec.md` is a compact index and detailed FRs / tech debt live in the child folders.

| # | Module | Sub-modules | Scope |
|---|---|---|---|
| 000 | [`000-architecture/`](000-architecture/) | — | Macro as-built: layers, DI, routing, auth inventory, data API endpoints. |
| 001 | [`001-query-pipeline/`](001-query-pipeline/) | **5 sub-modules** | LangGraph query pipeline — 16 nodes + 1 subgraph. |
| 001a | [`001-query-pipeline/001a-intake/`](001-query-pipeline/001a-intake/) | — | Classification, cache check, memory load, query preprocessing. |
| 001b | [`001-query-pipeline/001b-planning/`](001-query-pipeline/001b-planning/) | — | Planner + skill detection + coordinator + replan loop. |
| 001c | [`001-query-pipeline/001c-execution/`](001-query-pipeline/001c-execution/) | — | Step executor, connector dispatch, fallback injection. |
| 001d | [`001-query-pipeline/001d-analysis/`](001-query-pipeline/001d-analysis/) | — | Analyst + policy synthesis, chart/map building, META parsing. |
| 001e | [`001-query-pipeline/001e-finalization/`](001-query-pipeline/001e-finalization/) | — | Finalize node, background tasks, stream close. |
| 002 | [`002-connectors/`](002-connectors/) | **16 sub-modules** | Connector pattern + every specific connector (BCRA, Series Tiempo, CKAN, DDJJ, ...). |
| 003 | [`003-auth/`](003-auth/) | — | Auth inventory: JWT NextAuth, BACKEND_API_KEY service token, user API keys (`oarg_sk_*`), ADMIN_EMAILS allowlist, DATA_SERVICE_TOKEN (`svc_xxx`). |
| 004 | [`004-semantic-cache/`](004-semantic-cache/) | — | pgvector semantic cache + Redis exact cache + TTL by intent + cleanup beat task. |
| 005 | [`005-vector-search/`](005-vector-search/) | — | Hybrid retrieval: vector + lexical + RRF fusion. |
| 006 | [`006-datasets/`](006-datasets/) | **2 sub-modules** | Dataset catalog + ingestion pipeline. |
| 006a | [`006-datasets/006a-catalog/`](006-datasets/006a-catalog/) | — | Metadata, entity, `IDatasetRepository`, listing/stats/download endpoints. |
| 006b | [`006-datasets/006b-ingestion/`](006-datasets/006b-ingestion/) | — | Scraper → collector → embedder → S3; 5-chunk strategy; `_detect_schema_drift()`. |
| 007 | [`007-public-api/`](007-public-api/) | — | `/api/v1/ask` Bearer-token public endpoint + per-plan rate limiting. |
| 008 | [`008-developers-keys/`](008-developers-keys/) | — | API key CRUD for end users (SHA-256 hash, once-visible plaintext). |
| 009 | [`009-monitoring/`](009-monitoring/) | — | Health checks + in-memory `MetricsCollector` + `/metrics`. |
| 010 | [`010-sandbox-sql/`](010-sandbox-sql/) | **2 sub-modules** | SQL sandbox + NL2SQL. |
| 010a | [`010-sandbox-sql/010a-sql-sandbox/`](010-sandbox-sql/010a-sql-sandbox/) | — | Read-only executor, 3-layer validation, ThreadPoolExecutor. |
| 010b | [`010-sandbox-sql/010b-nl2sql/`](010-sandbox-sql/010b-nl2sql/) | — | LLM → SQL → execute → fix loop (subgraph dead code, see FIX-004). |
| 011 | [`011-table-catalog/`](011-table-catalog/) | — | Vector-indexed cached table metadata for NL2SQL grounding. |
| 012 | [`012-admin/`](012-admin/) | — | Admin task registry + orchestrator endpoints. |

Cross-cutting artifacts:

| Doc | Purpose |
|---|---|
| [`constitution.md`](constitution.md) | Non-negotiable principles (hexagonal, DI, async-first, anti-hallucination, ports before adapters). |
| [`FIX_BACKLOG.md`](FIX_BACKLOG.md) | Prioritized list of fixes discovered during spec review (8 items, 5 applied, 3 pending). |
| [`REVIEW_REPORT_2026-04-10.md`](REVIEW_REPORT_2026-04-10.md) | Senior-engineer review pass cross-checking specs against code. |

## Spec format

Each module has **two files**: `spec.md` (the WHAT/WHY) and `plan.md` (the HOW). The separation follows GitHub Spec Kit and respects the hexagonal boundary: `spec.md` lives in the domain language, `plan.md` speaks of infrastructure.

When a module is large enough to warrant sub-modules, its top-level `spec.md` and `plan.md` become compact **indices** that list sub-modules and point to them for detail. The sub-modules each have their own `spec.md` + `plan.md` following the standard template.

### `spec.md` — structure

```markdown
# Spec: <Name>

**Type**: Reverse-engineered | Forward
**Status**: Draft | Reviewed | Canonical
**Last synced with code**: YYYY-MM-DD
**Hexagonal scope**: Domain | Domain+App | Full-stack | Adapter-only

## 1. Context & Purpose
One paragraph. What this feature does for the user/system. Domain level, no technology.

## 2. Ubiquitous Language
Terms with their definitions. Domain only.

## 3. User Stories
Prioritized stories (P1/P2/P3). Each independently testable.
- **US-NNN (Pn)**: As a <role>, I want <action>, so that <benefit>.

## 4. Functional Requirements
Numbered list. Each FR is testable.
- **FR-NNN**: The system MUST ...

## 5. Success Criteria
Observable, tech-agnostic metrics.
- **SC-NNN**: ...

## 6. Assumptions & Out of Scope
What we take for granted and what doesn't belong.

## 7. Open Questions
- **[NEEDS CLARIFICATION CL-NNN]** questions that remain open
- **[RESOLVED CL-NNN]** once an answer is verified against code

## 8. Tech Debt Discovered
- **[DEBT-NNN]** findings from reverse engineering
- **[DEBT-NNN] — FIXED YYYY-MM-DD** once the debt is paid down
```

### `plan.md` — structure

```markdown
# Plan: <Name>

**Related spec**: ./spec.md
**Type**: Reverse-engineered | Forward
**Status**: Draft | Reviewed | Canonical
**Last synced with code**: YYYY-MM-DD

## 1. Hexagonal Mapping
What lives in what layer (Domain / Application / Infrastructure / Presentation).

## 2. As-Built Flow
ASCII diagram or sequence describing the real flow.

## 3. Ports & Contracts
Interfaces consumed/exposed.

## 4. External Dependencies
URLs, auth, rate limits, timeouts, retry policies.

## 5. Persistence
Tables read/written, key columns, referenced migrations.

## 6. Async / Scheduled Work
Celery tasks (name, schedule, queue, retry), LangGraph nodes.

## 7. Observability
Metrics, logs, audit trail.

## 8. Source Files
Map to real files (with paths relative to `src/`).

## 9. Deviations from Constitution
Known violations of `constitution.md` with justification or debt ticket.
```

## Format rules

1. **`spec.md` does not mention technology.** If "HTTP", "SQL", "Celery", "BCRA API" appears — it goes in `plan.md`.
2. **`plan.md` does not invent requirements.** It only describes what exists (reverse) or what was decided (forward).
3. **Every tech debt finding is explicitly marked** with `[DEBT-NNN]` in `spec.md` section 8.
4. **Ambiguities are marked with `[NEEDS CLARIFICATION CL-NNN]`** — they are opened as questions, not resolved by guessing.
5. **Traceability always present**: section 8 of `plan.md` points to real files with paths. If a spec cannot point to the code, it is outdated.
6. **`Last synced with code`** must be bumped whenever a spec is touched alongside a code change.
7. **English only** across all specs (the i18n strings inside code blocks are implementation artifacts and stay as-is).
