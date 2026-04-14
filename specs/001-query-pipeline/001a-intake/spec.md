# Spec: Query Pipeline — Phase A: Intake

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-13
**Hexagonal scope**: Application (pipeline nodes) + Infrastructure (Redis, pgvector semantic cache)
**Parent**: [../spec.md](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The **Intake phase** is the front door of the query pipeline. It receives the raw user question and decides — as cheaply as possible — whether the pipeline should keep running or short-circuit with a pre-canned / cached / clarification reply.

It covers the first four nodes of the LangGraph graph:

1. `classify` — regex/heuristic classification (casual, meta, injection, off_topic, educational, data).
2. `cache_check` — Redis exact-hash lookup + pgvector semantic cache lookup.
3. `load_memory` — conversational memory restoration (session + chat history).
4. `preprocess` — deterministic query normalization (acronyms, temporal, provinces, synonyms).

The phase ends when the state is either routed to a terminal fast-reply/cache-reply node (short-circuit) or advanced to the **Planning phase** with a preprocessed query + loaded memory context. In the current implementation, the pre-planner preparation overlaps independent I/O: Redis memory load, DB chat-history load, and catalog-hint discovery run concurrently instead of serially.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Classification** | Initial classification of the query: casual / meta / injection / off_topic / educational / data. |
| **Fast reply** | Pre-canned response without LLM for non-data classifications. |
| **Semantic cache** | Vector cache (pgvector HNSW) that returns previous responses for similar queries. |
| **Exact cache** | Redis hash-keyed cache for byte-identical queries. |
| **Memory context** | Chat history + session memory restored from Redis/DB by `conversation_id`. |
| **`memory_ctx`** | Full memory context fed to the planner. |
| **`memory_ctx_analyst`** | Light memory context fed to the analyst (avoids data bleed). |
| **Preprocessing** | Deterministic rewriting of the raw question (no LLM). |

## 3. User Stories

### US-001 (P1) — Conversational response with sources (intake portion)
**As** an end user, **I want** to ask a question in Spanish and have the pipeline start processing it. **Trigger**: POST `/api/v1/query/smart`. **Intake path**: classify → cache_check → load_memory → preprocess → (hand-off to planning).

### US-003 (P1) — Casual greeting and pre-canned response without LLM
**As** the system, **when** the user types "hola" or "gracias", **I need** to respond instantly without calling the LLM or the data pipeline. **Trigger**: the `classify` node detects the pattern → direct route to `fast_reply`.

### US-004 (P1) — Transparent semantic cache
**As** the system, **when** a question is semantically similar to a previous one, **I need** to return the cached response without re-executing the full pipeline. **Trigger**: `cache_check` finds a hit in Redis or the pgvector semantic cache → direct route to `cache_reply`.

### US-005 (P1) — Multi-turn conversational memory
**As** a user, **I want** to ask follow-up questions ("and in 2024?") and have the system understand the prior context. **Trigger**: `load_memory` restores the session by `conversation_id` and feeds `planner_ctx` and `memory_ctx_analyst`.

### US-008 (P1) — Prevent prompt injection
**As** the system, **I need** to detect prompt injection attempts and reject them with a canned response. **Trigger**: `classify` applies `is_suspicious()` → "injection" classification → `fast_reply`.

## 4. Functional Requirements

### Classification and routing
- **FR-001**: The system MUST classify every query into one of: `casual` (greeting/thanks/farewell), `meta` (who are you), `injection`, `off_topic`, `educational`, or `None` (data query).
- **FR-002**: The system MUST route non-data queries to `fast_reply` without invoking the LLM.
- **FR-003**: The system MUST run `classify` without LLM dependency (regex + heuristics).

### Cache
- **FR-004**: The system MUST check the Redis cache (exact hash) and the semantic cache (pgvector HNSW) in parallel before invoking the pipeline.
- **FR-005**: The system MUST skip the cache when `policy_mode=True`.
- *(Note: FR-006 — cache write — belongs to Phase E: Finalization.)*

### Memory
- **FR-007**: The system MUST load session memory by `conversation_id` from Redis.
- **FR-008**: The system MUST load chat history (last 6 messages) from the DB.
- **FR-009**: The system MUST maintain two versions of the context: full `memory_ctx` (planner) and light `memory_ctx_analyst` (to avoid data bleed in the analyst).
- **FR-009a**: The system MUST overlap independent pre-planner work where possible. Session memory load, chat-history load, and catalog-hint discovery MUST NOT be serialized behind one another when they can run concurrently.
- *(Note: FR-010 — memory update post-finalize — belongs to Phase E: Finalization.)*

### Security
- **FR-041**: The pipeline MUST detect and block prompt injection attempts in `classify`.

## 5. Success Criteria

- **SC-002**: Cache hit response in **<500ms (p95)**.
- **SC-003**: Fast reply (non-data classification) in **<100ms**.
- **SC-008**: Checkpointing allows resuming a conversation >30 minutes later without context loss. *(Depends on `load_memory` correctly restoring state per `conversation_id`.)*

## 6. Assumptions & Out of Scope

### Assumptions
- Redis is available for session memory and exact-hash cache.
- pgvector semantic cache table is populated and indexed (HNSW).
- The frontend sends a stable `conversation_id` for multi-turn sessions.

### Out of scope (this sub-module)
- **Semantic cache internals** — see `004-semantic-cache/`.
- **Embedding generation** — handled by the Bedrock Cohere adapter, see `003-embeddings/` (if present).
- **Cache and memory writes** — belong to Phase E: Finalization (see `../001e-finalization/`).
- **Planning logic** — belongs to Phase B: Planning (see `../001b-planning/`).

## 7. Open Questions

- **[RESOLVED CL-002]** — `last_embedding` **is reused**, not regenerated. In `finalize.py:100` it is passed to `write_cache(...)`, and in `cache_manager.py:152-154` it is used directly when not None (it only calls `get_embedding()` when None). It is the embedding of the original query, appropriate for the cache write.
- **[RESOLVED CL-003]** — `ttl_for_intent()` lives in `semantic_cache.py:47-70`. Exact mapping: **REALTIME 300s** (`dolar`, `riesgo_pais`, `cotizacion`), **DAILY 1800s** (`inflacion`, `series`, `emae`, `tipo_cambio`, `reservas`, `base_monetaria`, `desempleo`, `exportaciones`, `importaciones`, `balanza_comercial`), **STATIC 7200s** (`ddjj`, `sesiones`, `ckan`, `busqueda`, `catalogo`, `legisladores`, `georef`). Default: DAILY. It does token matching over the intent string, then substring fallback.

## 8. Tech Debt Discovered

- **[DEBT-003]** — ~~**Asymmetric cache write and memory update**~~ **FIXED 2026-04-10**: `cache_manager.py` now has `_retry_async()` helper with exponential backoff applied to both Redis and semantic-cache writes. Matches the memory update retry loop in `finalize.py`. *(Cross-reference: the write side lives in `../001e-finalization/`.)*

---

**End of spec.md** — See [./plan.md](./plan.md) for the as-built topology of this phase.
