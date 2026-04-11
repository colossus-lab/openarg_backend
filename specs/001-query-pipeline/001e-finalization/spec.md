# Spec: Query Pipeline — Phase E: Finalization

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Application (finalize node) + Infrastructure (Redis, pgvector semantic cache, PG audit, metrics)
**Parent**: [../spec.md](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The **Finalization phase** is the pipeline's exit ramp. It runs once per query (never on cache/fast-reply short-circuits — those terminate before reaching this phase) and is responsible for:

- Extracting structured **sources** and **documents** from the raw `data_results`.
- Auditing the query (intent, duration, `user_id`) to the `user_queries` table.
- Firing off the **cache write** (Redis + pgvector) as a background task.
- Firing off the **memory update** as a retryable background task.
- Computing `duration_ms` from `_start_time`.
- Closing the streaming event with `{type: "complete", ...}`.

Node covered:

1. `finalize` — the only node in this phase, emitting no streaming status of its own.

The phase also owns the `_background_tasks: set[asyncio.Task]` registry and the `_spawn_background()` helper that prevents weak-ref GC of fire-and-forget tasks (post-FIX-005).

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Finalize** | The terminal node of the full pipeline (not used by cache/fast-reply short-circuits). |
| **Sources** | List of `{name, url, portal, fetched_at}` dicts extracted from `data_results`. |
| **Documents** | Structured records (e.g. DDJJ cards) extracted from `data_results` for card rendering. |
| **Audit log** | Row written to `user_queries` with intent, duration, tokens, and user ID. |
| **Cache write** | Fire-and-forget task that stores the final answer in Redis + pgvector semantic cache. |
| **Memory update** | Fire-and-forget task that persists the turn into Redis session memory with retries. |
| **`_background_tasks`** | Module-level `set[asyncio.Task]` in `finalize.py` holding strong references to fire-and-forget tasks. |
| **`_spawn_background`** | Helper that adds the task to `_background_tasks` and attaches a `done_callback` logging exceptions. |
| **`complete` event** | Terminal streaming event containing the final answer, sources, charts, map, confidence, citations, documents, warnings, duration, tokens. |

## 3. User Stories

### US-001 (P1) — Conversational response with sources (finalization portion)
**As** an end user, **I want** the final response to include source citations and duration metadata so I can trust the answer and see how long it took. **Trigger**: `finalize` runs after the analyst (and optionally the policy node) on the normal path.

### US-014 (P2) — Offline execution (batch, finalization portion)
**As** the system, **when** the pipeline runs offline via Celery, **I need** finalize to still audit and persist results even though there is no streaming client. *(Offline runs skip the `complete` streaming event but still run side effects.)*

## 4. Functional Requirements

### Cache (write side)
- **FR-006**: The system MUST write to the cache post-finalize (fire-and-forget, non-blocking) with a TTL derived from the intent.

### Memory (update side)
- **FR-010**: The system MUST update memory post-finalize in a background task with retries.

### Finalize
- **FR-032**: Finalize MUST extract sources from `data_results` (name, url, portal, fetched_at).
- **FR-033**: Finalize MUST extract structured `documents` (e.g. DDJJ records) for card rendering.
- **FR-034**: Finalize MUST audit the query (intent, duration, user_id).
- **FR-035**: Finalize MUST compute `duration_ms` from `_start_time`.
- **FR-036**: Finalize MUST trigger cache write + memory update as fire-and-forget.

### Streaming (terminal event)
- **FR-037**: The pipeline MUST support both `updates` (completed nodes) and `custom` (events emitted by nodes) modes in `astream()`.
- **FR-038**: The pipeline MUST apply a **whitelist of keys** in custom events (`type, step, detail, progress, message, status, content, question, options, map_data`) to prevent leaking internal prompts.
- **FR-039**: The labels shown to the user MUST be in Spanish (Estratega / Investigador / Analista / Redactor).

### Checkpointing
- **FR-040**: When `AsyncPostgresSaver` is configured and `conversation_id` is present, the system MUST persist state by `thread_id` for resumable runs and multi-turn memory.

### Security (cross-reference)
- **FR-042**: The pipeline MUST NOT leak `analysis_prompt`, tracebacks, or internal prompts to the client. *(The streaming-key whitelist in the HTTP router is the enforcement point.)*

## 5. Success Criteria

- **SC-008**: Checkpointing allows resuming a conversation >30 minutes later without context loss. *(Depends on finalize persisting state via `AsyncPostgresSaver`.)*
- **SC-005**: **Zero internal prompts leaked** to the client via streaming. *(The `complete` event must respect the whitelist.)*
- **SC-001** *(partial)*: P1 response (normal path, cache miss) in **<15 seconds (p95)**. Finalize itself is negligible, but its background tasks must not block the response.

## 6. Assumptions & Out of Scope

### Assumptions
- Redis is available for session memory and cache writes.
- pgvector semantic cache supports insert with a pre-computed embedding (see CL-002).
- `AsyncPostgresSaver` is configured in production (checkpointing enabled).
- Background tasks are allowed to outlive the HTTP request (FastAPI / uvicorn event loop keeps running).

### Out of scope (this sub-module)
- **Semantic cache internals** — see `004-semantic-cache/`.
- **Memory store implementation** — see `005-memory/` (if present).
- **Metrics pipeline** — see `008-monitoring/`.
- **Audit table schema** — see `007-persistence/` (if present).
- **Frontend rendering of the `complete` event** — see the SSE events contract (CL-007).

## 7. Open Questions

*(No open clarifications specific to this phase. CL-002 — `last_embedding` reuse — was resolved in Phase A: Intake and is consumed here when writing to the cache.)*

## 8. Tech Debt Discovered

- **[DEBT-005]** — **Module-level `__getattr__` hack** in `nodes/__init__.py:40-70` to expose `_deps` via ContextVar. It works but confuses linters and humans. *(Cross-cutting: applies to all pipeline phases, tracked here because finalize is the last node that relies on it.)*
- **[DEBT-010]** — **WebSocket stream_mode handling has hardcoded node names** (`smart_query_v2_router.py:345`). If nodes are renamed, it breaks without visible errors.
- **[DEBT-012]** — ~~**`asyncio.create_task()` fire-and-forget**~~ **FIXED 2026-04-10**: `finalize.py` now uses a module-level `_background_tasks: set[asyncio.Task]` with a `_spawn_background()` helper that keeps a strong reference and attaches a `done_callback` logging any unhandled exception. Prevents CPython's weak-ref task GC and surfaces background failures in logs.
- **[DEBT-014]** — **`_get_or_compile_graph()` uses `threading.Lock` in async context** (`graph.py:41-48`). Thread safety in async is subtle and can cause rare bugs.
- **[DEBT-015]** — **`_get_checkpointer()` holds global state** with a double-check pattern. It does not reset if `DATABASE_URL` changes at runtime.
- **[DEBT-017]** — **Streaming key whitelist is hardcoded** (`smart_query_v2_router.py:324-335`). If a new node needs to emit a non-whitelisted key, it fails silently.

---

**End of spec.md** — See [./plan.md](./plan.md) for the as-built topology of this phase.
