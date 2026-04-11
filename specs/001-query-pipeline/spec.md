# Spec: Query Pipeline (LangGraph)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Application (core) + Infrastructure (nodes/adapters)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The **Query Pipeline** is the functional heart of OpenArg: the **stateful LangGraph graph** that transforms a user's natural-language question into a structured response with cited sources, optional charts, optional maps, and conversational memory. It coordinates **classification**, **semantic cache**, **multi-source retrieval**, **LLM analysis with streaming**, **adaptive replanning**, and **finalization with side effects**.

It is the composition of 16 nodes + 1 subgraph (NL2SQL) + ~6 helpers in a DAG topology with a controlled replanning loop. Exposed via HTTP sync (`/api/v1/query/smart`) and WebSocket streaming (`/api/v1/query/ws/smart`).

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Pipeline** | The complete LangGraph graph that processes a query. |
| **Node** | A stage of the graph (classify, plan, execute, analyze, etc.). |
| **State** | Shared state across nodes (`OpenArgState` TypedDict). |
| **ExecutionPlan** | Structured plan produced by the planner containing an ordered sequence of `PlanStep`. |
| **PlanStep** | Atomic unit of work (e.g. "query BCRA with casa=USD"). |
| **Agent** | Stage with a narrative role shown to the user: Strategist (planner), Investigator (executor), Analyst (analyst), Writer (finalize). |
| **Replan** | Adaptive retry when the initial data is insufficient (maximum 2 passes). |
| **Checkpointing** | State persistence by `conversation_id` using `AsyncPostgresSaver`. |
| **Streaming event** | Message emitted in real time to the WS client via `get_stream_writer()`. |
| **Classification** | Initial classification of the query: casual / meta / injection / off_topic / educational / data. |
| **Fast reply** | Pre-canned response without LLM for non-data classifications. |
| **Semantic cache** | Vector cache that returns previous responses for similar queries. |

## 3. User Stories

### US-001 (P1) — Conversational response with sources
**As** an end user, **I want** to ask a question about public data in Spanish and receive a structured response with source citations, optional charts, and maps when applicable. **Trigger**: POST `/api/v1/query/smart`. **Happy path**: classify → cache_check → load_memory → preprocess → skill_resolver → planner → execute_steps → analyst → coordinator → finalize → JSON response.

### US-002 (P1) — Real-time progress streaming
**As** a user, **I want** to see friendly progress messages ("Investigating sources...", "Analyzing data...", "Generating response...") while the pipeline works, so I'm not staring at a blank screen. **Trigger**: WS connection to `/api/v1/query/ws/smart`. **Happy path**: the pipeline emits `status`, `chunk`, `complete` events via `get_stream_writer()`.

### US-003 (P1) — Casual greeting and pre-canned response without LLM
**As** the system, **when** the user types "hola" or "gracias", **I need** to respond instantly without calling the LLM or the data pipeline. Trigger: the `classify` node detects the pattern → direct route to `fast_reply`.

### US-004 (P1) — Transparent semantic cache
**As** the system, **when** a question is semantically similar to a previous one, **I need** to return the cached response without re-executing the full pipeline. Trigger: `cache_check` finds a hit in Redis or the pgvector semantic cache → direct route to `cache_reply`.

### US-005 (P1) — Multi-turn conversational memory
**As** a user, **I want** to ask follow-up questions ("and in 2024?") and have the system understand the prior context. Trigger: `load_memory` restores the session by `conversation_id` and feeds `planner_ctx` and `memory_ctx_analyst`.

### US-006 (P1) — Clarification when the query is ambiguous
**As** a user, **when** my question is too vague, **I want** to receive clickable options to refine it. Trigger: the planner returns `plan.intent="clarification"` → direct route to `clarify_reply` with the special event `{"type": "clarification", "options": [...]}`.

### US-007 (P1) — Replanning when data is insufficient
**As** the system, **when** the first attempt returns little data or many connector errors, **I need** to retry with a different strategy (broaden / narrow / switch_source). Trigger: the `coordinator` returns `decision="replan"` → loop back to the planner with enriched context. Maximum 2 replans.

### US-008 (P1) — Prevent prompt injection
**As** the system, **I need** to detect prompt injection attempts and reject them with a canned response. Trigger: `classify` applies `is_suspicious()` → "injection" classification → `fast_reply`.

### US-009 (P2) — Policy mode (public policy analysis)
**As** an advanced user, **I can** enable the `policy_mode` flag so the response includes a public policy analysis section. Trigger: `policy_mode=True` → after the analyst, it goes to the `policy` node → enriches `clean_answer` before `finalize`.

### US-010 (P2) — Per-connector failure isolation
**As** the system, **when** a connector fails, **I need** to continue with the others and report a warning, without tearing down the entire query. Trigger: `execute_steps` catches per-step exceptions and accumulates them in `step_warnings`.

### US-011 (P2) — Deterministic and LLM-generated chart building
**As** a user, **I want** to see charts when the data allows. Priority: deterministic charts (`build_deterministic_charts()`) → fallback to LLM-generated charts via `<!--CHART:{}-->` tags in the analyst's response.

### US-012 (P2) — GeoJSON maps when the data is geographic
**As** a user, **I want** to see maps when the data includes `_geometry_geojson` fields. Trigger: `analyst` detects geo fields and builds deterministic `map_data`.

### US-013 (P3) — Skill system (optional plugins)
**As** the system, **I can** activate specific skills that inject additional context into the planner and analyst (e.g. "budget skill"). Trigger: `skill_resolver` matches a pattern → injects `skill_context` into state.

### US-014 (P2) — Offline execution (batch)
**As** the system, **I can** run the pipeline offline via a Celery task (`analyst_tasks.analyze_query`) for batch processing without blocking HTTP.

## 4. Functional Requirements

### Classification and routing
- **FR-001**: The system MUST classify every query into one of: `casual` (greeting/thanks/farewell), `meta` (who are you), `injection`, `off_topic`, `educational`, or `None` (data query).
- **FR-002**: The system MUST route non-data queries to `fast_reply` without invoking the LLM.
- **FR-003**: The system MUST run `classify` without LLM dependency (regex + heuristics).

### Cache
- **FR-004**: The system MUST check the Redis cache (exact hash) and the semantic cache (pgvector HNSW) in parallel before invoking the pipeline.
- **FR-005**: The system MUST skip the cache when `policy_mode=True`.
- **FR-006**: The system MUST write to the cache post-finalize (fire-and-forget, non-blocking) with a TTL derived from the intent.

### Memory
- **FR-007**: The system MUST load session memory by `conversation_id` from Redis.
- **FR-008**: The system MUST load chat history (last 6 messages) from the DB.
- **FR-009**: The system MUST maintain two versions of the context: full `memory_ctx` (planner) and light `memory_ctx_analyst` (to avoid data bleed in the analyst).
- **FR-010**: The system MUST update memory post-finalize in a background task with retries.

### Planning
- **FR-011**: The planner MUST use Bedrock Claude Haiku 4.5 to generate an `ExecutionPlan` with ordered steps and `intent` ("data" | "clarification" | "error" | ...).
- **FR-012**: The planner MUST discover relevant cached tables via vector search over `table_catalog`.
- **FR-013**: The system MUST inject a `search_datasets` fallback step when the plan has no data-retrieval action.

### Execution
- **FR-014**: The executor MUST dispatch steps to connectors by **dependency level** (topological sort), running independent ones in parallel.
- **FR-015**: The executor MUST isolate per-step failures (exceptions do not propagate to the graph).
- **FR-016**: The executor MUST retry failed steps with backoff (max 2 retries) on retryable errors (timeout, 5xx).
- **FR-017**: The executor MUST emit per-connector streaming status ("Querying series...", "Querying BCRA...").

### Analysis
- **FR-018**: The analyst MUST use streaming LLM (`chat_stream()`) with fallback to non-streaming.
- **FR-019**: The analyst MUST build the prompt contextualizing data, memory, errors, and skills.
- **FR-020**: The analyst MUST use the `analyst.md` prompt in the normal flow and `analyst_no_data.md` in the fallback (no data).
- **FR-021**: The analyst MUST extract `confidence` and `citations` from meta tags (`<!--META:confidence=X, citations=[...]-->`).
- **FR-022**: The analyst MUST build charts deterministically when there are temporal/categorical + numeric columns, with fallback to LLM-generated charts.
- **FR-023**: The analyst MUST build GeoJSON `map_data` when the records contain `_geometry_geojson`.
- **FR-024**: The analyst MUST buffer streaming chunks to avoid emitting incomplete tags.
- **FR-025**: The analyst MUST strip internal tags (`<!--CHART:-->`, `<!--META:-->`) from `clean_answer` before returning it.

### Replanning
- **FR-026**: The coordinator MUST decide `continue` | `replan` | `escalate` based on heuristics: elapsed time, amount of useful data, number of warnings, replan counter.
- **FR-027**: The system MUST apply a **time budget** of 20s (rule: if elapsed > 20s → escalate).
- **FR-028**: The system MUST limit replans to **a maximum of 2 passes** (`_MAX_REPLAN_DEPTH=2`).
- **FR-029**: The replan MUST reset `data_results` and `step_warnings` using the `RESET_LIST` sentinel (not `[]`) due to the custom reducer.
- **FR-030**: The system MUST support 3 replan strategies: `broaden`, `narrow`, `switch_source`.
- **FR-031**: On replan, the analyst MUST emit `{"type": "clear_answer"}` so the frontend replaces the previous response.

### Finalize
- **FR-032**: Finalize MUST extract sources from `data_results` (name, url, portal, fetched_at).
- **FR-033**: Finalize MUST extract structured `documents` (e.g. DDJJ records) for card rendering.
- **FR-034**: Finalize MUST audit the query (intent, duration, user_id).
- **FR-035**: Finalize MUST compute `duration_ms` from `_start_time`.
- **FR-036**: Finalize MUST trigger cache write + memory update as fire-and-forget.

### Streaming
- **FR-037**: The pipeline MUST support both `updates` (completed nodes) and `custom` (events emitted by nodes) modes in `astream()`.
- **FR-038**: The pipeline MUST apply a **whitelist of keys** in custom events (`type, step, detail, progress, message, status, content, question, options, map_data`) to prevent leaking internal prompts.
- **FR-039**: The labels shown to the user MUST be in Spanish (Estratega / Investigador / Analista / Redactor).

### Checkpointing
- **FR-040**: When `AsyncPostgresSaver` is configured and `conversation_id` is present, the system MUST persist state by `thread_id` for resumable runs and multi-turn memory.

### Security
- **FR-041**: The pipeline MUST detect and block prompt injection attempts in `classify`.
- **FR-042**: The pipeline MUST NOT leak `analysis_prompt`, tracebacks, or internal prompts to the client.

## 5. Success Criteria

- **SC-001**: P1 response (normal path, cache miss) in **<15 seconds (p95)**.
- **SC-002**: Cache hit response in **<500ms (p95)**.
- **SC-003**: Fast reply (non-data classification) in **<100ms**.
- **SC-004**: **Zero pipelines down** due to a single connector failure.
- **SC-005**: **Zero internal prompts leaked** to the client via streaming.
- **SC-006**: Replan loop converges in ≤2 iterations in ≥90% of cases.
- **SC-007**: Streaming emits at least **1 status message every 3 seconds** during normal processing.
- **SC-008**: Checkpointing allows resuming a conversation >30 minutes later without context loss.
- **SC-009**: Policy mode responds in **<25 seconds** (overhead ≤40% over the normal path).

## 6. Assumptions & Out of Scope

### Assumptions
- Bedrock Claude Haiku 4.5 is available for planning and analysis.
- The frontend understands the streaming event format (status, chunk, clarification, complete).
- LangGraph and its custom/updates stream_mode are stable.
- Connectors return `DataResult` honoring the contract.

### Out of scope (this spec)
- **Detail of each individual connector** — see specs under `002-connectors/`.
- **Internal mechanism of the semantic cache** — see `004-semantic-cache/`.
- **Vector search over `table_catalog`** — see `011-table-catalog/`.
- **NL2SQL implementation** — currently inlined in the sandbox connector (see `010-sandbox-sql/`); the `nl2sql.py` subgraph exists but is not integrated into the main graph.
- **Policy agent** — delegates to `policy_agent.py` (not part of the main graph).
- **Multi-language support** — the pipeline is designed for Spanish; support for other languages is not covered.
- **Executable code generation** — only read-only SQL sandbox, no Python/JS.

## 7. Open Questions

- **[RESOLVED CL-001]** — Broken token counting (`tokens_used=0` always in streaming mode, `analyst.py:239`). **Decision**: use stream metadata — the final Bedrock Streaming chunk has a `usage` field with the real count (`input_tokens` + `output_tokens` in the `message_stop` event). Precise, free, requires parsing the end of the stream. It is the industry standard. See [`FIX_BACKLOG.md#fix-006-token-counting-via-bedrock-stream-metadata`](../FIX_BACKLOG.md).
- **[RESOLVED CL-002]** — `last_embedding` **is reused**, not regenerated. In `finalize.py:100` it is passed to `write_cache(...)`, and in `cache_manager.py:152-154` it is used directly when not None (it only calls `get_embedding()` when None). It is the embedding of the original query, appropriate for the cache write.
- **[RESOLVED CL-003]** — `ttl_for_intent()` lives in `semantic_cache.py:47-70`. Exact mapping: **REALTIME 300s** (`dolar`, `riesgo_pais`, `cotizacion`), **DAILY 1800s** (`inflacion`, `series`, `emae`, `tipo_cambio`, `reservas`, `base_monetaria`, `desempleo`, `exportaciones`, `importaciones`, `balanza_comercial`), **STATIC 7200s** (`ddjj`, `sesiones`, `ckan`, `busqueda`, `catalogo`, `legisladores`, `georef`). Default: DAILY. It does token matching over the intent string, then substring fallback.
- **[RESOLVED CL-004]** — `SkillRegistry` is **hardcoded** — no filesystem discovery, no plugins. A module-level `_BUILTIN_SKILLS` tuple in `registry.py:260-266` with **5 frozen dataclass skills**: `verificar`, `comparar`, `presupuesto`, `perfil`, `precio`. The constructor (`registry.py:26-45`) populates the internal dict by iterating the tuple. Adding skills requires a code edit.
- **[RESOLVED CL-005]** — After a replan, the analyst on the 2nd pass sees **ONLY the 2nd-pass results**, not an accumulation. The `_resettable_add` reducer in `state.py:17-22` does `if right is RESET_LIST: return []`, discarding all prior accumulation. Confirmed in `replan.py:109`, which returns `data_results=RESET_LIST`. It is a hard reset, not a merge.
- **[RESOLVED CL-006]** — `_has_useful_data()` (`coordinator.py:32-38`) considers results with `source.startswith("pgvector:")` or `source.startswith("ckan:")` useful **even if they have no records**. Rationale (from the code): those prefixes represent **metadata discovery** (vector search and CKAN lookup) — they are valid responses even if they bring no concrete rows. Example: "I found dataset X about budget" without raw data yet. It prevents the coordinator from triggering an unnecessary replan when discovery was successful.
- **[PARTIAL CL-007]** — **The backend emits** in `planner.py:95-102` the exact event `{"type": "clarification", "question": str, "options": list}` via `get_stream_writer()`. The schema is consistent but **there is no formal cross-repo contract documented**. The frontend consumes it in `src/hooks/useSSEStream.ts` and renders it as chips (verified). **Recommendation**: create `specs/contracts/sse_events.md` with the full event schema (status/chunk/complete/clarification/error) as a shared contract between backend and frontend.
- **[RESOLVED CL-008]** — **Deliberate anti-hallucination decision**, documented in code. `analyst.py:108-117` explicitly excludes `memory_ctx_analyst=""` on the no-data path with the comment: *"Don't pass memory context in no-data mode — it causes the LLM to hallucinate 'we already discussed this' when no data was ever shown."* The `analyst_no_data.txt:16-19` prompt reinforces with an explicit ANTI-HALLUCINATION rule: *"NUNCA digas que 'ya exploramos'"*. **Impact on continuity**: mitigated because `load_memory` (memory.py) restores the full context for the NEXT query. It is a single-query safety valve, not conversational amnesia. **Good design, not a bug**.
- **[RESOLVED CL-009]** — `limit` **is honored**. In `connectors/vector_search.py:27-30`, `execute_search_datasets_step` reads `params.get("limit", 10)` and passes it to `search_datasets_hybrid(...)`. The `inject_fallbacks` node overrides the default 10 with 5.
- **[RESOLVED CL-010]** — **Design decision documented in the code**, not a workaround. Explicit comment in `analyst.py:222-226`: *"When map data is present, suppress charts (geo data doesn't chart well)"*. When `_build_map_data(results)` detects geo features, the analyst sets `charts = None`. Rationale: geographic distributions are better visualized on maps than on traditional charts (bar/line/pie).

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Token count always 0 in streaming mode** (`analyst.py:239`). LLM call cost is invisible → no budgetary visibility.
- **[DEBT-002]** — **Hardcoded magic constants** in coordinator (`_MAX_REPLAN_DEPTH=2`, `_TIME_BUDGET_SECONDS=20.0`). Not configurable via DI or settings.
- **[DEBT-003]** — **Asymmetric cache write and memory update**: cache write silently fails without retry; memory update has retry logic. One of the two has the logic that the other lacks.
- **[DEBT-004]** — **`RESET_LIST` is an identity object** — compared with `is`. If code accidentally creates a different empty list, the reducer doesn't detect it and doesn't reset.
- **[DEBT-005]** — **Module-level `__getattr__` hack** in `nodes/__init__.py:40-70` to expose `_deps` via ContextVar. It works but confuses linters and humans.
- **[DEBT-006]** — **Optional `on_step_start` callback** for streaming. Coupling between streaming and step dispatch, inconsistency: not all steps emit status.
- **[DEBT-007]** — **Fragile chart building heuristics** (chart_builder.py:15-100). Hardcoded column-name patterns (fecha, nombre, patrimonio) don't scale to new schemas.
- **[DEBT-008]** — **Token counting absent in the `policy` node** as well.
- **[DEBT-009]** — **Skill context injection via string concatenation** with hardcoded separators. Hard to maintain.
- **[DEBT-010]** — **WebSocket stream_mode handling has hardcoded node names** (smart_query_v2_router.py:345). If nodes are renamed, it breaks without visible errors.
- **[DEBT-011]** — **Analyst prompt building is string concat** — it doesn't truncate the context to the token budget limit. Prompts can blow up.
- **[DEBT-012]** — **`asyncio.create_task()` fire-and-forget** in memory update (finalize.py:110). If the task raises, it is lost silently in the event loop.
- **[DEBT-013]** — **Connector dispatch table is a dict of lambdas** (step_executor.py:70-105). Adding a new connector requires a manual update, with no auto-registration.
- **[DEBT-014]** — **`_get_or_compile_graph()` uses `threading.Lock` in async context** (graph.py:41-48). Thread safety in async is subtle and can cause rare bugs.
- **[DEBT-015]** — **`_get_checkpointer()` holds global state** with a double-check pattern. It does not reset if `DATABASE_URL` changes at runtime.
- **[DEBT-016]** — **NL2SQL subgraph exists but is not integrated into the main graph** (it's inlined in `connectors/sandbox.py`). Dead or abandoned code.
- **[DEBT-017]** — **Streaming key whitelist is hardcoded** (smart_query_v2_router.py:324-335). If a new node needs to emit a non-whitelisted key, it fails silently.

---

**End of spec.md** — See [./plan.md](./plan.md) for the full as-built topology.
