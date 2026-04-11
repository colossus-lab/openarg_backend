# Plan: Query Pipeline (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| **Presentation** | `smart_query_v2_router.py` | `presentation/http/controllers/query/smart_query_v2_router.py` |
| | `smart_query_router.py` (legacy?) | `presentation/http/controllers/query/smart_query_router.py` |
| **Application / Graph** | Graph builder | `application/pipeline/graph.py` |
| | State definition | `application/pipeline/state.py` |
| | Edges | `application/pipeline/edges.py` |
| **Application / Nodes** | 16 nodes | `application/pipeline/nodes/{classify,fast_reply,cache,load_memory,preprocess,skill_resolver,planner,clarify_reply,cache_reply,executor,analyst,coordinator,replan,policy,finalize}.py` |
| **Application / Subgraphs** | NL2SQL (not integrated into main) | `application/pipeline/subgraphs/nl2sql.py` |
| **Application / Helpers** | Context builder, cache manager, chart builder, step executor, classifiers, history | `application/pipeline/{context_builder,cache_manager,chart_builder,step_executor,classifiers,history}.py` |
| **Application / Connectors** | Pipeline steps per connector | `application/pipeline/connectors/*.py` |
| **Infrastructure** | LLM, Embeddings, Semantic Cache, Sandbox, Connectors | see individual specs |
| **Infrastructure / Celery** | Offline pipeline | `infrastructure/celery/tasks/analyst_tasks.py` |

## 2. Entry Points

### HTTP sync
`POST /api/v1/query/smart` → `smart_query_v2_router.py:131-198`
- Blocks until the pipeline completes
- Rate limited: 10/min, 50/day per user
- Returns: full JSON with answer, sources, chart_data, map_data, confidence, citations, documents, tokens_used, duration_ms, warnings

### WebSocket streaming
`WS /api/v1/query/ws/smart` → `smart_query_v2_router.py:234-384`
- Rate limited: 20 connections/min per user
- Uses `graph.astream(stream_mode=["updates", "custom"])`
- Emits events: status, chunk, clarification, complete

### Celery offline
`openarg.analyze_query` → `infrastructure/celery/tasks/analyst_tasks.py`
- Queue: `analyst` (concurrency 2)
- Invokes the full pipeline fire-and-forget

## 3. State Definition

```python
class OpenArgState(TypedDict, total=False):
    # Input
    question: str
    user_id: str
    conversation_id: str
    policy_mode: bool

    # Classification
    classification: str | None
    classification_response: str | None

    # Cache
    cached_result: dict | None
    last_embedding: list[float] | None

    # Preprocessing
    preprocessed_query: str

    # Memory
    memory: Any                           # MemoryContext
    memory_ctx: str                       # full (planner)
    memory_ctx_analyst: str               # light (analyst, no data bleed)
    planner_ctx: str                      # chat_history if available else memory_ctx

    # Skills
    active_skill: str | None
    skill_context: dict[str, str] | None  # {planner: "...", analyst: "..."}

    # Planning
    catalog_hints: str
    plan: ExecutionPlan | None
    plan_intent: str                      # "data" | "clarification" | "error" | ...

    # Execution (with custom reducer)
    data_results: Annotated[list[DataResult], _resettable_add]
    step_warnings: Annotated[list[str], _resettable_add]

    # Analysis
    analysis_prompt: str
    analysis_response: str
    clean_answer: str
    confidence: float
    citations: list[dict[str, Any]]

    # Charts & Maps
    chart_data: list[dict] | None
    map_data: dict[str, Any] | None

    # Policy
    policy_text: str | None

    # Output (set in finalize)
    sources: list[dict]
    documents: list[dict] | None
    tokens_used: int                      # always 0 in streaming mode [DEBT-001]
    duration_ms: int
    warnings: list[str]

    # Timing
    _start_time: float

    # Replanning
    replan_count: int
    replan_strategy: str | None
    coordinator_decision: str | None

    # Error
    error: str | None
```

**Custom reducer**: `_resettable_add` uses a `RESET_LIST` sentinel (identity object) to allow resetting `data_results` and `step_warnings` on replan (LangGraph's `operator.add` cannot reset with `[]`).

## 4. Graph Topology

```
START
  │
  ▼
classify ────────────────┐
  │                       │ (classification != None)
  │                       ▼
  │                   fast_reply ──▶ END
  │ (classification == None)
  ▼
cache_check ────────────┐
  │                      │ (cached_result)
  │                      ▼
  │                  cache_reply ──▶ END
  │ (no cache)
  ▼
load_memory
  │
  ▼
preprocess
  │
  ▼
skill_resolver
  │
  ▼
planner ──────────────────┐
  │                        │ (intent == "clarification")
  │                        ▼
  │                    clarify_reply ──▶ END
  │ (intent != "clarification")
  ▼
inject_fallbacks
  │
  ▼
execute_steps ◀─────────┐
  │                      │
  ▼                      │
analyst                  │
  │                      │
  ▼                      │
coordinator ─────────┐   │
  │                   │   │
  │                   │ (decision == "replan")
  │                   ▼   │
  │                replan─┘
  │ (decision == "continue" or "escalate")
  │
  ├─── (policy_mode) ─▶ policy ─┐
  │                              │
  └──────────────────────────────┤
                                 ▼
                            finalize ──▶ END
```

## 5. Node Specifications

### 5.1 `classify`
- **Entry point**: set `_start_time = time.monotonic()`
- **Logic**: `classify_request()` uses regex/heuristics (no LLM). Detects: greetings, thanks, farewells, meta questions, injection, off-topic, educational.
- **Output**: `classification`, `classification_response` (or None), `_start_time`
- **Streaming**: `{type: "status", step: "classifying", detail: "Analizando consulta..."}`
- **Error**: silent fallback to (None, None)

### 5.2 `fast_reply`
- **Input**: `classification`, `classification_response`
- **Logic**: terminal node. Build response from pre-canned text. No LLM.
- **Output**: `clean_answer`, `sources=[]`, `chart_data=None`, `confidence=1.0`, `citations=[]`, `documents=None`, `tokens_used=0`, `warnings=[]`, `plan_intent` (special: "injection_blocked" or "off_topic")

### 5.3 `cache_check`
- **Input**: `question`, `user_id`, `policy_mode`
- **Logic**: Skip if policy_mode. Else parallel Redis + pgvector semantic cache lookup. Reuse embedding for later cache write.
- **Output**: `cached_result` (dict or None), `last_embedding` (reused)
- **Streaming**: `{type: "status", step: "cache_check", detail: "Buscando en caché..."}`

### 5.4 `cache_reply`
- **Input**: `cached_result`
- **Logic**: Terminal node. Extract cached fields.
- **Output**: `clean_answer`, `sources`, `chart_data`, `map_data`, `tokens_used`, plus zeros for unused fields
- **Set**: `plan_intent="cached"`

### 5.5 `load_memory`
- **Input**: `conversation_id`, `user_id`
- **Logic**:
  - `load_memory()` from Redis (session memory)
  - `build_memory_context_prompt()` → `memory_ctx`
  - Build lightweight `memory_ctx_analyst`
  - `load_chat_history()` from DB (last 6 messages)
  - `planner_ctx = chat_history or memory_ctx`
- **Streaming**: `{type: "status", step: "loading_context", detail: "Cargando contexto..."}`

### 5.6 `preprocess`
- **Input**: `question`
- **Logic**: Deterministic (no LLM):
  - `expand_acronyms()`
  - `normalize_temporal()` ("hace 2 meses" → absolute date)
  - `normalize_provinces()`
  - `expand_synonyms()`
- **Output**: `preprocessed_query`

### 5.7 `skill_resolver`
- **Input**: `preprocessed_query`
- **Logic**: Match against `SkillRegistry()` singleton.
- **Output**: `active_skill` (or None), `skill_context` (dict with planner/analyst injections)
- **Streaming**: only if matched

### 5.8 `planner`
- **Input**: `preprocessed_query`, `planner_ctx`, `skill_context`
- **Logic**:
  1. `discover_catalog_hints_for_planner()` — vector search over `table_catalog`
  2. Inject skill context
  3. `generate_plan()` — **LLM call** (Bedrock Claude Haiku 4.5, 1-2s)
  4. Plan contains `steps: list[PlanStep]` + `intent: str`
- **Output**: `catalog_hints`, `plan`, `plan_intent`
- **Prompts**: `prompts/query_planner.txt` (note: `.txt` extension)
- **Streaming**: `{type: "status", step: "planning", detail: "Planificando estrategia..."}`

### 5.9 `clarify_reply`
- **Input**: `plan` (with clarification step)
- **Logic**: Terminal. Extract clarification question + options. Emit special event.
- **Streaming**: `{type: "clarification", question: "...", options: [...]}`
- **Output**: `clean_answer` (markdown), `plan_intent="clarification"`, zeros

### 5.10 `inject_fallbacks`
- **Input**: `plan`, `question`, `preprocessed_query`
- **Logic**: Check if the plan has a data action (query_sandbox, search_datasets, etc.). If not, insert a `search_datasets` fallback at position 0 with `{query: preprocessed_q, limit: 5}`.
- **Output**: `plan` (modified)

### 5.11 `execute_steps`
- **Input**: `plan`, `question`
- **Logic**:
  1. Build `ConnectorDeps` dataclass with all 13 connectors (series, arg_datos, georef, ckan, sesiones, ddjj, staff, bcra, sandbox, vector_search, llm, embedding, semantic_cache)
  2. Call `execute_steps()` (step_executor.py) — parallel by dependency level
  3. Per-step status callback emits connector label
  4. Collect `data_results` + `step_warnings`
- **Output**: `data_results`, `step_warnings`
- **Streaming**: `{type: "status", step: "searching", detail: "Consultando fuentes de datos..."}` + per-connector updates
- **Retry**: max 2 per step via `dispatch_step_with_retry()`

### 5.12 `analyst`
- **Input**: `question`, `plan`, `data_results`, `memory_ctx_analyst`, `step_warnings`, `skill_context`, `replan_count`
- **Logic**:
  1. Build prompt via `_build_analysis_prompt()` using `analyst.md` (or `analyst_no_data.md` fallback)
  2. `chat_stream()` — **LLM streaming call**
  3. Buffer chunks, strip tags, emit to stream
  4. Extract charts (deterministic first, then LLM-generated from `<!--CHART:-->` tags)
  5. Extract map_data (deterministic from `_geometry_geojson`)
  6. Parse `<!--META:confidence=X, citations=[...]-->`
  7. Strip internal tags from final answer
  8. If `replan_count > 0` → emit `{type: "clear_answer"}`
- **Output**: `clean_answer`, `chart_data`, `map_data`, `confidence`, `citations`, `analysis_prompt`, `analysis_response`, `tokens_used=0` **[DEBT-001]**
- **Prompts**: `prompts/analyst.txt`, `prompts/analyst_no_data.txt` (note: `.txt` extension)
- **Streaming**: `status` + per-chunk `{type: "chunk", content: "..."}`

### 5.13 `coordinator`
- **Input**: `data_results`, `step_warnings`, `replan_count`, `_start_time`
- **Logic**: Heuristic (no LLM):
  1. If elapsed > 20s → `escalate`
  2. Else if `_has_useful_data()` → `continue`
  3. Else if `replan_count >= 2` → `escalate`
  4. Else if `len(step_warnings) >= 3` → `escalate`
  5. Else → `replan` with strategy:
     - `replan_count == 0` → "broaden"
     - `replan_count == 1` and failures → "switch_source"
     - `replan_count == 1` and some data → "narrow"
- **Output**: `coordinator_decision`, `replan_strategy`
- **Magic constants**: `_MAX_REPLAN_DEPTH=2`, `_TIME_BUDGET_SECONDS=20.0` **[DEBT-002]**

### 5.14 `replan`
- **Input**: `preprocessed_query`, `planner_ctx`, `replan_strategy`, `replan_count`, `step_warnings`, `data_results`
- **Logic**:
  1. `replan_count += 1`
  2. Enrich planner context with warnings + result summary
  3. Append strategy hints ("AMPLIAR BÚSQUEDA" / "RESTRINGIR" / "CAMBIAR FUENTE")
  4. Re-call `generate_plan()` — **2nd LLM call**
  5. **Reset** `data_results`, `step_warnings` using the `RESET_LIST` sentinel **[DEBT-004]**
- **Output**: `replan_count`, `plan`, `plan_intent`, `data_results=RESET_LIST`, `step_warnings=RESET_LIST`

### 5.15 `policy` (optional)
- **Input**: `policy_mode`, `data_results`, `clean_answer`, `memory_ctx`
- **Logic**: Only runs if `policy_mode=True`. Calls `analyze_policy()` → LLM enrichment with policy framing. Appends to `clean_answer`.
- **Streaming**: `{type: "status", step: "policy_analysis", detail: "Evaluando políticas públicas..."}`
- **Note**: no token counting here either **[DEBT-008]**

### 5.16 `finalize`
- **Input**: `data_results`, `question`, `user_id`, `plan`, `clean_answer`, `chart_data`, `map_data`, `tokens_used`, `last_embedding`, `step_warnings`, `conversation_id`, `memory`, `_start_time`, `plan_intent`
- **Logic**:
  1. Extract sources from `data_results`
  2. Extract structured documents (DDJJ cards)
  3. Record metrics (if `tokens_used > 0`)
  4. Audit query
  5. **Fire-and-forget** `write_cache()` (Redis + pgvector)
  6. **Fire-and-forget** memory update via `asyncio.create_task()` with retry (max 2)
  7. Calculate `duration_ms = (time.monotonic() - _start_time) * 1000`
- **Output**: `clean_answer`, `sources`, `documents`, `map_data`, `warnings`, `duration_ms`
- **No streaming**

## 6. Conditional Edges

```python
# edges.py
def route_after_classify(state) -> Literal["fast_reply", "cache_check"]:
    return "fast_reply" if state.get("classification") else "cache_check"

def route_after_cache(state) -> Literal["cache_reply", "load_memory"]:
    return "cache_reply" if state.get("cached_result") else "load_memory"

def route_after_plan(state) -> Literal["clarify_reply", "inject_fallbacks"]:
    return "clarify_reply" if state.get("plan_intent") == "clarification" else "inject_fallbacks"

def route_after_coordinator(state) -> Literal["replan", "policy", "finalize"]:
    if state.get("coordinator_decision") == "replan":
        return "replan"
    return "policy" if state.get("policy_mode") else "finalize"
```

## 7. Dependencies (ConnectorDeps dataclass)

The executor receives a `ConnectorDeps` that aggregates the 13 providers:

```python
@dataclass
class ConnectorDeps:
    series: ISeriesTiempoConnector
    arg_datos: IArgentinaDatosConnector
    georef: IGeorefConnector
    ckan: ICKANSearchConnector
    sesiones: ISesionesConnector
    ddjj: DDJJAdapter              # [DEBT] no port
    staff: IStaffConnector
    bcra: BCRAAdapter              # [DEBT] no port
    sandbox: ISQLSandbox
    vector_search: IVectorSearch
    llm: ILLMProvider
    embedding: IEmbeddingProvider
    semantic_cache: SemanticCache  # [DEBT] no port
```

## 8. Streaming Model

### Event types emitted
```jsonc
// status update
{"type": "status", "step": "classifying|cache_check|loading_context|skill|planning|searching|generating|coordination|replanning|policy_analysis", "detail": "Texto en español"}

// chunk (analyst streaming)
{"type": "chunk", "content": "..."}

// clarification (from clarify_reply node)
{"type": "clarification", "question": "...", "options": [{"label": "...", "value": "..."}, ...]}

// clear previous answer (on replan)
{"type": "clear_answer"}

// terminal
{"type": "complete", "answer": "...", "sources": [...], "chart_data": [...], "map_data": {...}, "confidence": 0.X, "citations": [...], "documents": [...], "warnings": [...], "duration_ms": N, "tokens_used": N}
```

### Whitelist
Only these keys reach the client: `type, step, detail, progress, message, status, content, question, options, map_data`. Prevents leaking of `analysis_prompt`, tracebacks, error objects.

## 9. Source Files

| File | Purpose |
|---|---|
| `application/pipeline/graph.py` | `_get_or_compile_graph()`, `build_graph()` |
| `application/pipeline/state.py` | `OpenArgState` TypedDict, `_resettable_add`, `RESET_LIST` |
| `application/pipeline/edges.py` | Conditional edge functions |
| `application/pipeline/nodes/*.py` | 16 nodes + `__init__.py` with `__getattr__` hack |
| `application/pipeline/connectors/*.py` | Pipeline steps per connector |
| `application/pipeline/subgraphs/nl2sql.py` | NL2SQL subgraph (not integrated) **[DEBT-016]** |
| `application/pipeline/{context_builder,cache_manager,chart_builder,step_executor,classifiers,history}.py` | Helpers |
| `presentation/http/controllers/query/smart_query_v2_router.py` | HTTP + WS routers |
| `infrastructure/celery/tasks/analyst_tasks.py` | Offline pipeline invocation |
| `prompts/{query_planner,analyst,analyst_no_data,catalog_enrichment}.txt` | Prompt templates (`.txt` extension, not `.md`) |

## 10. Deviations from Constitution

- **Principle I (Hexagonal)**: 3 dependencies in `ConnectorDeps` are concrete adapters without a port (BCRA, DDJJ, semantic_cache). **[DEBT-001-cross]**
- **Principle III (DI via Dishka)**: the `_deps` ContextVar hack (nodes/__init__.py) is a workaround so nodes can access graph dependencies without going through Dishka on every call. Acceptable but non-standard.
- **Principle VII (Observability)**: tokens used always 0 in streaming; policy node without tracking.
- **Principle VIII (Alembic migrations)**: does not apply directly to the graph.

---

**End of plan.md** — The spec.md file contains the success criteria, user stories, and identified debt.
