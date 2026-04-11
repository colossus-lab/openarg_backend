# Plan: Query Pipeline (As-Built) — Index

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10

---

## 1. Scope

This top-level plan keeps only **graph-wide concerns**: hexagonal mapping, entry points, the shared state definition, graph topology, conditional edges, and global deviations from the constitution. Per-phase implementation details — node specs, dispatch tables, prompt paths, META-tag parsing, background-task management — live in the five sub-plans listed below.

| Sub-module | Sub-plan |
|---|---|
| Phase A: Intake | [`./001a-intake/plan.md`](./001a-intake/plan.md) |
| Phase B: Planning | [`./001b-planning/plan.md`](./001b-planning/plan.md) |
| Phase C: Execution | [`./001c-execution/plan.md`](./001c-execution/plan.md) |
| Phase D: Analysis | [`./001d-analysis/plan.md`](./001d-analysis/plan.md) |
| Phase E: Finalization | [`./001e-finalization/plan.md`](./001e-finalization/plan.md) |

## 2. Hexagonal Mapping (top level)

| Layer | Component | File |
|---|---|---|
| **Presentation** | `smart_query_v2_router.py` | `presentation/http/controllers/query/smart_query_v2_router.py` |
| **Application / Graph** | Graph builder, state, edges | `application/pipeline/{graph,state,edges}.py` |
| **Application / Nodes** | 16 nodes | `application/pipeline/nodes/*.py` |
| **Application / Subgraphs** | NL2SQL (not integrated) | `application/pipeline/subgraphs/nl2sql.py` |
| **Application / Helpers** | Context builder, cache manager, chart builder, step executor, classifiers, history | `application/pipeline/{context_builder,cache_manager,chart_builder,step_executor,classifiers,history}.py` |
| **Application / Connectors** | Pipeline steps per connector | `application/pipeline/connectors/*.py` |
| **Infrastructure / Celery** | Offline pipeline | `infrastructure/celery/tasks/analyst_tasks.py` |

## 3. Entry Points

### HTTP sync
`POST /api/v1/query/smart` → `smart_query_v2_router.py:131-198`
- Blocks until the pipeline completes; rate limited 10/min, 50/day per user.
- Returns: full JSON with answer, sources, chart_data, map_data, confidence, citations, documents, tokens_used, duration_ms, warnings.

### WebSocket streaming
`WS /api/v1/query/ws/smart` → `smart_query_v2_router.py:234-384`
- Rate limited 20 connections/min per user.
- Uses `graph.astream(stream_mode=["updates", "custom"])`; emits `status`, `chunk`, `clarification`, `complete` events.

### Celery offline
`openarg.analyze_query` → `infrastructure/celery/tasks/analyst_tasks.py` — analyst queue, concurrency 2, fire-and-forget.

## 4. Shared State Definition

```python
class OpenArgState(TypedDict, total=False):
    # Input
    question: str; user_id: str; conversation_id: str; policy_mode: bool
    # Classification
    classification: str | None; classification_response: str | None
    # Cache
    cached_result: dict | None; last_embedding: list[float] | None
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
    analysis_prompt: str; analysis_response: str; clean_answer: str
    confidence: float; citations: list[dict[str, Any]]
    # Charts & Maps
    chart_data: list[dict] | None; map_data: dict[str, Any] | None
    # Policy
    policy_text: str | None
    # Output (set in finalize)
    sources: list[dict]; documents: list[dict] | None
    tokens_used: int                      # post-FIX-006 now honored
    duration_ms: int; warnings: list[str]
    # Timing
    _start_time: float
    # Replanning
    replan_count: int; replan_strategy: str | None; coordinator_decision: str | None
    # Error
    error: str | None
```

**Custom reducer**: `_resettable_add` uses a `RESET_LIST` sentinel (identity object) so the `replan` node can clear `data_results` / `step_warnings` (LangGraph's `operator.add` cannot reset with `[]`).

## 5. Graph Topology (macro)

```
START → classify → [fast_reply ▶ END] OR cache_check
cache_check → [cache_reply ▶ END] OR load_memory
load_memory → preprocess → skill_resolver → planner
planner → [clarify_reply ▶ END] OR inject_fallbacks
inject_fallbacks → execute_steps → analyst → coordinator
coordinator → replan ▶ inject_fallbacks (loop, max 2)
           → policy (if policy_mode) → finalize ▶ END
           → finalize ▶ END
```

See each sub-plan for detailed per-node specifications.

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

## 7. Global Deviations from Constitution

- **Principle I (Hexagonal)**: 3 dependencies in `ConnectorDeps` are concrete adapters without a port (BCRA, DDJJ, `semantic_cache`). Tracked in `001c-execution/plan.md`.
- **Principle III (DI via Dishka)**: the `_deps` ContextVar hack (`nodes/__init__.py`) is a workaround so nodes can access graph dependencies without going through Dishka on every call. Acceptable but non-standard; applies to every sub-phase.
- **Principle VII (Observability)**: historically `tokens_used=0` in streaming mode and the policy node had no token counting. **FIXED 2026-04-10** via FIX-006 (see `001d-analysis/` and `001e-finalization/`).
- **Principle VIII (Alembic migrations)**: does not apply directly to the graph.

---

**End of plan.md index** — See sub-plans for node-level implementation details.
