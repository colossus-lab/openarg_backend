# Plan: Query Pipeline — Phase E: Finalization (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Parent plan**: [../plan.md](../plan.md)
**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10

---

## 1. Layer Mapping

| Layer | Component | File |
|---|---|---|
| **Application / Nodes** | `finalize` node | `application/pipeline/nodes/finalize.py` |
| **Application / Helpers** | Cache manager (write side, with `_retry_async`) | `application/pipeline/cache_manager.py` |
| | History writer | `application/pipeline/history.py` |
| **Application / Graph** | Graph builder + checkpointer | `application/pipeline/graph.py` |
| **Presentation / HTTP** | Streaming whitelist, WS router | `presentation/http/controllers/query/smart_query_v2_router.py` |
| **Infrastructure** | Redis cache adapter | `infrastructure/adapters/cache/redis_cache_adapter.py` |
| | pgvector semantic cache | see `004-semantic-cache/` |
| | `AsyncPostgresSaver` checkpointer | LangGraph stdlib + PG |
| | `MetricsCollector` | `infrastructure/monitoring/metrics.py` |
| | Celery offline runner | `infrastructure/celery/tasks/analyst_tasks.py` |

## 2. Node Specification

### 2.1 `finalize`
- **Input**: `data_results`, `question`, `user_id`, `plan`, `clean_answer`, `chart_data`, `map_data`, `tokens_used`, `last_embedding`, `step_warnings`, `conversation_id`, `memory`, `_start_time`, `plan_intent`
- **Logic**:
  1. Extract sources from `data_results`.
  2. Extract structured documents (e.g. DDJJ cards).
  3. Record metrics (`MetricsCollector.record_tokens_used()` — now sees real values post-FIX-006 if `tokens_used > 0`).
  4. Audit query (writes to `user_queries`).
  5. **Fire-and-forget** `write_cache()` (Redis + pgvector) via `_spawn_background()`.
  6. **Fire-and-forget** memory update via `_spawn_background()` with retry loop (max 2).
  7. Calculate `duration_ms = (time.monotonic() - _start_time) * 1000`.
- **Output**: `clean_answer`, `sources`, `documents`, `map_data`, `warnings`, `duration_ms`
- **No streaming** emitted from the finalize node itself — the terminal `{type: "complete", ...}` event is assembled by the HTTP/WS router from the final state.

## 3. Background Task Management

Post-FIX-005 / DEBT-012, `finalize.py` contains:

```python
_background_tasks: set[asyncio.Task] = set()

def _spawn_background(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    task.add_done_callback(_log_background_exception)
    return task
```

This keeps a strong reference to every fire-and-forget task (so CPython's weak-ref task GC does not cancel them mid-flight) and ensures any unhandled exception is logged via `_log_background_exception`. Both the cache write and the memory update loop use this helper.

## 4. Streaming (Terminal Event)

### 4.1 `complete` event shape

```jsonc
{"type": "complete", "answer": "...", "sources": [...], "chart_data": [...], "map_data": {...}, "confidence": 0.X, "citations": [...], "documents": [...], "warnings": [...], "duration_ms": N, "tokens_used": N}
```

### 4.2 Whitelist (enforcement point)

The HTTP router applies a hardcoded whitelist (`smart_query_v2_router.py:324-335`) to every custom event before forwarding it to the WS client:

```
type, step, detail, progress, message, status, content, question, options, map_data
```

Any other key (e.g. `analysis_prompt`, traceback, error object) is silently dropped. This is defense in depth — the analyst already strips internal tags from `clean_answer` (see Phase D), and the coordinator/planner/executor never populate the dropped keys in the first place.

### 4.3 Spanish labels

Agent labels shown to the user: **Estratega** (planner), **Investigador** (executor), **Analista** (analyst), **Redactor** (finalize). Defined alongside the status-emission helpers.

## 5. Checkpointing

When `AsyncPostgresSaver` is configured and `conversation_id` is present, LangGraph persists state by `thread_id` after every node (including finalize). This enables:

- Resumable runs across > 30 min gaps (SC-008).
- Multi-turn memory beyond what Redis session state can hold.

`_get_checkpointer()` (`graph.py`) holds global state via a double-check pattern (see DEBT-015) — it is initialized once per process and not reset if `DATABASE_URL` changes at runtime.

## 6. State Keys Owned by This Phase

Written by finalize:

- `sources`, `documents`
- `warnings` (copy of `step_warnings` for client consumption)
- `duration_ms`
- `tokens_used` (forwarded from analyst, post-FIX-006)

Read by finalize (from upstream phases):

- `clean_answer`, `chart_data`, `map_data`, `confidence`, `citations` (from Phase D: Analysis)
- `data_results`, `step_warnings` (from Phase C: Execution)
- `plan`, `plan_intent`, `memory` (from Phases A/B)
- `_start_time` (from Phase A: Intake)
- `last_embedding` (from Phase A: Intake — reused for cache write, CL-002)

## 7. Deviations from Constitution

- **Principle I (Hexagonal)**: `semantic_cache` is accessed as a concrete adapter (no port). Same deviation is tracked in Phase A (read side) and the parent plan.
- **Principle III (DI via Dishka)**: finalize accesses dependencies via the `_deps` ContextVar hack, same as every other pipeline node.
- **Principle VII (Observability)**: post-FIX-006, token counts are now honored for both the analyst and the optional policy node. Audit log captures intent + duration + user_id.

---

**End of plan.md** — See parent [`../plan.md`](../plan.md) for the full graph topology and state definition.
