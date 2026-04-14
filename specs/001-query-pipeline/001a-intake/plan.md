# Plan: Query Pipeline — Phase A: Intake (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Parent plan**: [../plan.md](../plan.md)
**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10

---

## 1. Layer Mapping

| Layer | Component | File |
|---|---|---|
| **Application / Nodes** | `classify` node | `application/pipeline/nodes/classify.py` |
| | `fast_reply` node (terminal short-circuit) | `application/pipeline/nodes/fast_reply.py` |
| | `cache_check` node | `application/pipeline/nodes/cache.py` |
| | `cache_reply` node (terminal short-circuit) | `application/pipeline/nodes/cache_reply.py` |
| | `load_memory` node | `application/pipeline/nodes/load_memory.py` |
| | `preprocess` node | `application/pipeline/nodes/preprocess.py` |
| **Application / Helpers** | Cache manager (read side) | `application/pipeline/cache_manager.py` |
| | History loader | `application/pipeline/history.py` |
| | Classifier helpers | `application/pipeline/classifiers.py` |
| **Application / Edges** | `route_after_classify`, `route_after_cache` | `application/pipeline/edges.py` |
| **Infrastructure** | Redis cache adapter | `infrastructure/adapters/cache/redis_cache_adapter.py` |
| | pgvector semantic cache | `infrastructure/.../semantic_cache.py` (see `004-semantic-cache/`) |

## 2. Node Specifications

### 2.1 `classify`
- **Entry point**: set `_start_time = time.monotonic()` *(this is the first node executed — owns the pipeline clock).*
- **Logic**: `classify_request()` uses regex/heuristics (no LLM). Detects: greetings, thanks, farewells, meta questions, injection, off-topic, educational.
- **Output**: `classification`, `classification_response` (or None), `_start_time`
- **Streaming**: `{type: "status", step: "classifying", detail: "Analizando consulta..."}`
- **Error**: silent fallback to (None, None).

### 2.2 `fast_reply` (terminal)
- **Input**: `classification`, `classification_response`
- **Logic**: terminal node. Build response from pre-canned text. No LLM.
- **Output**: `clean_answer`, `sources=[]`, `chart_data=None`, `confidence=1.0`, `citations=[]`, `documents=None`, `tokens_used=0`, `warnings=[]`, `plan_intent` (special: "injection_blocked" or "off_topic").

### 2.3 `cache_check`
- **Input**: `question`, `user_id`, `policy_mode`
- **Logic**: Skip if `policy_mode`. Else parallel Redis + pgvector semantic cache lookup. Reuse embedding for later cache write (`last_embedding` threaded to Phase E).
- **Output**: `cached_result` (dict or None), `last_embedding` (reused).
- **Streaming**: `{type: "status", step: "cache_check", detail: "Buscando en caché..."}`

### 2.4 `cache_reply` (terminal)
- **Input**: `cached_result`
- **Logic**: Terminal node. Extract cached fields.
- **Output**: `clean_answer`, `sources`, `chart_data`, `map_data`, `tokens_used`, plus zeros for unused fields.
- **Set**: `plan_intent="cached"`.

### 2.5 `load_memory`
- **Input**: `conversation_id`, `user_id`
- **Logic**:
  - Start `load_memory()` from Redis (session memory)
  - Start `load_chat_history()` from DB (last 6 messages) when `conversation_id` exists
  - Both run concurrently with deterministic query preprocessing and catalog-hint lookup
  - After both resolve: `build_memory_context_prompt()` → `memory_ctx`
  - Build lightweight `memory_ctx_analyst`
  - `planner_ctx = chat_history or memory_ctx`
- **Streaming**: `{type: "status", step: "loading_context", detail: "Cargando contexto..."}`

### 2.6 `preprocess`
- **Input**: `question`
- **Logic**: Deterministic (no LLM):
  - `expand_acronyms()`
  - `normalize_temporal()` ("hace 2 meses" → absolute date)
  - `normalize_provinces()`
  - `expand_synonyms()`
  - Immediately after `preprocessed_query` is available, kick off `discover_catalog_hints_for_planner()` so sandbox/catalog I/O overlaps with memory/history loading
- **Output**: `preprocessed_query`

## 3. Conditional Edges (phase-local)

```python
# edges.py
def route_after_classify(state) -> Literal["fast_reply", "cache_check"]:
    return "fast_reply" if state.get("classification") else "cache_check"

def route_after_cache(state) -> Literal["cache_reply", "load_memory"]:
    return "cache_reply" if state.get("cached_result") else "load_memory"
```

After `preprocess`, the graph unconditionally advances to `skill_resolver` (first node of Phase B: Planning).

## 4. State Keys Owned by This Phase

Written by intake:

- `_start_time` — pipeline clock (set in `classify`).
- `classification`, `classification_response`
- `cached_result`, `last_embedding`
- `memory`, `memory_ctx`, `memory_ctx_analyst`, `planner_ctx`
- `preprocessed_query`

## 5. Deviations from Constitution

- **Principle I (Hexagonal)**: `semantic_cache` is used as a concrete adapter in `ConnectorDeps` without a dedicated port. Tracked in the parent plan under Deviations.
- **Principle VII (Observability)**: cache hit/miss metrics are recorded via `MetricsCollector`; fast-reply path has no token cost by design.

---

**End of plan.md** — See parent [`../plan.md`](../plan.md) for the full graph topology and state definition.
