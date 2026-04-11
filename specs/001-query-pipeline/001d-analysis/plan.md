# Plan: Query Pipeline — Phase D: Analysis (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Parent plan**: [../plan.md](../plan.md)
**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10

---

## 1. Layer Mapping

| Layer | Component | File |
|---|---|---|
| **Application / Nodes** | `analyst` node | `application/pipeline/nodes/analyst.py` |
| | `policy` node (optional) | `application/pipeline/nodes/policy.py` |
| **Application / Helpers** | Context builder (analyst prompt assembly) | `application/pipeline/context_builder.py` |
| | Chart builder (deterministic) | `application/pipeline/chart_builder.py` |
| **Application / Agents** | Policy agent delegate | `application/agents/policy_agent.py` |
| **Infrastructure / LLM** | Bedrock streaming LLM (primary) | `infrastructure/adapters/llm/bedrock_llm_adapter.py` |
| | Gemini fallback | `infrastructure/adapters/llm/gemini_adapter.py` |
| **Prompts** | `analyst.txt`, `analyst_no_data.txt`, policy prompt | `prompts/analyst.txt`, `prompts/analyst_no_data.txt`, `prompts/policy_*.txt` |

## 2. Node Specifications

### 2.1 `analyst`
- **Input**: `question`, `plan`, `data_results`, `memory_ctx_analyst`, `step_warnings`, `skill_context`, `replan_count`
- **Logic**:
  1. Build prompt via `_build_analysis_prompt()` using `analyst.md` (or `analyst_no_data.md` fallback).
  2. `chat_stream()` — **LLM streaming call** (passes `usage_out` dict post-FIX-006).
  3. Buffer chunks, strip tags, emit to stream.
  4. Extract charts (deterministic first via `build_deterministic_charts()`, then LLM-generated from `<!--CHART:-->` tags).
  5. Extract `map_data` (deterministic from `_geometry_geojson`). If geo features present, **suppress charts** (CL-010).
  6. Parse `<!--META:confidence=X, citations=[...]-->`.
  7. Strip internal tags from final answer.
  8. If `replan_count > 0` → emit `{type: "clear_answer"}`.
- **Output**: `clean_answer`, `chart_data`, `map_data`, `confidence`, `citations`, `analysis_prompt`, `analysis_response`, `tokens_used` (now populated via `usage_out`, see DEBT-001 FIX).
- **Prompts**: `prompts/analyst.txt`, `prompts/analyst_no_data.txt` (note: `.txt` extension).
- **Streaming**: `status` + per-chunk `{type: "chunk", content: "..."}`.
- **No-data path**: explicitly clears `memory_ctx_analyst=""` (anti-hallucination, CL-008).

### 2.2 `policy` (optional)
- **Input**: `policy_mode`, `data_results`, `clean_answer`, `memory_ctx`
- **Logic**: Only runs if `policy_mode=True`. Calls `analyze_policy()` → LLM enrichment with policy framing. Appends to `clean_answer`.
- **Streaming**: `{type: "status", step: "policy_analysis", detail: "Evaluando políticas públicas..."}`
- **Note**: post-FIX-006, `usage_out` is threaded through the shared LLM port so the policy node also tracks tokens (DEBT-008 FIXED).

## 3. META-Tag Contract

The analyst emits a single META tag at the end of the streamed answer, which the backend parses and strips before the cleaned prose reaches the client:

```
<!--META:confidence=0.85, citations=[{"source": "bcra", "url": "..."}, ...]-->
```

Parser lives in `analyst.py` alongside the streaming loop. Any CHART tag (`<!--CHART:{...}-->`) is similarly stripped. The whitelist defined in the HTTP router (see `../001e-finalization/` and the top-level plan) provides defense in depth against tag leakage.

## 4. Chart Building Strategy

1. **Deterministic first**: `build_deterministic_charts(records)` in `chart_builder.py` inspects columns for temporal/categorical + numeric patterns. Produces a list of `{type, x, y, series}` dicts.
2. **LLM fallback**: if deterministic returns nothing, the analyst may emit one or more `<!--CHART:{json}-->` tags. The backend parses those JSON payloads into the same shape.
3. **Map suppression**: if `_build_map_data(results)` detects GeoJSON features, charts are forced to `None` (CL-010).

Heuristics are fragile — hardcoded patterns include `fecha`, `nombre`, `patrimonio` (see DEBT-007).

## 5. State Keys Owned by This Phase

Written by analysis:

- `analysis_prompt` (internal, must not be streamed)
- `analysis_response` (raw LLM output, internal)
- `clean_answer` (tag-stripped, streamable)
- `chart_data`, `map_data`
- `confidence`, `citations`
- `tokens_used` (post-FIX-006)
- `policy_text` (if policy mode)

Read by analysis (from upstream phases):

- `data_results`, `step_warnings` (from Phase C: Execution)
- `plan`, `skill_context`, `replan_count` (from Phase B: Planning)
- `memory_ctx_analyst` (from Phase A: Intake; cleared on no-data path)

## 6. Deviations from Constitution

- **Principle VII (Observability)**: historically the streaming analyst lost token counts (`tokens_used=0` always). **FIXED 2026-04-10** via FIX-006 — `usage_out` dict threaded through `chat_stream()` and populated from the Bedrock `message_stop` event.
- **Principle I (Hexagonal)**: policy agent is a sibling module that bypasses the standard node interface; acceptable because it is explicitly opt-in.

---

**End of plan.md** — See parent [`../plan.md`](../plan.md) for the full graph topology and state definition.
