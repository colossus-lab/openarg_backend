# Plan: Query Pipeline — Phase B: Planning (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Parent plan**: [../plan.md](../plan.md)
**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10

---

## 1. Layer Mapping

| Layer | Component | File |
|---|---|---|
| **Application / Nodes** | `skill_resolver` node | `application/pipeline/nodes/skill_resolver.py` |
| | `planner` node | `application/pipeline/nodes/planner.py` |
| | `clarify_reply` node (terminal short-circuit) | `application/pipeline/nodes/clarify_reply.py` |
| | `coordinator` node | `application/pipeline/nodes/coordinator.py` |
| | `replan` node | `application/pipeline/nodes/replan.py` |
| **Application / Skills** | `SkillRegistry` + builtin skills | `application/skills/registry.py` |
| **Application / Planning helpers** | Catalog hint discovery, plan generation | `application/pipeline/` (planner helpers) |
| **Application / Edges** | `route_after_plan`, `route_after_coordinator` | `application/pipeline/edges.py` |
| **Application / State** | `_resettable_add`, `RESET_LIST` sentinel | `application/pipeline/state.py` |
| **Infrastructure** | Bedrock Claude Haiku 4.5 (primary LLM) | `infrastructure/adapters/llm/bedrock_llm_adapter.py` |
| | Vector search over `table_catalog` | see `011-table-catalog/` |
| **Prompts** | `query_planner.txt` | `prompts/query_planner.txt` |

## 2. Node Specifications

### 2.1 `skill_resolver`
- **Input**: `preprocessed_query`
- **Logic**: Match against `SkillRegistry()` singleton.
- **Output**: `active_skill` (or None), `skill_context` (dict with planner/analyst injections).
- **Streaming**: only if matched.

### 2.2 `planner`
- **Input**: `preprocessed_query`, `planner_ctx`, `skill_context`
- **Logic**:
  1. `discover_catalog_hints_for_planner()` — vector search over `table_catalog`
  2. Inject skill context
  3. `generate_plan()` — **LLM call** (Bedrock Claude Haiku 4.5, 1–2s)
  4. Plan contains `steps: list[PlanStep]` + `intent: str`
- **Output**: `catalog_hints`, `plan`, `plan_intent`
- **Prompts**: `prompts/query_planner.txt` (note: `.txt` extension).
- **Streaming**: `{type: "status", step: "planning", detail: "Planificando estrategia..."}`
- **Clarification path**: emits `{"type": "clarification", "question": str, "options": list}` via `get_stream_writer()` (`planner.py:95-102`).

### 2.3 `clarify_reply` (terminal)
- **Input**: `plan` (with clarification step)
- **Logic**: Terminal. Extract clarification question + options. Emit special event.
- **Streaming**: `{type: "clarification", question: "...", options: [...]}`
- **Output**: `clean_answer` (markdown), `plan_intent="clarification"`, zeros.

### 2.4 `coordinator`
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

### 2.5 `replan`
- **Input**: `preprocessed_query`, `planner_ctx`, `replan_strategy`, `replan_count`, `step_warnings`, `data_results`
- **Logic**:
  1. `replan_count += 1`
  2. Enrich planner context with warnings + result summary
  3. Append strategy hints ("AMPLIAR BÚSQUEDA" / "RESTRINGIR" / "CAMBIAR FUENTE")
  4. Re-call `generate_plan()` — **2nd LLM call**
  5. **Reset** `data_results`, `step_warnings` using the `RESET_LIST` sentinel **[DEBT-004]**
- **Output**: `replan_count`, `plan`, `plan_intent`, `data_results=RESET_LIST`, `step_warnings=RESET_LIST`

## 3. Conditional Edges (phase-local)

```python
# edges.py
def route_after_plan(state) -> Literal["clarify_reply", "inject_fallbacks"]:
    return "clarify_reply" if state.get("plan_intent") == "clarification" else "inject_fallbacks"

def route_after_coordinator(state) -> Literal["replan", "policy", "finalize"]:
    if state.get("coordinator_decision") == "replan":
        return "replan"
    return "policy" if state.get("policy_mode") else "finalize"
```

The `replan` node loops back to `inject_fallbacks` (Phase C entry), closing the adaptive retry loop.

## 4. State Keys Owned by This Phase

Written by planning:

- `active_skill`, `skill_context`
- `catalog_hints`
- `plan`, `plan_intent`
- `replan_count`, `replan_strategy`, `coordinator_decision`
- `data_results`, `step_warnings` **reset to `RESET_LIST`** on replan

Read by planning (from upstream phases):

- `preprocessed_query`, `planner_ctx`, `memory_ctx` (from Phase A: Intake)
- `data_results`, `step_warnings`, `_start_time` (from Phase C/D during coordinator evaluation)

## 5. Deviations from Constitution

- **Principle III (DI via Dishka)**: the planner accesses graph dependencies through the `_deps` ContextVar hack (`nodes/__init__.py:40-70`). Shared across the whole pipeline, tracked in the parent plan.
- **Principle VII (Observability)**: planner token usage is recorded (not affected by the streaming token-count bug, which is specific to the analyst streaming path).

---

**End of plan.md** — See parent [`../plan.md`](../plan.md) for the full graph topology and state definition.
