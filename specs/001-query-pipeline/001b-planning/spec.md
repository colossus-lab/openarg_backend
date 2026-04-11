# Spec: Query Pipeline — Phase B: Planning

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Application (pipeline nodes) + Infrastructure (Bedrock Claude Haiku 4.5, pgvector table_catalog)
**Parent**: [../spec.md](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The **Planning phase** translates the preprocessed user query into an executable `ExecutionPlan` — an ordered list of atomic `PlanStep` instances that downstream nodes dispatch to connectors. It is the first phase that consumes LLM tokens (Bedrock Claude Haiku 4.5) and the phase responsible for the **adaptive replan loop** that gives the pipeline its resilience against insufficient data.

Nodes covered:

1. `skill_resolver` — match the query against `SkillRegistry` and inject skill context.
2. `planner` — generate an `ExecutionPlan` via LLM, grounded in `table_catalog` vector hints.
3. `clarify_reply` — terminal short-circuit when the planner decides the query is ambiguous.
4. `coordinator` — post-analysis heuristic that decides `continue | replan | escalate`.
5. `replan` — re-run planning with a strategy hint (broaden / narrow / switch_source).

The phase is bounded by `_MAX_REPLAN_DEPTH=2` and a `_TIME_BUDGET_SECONDS=20.0` hard stop.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **ExecutionPlan** | Structured plan produced by the planner containing an ordered sequence of `PlanStep`. |
| **PlanStep** | Atomic unit of work (e.g. "query BCRA with casa=USD"). |
| **Plan intent** | Discriminant on the plan object: `"data"`, `"clarification"`, `"error"`, etc. |
| **Catalog hints** | Vector-search snippets from `table_catalog` injected into the planner prompt. |
| **Skill** | Optional plugin that matches a query pattern and injects extra context into planner + analyst prompts. |
| **Replan** | Adaptive retry when the initial data is insufficient (maximum 2 passes). |
| **Replan strategy** | One of `broaden`, `narrow`, `switch_source`. |
| **Coordinator** | Heuristic (non-LLM) node that decides whether to replan, continue, or escalate. |
| **Time budget** | 20-second wall-clock budget enforced by the coordinator. |

## 3. User Stories

### US-006 (P1) — Clarification when the query is ambiguous
**As** a user, **when** my question is too vague, **I want** to receive clickable options to refine it. **Trigger**: the planner returns `plan.intent="clarification"` → direct route to `clarify_reply` with the special event `{"type": "clarification", "options": [...]}`.

### US-007 (P1) — Replanning when data is insufficient
**As** the system, **when** the first attempt returns little data or many connector errors, **I need** to retry with a different strategy (broaden / narrow / switch_source). **Trigger**: the `coordinator` returns `decision="replan"` → loop back to the planner with enriched context. Maximum 2 replans.

### US-013 (P3) — Skill system (optional plugins)
**As** the system, **I can** activate specific skills that inject additional context into the planner and analyst (e.g. "budget skill"). **Trigger**: `skill_resolver` matches a pattern → injects `skill_context` into state.

## 4. Functional Requirements

### Planning
- **FR-011**: The planner MUST use Bedrock Claude Haiku 4.5 to generate an `ExecutionPlan` with ordered steps and `intent` ("data" | "clarification" | "error" | ...).
- **FR-012**: The planner MUST discover relevant cached tables via vector search over `table_catalog`.
- *(FR-013 — `search_datasets` fallback injection — lives in Phase C: Execution, since it mutates the plan immediately before dispatch.)*

### Replanning
- **FR-026**: The coordinator MUST decide `continue` | `replan` | `escalate` based on heuristics: elapsed time, amount of useful data, number of warnings, replan counter.
- **FR-027**: The system MUST apply a **time budget** of 20s (rule: if elapsed > 20s → escalate).
- **FR-028**: The system MUST limit replans to **a maximum of 2 passes** (`_MAX_REPLAN_DEPTH=2`).
- **FR-029**: The replan MUST reset `data_results` and `step_warnings` using the `RESET_LIST` sentinel (not `[]`) due to the custom reducer.
- **FR-030**: The system MUST support 3 replan strategies: `broaden`, `narrow`, `switch_source`.
- **FR-031**: On replan, the analyst MUST emit `{"type": "clear_answer"}` so the frontend replaces the previous response. *(The emission itself happens in Phase D, but the trigger lives in this phase.)*

## 5. Success Criteria

- **SC-006**: Replan loop converges in ≤2 iterations in ≥90% of cases.
- **SC-001** *(partial)*: P1 response (normal path, cache miss) in **<15 seconds (p95)**. The planner budget (~1–2 s per LLM call) is a significant contributor.

## 6. Assumptions & Out of Scope

### Assumptions
- Bedrock Claude Haiku 4.5 is available for plan generation.
- `table_catalog` is populated with vector(1024) embeddings + HNSW index (see module `011-table-catalog/`).
- Skills are statically declared in code (no hot reload).

### Out of scope (this sub-module)
- **Skill discovery / plugin system** — hardcoded tuple, see CL-004.
- **Vector search over `table_catalog`** — see `011-table-catalog/`.
- **Step dispatch / per-connector execution** — see `../001c-execution/`.
- **Clarification event frontend contract** — see CL-007 and `specs/contracts/sse_events.md` (recommended, not yet written).

## 7. Open Questions

- **[RESOLVED CL-004]** — `SkillRegistry` is **hardcoded** — no filesystem discovery, no plugins. A module-level `_BUILTIN_SKILLS` tuple in `registry.py:260-266` with **5 frozen dataclass skills**: `verificar`, `comparar`, `presupuesto`, `perfil`, `precio`. The constructor (`registry.py:26-45`) populates the internal dict by iterating the tuple. Adding skills requires a code edit.
- **[RESOLVED CL-005]** — After a replan, the analyst on the 2nd pass sees **ONLY the 2nd-pass results**, not an accumulation. The `_resettable_add` reducer in `state.py:17-22` does `if right is RESET_LIST: return []`, discarding all prior accumulation. Confirmed in `replan.py:109`, which returns `data_results=RESET_LIST`. It is a hard reset, not a merge.
- **[RESOLVED CL-006]** — `_has_useful_data()` (`coordinator.py:32-38`) considers results with `source.startswith("pgvector:")` or `source.startswith("ckan:")` useful **even if they have no records**. Rationale (from the code): those prefixes represent **metadata discovery** (vector search and CKAN lookup) — they are valid responses even if they bring no concrete rows. Example: "I found dataset X about budget" without raw data yet. It prevents the coordinator from triggering an unnecessary replan when discovery was successful.
- **[PARTIAL CL-007]** — **The backend emits** in `planner.py:95-102` the exact event `{"type": "clarification", "question": str, "options": list}` via `get_stream_writer()`. The schema is consistent but **there is no formal cross-repo contract documented**. The frontend consumes it in `src/hooks/useSSEStream.ts` and renders it as chips (verified). **Recommendation**: create `specs/contracts/sse_events.md` with the full event schema (status/chunk/complete/clarification/error) as a shared contract between backend and frontend.

## 8. Tech Debt Discovered

- **[DEBT-002]** — **Hardcoded magic constants** in coordinator (`_MAX_REPLAN_DEPTH=2`, `_TIME_BUDGET_SECONDS=20.0`). Not configurable via DI or settings.
- **[DEBT-004]** — **`RESET_LIST` is an identity object** — compared with `is`. If code accidentally creates a different empty list, the reducer doesn't detect it and doesn't reset.
- **[DEBT-009]** — **Skill context injection via string concatenation** with hardcoded separators. Hard to maintain.

---

**End of spec.md** — See [./plan.md](./plan.md) for the as-built topology of this phase.
