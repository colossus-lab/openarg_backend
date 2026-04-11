# Spec: Query Pipeline (LangGraph) — Index

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Application (core) + Infrastructure (nodes/adapters)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The **Query Pipeline** is the functional heart of OpenArg: the **stateful LangGraph graph** that transforms a user's natural-language question into a structured response with cited sources, optional charts, optional maps, and conversational memory. It coordinates **classification**, **semantic cache**, **multi-source retrieval**, **LLM analysis with streaming**, **adaptive replanning**, and **finalization with side effects**.

It is the composition of 16 nodes + 1 subgraph (NL2SQL, not integrated) + ~6 helpers in a DAG topology with a controlled replanning loop. Exposed via HTTP sync (`/api/v1/query/smart`) and WebSocket streaming (`/api/v1/query/ws/smart`).

This top-level document is an **index**. Detailed functional requirements, tech debt, and open questions now live in the sub-modules below.

## 2. Sub-modules

The pipeline is split into five phases, each with its own `spec.md` and `plan.md`:

| Sub-module | Nodes covered | Summary |
|---|---|---|
| [`001a-intake/`](./001a-intake/spec.md) | `classify`, `cache_check` (+ `fast_reply`, `cache_reply` terminals), `load_memory`, `preprocess` | Intent classification, exact + semantic cache lookup, memory restoration, deterministic query normalization (temporal expansion, acronyms, provinces, synonyms). |
| [`001b-planning/`](./001b-planning/spec.md) | `skill_resolver`, `planner` (+ `clarify_reply` terminal), `coordinator`, `replan` | Plan generation via Bedrock Claude Haiku 4.5 grounded in `table_catalog` hints, skill detection, adaptive replan loop bounded by `_MAX_REPLAN_DEPTH=2` and `_TIME_BUDGET_SECONDS=20.0`. |
| [`001c-execution/`](./001c-execution/spec.md) | `inject_fallbacks`, `execute_steps` (+ connector dispatch table) | Parallel step dispatch by dependency level, per-connector failure isolation, retry with backoff, NL2SQL inlined in the sandbox connector. |
| [`001d-analysis/`](./001d-analysis/spec.md) | `analyst`, `policy` (optional) | Streaming LLM synthesis, chunk buffering, deterministic + LLM-generated chart building, GeoJSON map extraction, META-tag parsing for confidence and citations. |
| [`001e-finalization/`](./001e-finalization/spec.md) | `finalize` | Source/document extraction, audit log, fire-and-forget cache write + memory update via `_background_tasks` registry, `duration_ms` computation, terminal streaming event with hardcoded key whitelist. |

## 3. Pipeline nodes in execution order

```
START
  │
  ▼
classify ──(classification != None)──▶ fast_reply ──▶ END           [Phase A]
  │
  ▼
cache_check ──(cached_result)────────▶ cache_reply ──▶ END          [Phase A]
  │
  ▼
load_memory ─▶ preprocess                                            [Phase A]
  │
  ▼
skill_resolver ─▶ planner ──(intent="clarification")──▶ clarify_reply ──▶ END   [Phase B]
  │
  ▼
inject_fallbacks ─▶ execute_steps                                    [Phase C]
  │
  ▼
analyst                                                              [Phase D]
  │
  ▼
coordinator ──(decision="replan")──▶ replan ──▶ inject_fallbacks    [Phase B loop]
  │
  ├── (policy_mode) ──▶ policy ─┐                                    [Phase D]
  │                              │
  └──────────────────────────────┤
                                 ▼
                            finalize ──▶ END                         [Phase E]
```

See [`./plan.md`](./plan.md) for the conditional-edge functions and full state definition.

## 4. Where detailed content lives

- **User stories (US-001 … US-014)** — distributed across phase specs by the node(s) they touch.
- **Functional requirements (FR-001 … FR-042)** — IDs preserved, each FR lives in exactly one phase spec (with cross-references where it spans phases).
- **Success criteria (SC-001 … SC-009)** — partitioned by phase with explicit "partial" markers where multiple phases contribute.
- **Open questions (CL-001 … CL-010)** — copied verbatim into the phase where they apply.
- **Tech debt (DEBT-001 … DEBT-017)** — copied verbatim into the phase where they apply, preserving the FIXED 2026-04-10 markers (DEBT-001, DEBT-003, DEBT-008, DEBT-012).

## 5. Assumptions & Out of Scope (global)

### Global assumptions
- Bedrock Claude Haiku 4.5 is available for planning and analysis.
- The frontend understands the streaming event format (status, chunk, clarification, complete).
- LangGraph and its `custom` / `updates` `stream_mode` are stable.
- Connectors return `DataResult` honoring the contract.

### Out of scope (this whole module)
- **Detail of each individual connector** — see specs under `002-connectors/`.
- **Internal mechanism of the semantic cache** — see `004-semantic-cache/`.
- **Vector search over `table_catalog`** — see `011-table-catalog/`.
- **NL2SQL implementation** — inlined in the sandbox connector (see `010-sandbox-sql/`); the `nl2sql.py` subgraph exists but is not integrated (DEBT-016 in `001c-execution/`).
- **Policy agent** — delegates to `policy_agent.py` (not part of the main graph).
- **Multi-language support** — the pipeline is designed for Spanish.
- **Executable code generation** — only read-only SQL sandbox, no Python/JS.

---

**End of spec.md index** — See [`./plan.md`](./plan.md) for the macro topology and the five sub-modules for FRs, debt, and clarifications.
