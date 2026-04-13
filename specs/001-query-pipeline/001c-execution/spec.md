# Spec: Query Pipeline — Phase C: Execution

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-13
**Hexagonal scope**: Application (pipeline nodes + step executor) + Infrastructure (13 connectors, SQL sandbox)
**Parent**: [../spec.md](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The **Execution phase** is the pipeline's IO workhorse. It takes the `ExecutionPlan` produced by the planner and dispatches its `PlanStep` instances to the appropriate connectors, running independent steps in parallel (topological sort by dependency level) while isolating per-step failures. It also owns the NL2SQL in-connector loop — currently inlined in the sandbox connector rather than running as a separate subgraph.

Nodes covered:

1. `inject_fallbacks` — guarantees the plan contains at least one data-retrieval step.
2. `execute_steps` — the parallel dispatcher that builds `ConnectorDeps`, calls `execute_steps()` from `step_executor.py`, retries failed steps, and emits per-connector streaming status.
3. *(Implicit)* `connector_dispatch` — the dict-of-lambdas dispatch table that maps a `PlanStep.action` to a connector coroutine.

The phase exits with either populated `data_results` and `step_warnings`, or empty lists that will trigger the coordinator's replan heuristic in Phase B.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Step executor** | `step_executor.py` helper that resolves dependency levels and dispatches steps. |
| **Dependency level** | A topological "stratum" of the plan: all steps in the same level can run in parallel. |
| **ConnectorDeps** | Dataclass bundling the 13 connector adapters passed to each step. |
| **Connector dispatch table** | Dict-of-lambdas in `step_executor.py:70-105` that maps `action` → connector callable. |
| **`DataResult`** | Contract object returned by every connector (`source`, `records`, metadata). |
| **Step warning** | String accumulated in `step_warnings` when a step raises but is isolated. |
| **Fallback step** | Synthetic `search_datasets` step injected by `inject_fallbacks` when the plan has no data action. |
| **NL2SQL loop** | The natural-language-to-SQL iteration currently inlined in the sandbox connector. |

## 3. User Stories

### US-001 (P1) — Conversational response with sources (execution portion)
**As** an end user, **I want** the pipeline to consult multiple public-data sources for my query in parallel and gather useful records. **Trigger**: `execute_steps` receives a plan with N data-retrieval steps → dispatches them to connectors by dependency level.

### US-010 (P2) — Per-connector failure isolation
**As** the system, **when** a connector fails, **I need** to continue with the others and report a warning, without tearing down the entire query. **Trigger**: `execute_steps` catches per-step exceptions and accumulates them in `step_warnings`.

### US-014 (P2) — Offline execution (batch)
**As** the system, **I can** run the pipeline offline via a Celery task (`analyst_tasks.analyze_query`) for batch processing without blocking HTTP. *(The execution phase is the primary consumer of the offline runner's time budget.)*

## 4. Functional Requirements

### Plan fallback injection
- **FR-013**: The system MUST inject a `search_datasets` fallback step when the plan has no data-retrieval action.

### Execution
- **FR-014**: The executor MUST dispatch steps to connectors by **dependency level** (topological sort), running independent ones in parallel.
- **FR-015**: The executor MUST isolate per-step failures (exceptions do not propagate to the graph).
- **FR-016**: The executor MUST retry failed steps with backoff (max 2 retries) on retryable errors (timeout, 5xx).
- **FR-017**: The executor MUST emit per-connector streaming status ("Querying series...", "Querying BCRA...").
- **FR-017a**: The sandbox connector MUST short-circuit pure dataset-discovery questions (for example, "qué datasets de educación hay...") into a structured listing `DataResult` instead of forcing them through NL2SQL. Listing available datasets is discovery, not SQL analysis.

## 5. Success Criteria

- **SC-004**: **Zero pipelines down** due to a single connector failure.
- **SC-007**: Streaming emits at least **1 status message every 3 seconds** during normal processing. *(This phase is the dominant emitter — per-connector updates from the step executor.)*
- **SC-001** *(partial)*: P1 response (normal path, cache miss) in **<15 seconds (p95)**. The bulk of wall-clock time lives in this phase.

## 6. Assumptions & Out of Scope

### Assumptions
- Connectors honor the `DataResult` contract.
- Each connector implements its own retry (`@with_retry`) and circuit breaker at the infrastructure layer.
- The sandbox connector owns its internal NL2SQL loop and does not escalate back into the graph.

### Out of scope (this sub-module)
- **Individual connector specs** — see `002-connectors/`.
- **Circuit breaker + retry decorator** — see `009-resilience/` (if present).
- **SQL sandbox internals** — see `010-sandbox-sql/`.
- **NL2SQL subgraph internals** — owned by [`../../010-sandbox-sql/010b-nl2sql/`](../../010-sandbox-sql/010b-nl2sql/). This phase treats NL2SQL as an opaque sub-step: the sandbox connector runs as one of the `execute_steps` dispatch targets, and inside its own execution it hands off to the NL2SQL subgraph. The main pipeline does not see the subgraph's internal nodes.
- **LLM synthesis over the results** — see `../001d-analysis/`.

## 7. Open Questions

- **[RESOLVED CL-009]** — `limit` **is honored**. In `connectors/vector_search.py:27-30`, `execute_search_datasets_step` reads `params.get("limit", 10)` and passes it to `search_datasets_hybrid(...)`. The `inject_fallbacks` node overrides the default 10 with 5.

## 8. Tech Debt Discovered

- **[DEBT-006]** — **Optional `on_step_start` callback** for streaming. Coupling between streaming and step dispatch, inconsistency: not all steps emit status.
- **[DEBT-013]** — **Connector dispatch table is a dict of lambdas** (`step_executor.py:70-105`). Adding a new connector requires a manual update, with no auto-registration.
- **[DEBT-016]** — ~~**NL2SQL subgraph exists but is not integrated into the main graph**~~ **CLOSED 2026-04-11 via FIX-004**: the inline loop in `connectors/sandbox.py` has been removed; the sandbox connector now delegates the NL2SQL state machine to the compiled subgraph at `application/pipeline/subgraphs/nl2sql.py`. Contract and node topology are documented in [`../../010-sandbox-sql/010b-nl2sql/spec.md`](../../010-sandbox-sql/010b-nl2sql/spec.md). This sub-module still sees NL2SQL as opaque — the subgraph lives one layer down inside the sandbox connector's execution slot, not as a peer of the main-graph nodes.
- **[DEBT-019]** — **Connectors are not required to produce JSON-safe records**. `DataResult.records` is typed as `list[dict]` with no constraint on value types, so a connector is free to put `datetime.date`, `Decimal`, `UUID` or `bytes` into a row and the pipeline will happily carry it all the way to finalize. `FIX-017` (2026-04-12) shipped a defensive encoder at the serialization sinks, but the spec-level contract is still "records go in, whatever Python values come out". Follow-up: add an FR to `002-connectors/spec.md` requiring every adapter to return only JSON-primitive values (`str` / `int` / `float` / `bool` / `None` / `list` / `dict`), and enforce it with a shared assertion helper in the step executor that runs once per `DataResult` in non-prod and skips in prod. This closes the origin of FIX-017 cleanly, rather than absorbing the damage at the sink forever.
- **[DEBT-020]** — ~~**Dataset-listing questions could be misrouted into NL2SQL inside the sandbox connector, producing retries and last-resort output for what should have been a plain dataset discovery response.**~~ **FIXED 2026-04-13**: the sandbox connector now detects discovery/listing intents and returns a structured dataset inventory `DataResult` before invoking the NL2SQL subgraph.

---

**End of spec.md** — See [./plan.md](./plan.md) for the as-built topology of this phase.
