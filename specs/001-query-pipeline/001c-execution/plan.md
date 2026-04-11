# Plan: Query Pipeline — Phase C: Execution (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Parent plan**: [../plan.md](../plan.md)
**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10

---

## 1. Layer Mapping

| Layer | Component | File |
|---|---|---|
| **Application / Nodes** | `inject_fallbacks` node | `application/pipeline/nodes/` (inline in executor or its own module) |
| | `execute_steps` node | `application/pipeline/nodes/executor.py` |
| **Application / Step executor** | `execute_steps()`, `dispatch_step_with_retry()`, connector dispatch table | `application/pipeline/step_executor.py` |
| **Application / Connectors (pipeline steps)** | One module per connector step | `application/pipeline/connectors/*.py` |
| **Application / Subgraphs** | NL2SQL subgraph (**not integrated** — see DEBT-016) | `application/pipeline/subgraphs/nl2sql.py` |
| **Application / State** | `_resettable_add` reducer for `data_results` / `step_warnings` | `application/pipeline/state.py` |
| **Infrastructure / Connectors** | 13 providers (see `002-connectors/`) | `infrastructure/adapters/...` |
| **Infrastructure / SQL sandbox** | Read-only PG sandbox | `infrastructure/adapters/sandbox/pg_sandbox_adapter.py` |

## 2. Node Specifications

### 2.1 `inject_fallbacks`
- **Input**: `plan`, `question`, `preprocessed_query`
- **Logic**: Check if the plan has a data action (`query_sandbox`, `search_datasets`, etc.). If not, insert a `search_datasets` fallback at position 0 with `{query: preprocessed_q, limit: 5}`.
- **Output**: `plan` (modified).

### 2.2 `execute_steps`
- **Input**: `plan`, `question`
- **Logic**:
  1. Build `ConnectorDeps` dataclass with all 13 connectors (`series`, `arg_datos`, `georef`, `ckan`, `sesiones`, `ddjj`, `staff`, `bcra`, `sandbox`, `vector_search`, `llm`, `embedding`, `semantic_cache`).
  2. Call `execute_steps()` (`step_executor.py`) — parallel by dependency level.
  3. Per-step status callback emits connector label (see DEBT-006).
  4. Collect `data_results` + `step_warnings`.
- **Output**: `data_results`, `step_warnings`
- **Streaming**: `{type: "status", step: "searching", detail: "Consultando fuentes de datos..."}` + per-connector updates
- **Retry**: max 2 per step via `dispatch_step_with_retry()`.

### 2.3 Connector dispatch table

The step executor holds a module-level dict-of-lambdas (`step_executor.py:70-105`) that maps `action` → connector coroutine. Example structure:

```python
DISPATCH = {
    "query_series": lambda deps, step: deps.series.run(step.params),
    "query_bcra": lambda deps, step: deps.bcra.run(step.params),
    "search_datasets": lambda deps, step: deps.vector_search.search_datasets_hybrid(**step.params),
    "query_sandbox": lambda deps, step: deps.sandbox.execute_readonly(step.params["sql"]),
    # ...
}
```

Adding a new connector requires editing this dict **[DEBT-013]**.

## 3. ConnectorDeps Dataclass

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

Three concrete adapters (BCRA, DDJJ, semantic_cache) are passed without a domain port — tracked in the parent plan under Deviations from Constitution.

## 4. State Keys Owned by This Phase

Written by execution:

- `plan` (mutated by `inject_fallbacks`)
- `data_results` (accumulated via `_resettable_add` reducer)
- `step_warnings` (accumulated via `_resettable_add` reducer)

Read by execution (from upstream phases):

- `plan`, `plan_intent` (from Phase B: Planning)
- `question`, `preprocessed_query` (from Phase A: Intake)

## 5. NL2SQL Execution Model

The current as-built pipeline does **not** run the NL2SQL subgraph at `application/pipeline/subgraphs/nl2sql.py`. Instead, the sandbox connector (`connectors/sandbox.py` step + `infrastructure/adapters/sandbox/pg_sandbox_adapter.py`) handles NL2SQL inline:

1. Accept a natural-language query + table hints.
2. Prompt the LLM for a SQL query.
3. Execute read-only against PG.
4. Return rows as a `DataResult`.
5. If execution fails, the connector itself retries or surfaces a `step_warning`.

The standalone subgraph is dead code (**[DEBT-016]**). If reactivated, it would be invoked from within `execute_steps` for `query_sandbox`-style actions and would share state via a nested `OpenArgState`-like TypedDict.

## 6. Deviations from Constitution

- **Principle I (Hexagonal)**: Three `ConnectorDeps` entries (BCRA, DDJJ, semantic_cache) are concrete adapters without a port.
- **Principle III (DI via Dishka)**: executor reaches graph dependencies via the `_deps` ContextVar hack — same as all pipeline nodes.
- **Principle VII (Observability)**: per-connector success/failure is recorded via `MetricsCollector`; circuit breaker stats live in `infrastructure/resilience/circuit_breaker.py`.

---

**End of plan.md** — See parent [`../plan.md`](../plan.md) for the full graph topology and state definition.
