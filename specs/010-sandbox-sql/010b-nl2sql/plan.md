# Plan: NL2SQL (FIX-004 integrated subgraph)

**Related spec**: [./spec.md](./spec.md)
**Parent plan**: [../plan.md](../plan.md)
**Type**: Forward
**Status**: Draft
**Last synced with code**: 2026-04-13

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Application (subgraph) | Compiled `nl2sql` LangGraph | `application/pipeline/subgraphs/nl2sql.py` |
| Application (caller) | `execute_sandbox_step` — table discovery + state construction + subgraph invocation | `application/pipeline/connectors/sandbox.py` |
| Application (shared helper, **NEW**) | `spawn_background(coro, *, name)` + `_background_tasks: set` — strong-ref registry for fire-and-forget async tasks | `application/pipeline/_background_tasks.py` |
| Domain (ports consumed) | `ILLMProvider`, `ISQLSandbox`, `IEmbeddingProvider` | `domain/ports/llm/`, `domain/ports/sandbox/`, `domain/ports/llm/` |
| Infrastructure (prompts) | `nl2sql`, `sql_fixer` templates | `prompts/nl2sql.txt`, `prompts/sql_fixer.txt` |
| Infrastructure (persistence) | `save_successful_query`, `indec_live_fallback` helpers | `application/pipeline/connectors/sandbox.py` module-level + `application/pipeline/history.py` |
| Presentation | `/api/v1/sandbox/ask` handler (unchanged) | `presentation/http/controllers/sandbox/sandbox_router.py` |

**Boundary**: the subgraph owns the retry / fallback / save-success state machine. The caller owns table discovery (hint resolution, fnmatch, vector search, catalog enrichment, year filter). Neither side crosses the line.

**Shared helper rationale**: the background task registry was introduced earlier today in `finalize.py` (DEBT-012 fix) and is now needed by the NL2SQL subgraph as well. That is two real callers, which is exactly when §0 regla 2 of the constitution allows promoting a pattern to a shared helper ("abstractions pay rent only when a second caller exists"). The helper lives at `application/pipeline/_background_tasks.py` — pipeline-scoped, not broader, because no component outside the pipeline needs it today.

## 2. Node topology

```
                 START
                   │
                   ▼
           ┌───────────────┐
           │  generate_sql │       (FR-001, FR-010)
           └───────┬───────┘
                   ▼
           ┌───────────────┐
           │  execute_sql  │◀──────────────┐
           └───────┬───────┘               │
                   │                       │
         ┌─────────┼─────────┐             │
         │         │         │             │
    success     retry     last_resort      │
         │         │         │             │
         ▼         ▼         ▼             │
  ┌──────────┐ ┌───────┐  ┌───────────┐    │
  │ format_  │ │fix_sql│  │last_resort│    │
  │  result  │ └───┬───┘  └─────┬─────┘    │
  └────┬─────┘     │            │          │
       │           └────────────┼──────────┘
       │                        │
       │   ┌────────────────────┘
       │   │
       ▼   ▼
   ┌──────────────┐
   │_route_after_ │
   │   format     │
   └──┬────┬────┬─┘
      │    │    │
      │    │    └─────────▶ END
      │    │
      │    └─▶ save_success ──▶ END   (FR-016)
      │
      └─▶ indec_fallback ──▶ END        (FR-008)
```

**Edge functions**:

- `_route_after_execute(state)` → `"retry" | "success" | "last_resort"` based on `result.error` and `attempt < max_attempts`.
- `_route_after_format(state)` → `"indec_fallback" | "save_success" | "end"` based on `result.rows`, `indec_pattern_match`, and `used_fallback`.

**Why `_route_after_format` exists and not just edges from nodes**: LangGraph conditional edges can only fire once per node. The post-success branching (INDEC vs save-success vs end) needs to happen after `format_result_node` has built the `DataResult`, because the final metadata must reflect whichever branch was taken. Routing from `format_result` gives us that.

## 3. State schema

```python
class NL2SQLState(TypedDict, total=False):
    # --- inputs (caller provides) ---
    nl_query: str                 # natural-language question
    tables: list                  # list[CachedTable], already filtered
    tables_context: str           # rendered context block for the prompt
    table_notes: str              # optional year-coverage / dimension hints
    catalog_entries: dict         # dict[table_name -> catalog metadata]
    table_descriptions: list[str] # pre-computed display names
    few_shot_block: str           # rendered examples (may be empty)
    max_attempts: int             # default 2

    # --- working ---
    generated_sql: str
    result: SandboxResult | None
    attempt: int                  # incremented by fix_sql_node
    last_error: str               # cleared on success
    used_fallback: bool           # set True by last_resort_node

    # --- output ---
    data_results: list[DataResult]  # FR-012: exactly one element before END
```

Runtime dependencies (`llm`, `sandbox`, `embedding`, `semantic_cache`) are intentionally kept OUTSIDE the persisted LangGraph state. The caller sets them in a request-scoped `ContextVar` helper around `ainvoke()`, and node functions read them from that runtime context. This preserves testability without leaking non-serializable adapters into checkpoint snapshots.

## 4. Node responsibilities

| Node | Reads | Writes | Side effects |
|---|---|---|---|
| `generate_sql_node` | runtime `llm`, `nl_query`, `tables_context`, `few_shot_block` | `generated_sql`, `attempt=0`, `used_fallback=False` | 1 LLM call |
| `execute_sql_node` | runtime `sandbox`, `generated_sql` | `result`, `last_error` (or cleared) | 1 SQL execute |
| `fix_sql_node` | runtime `llm`, `generated_sql`, `last_error`, `tables_context` | `generated_sql`, `attempt++` | 1 LLM call |
| `last_resort_node` | runtime `sandbox`, `tables` | `result`, `generated_sql`, `used_fallback=True` | 1 SQL execute (best-effort) |
| `indec_fallback_node` | `nl_query` | `data_results` (replaces empty) | HTTP call to live INDEC API |
| `save_success_node` | `result`, `generated_sql`, `tables`, runtime `embedding`, runtime `semantic_cache`, `used_fallback` | — | Spawns fire-and-forget `save_successful_query` held by `_background_tasks` set |
| `format_result_node` | `result`, `generated_sql`, `table_descriptions`, `used_fallback`, `APP_ENV` | `data_results` | — |

## 5. Prompts

- **`nl2sql`** — system prompt for the first SQL generation. Loaded via `load_prompt("nl2sql", tables_context=..., few_shot_block=...)`. Body stays in the existing template file; no changes as part of FIX-004.
- **`sql_fixer`** — system prompt for the retry path. Loaded via `load_prompt("sql_fixer")`. Takes the failed SQL + error + top 2000 chars of tables context as user message. No changes.

## 6. Compile-once cache

```python
_compiled_subgraph: Any | None = None
_compile_lock = asyncio.Lock()

async def get_compiled_nl2sql_subgraph():
    global _compiled_subgraph
    if _compiled_subgraph is not None:
        return _compiled_subgraph
    async with _compile_lock:
        if _compiled_subgraph is None:
            _compiled_subgraph = build_nl2sql_subgraph()
    return _compiled_subgraph
```

Rationale: `build_nl2sql_subgraph()` walks `StateGraph.add_node/add_edge/compile` every time it's called, which is non-trivial at request time. The compiled graph is stateless across requests, so it's safe to share. Same pattern as `smart_query_v2_router._get_or_compile_graph` (which compiles the main pipeline once).

## 7. Background task registry — shared helper

Both `finalize.py` (memory update) and this subgraph (`save_success_node`) need the same "spawn fire-and-forget, keep strong ref, log unhandled exceptions" pattern. Rather than duplicate it, we promote it to a module used by both:

```python
# application/pipeline/_background_tasks.py
from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

_background_tasks: set[asyncio.Task[Any]] = set()


def spawn_background(coro: Any, *, name: str) -> asyncio.Task[Any]:
    """Schedule a fire-and-forget coroutine while keeping a strong reference.

    Without the module-level set, CPython's weak-ref GC may collect the
    task mid-execution — a well-known asyncio footgun. The done_callback
    removes the task from the set when it finishes AND surfaces any
    unhandled exception as a WARNING (silent swallowing is how DEBT-012
    survived so long).
    """
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)

    def _done(t: asyncio.Task[Any]) -> None:
        _background_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            logger.warning(
                "Background task %r raised: %s", name, exc, exc_info=exc
            )

    task.add_done_callback(_done)
    return task
```

Consumers:

- `application/pipeline/nodes/finalize.py` — imports `spawn_background` and deletes its local `_background_tasks` set + `_spawn_background` helper. The `_update_memory_bg` call becomes `spawn_background(coro, name="finalize.memory_update")`. **Net delete** of ~20 lines from `finalize.py`.
- `application/pipeline/subgraphs/nl2sql.py` — imports `spawn_background` and uses it inside `save_success_node` to schedule `save_successful_query(...)` with `name="nl2sql.save_success"`.

The `_background_tasks` set is intentionally module-private (leading underscore). External code uses `spawn_background(...)` as the only entry point. This keeps the scope narrow and stops callers from peeking at in-flight tasks.

## 8. Caller (sandbox.py) after refactor

The sandbox connector stops hosting the NL2SQL loop and becomes a thin state-construction wrapper:

```python
async def execute_sandbox_step(step, sandbox, llm, embedding, vector_search, semantic_cache, user_query=""):
    # 1. Table discovery (UNCHANGED — lines ~373-582 of today's file stay)
    tables = await sandbox.list_cached_tables()
    # ... hint resolution, catalog search, vector fallback, year filter,
    # ... catalog_entries enrichment, tables_context building ...

    # 2. Compute table_descriptions (moved from the inline implementation)
    table_descriptions = build_table_descriptions(tables, catalog_entries)

    # 3. Few-shot block via existing helper
    few_shot_block = await get_few_shot_examples(nl_query, embedding, semantic_cache)

    # 4. Delegate to the compiled subgraph
    compiled = await get_compiled_nl2sql_subgraph()
    with nl2sql_runtime(
        llm=llm,
        sandbox=sandbox,
        embedding=embedding,
        semantic_cache=semantic_cache,
    ):
        initial_state = {
            "nl_query": nl_query,
            "tables": tables,
            "tables_context": tables_context,
            "table_notes": table_notes,
            "catalog_entries": catalog_entries,
            "table_descriptions": table_descriptions,
            "few_shot_block": few_shot_block or "",
            "max_attempts": 2,
        }
        final_state = await compiled.ainvoke(initial_state)
    return final_state.get("data_results", []) or []
```

The ~170 lines of inline retry + fallback + save + format code are deleted. `sandbox.py` shrinks by roughly that amount.

## 9. Test plan

Tests live in `tests/unit/application/pipeline/subgraphs/test_nl2sql.py` and use pytest async fixtures. Each test constructs an initial state with mocked `llm` / `sandbox` and invokes the compiled subgraph directly — no need to stand up the full pipeline.

| Test | Setup | Asserts |
|---|---|---|
| `test_happy_path_single_generation` | Mock LLM returns valid SQL once; sandbox returns rows | `used_fallback` is False; `data_results[0].records` is non-empty; `save_success` was spawned (check `_background_tasks`) |
| `test_self_correction_one_retry` | First execute errors; second succeeds after `fix_sql` | `attempt == 1`; happy-path metadata; save-success spawned |
| `test_self_correction_exhausted_then_last_resort` | Both executes error; last_resort succeeds | `used_fallback` is True; metadata has `used_fallback=true`; save-success NOT spawned |
| `test_last_resort_also_fails` | All executes error including last_resort | Single `DataResult` with `metadata.error` populated, `records=[]` |
| `test_empty_indec_triggers_fallback` | Execute succeeds with zero rows; `indec_pattern_match=True`; mock `indec_live_fallback` returns data | `data_results` comes from the INDEC fallback |
| `test_empty_non_indec_goes_to_format` | Execute succeeds with zero rows; `indec_pattern_match=False` | Normal empty-result formatting; no INDEC call |
| `test_prod_env_redacts_generated_sql` | `APP_ENV=prod` monkeypatched; success path | `metadata.generated_sql` is NOT in the output |
| `test_local_env_includes_generated_sql` | `APP_ENV=local`; success path | `metadata.generated_sql` is in the output |
| `test_missing_embedding_skips_save_success` | Success with `embedding=None` | `_background_tasks` did not grow |
| `test_compile_once_is_idempotent` | Call `get_compiled_nl2sql_subgraph()` twice | Same object returned |

Behavior-parity smoke test (can be integration, not unit):
- **Replay**: pick 20 queries from the `successful_queries` table, run them through the new subgraph with the real sandbox, assert the shape and key fields of the `DataResult` match what the pre-FIX-004 implementation produced.

## 10. Observability

| Signal | Level | Where |
|---|---|---|
| SQL generated | DEBUG | `generate_sql_node` — `generated_sql[:200]` |
| SQL retry | WARNING | `fix_sql_node` — includes attempt, error, SQL prefix |
| Last-resort used | INFO | `last_resort_node` — includes fallback table name |
| Last-resort failed | WARNING | `last_resort_node` — includes error |
| Save-success raised | WARNING | `_spawn_background` done_callback — includes exception |
| Subgraph compile | (implicit via LangGraph) | First invocation only |

No new metrics in the `MetricsCollector` as part of FIX-004 (keep-it-simple). Tracking retry rate, fallback usage rate, and save-success failure rate are follow-up work; if tracked, add them as `connector:nl2sql` under the existing metrics collector, not as a new collection.

## 11. Deviations from Constitution

- **None**. The refactor reduces duplication (§0: "one way to do something" — the shared `spawn_background` helper dedupes what was already copied into `finalize.py`), improves testability (§III explicit DI — state fields instead of closure capture), preserves the hexagonal boundary (the subgraph lives in `application/`, the sandbox adapter stays in `infrastructure/`), and does not introduce new dependencies.

- **On the 7-node count vs §0 simplicity axiom**: the subgraph has seven nodes (`generate_sql`, `execute_sql`, `fix_sql`, `last_resort`, `indec_fallback`, `save_success`, `format_result`). Each one maps **1:1 with a behavior documented in the spec**: FR-001 → `generate_sql`, FR-002 → `execute_sql`, FR-003 → `fix_sql`, FR-006 → `last_resort`, FR-008 → `indec_fallback`, FR-016 → `save_success`, FR-012 → `format_result`. There are zero "utility" nodes, zero helpers disguised as nodes, zero glue. The node graph **is** the spec, rendered as Python.

  The plausible alternative — collapsing to 5 nodes by folding `indec_fallback` and `save_success` into `format_result` — was rejected because it would create a "god node" that mixes three unrelated responsibilities (format the DataResult, branch on INDEC applicability, spawn a fire-and-forget side effect). That is **more** complex under §0 regla 4 ("data flow over control flow") and regla 5 ("one way to do something"), not less. The 7-node topology keeps responsibility-per-node at exactly one, which is what the axiom is asking for.

  This also satisfies the "complexity requires inline justification" rule at the end of §0: the choice is documented here, with the specific trade-off named, and bounded by a testable success criterion (SC-006 — behavior parity with the pre-FIX-004 implementation, verified by a replay suite).

## 12. Rollout

1. Land this spec + plan (no code).
2. User reviews and approves.
3. Implement the subgraph rewrite.
4. Implement the sandbox.py refactor.
5. Write unit tests.
6. Run the behavior-parity smoke test against staging.
7. Single commit: `refactor(nl2sql): integrate subgraph, dedupe inline loop (FIX-004)`.

## 13. Source Files (post-implementation)

- `application/pipeline/_background_tasks.py` — **new file**, ~35 lines. Exports `spawn_background(coro, *, name)` as the single public entry point. Imported by both `nodes/finalize.py` and `subgraphs/nl2sql.py`.
- `application/pipeline/nodes/finalize.py` — **shrinks by ~20 lines**. Local `_background_tasks` set and `_spawn_background` helper are deleted. The `_update_memory_bg` call becomes `spawn_background(..., name="finalize.memory_update")`. Import added at top of file.
- `application/pipeline/subgraphs/nl2sql.py` — grows from 252 to ~350 lines. Adds the 3 new nodes (`last_resort`, `indec_fallback`, `save_success`), the compile-once cache (`get_compiled_nl2sql_subgraph` + `_compile_lock`), internal `INDEC_PATTERN` pattern check (FR-011), and the routing helpers (`_route_after_execute`, `_route_after_format`). Does NOT grow to 400 because the background-task helper is extracted.
- `application/pipeline/connectors/sandbox.py` — **shrinks by ~150 lines**. Lines 591-736 of today's file (NL2SQL prompt load, LLM call, retry loop, last-resort fallback, INDEC fallback, save-success fire-and-forget, DataResult formatting) are deleted and replaced with a ~25-line state-construction + `compiled_subgraph.ainvoke(initial_state)` block. Table discovery logic above that (lines 373-582) is unchanged.
- `tests/unit/application/pipeline/subgraphs/test_nl2sql.py` — **new file** with the 10 unit tests listed in §9.
- `tests/unit/application/pipeline/test_background_tasks.py` — **new file**, ~30 lines, verifies: (a) spawn_background keeps a strong reference, (b) done_callback removes the task from the set, (c) an exception in the coroutine is surfaced as a WARNING log, not swallowed.

**Net line-count effect of FIX-004**: `-150 (sandbox) - 20 (finalize) + 100 (nl2sql growth, 252→~350) + 35 (helper) + ~180 (new tests) ≈ +145 lines`. Most of that growth is tests — the production code is roughly flat while duplication drops to zero.

---

**End of plan.md**
