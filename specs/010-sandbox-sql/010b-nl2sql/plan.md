# Plan: NL2SQL (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Parent plan**: [../plan.md](../plan.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Application (subgraph) | `generate_sql_node`, `execute_sql_node`, `fix_sql_node` | `application/pipeline/connectors/sandbox.py` (current inline home) |
| Application (target) | `nl2sql` subgraph | `application/pipeline/subgraphs/nl2sql.py` (exists but **not integrated** — FIX-004) |
| Presentation | `/api/v1/sandbox/ask` handler | `presentation/http/controllers/sandbox/sandbox_router.py` |
| Prompt | `nl2sql.md` | `prompts/nl2sql.md` |
| Dependency | `ISQLSandbox.execute_readonly` | owned by `010a-sql-sandbox` |
| Dependency | `table_catalog` matcher | owned by module `000-architecture` / optimizations Cat 3 |

## 2. Endpoint

| Method | Path | Rate Limit | Behavior |
|---|---|---|---|
| POST | `/api/v1/sandbox/ask` | 10/min | NL2SQL with self-correction |

## 3. NL2SQL Flow

```
POST /sandbox/ask {"question": "..."}
    ↓
is_suspicious(question)? → 400 if yes
    ↓
Load table metadata (table_catalog vector search + fnmatch fallback) → build context
    ↓
LLM generate SQL (prompt "nl2sql.md", few-shot block)
    ↓
execute_readonly(sql)            [delegated to 010a-sql-sandbox]
    ├→ success → return {sql, rows, ...}
    └→ error → LLM fix_sql(sql, error) → execute
              ├→ success
              └→ error → LLM fix again (attempt 2)
                        ├→ success
                        └→ give up → return {error: "..."}
```

## 4. Subgraph Nodes (conceptual)

- `generate_sql_node(state)` — input: question + tables_context + few_shot; output: candidate SQL.
- `execute_sql_node(state)` — calls `ISQLSandbox.execute_readonly`; updates state with rows or error.
- `fix_sql_node(state)` — on error, calls LLM with original SQL + error message; increments `retry_count`.
- Edge: `execute_sql_node → success? END : fix_sql_node` until `retry_count >= 2`.

## 5. Prompt Strategy

- Template file: `prompts/nl2sql.md`
- Loaded via `load_prompt("nl2sql", tables_context, few_shot_block)`
- Tables context built from `table_catalog` — see project optimizations (Cat 3) for how `table_catalog` is populated.
- Few-shot block: static question→SQL examples embedded in the prompt.

## 6. Integration with Query Pipeline

The NL2SQL subgraph is currently called **inline** from `application/pipeline/connectors/sandbox.py` — the same file that acts as a query-pipeline connector. This is **dead code duplication** with the dedicated `subgraphs/nl2sql.py` file. Tracked as **FIX-004**:

- Move the three nodes out of `connectors/sandbox.py`.
- Wire `subgraphs/nl2sql.py` into the main LangGraph pipeline.
- `connectors/sandbox.py` should only build the connector adapter, not host subgraph logic.

## 7. Source Files

- `application/pipeline/connectors/sandbox.py` — current inline home of the 3 nodes.
- `application/pipeline/subgraphs/nl2sql.py` — target home, not integrated yet (FIX-004).
- `presentation/http/controllers/sandbox/sandbox_router.py` — `/sandbox/ask` handler.
- `prompts/nl2sql.md` — prompt template.

## 8. Deviations from Constitution

- Complies with hexagonal and pipeline principles except for the FIX-004 dead-code issue above.

---

**End of plan.md**
