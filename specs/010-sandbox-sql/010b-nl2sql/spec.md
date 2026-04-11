# Spec: NL2SQL (LLM → SQL → Execute Retry Loop)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Application (pipeline) + Presentation
**Parent module**: [../spec.md](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Natural-language-to-SQL flow that lets a user ask "give me the 10 jurisdictions with the highest spending in 2025" and receive rows. Uses the LLM to generate SQL against the `cache_*` schema, executes it through the SQL sandbox (`010a-sql-sandbox`), and retries with error context on failure (**self-correction**, max 2 retries). Integrates with the **table catalog** (see module `000-architecture` / table_catalog) for context selection and uses few-shot examples from the `nl2sql.md` prompt.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **NL2SQL** | SQL generation from natural language using an LLM. |
| **Self-correction** | Loop that retries SQL fixing when execute fails (max 2 retries). |
| **Prompt injection detector** | `is_suspicious()` — detects attempts to manipulate the LLM from the user's query. |
| **Table catalog** | `table_catalog` table with vector(1024) + HNSW for picking relevant `cache_*` tables. |
| **Few-shot block** | Canned question→SQL examples injected into the prompt. |

## 3. User Stories

### US-003 (P1) — NL2SQL
**As** a chat user, **I want** to ask "give me the 10 jurisdictions with the highest spending in 2025" and have the system generate + execute the SQL automatically.

### US-004 (P2) — Self-correction in NL2SQL
**As** the system, **when** the generated SQL fails, **I need** to retry with the error as context.

## 4. Functional Requirements

- **FR-007**: The NL2SQL endpoint MUST:
  1. Detect prompt injection via `is_suspicious(question)`
  2. Load table context via `load_prompt("nl2sql", tables_context, few_shot_block)`
  3. Generate SQL via LLM
  4. Execute via `ISQLSandbox.execute_readonly` (owned by `010a-sql-sandbox`)
  5. If it fails: retry with the error as context (max 2 retries)
- **FR-008** (partial): MUST apply rate limiting (SlowAPI: 10/min) to `/api/v1/sandbox/ask`.
- **FR-012**: MUST integrate with `table_catalog` to select relevant `cache_*` tables for the prompt context (vector search + fnmatch fallback as documented in the optimizations plan).

## 5. Success Criteria

- **SC-002**: NL2SQL finishes in **<8 seconds (p95)** with 0-1 retries.
- **SC-004**: Self-correction resolves ≥70% of initial failures.

## 6. Assumptions & Out of Scope

### Assumptions
- The LLM (Bedrock Claude Haiku 4.5) can generate correct PostgreSQL most of the time.
- 2 retries are enough for self-correction in typical cases.
- The `table_catalog` metadata is fresh enough for accurate context.

### Out of scope
- **Writes / DDL** — owned by `010a-sql-sandbox` validation layer.
- **Multi-model NL2SQL** (Cohere/Sonnet) — Haiku only.
- **User-specific fine-tuning** of the prompt.
- **Multi-turn clarification** inside NL2SQL itself (handled at the planner level in the query pipeline).

## 7. Open Questions

- **[RESOLVED]** — The 2-retry cap is intentional; deeper retries tend to thrash on complex schemas. See DEBT-002.

## 8. Tech Debt Discovered

- **[DEBT-002]** — **NL2SQL self-correction limited to 2 retries** — can fail on complex schemas.
- **[FIX-004 PENDING]** — The `generate_sql_node → execute_sql_node → fix_sql_node` subgraph is currently **dead code living inline in `connectors/sandbox.py`**. It should be moved to `application/pipeline/subgraphs/nl2sql.py` and wired as a proper LangGraph subgraph. Tracked as FIX-004.

---

**End of spec.md**
