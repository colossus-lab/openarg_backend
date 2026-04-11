# Spec: SQL Sandbox + NL2SQL

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Infrastructure + Presentation
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Module that allows **executing read-only SQL** over the `cache_*` tables populated by the connectors. Includes **3-layer validation** (regex, allowlist, sqlglot AST) to prevent SQL injection and sandbox escape. Also includes an **NL2SQL** endpoint that uses the LLM to generate SQL from a natural-language question with **self-correction** (max 2 retries).

It is the channel through which the query pipeline accesses structured cached data (exchange rates, budget, staff, etc.) for aggregate analysis.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Sandbox** | Read-only environment with table allowlist and SQL validation. |
| **NL2SQL** | SQL generation from natural language using an LLM. |
| **Self-correction** | Loop that retries SQL fixing when execute fails (max 2 retries). |
| **Prompt injection detector** | `is_suspicious()` — detects attempts to manipulate the LLM from the user's query. |
| **Allowlist** | Only tables with the `cache_` prefix in the `public` schema. |

## 3. User Stories

### US-001 (P1) — Execute raw SQL
**As** an expert developer, **I want** to send SELECT directly to the sandbox and get rows.

### US-002 (P1) — List available tables
**As** a developer, **I want** to see which `cache_*` tables are available with their columns.

### US-003 (P1) — NL2SQL
**As** a chat user, **I want** to ask "give me the 10 jurisdictions with the highest spending in 2025" and have the system generate + execute the SQL automatically.

### US-004 (P2) — Self-correction in NL2SQL
**As** the system, **when** the generated SQL fails, **I need** to retry with the error as context.

## 4. Functional Requirements

- **FR-001**: MUST expose the `ISQLSandbox` port with: `execute_readonly`, `list_cached_tables`, `get_column_types`.
- **FR-002**: `execute_readonly` MUST validate SQL in **3 layers**:
  1. Regex forbidden keywords (INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, etc.)
  2. Allowlist: only SELECT/WITH, only FROM/JOIN over `cache_*` tables in the `public` schema
  3. sqlglot AST parsing: reject CTEs with DML/DDL, validate table refs
- **FR-003**: MUST execute in a `ThreadPoolExecutor(max_workers=2)` with a default 10s `statement_timeout`.
- **FR-004**: MUST truncate results to a **maximum of 1000 rows** (`MAX_ROWS`).
- **FR-005**: MUST return `SandboxResult(columns, rows, row_count, truncated, error)`.
- **FR-006**: `list_cached_tables` MUST return `CachedTableInfo(table_name, dataset_id, row_count, columns)` for all `cache_*`.
- **FR-007**: The NL2SQL endpoint MUST:
  1. Detect prompt injection via `is_suspicious(question)`
  2. Load table context via `load_prompt("nl2sql", tables_context, few_shot_block)`
  3. Generate SQL via LLM
  4. Execute and return rows
  5. If it fails: retry with the error as context (max 2 retries)
- **FR-008**: MUST apply rate limiting (SlowAPI: 10/min) to the endpoints.

## 5. Success Criteria

- **SC-001**: SQL execution responds in **<3 seconds (p95)** including validation.
- **SC-002**: NL2SQL finishes in **<8 seconds (p95)** with 0-1 retries.
- **SC-003**: **Zero successful SQL injections** (the 3 layers block them).
- **SC-004**: Self-correction resolves ≥70% of initial failures.

## 6. Assumptions & Out of Scope

### Assumptions
- sqlglot parses the PostgreSQL dialect correctly.
- `statement_timeout` is respected at the DB level.
- Users do not need writes (read-only is sufficient).

### Out of scope
- **Writes** — reads only.
- **DDL** — creating/altering tables is not allowed.
- **Tables outside the `public` schema** — blocked.
- **Tables without the `cache_` prefix** — blocked.
- Explicit user **transactions**.

## 7. Open Questions

- **[RESOLVED CL-001]** — Mechanism = **PostgreSQL-level cancel only**, no asyncio cancellation. `execute_readonly` wraps in `run_in_executor()` (`pg_sandbox_adapter.py:242-249`) and sets `SET statement_timeout = {ms}` (`_execute_sync:194`). PG cancels the query at the DB level, but if the post-fetch thread is CPU-bound in Python, there is no thread kill. **Structural debt**: the timeout may not effectively interrupt in all cases (slow DB queries yes; post-result Python loops no).
- **[RESOLVED CL-002]** — SQL error messages exposed to the user are **acceptable**. OpenArg is open source: the schema of `cache_*` tables is public, there is no sensitive information to leak. Sandbox errors help the user correct their SQL with no security risk.
- **[NEEDS CLARIFICATION CL-003]** — The `max_workers=2` of the ThreadPoolExecutor is a known bottleneck under high load.

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Static ThreadPoolExecutor** `max_workers=2` — does not scale with load.
- **[DEBT-002]** — **NL2SQL self-correction limited to 2 retries** — can fail on complex schemas.
- **[DEBT-003]** — **Exposed SQL error messages** in the response — potential schema info leak.
- **[DEBT-004]** — **No structured logging** of executed SQL for auditing.
- **[DEBT-005]** — **No metrics** for success/failure per table.

---

**End of spec.md**
