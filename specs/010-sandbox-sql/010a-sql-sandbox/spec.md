# Spec: SQL Sandbox (Read-only Executor)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Infrastructure + Presentation
**Parent module**: [../spec.md](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Read-only SQL executor over the `cache_*` tables populated by the connectors. Enforces safety via **3-layer validation** (regex forbidden keywords, allowlist over SELECT/WITH on `cache_*` tables, sqlglot AST parsing) and runs queries in a `ThreadPoolExecutor(max_workers=2)` with a DB-level `statement_timeout`. Exposes raw-SQL HTTP endpoints. Consumed by the NL2SQL sub-module (`010b-nl2sql`) and by the query pipeline connectors.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Sandbox** | Read-only environment with table allowlist and SQL validation. |
| **Allowlist** | Only tables with the `cache_` prefix in the `public` schema. |
| **SandboxResult** | Typed return with `columns`, `rows`, `row_count`, `truncated`, `error`. |

## 3. User Stories

### US-001 (P1) — Execute raw SQL
**As** an expert developer, **I want** to send SELECT directly to the sandbox and get rows.

### US-002 (P1) — List available tables
**As** a developer, **I want** to see which `cache_*` tables are available with their columns.

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
- **FR-008**: MUST apply rate limiting (SlowAPI: 10/min) to the endpoints (`/api/v1/sandbox/query`).

## 5. Success Criteria

- **SC-001**: SQL execution responds in **<3 seconds (p95)** including validation.
- **SC-003**: **Zero successful SQL injections** (the 3 layers block them).

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
- **LLM-based SQL generation** — lives in `010b-nl2sql`.

## 7. Open Questions

- **[RESOLVED CL-001]** — Mechanism = **PostgreSQL-level cancel only**, no asyncio cancellation. `execute_readonly` wraps in `run_in_executor()` (`pg_sandbox_adapter.py:242-249`) and sets `SET statement_timeout = {ms}` (`_execute_sync:194`). PG cancels the query at the DB level, but if the post-fetch thread is CPU-bound in Python, there is no thread kill. **Structural debt**: the timeout may not effectively interrupt in all cases (slow DB queries yes; post-result Python loops no).
- **[RESOLVED CL-002]** — SQL error messages exposed to the user are **acceptable**. OpenArg is open source: the schema of `cache_*` tables is public, there is no sensitive information to leak. Sandbox errors help the user correct their SQL with no security risk.
- **[NEEDS CLARIFICATION CL-003]** — The `max_workers=2` of the ThreadPoolExecutor is a known bottleneck under high load.

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Static ThreadPoolExecutor** `max_workers=2` — does not scale with load.
- **[DEBT-003]** — **Exposed SQL error messages** in the response — potential schema info leak.
- **[DEBT-004]** — **No structured logging** of executed SQL for auditing.
- **[DEBT-005]** — **No metrics** for success/failure per table.

---

**End of spec.md**
