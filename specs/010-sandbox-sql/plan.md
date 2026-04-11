# Plan: SQL Sandbox + NL2SQL (Index)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Purpose

Top-level plan for the `010-sandbox-sql` module. The module is split into two independent sub-plans; this file only describes the split and the shared context.

## 2. Sub-plan Map

| Sub-module | Plan | Scope |
|---|---|---|
| 010a-sql-sandbox | [010a-sql-sandbox/plan.md](./010a-sql-sandbox/plan.md) | `ISQLSandbox`, `PgSandboxAdapter`, 3-layer validation, ThreadPoolExecutor, `/query` + `/tables` endpoints. |
| 010b-nl2sql | [010b-nl2sql/plan.md](./010b-nl2sql/plan.md) | LLM generate/execute/fix subgraph, table catalog context, `nl2sql.md` prompt, `/ask` endpoint, FIX-004 migration target. |

## 3. Shared Hexagonal Mapping

| Layer | Component | Owner |
|---|---|---|
| Domain Port | `ISQLSandbox` | 010a-sql-sandbox |
| Infrastructure Adapter | `PgSandboxAdapter`, `table_validation.py` | 010a-sql-sandbox |
| Application (subgraph) | `generate_sql_node`, `execute_sql_node`, `fix_sql_node` | 010b-nl2sql |
| Presentation | `sandbox_router.py` — `/query` and `/tables` | 010a-sql-sandbox |
| Presentation | `sandbox_router.py` — `/ask` | 010b-nl2sql |
| Prompt | `prompts/nl2sql.md` | 010b-nl2sql |

## 4. Shared Dependencies

- **PostgreSQL** — the `cache_*` tables and `table_catalog` live here.
- **sqlglot** — AST-level validation in 010a.
- **Bedrock Claude Haiku 4.5** — LLM backbone for 010b.
- **SlowAPI** — 10/min rate limiting on both `/query` and `/ask`.

## 5. Deviations from Constitution

- Complies with security and hexagonal principles. The ThreadPoolExecutor is an accepted exception (sync sqlalchemy engine) in 010a.
- FIX-004 tracked in 010b: NL2SQL subgraph currently dead code inline in `connectors/sandbox.py`.

---

**End of plan.md (index)**
