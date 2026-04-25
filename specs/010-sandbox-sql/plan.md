# Plan: SQL Sandbox + NL2SQL (Index)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-25

## WS3 hybrid discovery addendum (2026-04-25)

`discover_catalog_hints_for_planner` (in `application/pipeline/connectors/sandbox.py`) is now hybrid-aware:

- When `OPENARG_HYBRID_DISCOVERY=1`, it **appends** matches from `catalog_resources` to the existing `table_catalog` block (`_hybrid_logical_hints` helper).
- When `OPENARG_CATALOG_ONLY=1`, it **bypasses** the legacy `table_catalog` SQL entirely and serves discovery from `catalog_resources` alone. Used in the staging cutover after `scripts/staging_reset.py`.
- When neither flag is set, behaviour is unchanged from 2026-04-13.

The new layer surfaces `materialization_status` (PENDING / READY / LIVE_API / NON_TABULAR) and `resource_kind` to the planner so it can route to a connector endpoint instead of demanding a materialized table that doesn't exist.

See [015-catalog-resources](../015-catalog-resources/spec.md) for the full WS2/WS3/WS4 spec and [collector_plan.md](../../../collector_plan.md) WS3 for the original design.

---

## 1. Purpose

Top-level plan for the `010-sandbox-sql` module. The module is split into two independent sub-plans; this file only describes the split and the shared context.

## 2. Sub-plan Map

| Sub-module | Plan | Scope |
|---|---|---|
| 010a-sql-sandbox | [010a-sql-sandbox/plan.md](./010a-sql-sandbox/plan.md) | `ISQLSandbox`, `PgSandboxAdapter`, 3-layer validation, ThreadPoolExecutor, `/query` + `/tables` endpoints. |
| 010b-nl2sql | [010b-nl2sql/plan.md](./010b-nl2sql/plan.md) | LLM generate/execute/fix subgraph, table catalog context, `nl2sql.md` prompt, `/ask` endpoint. |

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
- FIX-004 is no longer pending: the inline NL2SQL loop was removed from `connectors/sandbox.py`, and 010b now documents the integrated subgraph as the live execution path.

---

**End of plan.md (index)**
