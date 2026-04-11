# Spec: SQL Sandbox + NL2SQL (Index)

**Type**: Reverse-engineered (top-level index)
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Application + Infrastructure + Presentation
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Module that allows **executing read-only SQL** over the `cache_*` tables populated by the connectors, plus a **natural-language-to-SQL** flow with LLM self-correction. It is the channel through which the query pipeline accesses structured cached data for aggregate analysis.

This module has been split into two sub-modules:

| Sub-module | Scope |
|---|---|
| [010a-sql-sandbox](./010a-sql-sandbox/spec.md) | Read-only executor, 3-layer SQL validation (regex + allowlist + sqlglot AST), `ThreadPoolExecutor(max_workers=2)`, `/api/v1/sandbox/query` and `/api/v1/sandbox/tables`. |
| [010b-nl2sql](./010b-nl2sql/spec.md) | LLM → SQL → execute retry loop, `generate_sql_node → execute_sql_node → fix_sql_node` subgraph (FIX-004 pending), table catalog + few-shot integration, `/api/v1/sandbox/ask`. |

## 2. Sub-module Map

```
010-sandbox-sql/
├── spec.md                 # this file
├── plan.md
├── 010a-sql-sandbox/       # executor + validation (safety kernel)
│   ├── spec.md
│   └── plan.md
└── 010b-nl2sql/            # LLM-driven SQL generation + self-correction
    ├── spec.md
    └── plan.md
```

## 3. Cross-references

- **Query pipeline** that consumes NL2SQL: see modules `008-query-pipeline` / `009-agents`.
- **Table catalog** used for prompt context: see `000-architecture` and project optimizations Cat 3.
- **Dataset ingestion** that populates `cache_*`: see `006-datasets/006b-ingestion`.

## 4. Spec ID Registry

Full definitions live in the sub-module specs. This table only maps IDs → owner.

| ID | Owner |
|---|---|
| FR-001, FR-002, FR-003, FR-004, FR-005, FR-006 | 010a-sql-sandbox |
| FR-007, FR-012 | 010b-nl2sql |
| FR-008 | split (010a owns `/query`, 010b owns `/ask`) |
| SC-001, SC-003 | 010a-sql-sandbox |
| SC-002, SC-004 | 010b-nl2sql |
| CL-001, CL-002 (RESOLVED) | 010a-sql-sandbox |
| CL-003 (NEEDS CLARIFICATION) | 010a-sql-sandbox |
| DEBT-001, DEBT-003, DEBT-004, DEBT-005 | 010a-sql-sandbox |
| DEBT-002, FIX-004 | 010b-nl2sql |

---

**End of spec.md (index)**
