# Spec: Table Catalog (NL2SQL Discovery)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Infrastructure (meta-task, cross-cutting)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Vector registry of **semantic metadata for cached tables**, used by the query pipeline to **discover which table to query** given a natural-language question. It is populated automatically via `catalog_enrichment_tasks` (see `002o-catalog-enrichment/`) which uses Bedrock Claude Haiku to generate descriptions + Bedrock Cohere for embeddings.

It was part of **Optimization Category 3 (Mar 2026)** — migration 0019 created the table, auto-enrichment in the collector, context for NL2SQL.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Table catalog** | `table_catalog` table with an entry for each `cache_*` + metadata + embedding. |
| **Metadata** | Dict with display_name, description, domain, subdomain, key_columns, column_types, sample_queries, tags. |
| **NL2SQL context** | Text added to the NL2SQL generator prompt with the list of relevant tables + their schemas. |
| **Catalog discovery** | Vector search over `table_catalog.embedding` to find tables relevant to a question. |

## 3. User Stories

### US-001 (P1) — NL2SQL finds the right table
**As** the NL2SQL system, **when** the user asks "ministry of health spending", **I need** to match against `cache_presupuesto_credito` and not against `cache_staff_hcdn`.

### US-002 (P2) — Auto-enrichment when creating a cache
**As** the system, **when** a connector creates a new `cache_*` table, **I want** to auto-enrich it with metadata + embedding without manual intervention.

### US-003 (P2) — Re-enrichment on demand
**As** an admin, **I want** to re-enrich a table when its schema changed.

## 4. Functional Requirements

- **FR-001**: The `table_catalog` table MUST have: `table_name` (PK), `display_name`, `description`, `domain`, `subdomain`, `key_columns` (array), `column_types` (JSONB), `sample_queries` (array), `tags` (array), `embedding vector(1024)`, timestamps.
- **FR-002**: MUST have an HNSW index over `embedding`.
- **FR-003**: MUST support UPSERT by `table_name`.
- **FR-004**: Auto-enrichment MUST be triggered on create/update of a `cache_*` table.
- **FR-005**: `discover_catalog_hints_for_planner(query_embedding)` MUST return the top-K most similar tables to inject into the planner's context.
- **FR-006**: NL2SQL MUST use the catalog to build its `tables_context`.

## 5. Success Criteria

- **SC-001**: Lookup of top-5 relevant tables in **<100ms (p95)**.
- **SC-002**: ≥95% of existing `cache_*` tables have an entry in `table_catalog`.
- **SC-003**: NL2SQL with catalog discovery answers correctly on ≥80% of common queries (empirical metric, not measured today).

## 6. Assumptions & Out of Scope

### Assumptions
- Cohere Embed v3 is sufficient for metadata matching in Spanish.
- The LLM (Claude Haiku) generates metadata of acceptable quality.
- The catalog does not grow massively (hundreds, not millions).

### Out of scope
- **Dynamic `schema.table`** — `public` schema only.
- **Learning from user feedback** — no improvement loop.
- **Manual editing** of metadata — no UI.
- **Catalog versioning** — updates overwrite.

## 7. Open Questions

- **[NEEDS CLARIFICATION CL-001]** — What happens if a `cache_*` table is renamed? Orphan in `table_catalog`?
- **[NEEDS CLARIFICATION CL-002]** — Is there a mechanism to detect that metadata is stale (e.g., columns changed)?
- **[RESOLVED CL-003]** — **NOT validated**. The LLM generates 3 sample_queries in natural language (prompt `catalog_enrichment.txt:9`), they are parsed to dict in `catalog_enrichment_tasks.py:89` and upserted to `table_catalog` directly (lines 166-204). Without executing against the sandbox. If the LLM generates nonsensical or non-executable queries, they persist as-is. **Concrete debt already captured in `DEBT-003`**.

## 8. Tech Debt Discovered

- **[DEBT-001]** — **No orphan detection** when cache tables are deleted.
- **[DEBT-002]** — **No staleness detection** — metadata can silently go out of date.
- **[DEBT-003]** — **`sample_queries` without validation** — can be invalid SQL.
- **[DEBT-004]** — **`public` schema only** — hardcoded in the task.
- **[DEBT-005]** — **No metrics** of catalog hit rate in NL2SQL.

---

**End of spec.md**
