# Spec: Catalog Enrichment (LLM-Generated Metadata)

**Type**: Reverse-engineered
**Status**: Draft
**Hexagonal scope**: Infrastructure (task only, cross-cutting)
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

A meta task that uses **Bedrock Claude Haiku 4.5** to generate semantic metadata (display_name, description, domain, tags, column_types, sample_queries) for cached tables in the system, and **Bedrock Cohere Embed** to compute 1024-dim embeddings. The results are persisted in the `table_catalog` table with an HNSW index, feeding the sandbox's **NL2SQL matching**.

This is the task that connects the snapshot of any connector to the vector catalog of queryable tables.

## 2. User Stories

- **US-001 (P1)**: As the NL2SQL system, I want to find the correct table for a natural-language question via vector search over enriched metadata.
- **US-002 (P2)**: As an admin, I want to enrich a new table on-demand from the admin panel.

## 3. Functional Requirements

- **FR-001**: MUST generate semantic metadata for any cached table using Bedrock Claude Haiku.
- **FR-002**: MUST generate a 1024-dim embedding of the combined text (title + description + tags) using Bedrock Cohere.
- **FR-003**: MUST persist to `table_catalog` with an HNSW index on `embedding`.
- **FR-004**: MUST handle a fallback when the LLM fails (generate basic programmatic metadata).
- **FR-005**: MUST include generated sample_queries for NL2SQL few-shot examples.

## 4. Success Criteria

- **SC-001**: Enrichment of 1 complete table in **<10 seconds** (LLM + embedding).
- **SC-002**: ≥95% of sandbox tables have an entry in `table_catalog`.
- **SC-003**: NL2SQL matching via vector search resolves correctly for ≥80% of common queries.

## 5. Open Questions

- **[RESOLVED CL-001]** — **On-demand only**. There is no auto-trigger from `collector_tasks.py` nor an event hook. It is only invoked manually: `enrich_single_table(table_name)` (one table) or `enrich_all_tables(batch_size=50)` (all non-enriched tables). Queue: `embedding` (`celery/app.py:121-122`). **Note**: this contradicts the claim in `MEMORY.md` about "auto-enrichment in collector" (Cat 3 optimizations Mar 2026) — it seems that optimization was not completed or was implemented elsewhere.
- **[NEEDS CLARIFICATION CL-002]** — The programmatic fallback on LLM failure — when are those entries reviewed?
- **[NEEDS CLARIFICATION CL-003]** — Does the task support re-enrichment of existing tables (to refresh stale metadata)?

## 6. Tech Debt Discovered

- **[DEBT-001]** — **Fallback metadata** generates basic strings without human review. Low-quality metadata may reach production.
- **[DEBT-002]** — **Embeddings only via Bedrock Cohere** — no fallback if the service is unavailable.
- **[DEBT-003]** — **Does not handle `schema.table` notation** — assumes the public schema.

---

**End of spec.md**
