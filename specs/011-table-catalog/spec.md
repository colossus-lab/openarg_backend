# Spec: Table Catalog (NL2SQL Discovery)

**Type**: Reverse-engineered
**Status**: Draft — being supplanted by [015-catalog-resources](../015-catalog-resources/spec.md)
**Last synced with code**: 2026-04-25
**Hexagonal scope**: Infrastructure (meta-task, cross-cutting)
**Related plan**: [./plan.md](./plan.md)
**Successor**: [015-catalog-resources](../015-catalog-resources/spec.md) (logical catalog, post-WS2)

---

## 1. Context & Purpose

Vector registry of **semantic metadata for cached tables**, used by the query pipeline to **discover which table to query** given a natural-language question. It is populated automatically via `catalog_enrichment_tasks` (see `002o-catalog-enrichment/`) which uses Bedrock Claude Haiku to generate descriptions + Bedrock Cohere for embeddings.

It was part of **Optimization Category 3 (Mar 2026)** — migration 0019 created the table, auto-enrichment in the collector, context for NL2SQL.

### Transition status (Apr 2026)

`table_catalog` is being supplanted by [`catalog_resources`](../015-catalog-resources/spec.md) (WS2). During the transition:
- `table_catalog` continues to serve materialized-table discovery for the existing planner path.
- `catalog_resources` is consulted in addition (`OPENARG_HYBRID_DISCOVERY=1`) or instead (`OPENARG_CATALOG_ONLY=1`) of `table_catalog`.
- After the staging cutover (see `scripts/staging_reset.py`), staging runs in catalog-only mode and `table_catalog` is empty there.
- Production keeps both layers until WS6 cutover criteria are met.

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
- **FR-007**: **Orphan cleanup** — the system MUST periodically remove `table_catalog` rows whose `table_name` no longer exists in `information_schema.tables (table_schema = 'public')`. A Celery beat task named `openarg.cleanup_orphan_catalog_entries` runs once a day on the `ingest` queue and executes a single `DELETE FROM table_catalog WHERE table_name NOT IN (SELECT table_name FROM information_schema.tables WHERE table_schema = 'public')`. The deletion is atomic per-task-run, logs the row count dropped, and is idempotent (re-running it when nothing is orphaned is a no-op). This closes the hole where a rename or drop of a `cache_*` table would leave the vector index returning stale table names that the NL2SQL step then compiles into SQL against non-existent tables (absorbed today by the last-resort fallback, but still a data-quality problem).

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

- **[RESOLVED CL-001]** — ~~Orphan catalog rows accumulate when cache tables are renamed/dropped~~ **FIXED 2026-04-11** via FR-007: a daily `cleanup_orphan_catalog_entries` beat task now DELETEs `table_catalog` rows whose `table_name` is not in `information_schema.tables (public)`. Idempotent, logs the row count, runs on the `ingest` queue alongside the other cleanup tasks. See DEBT-001 below for the historical rationale.
- **[RESOLVED CL-002]** — **No.** `enrich_all_tables` (catalog_enrichment_tasks.py:244-281) only targets tables that have NO catalog entry yet (`WHERE tc.id IS NULL`). There is no periodic re-enrichment, no column-diff check, no `updated_at` staleness filter. The only way to refresh metadata is to call `enrich_single_table` manually per table (which does UPSERT). Note: the collector does emit a `schema_drift_detected` log at `collector_tasks.py:882` when a cached table's columns change, but this log is NOT wired to invalidate the catalog. Tracked as `DEBT-002`. (resolved 2026-04-11 via code inspection)
- **[RESOLVED CL-003]** — **NOT validated**. The LLM generates 3 sample_queries in natural language (prompt `catalog_enrichment.txt:9`), they are parsed to dict in `catalog_enrichment_tasks.py:89` and upserted to `table_catalog` directly (lines 166-204). Without executing against the sandbox. If the LLM generates nonsensical or non-executable queries, they persist as-is. **Concrete debt already captured in `DEBT-003`**.

## 8. Tech Debt Discovered

- **[DEBT-001]** — ~~**No orphan detection** when cache tables are deleted~~ **FIXED 2026-04-11** via FR-007: the daily `cleanup_orphan_catalog_entries` beat task removes rows whose target table no longer exists in `information_schema.tables`. Before this fix, a rename or drop of any `cache_*` table left a stale entry in `table_catalog.embedding` that the vector search would still rank high, causing NL2SQL to compile SQL against non-existent tables. The last-resort fallback of the NL2SQL subgraph (see `010-sandbox-sql/010b-nl2sql/` FR-006) was absorbing the final failure, but the data-quality drift is now closed at the source.
- **[DEBT-002]** — **No staleness detection** — metadata can silently go out of date.
- **[DEBT-003]** — **`sample_queries` without validation** — can be invalid SQL.
- **[DEBT-004]** — **`public` schema only** — hardcoded in the task.
- **[DEBT-005]** — **No metrics** of catalog hit rate in NL2SQL.

---

**End of spec.md**
