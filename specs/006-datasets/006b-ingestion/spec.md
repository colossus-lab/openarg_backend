# Spec: Dataset Ingestion Pipeline

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-12
**Hexagonal scope**: Infrastructure (Workers) + Domain
**Parent module**: [../spec.md](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The ingestion pipeline turns a portal URL into a queryable, semantically searchable dataset. It runs as **three chained Celery workers**: `scrape_catalog → collect_dataset → index_dataset_embedding`, plus `s3_tasks` for large-file storage. Owns the lifecycle of `cached_datasets`, the dynamic `cache_*` tables, and the `dataset_chunks` embedded fragments.

Important architectural clarification: the collector no longer writes directly into a single shared table per logical dataset group. The **primary materialization path is per-resource staging** (`..._r<id>`), followed by **best-effort consolidation** into schema-compatible group tables (`..._g<hash>`). This change was introduced to maximize coverage and avoid dropping parseable resources due to schema drift, table caps, or sibling incompatibilities.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Scrape** | Download a portal's catalog and create/update records in `datasets`. |
| **Collect** | Download a dataset's physical file, parse it, and cache it in PG. |
| **Index embedding** | Generate 5 chunks + embeddings and persist them in `dataset_chunks`. |
| **DatasetChunk** | Embedded fragment of a dataset (5 per dataset: main/columns/contextual/+2). |
| **CachedData** | Lifecycle state of the physically cached dataset (pending/downloading/ready/error/permanently_failed). |
| **Resource staging table** | Primary cache table for a single dataset resource. Named as `cache_*_r<dataset-id-fragment>`. |
| **Consolidated group table** | Best-effort merged table for multiple staged resources that share a schema. Named as `cache_*_g<schema-hash>`. |
| **Format duplicate alias** | A lower-priority resource variant (for example XLSX vs CSV for the same URL stem) that is resolved as a SQL view over the canonical staged table instead of being re-collected independently. |
| **Schema drift** | Upstream schema change detected when a re-collect produces different columns/types than the current `cache_*` table. |
| **SQL-safe name** | Sanitized identifier derived from the original dataset column name, safe to use as a PG column. |

## 3. User Stories

### US-003 (P2) — Trigger a manual scrape
**As** an admin, **I want** to trigger a specific scrape when a portal publishes new data.

### US-004 (P1) — Auto indexing after collect
**As** the system, **when** a dataset is downloaded and cached, **I need** to auto-trigger the embedding indexing.

### US-005 (P2) — Bulk reindex
**As** an operator, **I need** to reindex all embeddings when we change the model.

## 4. Functional Requirements

- **FR-003**: The worker `scrape_catalog(portal)` MUST fetch the portal's CKAN catalog and upsert all its datasets.
- **FR-004**: The worker `collect_dataset(dataset_id)` MUST download the file, parse it with pandas, cache it in a SQL-visible `cache_*` table, and upsert `cached_datasets`.
- **FR-005**: The worker `index_dataset_embedding(dataset_id)` MUST generate **5 chunks** per dataset with a 1024-dim embedding via Bedrock Cohere. They include at least: main (title + description), columns (names), contextual (natural-language narrative with use-case + keywords). For the remaining 2 see `scraper_tasks.py:592-695`.
- **FR-006**: Collect MUST trigger index when it finishes successfully, including successful re-collects that materially change the cached representation (table, rows, columns, or sampled note).
- **FR-007**: `reindex_all_embeddings` MUST allow reprocessing all datasets.
- **FR-009**: Large storage MUST go to S3 (tasks in the `s3` queue).
- **FR-010**: The collector MUST sanitize dataset column names to SQL-safe identifiers via `_to_sql_safe()` before `df.to_sql()`.
- **FR-011**: The collector MUST detect **schema drift** via `_detect_schema_drift()` on re-collect. When the incoming DataFrame schema no longer matches an existing cache table, the worker MUST route the resource into a schema-compatible table variant or schedule it for retry; it MUST NOT silently drop parseable data.
- **FR-012**: The collector MUST treat per-resource staging tables as the primary write target and use consolidation as a follow-up best-effort step.
- **FR-013**: The collector MUST serialize nested JSON-like cell values (`dict`/`list`/`tuple`/`set`) before SQL insertion so structured payloads do not fail with adapter errors.
- **FR-014**: Large-group format duplicates MAY reuse an already-collected canonical resource, but the reused dataset MUST remain SQL-addressable through an alias representation instead of being marked cached without a table.
- **FR-015**: Legacy `schema_mismatch` rows MUST be moved back into the retry path automatically during bulk collection so they do not become dead-end states.
- **FR-016**: The collector MUST enforce single-flight semantics per `dataset_id` so concurrent duplicate `collect_dataset` runs do not download and materialize the same resource in parallel.
- **FR-017**: `bulk_collect_all` MUST behave as a singleton orchestration pass. If another run is already in progress, the new invocation MUST exit without dispatching duplicate work.
- **FR-018**: Deterministic oversized/empty-file ingestion errors (for example `file_too_large`, empty CSV payloads, and unsupported Excel payloads) MUST bypass Celery retries and transition directly to a terminal collector error state.
- **FR-019**: ZIP datasets MUST parse one nested ZIP layer when the outer archive wraps the real payload (for example nested GeoJSON or shapefile bundles) instead of failing immediately as `zip_no_parseable_file`.
- **FR-020**: Sandbox-facing collector consumers MUST normalize known legacy cache table aliases (`cache_series_tiempo_*`, `cache_bcra_principales_variables`, `cache_presupuesto_nacional`) toward canonical current tables or patterns before generating/executing SQL.

## 5. Success Criteria

- **SC-002**: Scrape of a medium portal (~500 datasets) completes in **<5 minutes**.
- **SC-003**: Collect + index of a typical dataset (10K rows) completes in **<2 minutes**.
- **SC-004**: ≥95% of scraped datasets end with `cached_datasets.status="ready"`.
- **SC-005**: `cached_datasets.status="ready"` MUST always imply that the dataset is SQL-addressable either through a physical table or a collector-managed alias view.
- **SC-006**: A duplicate dispatch storm for the same `dataset_id` MUST result in at most one active collector execution; concurrent followers exit as `already_collecting`.
- **SC-007**: Overlapping `bulk_collect_all` invocations MUST not dispatch overlapping waves of collector tasks.
- **SC-008**: A ZIP containing a single nested ZIP with a supported payload MUST be collected successfully without manual unpacking.
- **SC-009**: Known legacy sandbox table aliases MUST no longer produce direct `Table missing` failures when a canonical replacement exists.

## 6. Assumptions & Out of Scope

### Assumptions
- Bedrock Cohere Embed is available for embedding.
- CKAN portals return metadata in the standard format.
- S3 is available for large files.
- The 5-chunk strategy (main/columns/contextual/+2) is sufficient.

### Out of scope
- **Dataset versioning** — updates overwrite, no change audit.
- **Dataset diffing** between snapshots.
- **More sophisticated chunking** (sliding window, overlapping chunks).
- **Multi-model embeddings** (Cohere Embed v3 only).

## 7. Open Questions

- **[RESOLVED CL-001]** — **Important correction**: there are **5 chunks** per dataset, not 3 (error in the initial reverse-engineering). The "contextual chunk" (Chunk 3 of 5) lives in `scraper_tasks.py:662-677` and contains a **natural-language narrative**: *"Para consultar datos sobre {title.lower()} en Argentina, existe el dataset '{title}' publicado por {organization}. Disponible en formato {format} desde {portal}. Detalle: {description[:500]}. Los datos están cacheados localmente ({row_count} filas) para consulta SQL directa. Palabras clave: {tags}"*. Designed to match "how-to" / "how to analyze X" queries.
- **[RESOLVED CL-002]** — **S3 key format**: `datasets/{portal}/{dataset_id}/{filename}`. See `collector_tasks.py:496` and `s3_tasks.py:47`. **Collision-safe** because `dataset_id` is a globally unique UUID and the `portal` adds an additional prefix. **Caveat**: if the same dataset is re-collected with a different file source, it overwrites the existing key (intentional cache-busting, no versioning or timestamp).
- **[RESOLVED CL-003]** — `reindex_all_embeddings` is **manual only**. There is no beat schedule. It is only triggered via `POST /api/v1/admin/tasks/reindex_all_embeddings/run` from the admin orchestrator. See `embedding_tasks.py:36-64` (task) + `admin/tasks_router.py:207-213` (registry entry).

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Hardcoded 5-chunk strategy** (`scraper_tasks.py:592-695`) — not configurable, no per-dataset override mechanism.
- **[DEBT-002]** — **Undocumented S3 key naming convention** — possible collisions.
- **[DEBT-004]** — **`sample_rows` with no explicit size limit** — may inflate the "contextual" chunk.
- **[DEBT-005]** — **No metrics** for embedding failures.
- **[DEBT-006]** — **No sophisticated auto-retry** on collect (only basic Celery retry).
- **[DEBT-007]** — **Spec drift risk**: the ingestion implementation now relies on resource staging + consolidation, but downstream modules still sometimes assume a single canonical cache table per dataset.
- **[DEBT-008]** — **Bulk orchestration still shares the same worker queue as heavy data collection**. Singleton locking prevents overlap, but `bulk_collect_all` still competes with long-running `collect_dataset` tasks for worker slots.
- **[DEBT-009]** — **Some semantic routes still depend on broad budget hints instead of domain-specific cache tables**. `coparticipacion` is now routed safely to generic budget tables, but a dedicated ingest or catalog entry is still missing.

---

**End of spec.md**
