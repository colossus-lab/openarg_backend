# Spec: Dataset Ingestion Pipeline

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Infrastructure (Workers) + Domain
**Parent module**: [../spec.md](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The ingestion pipeline turns a portal URL into a queryable, semantically searchable dataset. It runs as **three chained Celery workers**: `scrape_catalog → collect_dataset → index_dataset_embedding`, plus `s3_tasks` for large-file storage. Owns the lifecycle of `cached_datasets`, the dynamic `cache_*` tables, and the `dataset_chunks` embedded fragments.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Scrape** | Download a portal's catalog and create/update records in `datasets`. |
| **Collect** | Download a dataset's physical file, parse it, and cache it in PG. |
| **Index embedding** | Generate 5 chunks + embeddings and persist them in `dataset_chunks`. |
| **DatasetChunk** | Embedded fragment of a dataset (5 per dataset: main/columns/contextual/+2). |
| **CachedData** | Lifecycle state of the physically cached dataset (pending/downloading/ready/error/permanently_failed). |
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
- **FR-004**: The worker `collect_dataset(dataset_id)` MUST download the file, parse it with pandas, cache it in a `cache_*` table, and upsert `cached_datasets`.
- **FR-005**: The worker `index_dataset_embedding(dataset_id)` MUST generate **5 chunks** per dataset with a 1024-dim embedding via Bedrock Cohere. They include at least: main (title + description), columns (names), contextual (natural-language narrative with use-case + keywords). For the remaining 2 see `scraper_tasks.py:592-695`.
- **FR-006**: Collect MUST trigger index when it finishes successfully.
- **FR-007**: `reindex_all_embeddings` MUST allow reprocessing all datasets.
- **FR-009**: Large storage MUST go to S3 (tasks in the `s3` queue).
- **FR-010**: The collector MUST sanitize dataset column names to SQL-safe identifiers via `_to_sql_safe()` before `df.to_sql()`.
- **FR-011**: The collector MUST detect **schema drift** via `_detect_schema_drift()` on re-collect — when the incoming DataFrame schema no longer matches the existing `cache_*` table, the worker MUST either drop+recreate or mark the cached row as needing manual intervention (see plan).

## 5. Success Criteria

- **SC-002**: Scrape of a medium portal (~500 datasets) completes in **<5 minutes**.
- **SC-003**: Collect + index of a typical dataset (10K rows) completes in **<2 minutes**.
- **SC-004**: ≥95% of scraped datasets end with `cached_datasets.status="ready"`.

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

---

**End of spec.md**
