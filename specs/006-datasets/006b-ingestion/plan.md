# Plan: Dataset Ingestion Pipeline (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Parent plan**: [../plan.md](../plan.md)
**Last synced with code**: 2026-04-13

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Entity | `DatasetChunk` | `domain/entities/dataset/` |
| Infrastructure LLM | `BedrockEmbeddingAdapter` | `infrastructure/adapters/llm/bedrock_embedding_adapter.py` |
| Workers — scraper | `scrape_catalog`, chunk generators | `infrastructure/celery/tasks/scraper_tasks.py` |
| Workers — collector | `collect_dataset`, `_to_sql_safe`, `_detect_schema_drift` | `infrastructure/celery/tasks/collector_tasks.py` |
| Workers — embedding | `index_dataset_embedding`, `reindex_all_embeddings` | `infrastructure/celery/tasks/embedding_tasks.py` |
| Workers — S3 | `upload_to_s3` | `infrastructure/celery/tasks/s3_tasks.py` |

## 2. Entity

```python
@dataclass
class DatasetChunk:
    id: UUID
    dataset_id: UUID
    content: str           # chunk text
    embedding: list[float] # vector(1024)
    chunk_type: str        # "main" | "columns" | "contextual" | ...
    created_at: datetime
```

## 3. Celery Task Pipeline

```
scrape_catalog(portal)  [queue: scraper, concurrency 2]
    ↓ (per new/modified dataset)
index_dataset_embedding(dataset_id)  [queue: embedding, concurrency 8]
    ↓ (for datasets with tabular format)
collect_dataset(dataset_id)  [queue: collector, concurrency 4]
    ↓
(on success)
index_dataset_embedding(dataset_id)  [re-trigger]
    ↓ (large files)
upload_to_s3(dataset_id)  [queue: s3, concurrency 2]
```

## 4. Embedding Strategy (5 chunks per dataset)

The canonical code lives in `scraper_tasks.py:592-695`.

Known chunks (3 of 5 documented):

- **Chunk 1 — main**: `{title} — {description}`
- **Chunk 2 — columns**: joined column names
- **Chunk 3 — contextual** (`scraper_tasks.py:662-677`): natural-language narrative:

```python
context_parts = [
    f"Para consultar datos sobre {row.title.lower()} en Argentina,",
    f"existe el dataset '{row.title}' publicado por {row.organization or 'el gobierno'}.",
    f"Disponible en formato {row.format or 'CSV'} desde {portal_name}.",
]
if row.description and len(row.description) > 20:
    context_parts.append(f"Detalle: {row.description[:500]}")
if row.is_cached:
    context_parts.append(
        f"Los datos están cacheados localmente ({row.row_count or '?'} filas) "
        f"para consulta SQL directa."
    )
if row.tags:
    context_parts.append(f"Palabras clave: {row.tags}")
chunks.append("\n".join(context_parts))
```

- **Chunks 4-5**: TBD — verify exact breakdown in `scraper_tasks.py:592-695`.

For each chunk: calls Bedrock Cohere Embed v3 with `input_type="search_document"`, persists in `dataset_chunks`.

## 5. Collector Specifics

### 5.1 `_to_sql_safe(column_name)`

Sanitizes dataset column names before `df.to_sql()` so that arbitrary upstream headers (spaces, accents, symbols) become valid PG identifiers. Lives in `collector_tasks.py`.

### 5.2 `_detect_schema_drift(dataset_id, new_df)`

Detects when a re-collect produces a schema that no longer matches the existing `cache_*` table. Compares current PG table columns/types against the incoming DataFrame schema. Triggered on every re-collect before the `to_sql(if_exists="replace" | "append")` path. On drift:
- Log the delta (added/removed/retyped columns).
- Drop & recreate the `cache_*` table with the new schema (current behavior), updating `cached_datasets.columns_json`.
- Re-trigger `index_dataset_embedding` because the columns chunk is now stale.

### 5.3 Single-flight locking

- `collect_dataset(dataset_id)` now acquires a PostgreSQL advisory lock derived from `dataset_id` before any network or SQL write work starts.
- If the lock is already held, the duplicate invocation exits immediately with `status="already_collecting"`.
- `bulk_collect_all()` now acquires a dedicated advisory lock before running reconciliation / recycle / dispatch logic. Overlapping runs exit as `status="skipped_already_running"` instead of dispatching duplicate collector waves.
- `bulk_collect_all()` now also behaves as a bounded convergence loop: if a pass still sees eligible uncached work, deferred work, or inflight collector activity, it re-enqueues itself after a delay and only stops on convergence or after hitting the configured max chain depth.
- The convergence loop now short-circuits the expensive recount path when the pass already has enough evidence to continue, and its soft/hard Celery limits are configurable (`OPENARG_BULK_COLLECT_SOFT_TIME_LIMIT`, `OPENARG_BULK_COLLECT_TIME_LIMIT`) so rebuild waves do not die on a fixed 300s edge.

### 5.4 Deterministic failure short-circuit

- Some ingestion errors are now treated as terminal on the first failure instead of burning the retry budget:
  - `file_too_large`
  - empty CSV payloads (`No columns to parse from file`)
  - unsupported/invalid Excel payloads where pandas cannot determine an engine
- Goal: reduce retry churn and free collector slots for genuinely recoverable failures.

### 5.5 Nested ZIP handling

- The collector now parses one nested ZIP layer before giving up on `zip_no_parseable_file`.
- This specifically covers portals that publish bundles like `dataset.zip -> archivo_geojson.zip` or `dataset.zip -> archivo_shp.zip`.
- The nested archive inherits the same row caps and schema-routing logic as direct ZIP members.

### 5.6 ZIP diagnostics and document-bundle classification

- Before parsing ZIP members, the collector now logs a structured summary:
  - file count
  - directory count
  - max directory depth
  - extension histogram
  - parseable vs documentary member counts
  - sample member names
- If a ZIP contains only documentary payloads (`.pdf`, office docs, images) and no parseable/tabular members, the collector short-circuits to `zip_document_bundle` instead of lumping it under `zip_no_parseable_file`.

### 5.7 Sandbox compatibility normalization

- Query-planner hints and NL2SQL execution now normalize several stale cache names seen in staging logs:
  - `cache_series_tiempo_*` -> `cache_series_*`
  - `cache_bcra_principales_variables` -> `cache_bcra_cotizaciones`
  - `cache_presupuesto_nacional` -> latest matching `cache_presupuesto_*`
- `coparticipacion` no longer routes to a non-existent dedicated cache table; it now points to generic budget tables plus table notes so NL2SQL searches the correct domain without inventing a table name.

### 5.8 Nested geospatial bundles and pipe-delimited CSVs

- Some remaining staging ZIP failures are not malformed archives but mixed bundles:
  - outer ZIPs that contain multiple inner geospatial variants (`geojson`, `shp`, `kml`)
  - nested CSV bundles such as SEPA / Precios Claros, where members use UTF-8 BOM headers and `|` as delimiter
- The collector now prioritizes nested geospatial ZIP/KMZ members deterministically:
  - nested GeoJSON/JSON first
  - then shapefiles
  - then KML/KMZ
- The ZIP parser now supports KML payloads via Fiona-backed extraction.
- CSV dialect detection now recognizes `utf-8-sig`, `|`, and tab delimiters in addition to the previous `,` / `;`.

### 5.9 Multi-member ZIP traversal

- SEPA-style archives still exposed a second issue after delimiter detection: the collector stopped after the first parseable inner ZIP member.
- The collector now keeps traversing parseable members inside the same outer ZIP and aggregates rows by target table when the routed schema stays compatible.
- This keeps the current single-dataset materialization model, while still allowing commercial bundles to append repeated same-schema members across many nested ZIPs.
- Remaining follow-up: when one commercial bundle contains several different schemas (`comercio.csv`, `sucursales.csv`, `productos.csv`), keep improving the operator-facing metadata so every sibling table is easier to inspect from admin/status surfaces.

### 5.10 Partial progress visibility for sibling tables

- Multi-table bundles such as SEPA can run for long enough that operators need partial visibility before the final `ready` transition.
- The collector now writes incremental `cached_datasets` progress for each sibling table as soon as that table is materialized:
  - `status='downloading'`
  - `row_count`
  - `columns_json`
  - `size_bytes`
  - `s3_key`
- This keeps the final lifecycle unchanged while making in-flight runs observable from the DB and admin tooling.

### 5.11 Exact schema routing for append-mode bundles

- Staging exposed one more collector edge case after partial progress landed: a long SEPA run could still hit `_to_sql_safe()` drop/recreate retries when a later member had a subset/superset of columns for an existing sibling table.
- The collector now treats append compatibility as an exact normalized column-set match, not subset/superset compatibility.
- The routing check now runs on the same sanitized column names that `_to_sql_safe()` writes, so schema decisions and SQL inserts stay aligned.
- Effect: later members with a slightly different shape are routed into their own schema-variant table early, preserving already materialized sibling tables and their partial progress instead of dropping them mid-run.
- Remaining debt: staging still shows SEPA sibling-table oscillation in some long runs, so the next pass should promote schema routing to a stable per-bundle signature registry instead of reusing the mutable “current table” cursor across members.

## 6. `cached_datasets` Lifecycle

```
pending → downloading → ready
                    ↘ error → (retry up to N) → permanently_failed
                    ↘ permanently_failed   # deterministic non-retryable failures
```

- Row created on first `collect_dataset` dispatch.
- `status` transitions owned exclusively by `collector_tasks.py`.
- `table_name` filled when the `cache_*` PG table is created.
- `s3_key` filled when `upload_to_s3` completes.
- `retry_count` incremented on soft/hard failures (see also module `003-resilience`).

## 7. S3 Storage

- Queue: `s3`, concurrency 2.
- Key format: `datasets/{portal}/{dataset_id}/{filename}` (see `collector_tasks.py:496`, `s3_tasks.py:47`).
- Collision-safe by UUID, but re-collects overwrite (no versioning).

## 8. Persistence Tables

See `000-architecture/plan.md` section 6 for the complete list. Key tables owned by this sub-module:

- `dataset_chunks` (HNSW index over embedding)
- `cached_datasets` (UNIQUE `table_name`)
- `cache_*` (dynamic, created by workers)

## 9. External Dependencies

- **AWS Bedrock Cohere Embed Multilingual v3** (embeddings)
- **S3** — large file storage
- **PostgreSQL** — chunks + cache tables
- **Celery (Redis broker)** — task queues
- **CKAN portals** — sources of metadata and files

## 10. Source Files

- `domain/entities/dataset/` (DatasetChunk)
- `infrastructure/adapters/llm/bedrock_embedding_adapter.py`
- `infrastructure/celery/tasks/scraper_tasks.py`
- `infrastructure/celery/tasks/collector_tasks.py`
- `infrastructure/celery/tasks/embedding_tasks.py`
- `infrastructure/celery/tasks/s3_tasks.py`

## 11. Deviations from Constitution

- **Principle VIII (Alembic Migrations)**: violated by dynamic `cache_*` tables created via `df.to_sql()` — accepted exception, owned by this sub-module.

---

**End of plan.md**
