# Plan: Dataset Ingestion Pipeline (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Parent plan**: [../plan.md](../plan.md)
**Last synced with code**: 2026-04-10

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

## 6. `cached_datasets` Lifecycle

```
pending → downloading → ready
                    ↘ error → (retry up to N) → permanently_failed
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
