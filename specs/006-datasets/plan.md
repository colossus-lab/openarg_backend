# Plan: Datasets (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Entities | `Dataset`, `DatasetChunk`, `CachedData` | `domain/entities/dataset/` |
| Domain Port | `IDatasetRepository` | `domain/ports/dataset/dataset_repository.py` |
| Infrastructure Adapter | `DatasetRepositorySQLA` | `infrastructure/adapters/dataset/dataset_repository_sqla.py` |
| Infrastructure LLM | `BedrockEmbeddingAdapter` | `infrastructure/adapters/llm/bedrock_embedding_adapter.py` |
| Workers | `scraper`, `collector`, `embedding` queues | `celery/tasks/` |
| Presentation | `datasets_router.py` | `presentation/http/controllers/datasets/datasets_router.py` |

## 2. Entities

```python
@dataclass
class Dataset:
    id: UUID
    source_id: str
    title: str
    description: str
    organization: str
    portal: str
    url: str
    download_url: str
    format: str
    columns: dict          # JSON
    sample_rows: list[dict] # JSON
    tags: str
    is_cached: bool
    row_count: int | None
    created_at: datetime
    updated_at: datetime

@dataclass
class DatasetChunk:
    id: UUID
    dataset_id: UUID
    content: str           # chunk text
    embedding: list[float] # vector(1024)
    chunk_type: str        # "main" | "columns" | "contextual"
    created_at: datetime

@dataclass
class CachedData:
    id: UUID
    dataset_id: UUID
    status: str            # pending | downloading | ready | error | permanently_failed
    table_name: str | None
    s3_key: str | None
    row_count: int | None
    columns_json: dict
    retry_count: int
    cached_at: datetime | None
    updated_at: datetime
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
```

## 4. Endpoints

| Method | Path | Behavior |
|---|---|---|
| GET | `/api/v1/datasets/` | List indexed datasets (pagination, filter by portal) |
| GET | `/api/v1/datasets/stats` | Count per portal |
| POST | `/api/v1/datasets/scrape/{portal}` | Trigger scrape (admin) |
| GET | `/api/v1/datasets/{id}/download` | Download file (redirect to S3 or upstream) |

## 5. Embedding Strategy (5 chunks per dataset)

**Correction**: there are **5 chunks** per dataset, not 3 (error in the original reverse-engineering). The canonical code lives in `scraper_tasks.py:592-695`.

Known chunks (3 of 5 documented):

- **Chunk 1 — main**: `{title} — {description}`
- **Chunk 2 — columns**: joined column names
- **Chunk 3 — contextual** (`scraper_tasks.py:662-677`): **not sample_rows** as I originally thought, but a natural-language narrative:

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

- **Chunks 4-5**: TBD — verify exact breakdown in `scraper_tasks.py:592-695`. Not yet documented in this spec.

For each chunk: calls Bedrock Cohere Embed v3 with `input_type="search_document"`, persists in `dataset_chunks`.

## 6. Persistence Tables

See `000-architecture/plan.md` section 6 for the complete list. Key tables:

- `datasets` (UNIQUE `source_id + portal`)
- `dataset_chunks` (HNSW index over embedding)
- `cached_datasets` (UNIQUE `table_name`)
- `cache_*` (dynamic, created by workers)

## 7. External Dependencies

- **AWS Bedrock Cohere Embed Multilingual v3** (embeddings)
- **S3** — large file storage
- **PostgreSQL** — metadata + chunks + cache tables
- **Celery (Redis broker)** — task queues
- **CKAN portals** — sources of metadata and files

## 8. Source Files

- `domain/entities/dataset/`
- `domain/ports/dataset/dataset_repository.py`
- `infrastructure/adapters/dataset/dataset_repository_sqla.py`
- `infrastructure/adapters/llm/bedrock_embedding_adapter.py`
- `infrastructure/celery/tasks/{scraper,collector,embedding}_tasks.py`
- `presentation/http/controllers/datasets/datasets_router.py`

## 9. Deviations from Constitution

- **Principle VIII (Alembic Migrations)**: violated by dynamic `cache_*` tables created via `df.to_sql()` — accepted exception.

---

**End of plan.md**
