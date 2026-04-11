# Plan: Dataset Catalog (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Parent plan**: [../plan.md](../plan.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Entities | `Dataset`, `CachedData` | `domain/entities/dataset/` |
| Domain Port | `IDatasetRepository` | `domain/ports/dataset/dataset_repository.py` |
| Infrastructure Adapter | `DatasetRepositorySQLA` | `infrastructure/adapters/dataset/dataset_repository_sqla.py` |
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

> `DatasetChunk` (embedding chunks) lives in `006b-ingestion/` — it is produced and consumed by the embedding pipeline, not by the catalog endpoints.

## 3. Endpoints

| Method | Path | Behavior |
|---|---|---|
| GET | `/api/v1/datasets/` | List indexed datasets (pagination, filter by portal) |
| GET | `/api/v1/datasets/stats` | Count per portal |
| POST | `/api/v1/datasets/scrape/{portal}` | Trigger scrape (admin) — dispatches worker owned by `006b-ingestion` |
| GET | `/api/v1/datasets/{id}/download` | Download file (redirect to S3 or upstream) |

## 4. Repository Contract

`IDatasetRepository` exposes:
- `save(dataset)` — persist a new row.
- `get_by_id(id)` — fetch by UUID.
- `get_by_source_id(source_id, portal)` — fetch by natural key.
- `list_by_portal(portal, limit, offset)` — listing endpoint backing query.
- `upsert(dataset)` — idempotent by `(source_id, portal)` UNIQUE.

Implemented by `DatasetRepositorySQLA` using async SQLAlchemy 2.0.

## 5. Persistence Tables

- `datasets` (UNIQUE `source_id + portal`)
- `cached_datasets` (UNIQUE `table_name`) — state of the physically cached copy

## 6. Source Files

- `domain/entities/dataset/`
- `domain/ports/dataset/dataset_repository.py`
- `infrastructure/adapters/dataset/dataset_repository_sqla.py`
- `presentation/http/controllers/datasets/datasets_router.py`

## 7. Deviations from Constitution

None in the catalog sub-module itself. The `cache_*` dynamic-table exception is owned by `006b-ingestion`.

---

**End of plan.md**
