# Plan: Datasets (Index)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Purpose

Top-level plan for the `006-datasets` module. The module is split into two independent sub-plans; this file only describes the split and the shared context.

## 2. Sub-plan Map

| Sub-module | Plan | Scope |
|---|---|---|
| 006a-catalog | [006a-catalog/plan.md](./006a-catalog/plan.md) | `Dataset`/`CachedData` entities, `IDatasetRepository`, `datasets_router.py` listing/stats/download. |
| 006b-ingestion | [006b-ingestion/plan.md](./006b-ingestion/plan.md) | scraper/collector/embedding/S3 Celery tasks, 5-chunk embedding strategy, `_to_sql_safe`, `_detect_schema_drift`, `cache_*` lifecycle. |

## 3. Shared Hexagonal Mapping

| Layer | Component | Owner |
|---|---|---|
| Domain Entities | `Dataset`, `CachedData` | 006a-catalog |
| Domain Entity | `DatasetChunk` | 006b-ingestion |
| Domain Port | `IDatasetRepository` | 006a-catalog |
| Infrastructure Adapter | `DatasetRepositorySQLA` | 006a-catalog |
| Infrastructure LLM | `BedrockEmbeddingAdapter` | 006b-ingestion |
| Workers (scraper/collector/embedding/s3) | Celery tasks | 006b-ingestion |
| Presentation | `datasets_router.py` | 006a-catalog |

## 4. Shared Dependencies

- **AWS Bedrock Cohere Embed Multilingual v3** — embeddings (006b).
- **S3** — large file storage (006b).
- **PostgreSQL** — metadata, chunks, dynamic cache tables (both).
- **Celery (Redis broker)** — task queues (006b).
- **CKAN portals** — source of truth (006b).

## 5. Deviations from Constitution

- **Principle VIII (Alembic Migrations)**: violated by dynamic `cache_*` tables created via `df.to_sql()` — accepted exception, detail in 006b-ingestion.

---

**End of plan.md (index)**
