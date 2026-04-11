# Plan: Catalog Enrichment (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Infrastructure Task | `enrich_catalog`, `enrich_single_table` | `celery/tasks/catalog_enrichment_tasks.py` |

## 2. External Dependencies

- **AWS Bedrock Claude Haiku 4.5** — metadata generation via LLM
- **AWS Bedrock Cohere Embed Multilingual v3** — embedding (1024-dim)
- **PostgreSQL + pgvector HNSW** — `table_catalog` table

## 3. Metadata Output Schema

```json
{
  "display_name": "...",
  "description": "...",
  "domain": "legislativo | economico | salud | educacion | ...",
  "subdomain": "...",
  "key_columns": ["...", "..."],
  "column_types": {"col1": "TEXT", "col2": "INTEGER"},
  "sample_queries": ["SELECT ... FROM ...", ...],
  "tags": ["...", "..."]
}
```

## 4. Flow

```
_enrich_table(engine, table_name)
    ↓
fetch columns from information_schema.columns
    ↓
fetch sample rows (SELECT * LIMIT 5)
    ↓
_generate_metadata_for_table(table_name, columns, sample_rows, row_count)
    ├→ LLM call (Claude Haiku with prompt "catalog_enrichment.md")
    └→ fallback: programmatic basic metadata on failure
    ↓
_embed_text(combined_metadata_text) → Cohere embedding 1024-dim
    ↓
UPSERT table_catalog (table_name, metadata JSONB, embedding vector(1024))
```

## 5. Persistence

### `table_catalog`
- `table_name` PK
- `metadata jsonb` — the complete dict
- `embedding vector(1024)`
- `created_at`, `updated_at`
- HNSW index on `embedding` for search

## 6. Source Files

- `infrastructure/celery/tasks/catalog_enrichment_tasks.py`
- `prompts/catalog_enrichment.md`

---

**End of plan.md**
