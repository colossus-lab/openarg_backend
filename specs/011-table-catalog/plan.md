# Plan: Table Catalog (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Infrastructure Task | `enrich_catalog` (catalog_enrichment) | `celery/tasks/catalog_enrichment_tasks.py` (see `002o-catalog-enrichment/`) |
| Migration | `table_catalog` table | `alembic/versions/2026_XX_XX_0019_table_catalog.py` |
| Pipeline helper | `discover_catalog_hints_for_planner()` | `application/pipeline/nodes/planner.py` or similar |

## 2. Schema

```sql
CREATE TABLE table_catalog (
    table_name TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL,
    domain TEXT,
    subdomain TEXT,
    key_columns TEXT[],
    column_types JSONB,
    sample_queries TEXT[],
    tags TEXT[],
    embedding vector(1024) NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_table_catalog_embedding
  ON table_catalog USING hnsw (embedding vector_cosine_ops);

CREATE INDEX idx_table_catalog_domain ON table_catalog(domain);
```

## 3. Discovery Flow

```python
async def discover_catalog_hints_for_planner(query: str) -> str:
    query_emb = await embedding.embed_query(query)
    rows = await db.fetch_all("""
        SELECT table_name, display_name, description, key_columns, sample_queries,
               1 - (embedding <=> CAST(:emb AS vector)) AS score
        FROM table_catalog
        ORDER BY embedding <=> CAST(:emb AS vector)
        LIMIT 5
    """, {"emb": emb_to_str(query_emb)})

    hints = "\n\n".join([
        f"- {r.display_name} (tabla: {r.table_name}):\n  {r.description}"
        for r in rows
    ])
    return hints
```

## 4. Enrichment Task (see 002o-catalog-enrichment)

Triggered automatically from `collector_tasks.py` when a new `cache_*` table is created. Also triggerable manually from admin.

## 5. Source Files

- Migration: `alembic/versions/*_table_catalog.py` (identify exact one)
- Task: `infrastructure/celery/tasks/catalog_enrichment_tasks.py`
- Helper: `application/pipeline/` — `discover_catalog_hints_for_planner()`
- Prompt: `prompts/catalog_enrichment.md`

---

**End of plan.md**
