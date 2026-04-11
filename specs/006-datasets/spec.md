# Spec: Datasets (Index)

**Type**: Reverse-engineered (top-level index)
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Infrastructure + Presentation + Workers
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Central catalog of **datasets indexed** by OpenArg plus the **chunking and embedding** pipeline for semantic search. It is the backbone that connects connectors (which produce datasets) with vector search (which makes them queryable).

This module has been split into two sub-modules:

| Sub-module | Scope |
|---|---|
| [006a-catalog](./006a-catalog/spec.md) | `Dataset` entity, metadata, `IDatasetRepository`, `/api/v1/datasets/` listing/stats/download endpoints. |
| [006b-ingestion](./006b-ingestion/spec.md) | `scrape_catalog → collect_dataset → index_dataset_embedding` pipeline, `cached_datasets` lifecycle, dynamic `cache_*` tables, 5-chunk embedding strategy, S3 upload. |

## 2. Sub-module Map

```
006-datasets/
├── spec.md           # this file
├── plan.md
├── 006a-catalog/     # metadata & listing (read side)
│   ├── spec.md
│   └── plan.md
└── 006b-ingestion/   # scraper → collector → embedder → S3 (write side)
    ├── spec.md
    └── plan.md
```

## 3. Cross-references

- **Vector search** that consumes `dataset_chunks`: see module `007-vector-search`.
- **Connectors** that feed the scraper: see modules `002-connectors` and `005-connectors-*`.
- **Architecture macro**: `000-architecture/`.

## 4. Spec ID Registry

Full definitions live in the sub-module specs. This table only maps IDs → owner.

| ID | Owner |
|---|---|
| FR-001, FR-002, FR-008 | 006a-catalog |
| FR-003, FR-004, FR-005, FR-006, FR-007, FR-009, FR-010, FR-011 | 006b-ingestion |
| SC-001 | 006a-catalog |
| SC-002, SC-003, SC-004 | 006b-ingestion |
| CL-001, CL-002, CL-003 (RESOLVED) | 006b-ingestion |
| CL-004 (NEEDS CLARIFICATION) | 006a-catalog |
| DEBT-001, DEBT-002, DEBT-004, DEBT-005, DEBT-006 | 006b-ingestion |
| DEBT-003 | 006a-catalog |

---

**End of spec.md (index)**
