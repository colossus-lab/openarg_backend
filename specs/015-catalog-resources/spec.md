# Spec 015 — Logical Catalog (`catalog_resources`)

**Type**: Forward-engineered (collector_plan.md WS2/WS3/WS4)
**Status**: Implemented (code only — pending operator review and rollout)
**Hexagonal scope**: Domain (entity) + Infrastructure (mapping + migration + backfill + discovery)
**Related plan**: [./plan.md](./plan.md)
**Implements**: WS2 (logical catalog), WS3 (hybrid serving), WS4 (deterministic naming) of [collector_plan.md](../../../collector_plan.md)

---

## 1. Context & Purpose

`table_catalog` is a semantic index of *materialized* tables. The new `catalog_resources` is the **authoritative logical catalog** — a row per consultable resource, even when:

- the physical table doesn't exist yet (planner can request lazy materialization);
- the resource is a connector endpoint (BCRA, Series Tiempo, argentina_datos…) with no table;
- the resource is non-tabular (ACUMAR PDFs as `document_bundle`);
- the resource is a ZIP entry expanded into N rows via `parent_resource_id`.

This spec governs the schema, the deterministic naming (canonical_title + materialized_table_name), the backfill from current `datasets`+`cached_datasets`, and the WS3 hybrid-serving layer that lets the planner consult the new catalog without breaking the legacy `cache_*` path.

## 2. Schema (`catalog_resources`)

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | gen_random_uuid |
| `resource_identity` | VARCHAR(255) UNIQUE | Stable key — `{portal}::{source_id}[::{sub_path}]` |
| `dataset_id` | UUID FK→datasets | Optional (connector endpoints have no dataset) |
| `parent_resource_id` | UUID FK→self CASCADE | For ZIP children, sheets, cuadros |
| `portal`, `source_id` | VARCHAR | Provenance |
| `s3_key`, `filename`, `sub_path`, `sheet_name`, `cuadro_numero`, `provincia` | VARCHAR | Sub-resource locators |
| `raw_title` | TEXT | As scraped |
| `canonical_title` | TEXT | Deterministic, no LLM |
| `display_name` | TEXT | Human-friendly (per-portal templates) |
| `title_source` | VARCHAR(30) | `index | caratula | metadata | fallback | manual` |
| `title_confidence` | FLOAT [0..1] | CHECK constraint |
| `domain`, `subdomain`, `taxonomy_key` | VARCHAR | Taxonomy hooks |
| `resource_kind` | VARCHAR(30) | `file | sheet | cuadro | zip_member | document_bundle | connector_endpoint` (CHECK-constrained) |
| `materialization_status` | VARCHAR(40) | `pending | ready | live_api | non_tabular | materialization_corrupted | failed` (CHECK-constrained) |
| `materialized_table_name` | VARCHAR(255) | Set by `physical_namer` |
| `parser_version`, `normalization_version` | VARCHAR | For invalidation when parser changes |
| `embedding` | vector(1024) | HNSW index, cosine |
| `created_at`, `updated_at` | TIMESTAMPTZ | |

### Indexes
- HNSW on `embedding` (cosine ops).
- B-tree on `(portal, source_id)`, `materialization_status`, `materialized_table_name`, `parent_resource_id`, `dataset_id`.

## 3. Naming (WS4)

### Canonical title — `title_extractor.py`
Priority: `index_title` → `caratula_title` → metadata combination (`dataset_title + sheet + cuadro + provincia`) → filename fallback. Per-portal display templates (e.g. INDEC: `INDEC — {provincia?} — Cuadro {cuadro_numero?} — {raw}`). Never LLM.

### Physical table name — `physical_namer.py`
- Hard limit 63 chars (Postgres `typname`).
- Charset `[a-z0-9_]`.
- Format: `cache_<portal_slug>_<title_slug>__<8-char blake2b hash of (portal, source_id)>`.
- Deterministic discriminator (NOT random) — same logical resource always maps to the same table.
- Resolves the 10 `pg_type_typname_nsp_index` collisions confirmed in prod.

## 4. Hybrid serving (WS3)

Two flags govern visibility:

- `OPENARG_HYBRID_DISCOVERY=1` — append `catalog_resources` matches to existing `table_catalog` matches in the planner hints.
- `OPENARG_CATALOG_ONLY=1` — replace `table_catalog` entirely. Used in the staging cutover after `scripts/staging_reset.py`.

Both flags disabled by default in production; staging will run with `OPENARG_CATALOG_ONLY=1` once the legacy state is wiped.

## 5. Functional Requirements

- **FR-001**: `resource_identity` MUST be deterministic — same `(portal, source_id, sub_path)` always produces the same key.
- **FR-002**: `materialized_table_name` MUST be produced by `physical_namer` for any new resource. Legacy collisions remain until backfill rewrites them.
- **FR-003**: A ZIP container MUST produce N child `catalog_resources` rows with `parent_resource_id` pointing to the ZIP's row. ZIPs that contain only documents are catalogued as a single row with `resource_kind='document_bundle'`.
- **FR-004**: A connector endpoint (BCRA, Series Tiempo, argentina_datos, Georef, CKAN search, sesiones, DDJJ, staff, sandbox) MUST register exactly one `catalog_resources` row per endpoint with `resource_kind='connector_endpoint'`, `materialization_status='live_api'` and a description embedding. `openarg.seed_connector_endpoints` performs the initial seed deterministically.
- **FR-005**: The backfill task (`openarg.catalog_backfill`) MUST be idempotent — re-runs UPSERT by `resource_identity` without duplicating rows.
- **FR-005b**: `openarg.catalog_backfill` MUST run under a singleton advisory lock. If another backfill is already in progress, the new invocation MUST exit as `status='skipped_already_running'` instead of racing a second full-table UPSERT wave.
- **FR-006**: When a dataset has multiple `cached_datasets` rows (schema variants), the backfill MUST pick the most authoritative one (preferring `status='ready'`, then most recently updated) so each `resource_identity` resolves to a single deterministic row.
- **FR-006b**: The backfill MUST not collapse terminal materialization states into `pending`. At minimum it MUST map `cached_datasets.status='permanently_failed'` to `catalog_resources.materialization_status='failed'`, and `zip_document_bundle` / `zip_no_parseable_file` terminal rows to `materialization_status='non_tabular'`.
- **FR-007**: `OPENARG_CATALOG_ONLY=1` MUST bypass the legacy `table_catalog` query in the planner hints path. Other serving paths remain on the legacy lookup until the hybrid cutover work is completed.
- **FR-008**: `scripts/staging_reset.py` MUST auto-dispatch the full rebuild chain after a destructive wipe: `seed_connector_endpoints`, `scrape_catalog` (when `--reset-datasets` is used), a first `catalog_backfill`, `bulk_collect_all`, `reconcile_cache_coverage`, and a final `catalog_backfill`. A reset that stops at scrape-only state is considered incomplete because `catalog_resources.materialization_status` would remain stale in `pending`.
- **FR-009**: The `bulk_collect_all` phase in that rebuild chain MUST be convergence-driven: one reset-triggered dispatch MUST keep chaining follow-up passes until no eligible materialization work remains, or until the configured bounded depth guard is reached and logged.
- **FR-009b**: During long rebuilds, beat MUST refresh `openarg.catalog_backfill` periodically on the `ingest` queue so `catalog_resources.materialization_status` does not stay stale until the very end of a multi-hour convergence wave.

## 6. Success Criteria

- **SC-001**: After backfill, `catalog_resources.count` ≥ `datasets.count` (every dataset has a row, plus connector endpoints + ZIP expansions).
- **SC-002**: `physical_namer` produces zero collisions across the full prod dataset.
- **SC-003**: With `OPENARG_HYBRID_DISCOVERY=1`, the planner sees both legacy `table_catalog` hits and `catalog_resources` hits in the same hints block. With `OPENARG_CATALOG_ONLY=1`, only the new ones.
- **SC-004**: `staging_reset.py --dry-run` reports exactly the cache_* table count + the 5–6 base tables it would truncate. With `--i-understand-this-deletes-data`, executes them in <60 s on staging-sized data.
- **SC-005**: After `staging_reset.py --i-understand-this-deletes-data --reset-datasets`, staging MUST eventually converge to a state where `catalog_resources.materialization_status='ready'` tracks post-reset `cached_datasets.status='ready'` without requiring a manual operator rerun of `catalog_backfill`.
- **SC-006**: Operators MUST not need to manually re-trigger `bulk_collect_all` after a reset just because the first wave drained before all eligible datasets were materialized.

## 7. Out of Scope

- Automatic scheduling of `openarg.populate_catalog_embeddings` on every deployment — operators trigger it explicitly after backfill or as part of rollout.
- Live-API connector self-discovery — connectors register themselves explicitly (T1.5 in collector_backlog).

## 8. Tech Debt

- **DEBT-015-001**: `openarg.populate_catalog_embeddings` is an explicit batch task, not an incremental worker. New `catalog_resources` rows stay `embedding=NULL` until operators run it (or future incremental hooks land).
- **DEBT-015-002**: When backfill assigns a new `physical_namer` table name to a resource that already has a different legacy `cache_*` table, the legacy table is NOT renamed — discovery still resolves to the legacy name. A separate "physical-rename" sweep is needed for the cutover.
