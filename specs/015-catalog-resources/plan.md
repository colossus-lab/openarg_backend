# Plan 015 — Logical Catalog (`catalog_resources`)

## File map

| Layer | File | Purpose |
|---|---|---|
| Domain | `src/app/domain/entities/dataset/catalog_resource.py` | `CatalogResource` dataclass + status/kind enum constants |
| Mapping | `src/app/infrastructure/persistence_sqla/mappings/dataset_mappings.py` | `catalog_resources_table` Imperative SQLA mapping |
| Migration | `alembic/versions/2026_04_25_0035_add_catalog_resources.py` | Table + indexes + HNSW vector(1024) |
| Application | `src/app/application/catalog/title_extractor.py` | Deterministic canonical title |
| Application | `src/app/application/catalog/physical_namer.py` | Deterministic table name + discriminator |
| Application | `src/app/application/discovery/catalog_discovery.py` | WS3 hybrid + catalog-only mode |
| Worker | `src/app/infrastructure/celery/tasks/catalog_backfill.py` | UPSERT from datasets+cached_datasets |
| Worker | `src/app/infrastructure/celery/tasks/catalog_backfill.py` | `seed_connector_endpoints` for live API connector rows |
| Worker | `src/app/infrastructure/celery/tasks/censo2022_ingest.py` | Cuadro-by-cuadro seed |
| Wire-in | `src/app/application/pipeline/connectors/sandbox.py` | `_hybrid_logical_hints` + catalog-only short-circuit |
| Script | `scripts/staging_reset.py` | Destructive wipe of legacy cache_* / cached_datasets / table_catalog |
| Tests | `tests/unit/test_catalog_naming.py`, `test_catalog_discovery.py`, `test_staging_reset_guard.py` | Naming + discovery + safety |

## Feature flags
- `OPENARG_HYBRID_DISCOVERY=1` — append catalog_resources hits.
- `OPENARG_CATALOG_ONLY=1` — replace table_catalog entirely.
- `OPENARG_CENSO2022_CONFIG` — path to cuadro JSON (default `config/censo2022_cuadros.json`).

## Rollout (staging cutover)
1. `alembic upgrade head` — applies migrations 0033..0036.
2. `APP_ENV=staging python -m scripts.staging_reset --dry-run` — confirm what will be wiped.
3. `APP_ENV=staging python -m scripts.staging_reset --i-understand-this-deletes-data --reset-datasets` — wipe.
4. Wait for `scrape_catalog` to refill `datasets`.
5. Dispatch `openarg.catalog_backfill` (auto-dispatched by the reset script).
6. Dispatch `openarg.seed_connector_endpoints`.
7. If Censo 2022 config is absent, `openarg.ingest_censo2022` returns `reason=missing_or_empty_config:...` rather than silently succeeding with zero rows.
8. Set `OPENARG_CATALOG_ONLY=1` in staging env.
9. Smoke-test the planner's discovery path.

## Rollout (production — hybrid only, no wipe)
1. `alembic upgrade head`.
2. `openarg.catalog_backfill` populates catalog_resources alongside existing state.
3. Set `OPENARG_HYBRID_DISCOVERY=1` once staging validation has passed.
4. Decision on `OPENARG_CATALOG_ONLY=1` for prod is **out of scope of this rollout** — depends on WS6 cutover criteria.
