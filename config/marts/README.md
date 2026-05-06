# Marts (semantic views)

This directory holds the per-domain marts the LangGraph pipeline serves.
Each YAML defines one materialized view in schema `mart`.

## File layout

One YAML per mart, named `<mart_id>.yaml`. Filename order is purely
cosmetic — marts are independent of each other.

The shipped YAMLs are **skeletons**: shape is correct, SQL is illustrative.
Operators flesh them out as the staging layer fills with real data.

## How a mart goes from YAML to served data

1. `openarg.build_mart(mart_id)` reads the YAML.
2. `DROP MATERIALIZED VIEW IF EXISTS` (cascade) + `CREATE MATERIALIZED VIEW`
   from `sql:`.
3. If `refresh.unique_index` is set, `CREATE UNIQUE INDEX` so future
   refreshes can use `CONCURRENTLY`.
4. `COMMENT ON COLUMN` for each `canonical_columns[*].description` —
   the pipeline reads these via `information_schema` for planner hints.
5. UPSERTs `mart_definitions(mart_id, ...)`.

## Refresh

`openarg.refresh_mart(mart_id)` runs `REFRESH MATERIALIZED VIEW
[CONCURRENTLY] mart.<id>`. CONCURRENTLY is used iff a unique index exists.

`refresh.policy` semantics:
  - `manual`              — only when the operator runs the task.
  - `daily` / `hourly`    — Celery beat refreshes on schedule.
  - `on_upstream_change`  — `staging_promotion` enqueues a refresh when a
                            staging table whose `contract_id` is in
                            `sources` is repopulated.

## Versioning

`version` is semver. Bump on schema changes. The mart_id stays the same
across versions; the materialized view is recreated in place.
