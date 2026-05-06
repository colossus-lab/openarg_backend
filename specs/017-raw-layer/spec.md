# Spec 017 — Raw Layer (medallion L1)

**Type**: Forward-engineered
**Status**: Implemented 2026-05-04, hardened 2026-05-06 (Sprint OOM Protection + Sprint RLM cleanup).
**Hexagonal scope**: Application (naming) + Infrastructure (collector + registry + retention + lifecycle)

**Recent updates (2026-05-06)**:
- **`cleanup_raw_orphans` task**: drops `raw.*` tables abandoned across `resource_identity` migrations (CKAN re-scrape generates new UUID, old physical name has no `cd` row pointing). Beat schedule every 6h, `max_drops=50`. **Safety net**: pre-loads identities referenced by `mart_definitions.sql_definition` (parses `live_table('id')` and `live_tables_by_pattern('id::*')` with regex) and excludes them from drop candidates — protects mart sources. After incident on 2026-05-06 morning where `escuelas_argentina` mart broke after orphan sweep dropped its source table.
- **`cleanup_raw_orphans` does double-check before drop**: between SELECT candidate and UPDATE, re-verifies the table is STILL missing in `information_schema.tables`. Prevents false positives caused by concurrent DROP+CREATE in `_to_sql_safe schema_mismatch_recreate` — Postgres `information_schema` is NOT MVCC-snapshotted, so brief DROP-pre-CREATE windows look like "missing" to the orphan detector. After incident: 5,578 false-positive "Table missing" marks in a single sweep, recovered manually.
- **OOM protection layer (Sprint OOM)**:
  - `_probe_download_size(url)` HTTP HEAD pre-check: if `Content-Length > cap_per_format`, mark `policy_too_large` without spawning worker. Caps configurable via env: `OPENARG_MAX_DOWNLOAD_ZIP_BYTES=200MB`, `_XLSX_BYTES=100MB`, `_XLS_BYTES=80MB`, `_CSV_BYTES=500MB`, `_JSON_BYTES=100MB`, default 200MB.
  - Size-based routing: 50MB < `Content-Length` < cap → reroute to `worker-collector-heavy` (1.5GB memory limit) instead of standard collector (400MB). Configurable via `OPENARG_HEAVY_ROUTE_BYTES=50MB`.
  - Browser User-Agent + `Accept-Language: es-AR` headers in HEAD/GET to bypass `403 Forbidden` from portals that block default httpx UA (confirmed `datos.cultura.gob.ar`).
- **`worker-collector-heavy` memory limit raised** from 800MB → 1.5GB. Concurrency 4. After repeated SIGKILL on Argentine government ZIP shapefiles (300-400MB compressed expanding to 1+GB on parse).
- **Magic-byte format detector**: `_detect_format_from_bytes(path)` peeks first 16 bytes. `PK\x03\x04` → xlsx; `\xd0\xcf\x11\xe0` → xls; `<!DOCTYPE/<html` → html_as_data (skip); `,`/`;`/`\t` in line 1 → csv. Used before `pd.read_excel` to avoid "Excel file format cannot be determined" errors when portals serve CSV/HTML with `.xls` extension.

---

## 1. Context & Purpose

Historically, every materialization landed in `public.cache_<portal>_<slug>__<hash>` with no versioning, no PK, and no lineage columns. Re-ingest overwrote silently; there was no way to tell when a dataset was last refreshed; the same physical name carried any number of upstream-shape variations.

The raw layer is the medallion's bottom tier:
- **Schema**: dedicated `raw` schema (separate from `public`) so legacy code that scans `public` is isolated.
- **Versioning**: every successful ingest produces a new `raw.<portal>__<slug>__<discrim>__v<N>` table. Old versions stay until retention drops them.
- **Lineage**: every row carries `_source_url`, `_source_file_hash`, `_parser_version`, `_collector_version`. Server-side `_ingested_at TIMESTAMPTZ DEFAULT NOW()` and `_ingest_row_id BIGSERIAL PRIMARY KEY` close the deduplication gap.
- **Registry**: `raw_table_versions` is the source of truth for "which table backs which logical resource at which version".

## 2. Schema

### `raw` (Postgres schema)

Created by mig 0039 with `CREATE SCHEMA IF NOT EXISTS raw`. Tables created by the collector follow the naming `<portal>__<slug>__<discrim>__v<N>` (see §3).

### `raw_table_versions`

| Column | Type | Notes |
|---|---|---|
| `resource_identity` | TEXT | `{portal}::{source_id}` (matches `catalog_resources`). PK part 1. |
| `version` | INTEGER | Monotonic per resource. PK part 2. |
| `schema_name` | TEXT | Default `'raw'`. |
| `table_name` | TEXT | Bare (`<portal>__<slug>__<discrim>__v<N>`). |
| `row_count`, `size_bytes` | BIGINT | Reported by collector at write time. |
| `source_url`, `source_file_hash` | TEXT | Lineage. |
| `parser_version`, `collector_version` | TEXT | Bumped when those components change. |
| `created_at` | TIMESTAMPTZ | Default `NOW()`. |
| `superseded_at` | TIMESTAMPTZ | Set when version N+1 lands. NULL for the live version. |

UNIQUE INDEX on `(schema_name, table_name)` — defends against accidental same-name collisions across resources.

## 3. Naming (`RawPhysicalNamer`)

`app.application.catalog.physical_namer.RawPhysicalNamer`:

- Format: `<portal>__<slug>__<8-char blake2b hash>__v<N>`.
- Identity-stable: same `(portal, source_id)` → same `stem` (everything except `__v<N>`). Bumping versions only swaps the suffix.
- Charset `[a-z0-9_]`; Postgres 63-char limit enforced (slug truncated last).
- Leading-digit slugs are prefixed with `t_`.

`raw_table_name(portal, source_id, *, version, slug_hint)` and `raw_table_qualified(...)` are convenience helpers.

## 4. Collector integration

### Destination resolver

`_resolve_collect_destination(engine, dataset_id, *, portal, source_id, title)` returns `_CollectDestination(schema, bare_name, version, resource_identity)`:

- When `OPENARG_USE_RAW_LAYER=1`: bumps the version via `_resolve_raw_table_for_dataset()` and returns a `raw`-schema destination.
- When unset/`0`: returns the legacy `cache_<portal>_<slug>__<hash>` destination in schema `public` (bit-for-bit unchanged).

The flag is the single rollback knob. Switching it back to `0` restarts producing legacy `cache_*` tables; in-flight raw tables are unaffected.

### Concurrency

`_resolve_raw_table_for_dataset()` acquires a transaction-scoped `pg_advisory_xact_lock(_lock_key("raw_table_version_bump", resource_identity))` before reading `MAX(version)`. Two collectors materializing the same dataset serialize on this lock; both get distinct versions.

### Metadata injection

`_inject_raw_metadata_columns(df, *, source_url, source_file_hash, parser_version, collector_version)` adds `_source_url`, `_source_file_hash`, `_parser_version`, `_collector_version` columns to the DataFrame before `to_sql()`. Lineage columns are recognised by the auto-typer (which skips them) and dropped before contract validation.

`_ingest_row_id` and `_ingested_at` are *table-level* (added by Postgres, not the DataFrame): `BIGSERIAL PRIMARY KEY` and `TIMESTAMPTZ DEFAULT NOW()` respectively.

### Version registration

`_register_raw_version()` UPSERTs into `raw_table_versions` with `ON CONFLICT (resource_identity, version) DO NOTHING` (idempotent). It also sets `superseded_at = NOW()` on prior versions of the same resource. The call is part of the same transaction-protected critical section as the catalog pointer update — registration + catalog UPDATE happen together; if either fails, the auto-promote to staging is skipped.

## 5. Retention

`openarg.retain_raw_versions(keep_last=3, dry_run=False)` (in `infrastructure/celery/tasks/ops_fixes.py`):
1. For each `resource_identity` with > `keep_last` versions, list the older ones.
2. `_record_cache_drop(table_name=qualified, reason='retain_raw_versions')` for audit.
3. `DROP TABLE` and `DELETE FROM raw_table_versions`.

`dry_run=True` reports candidates without executing. Recommended schedule: weekly via Celery beat once the raw layer is stable.

## 6. Functional Requirements

- **FR-001**: Every materialization to `raw.*` MUST come with an entry in `raw_table_versions`. The two writes happen in the same critical section.
- **FR-002**: `_register_raw_version` MUST mark older versions as `superseded_at = NOW()` so consumers can identify the live version.
- **FR-003**: Version selection MUST be serialized per `resource_identity` via `pg_advisory_xact_lock` to prevent races.
- **FR-004**: `OPENARG_USE_RAW_LAYER` MUST be the single rollback knob; setting it to `0` reverts the destination resolver to the legacy `cache_*` behaviour with zero other changes.
- **FR-005**: `_apply_cached_outcome` MUST validate `raw_schema` against a closed allow-list (`{"raw"}`) before composing it into a qualified name.
- **FR-006**: ~~Auto-promotion to staging MUST NOT fire when `_register_raw_version` failed~~ **OBSOLETE 2026-05-04** — the staging layer was removed (mig 0042). Replaced by FR-007.
- **FR-007**: When a raw landing transitions to `ready` AND `_register_raw_version` succeeded, `_apply_cached_outcome` MUST enqueue `refresh_mart` for every mart whose `source_portals` includes the landed portal. The dispatch uses a per-mart `task_id` so multiple landings within the same window collapse to one refresh.
- **FR-008**: When a raw version is superseded (a newer version of the same `resource_identity` lands), any `cached_datasets` row in `status='ready'` that points to the superseded table name MUST be deleted in the same transaction. Without this, the orphan ready rows accumulate as the catalog re-processes datasets, and discovery surfaces stale `table_name`s.

## 7. Out of Scope

- Multi-file ZIP / multi-sheet Excel expansion (lives in `application/expander/`; not yet wired into the raw layer — see DEBT-015-004).
- Backfill of pre-Fase 1.5 `cache_*` tables. Operators run `truncate cached_datasets + cache_* drop` and the next collector run produces raw tables from scratch.

## 8. Tech Debt

- **DEBT-017-001**: The advisory lock key in `_resolve_raw_table_for_dataset` is derived from `resource_identity` via `_lock_key()` (a hash). Two unrelated resources whose hashes collide would serialize unnecessarily — low probability with a 64-bit hash, but worth measuring.
- **DEBT-017-002**: `retain_raw_versions` runs DROP unconditionally on superseded tables. A future enhancement is "soak before drop" — keep the table for N days after `superseded_at` so audit replays work.
