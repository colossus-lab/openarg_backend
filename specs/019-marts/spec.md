# Spec 019 — Marts (medallion L2, post-rebuild)

**Type**: Forward-engineered
**Status**: Implemented 2026-05-04, hardened 2026-05-06 (Sprint Marts Overhaul).
**Hexagonal scope**: Application (mart loader + builder + sql_macros) + Infrastructure (Celery tasks + adapter integration)
**Sister specs**: [015-catalog-resources](../015-catalog-resources/spec.md), [016-serving-port](../016-serving-port/spec.md), [017-raw-layer](../017-raw-layer/spec.md). [018-contracts-staging](../018-contracts-staging/spec.md) is **deprecated**.

**Recent updates (2026-05-06)**:
- 7 marts in production: `escuelas_argentina` (4,596), `presupuesto_consolidado` (4,959), `legislatura_actividad` (328), `series_economicas` (39), `staff_estado` (4,536), `salud_establecimientos` (113,133, NEW), `demo_energia_pozos` (0, awaits raw). Total ~127K rows in `mart.*`.
- **Macros now accept `expected_columns` kwarg**: when 0 matches → emit `SELECT NULL::text AS c1, ... WHERE FALSE` typed-empty fallback; when N heterogeneous matches → schema-intersection. Solves the case where CKAN portals (BA Prov, Corrientes) regenerate UUIDs and break `live_table('portal::UUID')` hardcoded references.
- **Macros parse via `ast.literal_eval`** (not bare regex), supporting multi-arg calls like `live_tables_by_table_pattern('pat', expected_columns=[...])`.
- **Cast uniforme `::text`** in UNION ALL of `live_tables_by_table_pattern` + `expected_columns` — outer SELECT applies its own type casts. Prevents `bigint vs text` mismatches across heterogeneous source tables.
- **Pipeline discovers marts directly** via `mart_definitions WHERE last_row_count > 0` (`sandbox.py:691-733`), independent of `table_catalog` enrichment. No need to register marts in `table_catalog` for discovery.

---

## 1. Context & Purpose

Marts are the layer the LangGraph pipeline serves. The post-rebuild design
sits **directly on top of `raw.*`** (no intermediate staging layer) — a
mart is a materialized view that joins, filters and casts raw resources
into a canonical, domain-named surface.

This eliminates the "contracts as gatekeepers" problem of the previous
design (spec 018, deprecated): every raw landing can be exposed via a mart
that defines exactly the SQL needed for its domain. No data is held back
because no contract matched it.

## 2. Module layout

`app.application.marts`:
- `mart.py` — `Mart` dataclass + `MartCanonicalColumn` + `MartRefreshPolicy` + YAML loader. Validates `id` charset and rejects `sources.portals=[]` (silent under-refresh trap).
- `builder.py` — `build_create_view_sql(mart)` / `build_comment_sql(mart)` / `build_refresh_sql(mart)`.
- `sql_macros.py` — resolves `{{ live_table('rid') }}` / `{{ live_tables_by_portal('p') }}` / `{{ live_tables_by_pattern('p::*x*') }}` against `raw_table_versions` live rows.

Tasks (`app.infrastructure.celery.tasks.mart_tasks`):
- `openarg.build_mart(mart_id)` — DROP + CREATE materialized view, install COMMENTs, count rows, UPSERT `mart_definitions`.
- `openarg.refresh_mart(mart_id)` — when the resolved SQL changed (raw versions advanced or YAML edited): full rebuild. Otherwise: `REFRESH MATERIALIZED VIEW [CONCURRENTLY]` + count.
- `find_marts_for_portal(portal)` — lookup helper used by the collector to enqueue refreshes per portal.

## 3. Mart YAML shape

```yaml
id: presupuesto_consolidado          # [a-z][a-z0-9_]{0,62}
version: 1.0.0
description: |
  Vista unificada del presupuesto nacional + provincial.
domain: presupuesto

sources:
  portals: [presupuesto_abierto, datos_gob_ar]   # required, non-empty
  resource_patterns: ["*presupuesto*", "*ejecucion*"]   # optional fnmatch-style

canonical_columns:
  - {name: anio, type: int, description: "Año del ejercicio"}
  - {name: jurisdiccion, type: text}
  - {name: monto_pesos, type: numeric}

sql: |
  SELECT
    NULLIF(anio, '')::int AS anio,
    COALESCE(NULLIF(jurisdiccion, ''), 'NACION') AS jurisdiccion,
    NULLIF(monto, '')::numeric AS monto_pesos
  FROM {{ live_table('presupuesto_abierto::credito_2024') }}
  WHERE NULLIF(anio, '') IS NOT NULL

refresh:
  policy: on_upstream_change
  unique_index: [anio, jurisdiccion]    # optional; required for CONCURRENTLY
```

`type` ∈ `{text, int, bigint, float, numeric, date, timestamp, bool, jsonb}`.
`policy` ∈ `{manual, daily, hourly, on_upstream_change}`.

## 4. SQL macros (`sql_macros.py`)

Build-time only — no runtime DB access from the mart itself. Macro syntax
is intentionally narrow (no Jinja-style logic) to keep the resolver
predictable and easy to validate.

| Macro | Resolves to |
|---|---|
| `{{ live_table('portal::source_id') }}` | `raw."<live_table_name>"` for that exact resource_identity (`MAX(version)` row with `superseded_at IS NULL`). |
| `{{ live_tables_by_portal('bcra') }}` | `(SELECT * FROM raw."t1" UNION ALL SELECT * FROM raw."t2" ...)` — every live in that portal. |
| `{{ live_tables_by_pattern('bcra::*tasa*') }}` | Same as above but filtered by SQL-LIKE on `resource_identity`. The `*` is the glob wildcard. |

When a macro resolves to **zero rows**, the placeholder is
`(SELECT NULL::text AS dummy WHERE FALSE)` — a deterministic empty
subquery that is valid in JOIN positions (the older `(SELECT WHERE FALSE)`
form is invalid in some contexts). Marts using a not-yet-landed resource
**still build** with empty rows and stay empty until upstream lands.

## 5. `mart` schema + `mart_definitions`

`mart_definitions` (mig 0041, then 0043 added `source_portals` + `last_row_count`):

| Column | Type | Notes |
|---|---|---|
| `mart_id` | TEXT PK | `[a-z][a-z0-9_]{0,62}` (validated at YAML load). |
| `mart_schema`, `mart_view_name` | TEXT | UNIQUE on the pair. |
| `domain` | TEXT | For filtering during discovery. |
| `source_portals` | TEXT[] | **The auto-refresh routing key** — when a raw landing for one of these portals occurs, this mart's refresh is enqueued. Validated non-empty. |
| `sql_definition` | TEXT | The macro-resolved SQL persisted at last build. Used by `refresh_mart` to detect drift (`_normalize_sql` collapses whitespace before comparing — cosmetic YAML edits do NOT trigger rebuild). |
| `canonical_columns_json` | JSONB | `[{name, type, description}, ...]`. |
| `refresh_policy` | TEXT | CHECK-constrained. |
| `unique_index_columns` | TEXT[] | Required for `REFRESH ... CONCURRENTLY`. |
| `last_refreshed_at` | TIMESTAMPTZ | |
| `last_refresh_status` | TEXT | `built` / `refreshed` / `build_failed` / `refresh_failed`. |
| `last_refresh_error` | TEXT | Error tail (500 chars) when failed. |
| `last_row_count` | BIGINT | Row count post-build/refresh. **The Serving Port adapter filters discovery on `COALESCE(last_row_count, 0) > 0`** so empty marts never appear to the planner. |
| `embedding` | vector(1024) | **Populated 2026-05-05** for all 6 marts via `openarg.backfill_mart_embeddings` (Bedrock Cohere multilingual v3). `_upsert_mart_definition` now generates the embedding on every successful build. See DEBT-019-001 update. |
| `source_contract_ids` | TEXT[] | **ZOMBIE** — leftover from spec 018, no readers. Drop in a future migration. |

## 6. Refresh flow

**Build vs refresh decision** (`refresh_mart`):

1. Resolve macros → get current `resolved_sql`.
2. Read `mart_definitions.sql_definition` (the SQL persisted at last build).
3. If the materialized view does not exist OR
   `_normalize_sql(prior) != _normalize_sql(resolved_sql)`:
   → **full `build_mart`** (DROP + CREATE).
4. Otherwise → `REFRESH MATERIALIZED VIEW [CONCURRENTLY]`.
5. Count rows; UPSERT `mart_definitions(last_row_count, last_refresh_status='refreshed')`.

`_normalize_sql` (in `mart_tasks.py`) collapses whitespace so a YAML edit
that only adjusts indentation does not trigger a costly rebuild.

**Auto-refresh trigger** (collector → mart):

In `_apply_cached_outcome`, when a raw landing transitions to `ready`:

```python
for mart_id in find_marts_for_portal(portal):
    refresh_mart.apply_async(
        args=[mart_id], queue="ingest",
        task_id=f"refresh_mart:{mart_id}",   # debounce key
        expires=120,
    )
```

`task_id` is per-mart-id (no time bucket): Celery rejects duplicate
`task_id`s while the prior task is queued or running. 50 raw landings for
the same portal within one mart's refresh window collapse to ONE refresh.
The previous bucket-based attempt had a boundary-race bug where landings
on either side of `floor(now / 120s)` ended up in different buckets and
ran back-to-back.

## 7. Pipeline integration

`LegacyServingAdapter._discover_marts` reads `mart_definitions WHERE
COALESCE(last_row_count, 0) > 0` so empty / not-yet-populated marts are
invisible to the planner. The planner falls through to raw / cache_legacy
when no curated mart has data. `_get_mart_schema` resolves
`mart_definitions → information_schema.columns` and pulls
`pg_catalog.col_description` for `Schema.semantics`.

## 8. Functional Requirements

- **FR-001**: `build_mart` MUST be idempotent on (mart_id, raw_table_versions live snapshot, YAML hash).
- **FR-002**: `refresh_mart` MUST detect drift via SQL comparison (whitespace-normalized) and delegate to `build_mart` when the resolved SQL changed.
- **FR-003**: When `unique_index` is declared, refresh MUST use `CONCURRENTLY`. Otherwise non-concurrent.
- **FR-004**: `build_mart` MUST install `COMMENT ON COLUMN` for every canonical column with a description. The pipeline reads these via `pg_description` for planner hints.
- **FR-005**: Build / refresh failures MUST UPSERT `last_refresh_status` ∈ `{build_failed, refresh_failed}` plus the error tail in `last_refresh_error`.
- **FR-006**: Auto-refresh MUST be gated by `OPENARG_AUTO_REFRESH_MARTS` so operators can run raw-only for a soak period.
- **FR-007**: Macro resolution failures MUST surface as `MacroResolutionError` and mark the mart `build_failed`. The mart MUST NOT silently produce a wrong-shape view.
- **FR-008**: `mart_id` MUST match `^[a-z][a-z0-9_]{0,62}$`. Any other charset is rejected at YAML load — defense against SQL injection if `mart.id` is ever interpolated unquoted.
- **FR-009**: `sources.portals` MUST be non-empty. Empty portals means no raw landing triggers refresh — almost always a YAML mistake.
- **FR-010**: Empty-resolution macros (zero matching live rows) MUST emit `(SELECT NULL::text AS dummy WHERE FALSE)` — valid in any SQL position.
- **FR-011**: The Serving Port discovery MUST filter `last_row_count > 0` so the planner never sees empty marts.

## 9. Success Criteria

- **SC-001**: `build_mart('demo_energia_pozos')` produces `mart.demo_energia_pozos` with `last_row_count` matching `SELECT COUNT(*)` of the materialized view (validated 2026-05-04: 499,670 rows).
- **SC-002**: With `OPENARG_DISCOVER_MARTS=1`, `IServingPort.discover('petroleo')` returns `mart::demo_energia_pozos` and skips the empty mart skeletons.
- **SC-003**: When a new raw version of any resource in `source_portals` lands, `refresh_mart` runs at most once per 120s window per mart.
- **SC-004**: `served_from_layer` metric in `connector_call:sandbox:served_from:*` increases for `mart` whenever a query lands on a mart table.

## 10. Out of Scope

- Population of `mart_definitions.embedding` (DEBT-019-001).
- Cross-mart joins or layered marts (intermediate "silver" marts) — every mart sources from `raw.*` directly today.
- Drop of zombie column `source_contract_ids` (DEBT-019-004).

## 11. Tech Debt

- **DEBT-019-001 (CLOSED 2026-05-05)**: `mart_definitions.embedding` was provisioned but unpopulated. Sprint 0.2 added `openarg.backfill_mart_embeddings` (Bedrock Cohere multilingual v3, 1024-dim) and wired `_upsert_mart_definition` to compute the embedding on each successful build. All 6 marts now carry an embedding. `/data/search` already reranks marts via cosine similarity. Switching `discover()` from ILIKE to HNSW is the next consumer to add.
- **DEBT-019-002 (PARTIAL 2026-05-05)**: 6 marts shipped, of which 5 are still skeletons (`presupuesto_consolidado`, `series_economicas`, `staff_estado`, `escuelas_argentina`, `legislatura_actividad`) — their SQL contains `WHERE FALSE` placeholders or hardcoded subsets that don't reflect a real portal landing yet. The 6th, `demo_energia_pozos`, is the only fully-populated mart (499,670 rows, year 2018 only — see SC-001). The skeletons become real once datasets land for their portals and the per-resource cast logic gets written. **Sprint 0.6 audit flagged**: `escuelas_argentina` is hardcoded to Corrientes (1/24 provinces); `legislatura_actividad` covers only HCDN (Senado missing); `series_economicas` is just BCRA cotizaciones (39 rows) not a real series catalog. These are tracked separately as Sprint 1 items, not under this DEBT.
- **DEBT-019-003**: `refresh_mart` task_id (`refresh_mart:<mart_id>`) is per-mart, not per-portal. If two portals' landings disrupt the same mart within 120s, the second one is debounced even though it carries new data — acceptable trade for the dedup behavior. If this becomes a real concern, switch to a Redis-backed sliding window.
- **DEBT-019-004**: `mart_definitions.source_contract_ids` is a zombie column. No readers since 2026-05-04. Drop in a future migration.
