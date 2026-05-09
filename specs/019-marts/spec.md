# Spec 019 — Marts (medallion L2, post-rebuild)

**Type**: Forward-engineered
**Status**: Implemented 2026-05-04, hardened 2026-05-06 (Sprint Marts Overhaul), expanded 2026-05-09 (analytics-driven sprint). **47 marts healthy in staging, 40.9M rows total.**
**Hexagonal scope**: Application (mart loader + builder + sql_macros) + Infrastructure (Celery tasks + adapter integration)
**Sister specs**: [015-catalog-resources](../015-catalog-resources/spec.md), [016-serving-port](../016-serving-port/spec.md), [017-raw-layer](../017-raw-layer/spec.md). [018-contracts-staging](../018-contracts-staging/spec.md) is **deprecated**.

**Recent updates (2026-05-09, analytics-driven sprint)**:
- **+11 marts shipped**, total 47 healthy. Selection driven by 2,667 successful_queries from prod (47 days, 38 unique users) cross-referenced with raw availability:
  1. `pobreza_indec_aglomerados` (1,224 rows) — 19 semestres × 17 sub-cuadros, UNPIVOT manual de time-pivoted INDEC
  2. `mendoza_ejecucion_acumulada_fondo` (21,888 rows) — votado/devengado/pagado × 13 meses UNPIVOT
  3. `presupuesto_servicios_administrativos` (3,712 rows) — sub-shape 5-col del cluster
  4. `presupuesto_finalidad_funcion` (865 rows) — sub-shape 7-col
  5. `presupuesto_clasificador_economico` (21,662 rows) — sub-shape 11-col (4-niveles del clasificador)
  6. `pami_compras_publicas` (409 rows) — post-repair `title_as_columns` de 13 tablas raw (ver spec 021)
  7. `bcra_principales_indicadores` (99,198 rows) — UNPIVOT reservas+tasas (responde a "Mostrame la evolución de las reservas del BCRA" repetida 7+ veces en prod)
  8. `comercio_exterior_argentina` (691,159 rows) — exportaciones+importaciones × 4 niveles (clae2/clae3/clae6/letra) UNION ALL
  9. `empleo_formal_argentina` (625,824 rows) — distribución por departamento × género × remuneración media/mediana
  10. `magyp_cultivos_principales` (169,706 rows) — UNION 17 cultivos `*_siembra_cosecha_produccion_rendimi*`
  11. `magyp_senasa_movimientos_pecuarios` (10,789,965 rows) — UNPIVOT 5 especies (bovinos/porcinos/avicolas/equinos/ovinos) × 31 categorías de animal
- **Macros now accept `require_all_columns=True` kwarg**: filtra tablas que NO tienen TODAS las `expected_columns` antes del cap check. Resuelve el bug donde patterns muy amplios (`presupuesto_de_la_administracion_pu*` matchea 597 tablas) excedían el cap de 200, cuando en realidad solo ~32 tablas tienen el shape canónico FULL. Validación al parser: `require_all_columns=True` requiere `expected_columns` non-empty.
- **`SELECT DISTINCT` recommended for catalog/dimension marts**: descubrimos durante el sprint que las 3 marts dimensionales del cluster presupuesto (~600M rows post-build sin DISTINCT) tienen catálogos repetidos en cada raw. Post `SELECT DISTINCT` o `ROW_NUMBER() OVER (PARTITION BY <business_key>) WHERE rn=1` los row counts caen 99%+. La segunda forma es preferible cuando hay una col `ultima_actualizacion_fecha` para tomar el snapshot más reciente y evitar fanout en joins.

**Recent updates (2026-05-06)**:
- 7 marts in production: `escuelas_argentina` (4,596), `presupuesto_consolidado` (4,959), `legislatura_actividad` (328), `series_economicas` (39), `staff_estado` (4,536), `salud_establecimientos` (113,133, NEW), `demo_energia_pozos` (0, awaits raw). Total ~127K rows in `mart.*`.
- **Macros now accept `expected_columns` kwarg**: when 0 matches → emit `SELECT NULL::text AS c1, ... WHERE FALSE` typed-empty fallback; when N heterogeneous matches → schema-intersection. Solves the case where CKAN portals (BA Prov, Corrientes) regenerate UUIDs and break `live_table('portal::UUID')` hardcoded references.
- **Macros parse via `ast.literal_eval`** (not bare regex), supporting multi-arg calls like `live_tables_by_table_pattern('pat', expected_columns=[...])`.
- **Cast uniforme `::text`** in UNION ALL of `live_tables_by_table_pattern` + `expected_columns` — outer SELECT applies its own type casts. Prevents `bigint vs text` mismatches across heterogeneous source tables.
- **Pipeline discovers marts directly** via `mart_definitions WHERE last_row_count > 0` (`sandbox.py:691-733`), independent of `table_catalog` enrichment. No need to register marts in `table_catalog` for discovery.

**Recent updates (2026-05-08, prod-driven sprint)**:
- **+7 marts shipped** (total 14 defined, 13 operational with rows). Selection driven by analytics over 47 days of `successful_queries` from prod (~2,666 queries):
  1. `caba_transportes_autorizados` (99,098 rows) — taxis/remises CABA
  2. `autoridades_pen` (6,138 rows) — Mapa del Estado, sueldos/cargos PEN. **82 prod hits**
  3. `compras_publicas_bac` (500,000 rows) — adjudicaciones BAC. **57 prod hits**
  4. `decretos_presidenciales` (19,892 rows) — decretos por número/fecha/extracto. **21 prod hits**
  5. `legisladores_argentina` (~116 rows) — UNION diputados+senadores con bloque. **42 prod hits**
  6. `geografia_administrativa` (6,672 rows) — UNION provincias+departamentos+municipios+localidades GeoRef. **67 prod hits**
  7. `indicadores_provinciales` (72 rows) — IDH y otros rankings provinciales
- **Routing infrastructure improved (Fase 1+2+2.5)**:
  - `discover_tables_by_catalog_search` ahora retorna marts + raws en una sola lista mergeada con scores comparables.
  - **Boost +0.17** a marts en el ranking final (calibrado contra diagnostic harness `scripts/diagnose_mart_routing.py`: 26 % expected-hit pre-fix → 100 % post-fix con 22 queries representativas).
  - **Sample-query gating** (new table `mart_sample_queries`): el boost se aplica SOLO cuando la query tiene cosine ≥ 0.70 con al menos UNA `sample_text` curada del mart. Evita over-rotear queries genéricas (false positive original: "elecciones PASO" routeaba a `mart.legislatura_actividad`). 14 marts cargados con 6-10 sample queries cada uno.
- **Query analytics persistence (Fase 3)**: nueva tabla `query_analytics` + función `save_query_attempt` cableada en `nl2sql.save_success_node` (persiste TODA query, no solo successful). 7 admin endpoints bajo `/api/v1/admin/analytics/*` (auth via `X-Admin-Key`).
- **Marts source from legacy `public.cache_*`**: 6 de los 7 nuevos referencian directamente tablas legacy (Mapa del Estado, BAC, Senado decretos, Diputados bloques, GeoRef, datos_gob_ar) porque el connector upstream no migró a raw todavía. SQL inlinea el nombre `FROM public.cache_*` en lugar de pasar por macros. Tracked como DEBT-019-006.
- **INDEC unpivot fix** (`indec_tasks.py`): nueva función `_unpivot_if_time_pivoted` detecta tablas time-pivoted (≥ 50 % cols matchean date patterns) y emite `pd.melt` a formato long `(concepto, periodo, valor)`. IPC mensual pasó de 91 cols × 267 rows → 3 cols × 29,415 rows. Habilita `mart.inflacion_argentina` (DEBT-019-005).

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
| `{{ live_tables_by_table_pattern('caba__*establecimientos*') }}` | Same as above but matching on `table_name` (slug-stable) instead of `resource_identity` (often a volatile UUID). Use cuando upstream regenera UUIDs. |

**Optional kwargs supported (2026-05-04 / 2026-05-09)**:
- `expected_columns=['c1','c2',...]` — Schema-intersection projection. With 0 matches → typed-empty `(SELECT NULL::text AS c1, ... WHERE FALSE)`. With N matches and heterogeneous schemas → emits `SELECT "c1"::text AS "c1", ... FROM <each>` with NULL fallback for cols not in that table. Prevents `each UNION query must have the same number of columns` errors.
- `require_all_columns=True` (2026-05-09) — Filters out tables that do NOT have ALL `expected_columns` before the cap check. Useful when a pattern matches many sub-shapes (e.g. fact + dimension tables in the same slug cluster) and only the FULL shape is desired. Requires `expected_columns` non-empty.

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

- **SC-001**: `build_mart('demo_energia_pozos')` produces `mart.demo_energia_pozos` with `last_row_count` matching `SELECT COUNT(*)` of the materialized view (validated 2026-05-09: 15,302 rows after v0.3.0 fix that re-pointed pattern to `*produccion_de_petroleo_y_gas*` post UUID regen by datos.gob.ar; original v0.1 had 499,670 rows but pinned to a UUID that was retired upstream).
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
- **DEBT-019-005 (PARTIAL 2026-05-08)**: 4 marts identificados con data ya en staging. Estado actual:
  - ✅ `inflacion_argentina` (built 2026-05-08): post INDEC unpivot fix. UNION ALL de las 3 representaciones (`cache_indec_ipc_variaci_n_mensual_aperturas`, `cache_indec_ipc_var_interanual_aperturas`, `cache_indec_ipc_ndices_aperturas`) con `tipo_medida` discriminator. 83,836 rows materialized. Routing 4/4 OK con boost gating.
  - ✅ `mujer_centros_atencion` (built 2026-05-08): solo las 4 versiones del padrón `caba__centros_integrales_de_la_mujer` (schema cohesivo: `nombre, dirreccion, barrio, comuna, email, destinatar`). 64 rows. **Reduced scope**: el cluster `caba__mapa_de_violencia_de_genero*` (17 tablas) quedó fuera porque tiene schema heterogéneo extremo (cada tabla es de un mes/categoría con cols únicos como `Cantidad`, `Casos`, meses literales). Tracked como gap menor — NL2SQL puede acceder a esas tablas raw directamente.
  - ✅ `demografia_caba` (built 2026-05-08): `caba__estructura_demografica*` con `expected_columns=[anio, grupo_edad, nacionalidad, POBLACION, porc_poblacion]`. 4 de 12 versiones matchean schema canónico (4,106 rows). Las otras 8 traen subsets distintos (por sexo/origen/año parcial) — quedan en raw.
  - ❌ `afiliados_obras_sociales` (NOT VIABLE 2026-05-08): tablas `pami__padron_de_afiliados*` y `datos_gob_ar__padron_de_afiliados*` están MUY rotas — son PDFs mal scrapeados, las cols son URLs literales y placeholders como `Páginas:`, `Páginas:_2`, `Páginas:_3`. Solo 2/8 tablas tienen cols reales (`Grupo Etario`, `UGL Beneficiario N°`). NO mart-eable hasta arreglar el scraper PAMI/datos_gob_ar (PDF parser). Movido a DEBT-019-007 como subitem (otro tipo de bug de parser).
- **DEBT-019-006 (PARTIAL 2026-05-09)**: 7 marts referenciaban `public.cache_*` legacy. Estado actualizado:
  - ✅ `legisladores_argentina` v0.2.0 (2026-05-09): parte diputados migrada a `live_tables_by_table_pattern('diputados__bloques*', expected_columns=[BLOQUE, APELLIDO, NOMBRES, DISTRITO, PERIODO], require_all_columns=True)`. Rows pasaron de 116 → 584 (más versiones disponibles en raw). Senadores sigue en `cache_senado_listado_senadores` porque el connector senado todavía no escribe a raw — bloqueante de connector, no de mart.
  - 🔴 6 marts pendientes: `autoridades_pen` (mapa_estado), `compras_publicas_bac` (bac), `decretos_presidenciales` (senado), `geografia_administrativa` (georef), `indicadores_provinciales` (datos_gob_ar parcial), `inflacion_argentina` (indec). Verificación 2026-05-09: ninguno de esos connectors escribe a raw todavía (`SELECT split_part(resource_identity,':',1) FROM raw_table_versions` no devuelve mapa_estado, bac, georef ni filas de senado/INDEC para esos slugs).
  - Riesgo real: bajo. `cleanup_orphan_cache_tables` tiene safety net que parsea `mart_definitions.sql_definition` con regex y excluye las tablas referenciadas (`_mart_dependency_guards` en ops_fixes.py). Frágil contra cambios de macro signature, pero no se ha roto.
  - Trade-offs vs migración: cache_* no tiene versionado (single-snapshot, sobrescritura on re-ingest), no tiene lineage cols (`_source_url`/`_source_file_hash`/`_parser_version`/`_collector_version`/`_ingested_at`), no permite rollback ni reproducir bugs históricos. Migración correcta requiere reescribir 6 connectors (2-3 hs c/u, total 12-18 hs). Backlog técnico racional, no urgente.
- **DEBT-019-007 (Open, 2026-05-08)**: INDEC unpivot fix cubre time-pivoted layout (~14 tablas IPC/PIB/IPI), pero quedan ~9 tablas INDEC con otros bugs de parser:
  - DuplicateColumnError en `cache_indec_balance_pagos_fob_cif`, `cache_indec_turismo_*`, `cache_indec_pobreza_*`: cuadros con headers idénticos (3 cols todas se llaman "Total mensual" o "Período"). Necesita dedup post-parse antes del `to_sql`.
  - Headers degradados a `col_1`, `col_2`, `col_3` ... en ~6 tablas: la fila de headers reales no se detecta. Necesita mejorar `_detect_data_header_row` para cuadros INDEC con cell-merging extremo.
