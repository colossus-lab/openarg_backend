# Spec 019 — Marts (medallion L2, post-rebuild)

**Type**: Forward-engineered
**Status**: Implemented 2026-05-04, hardened 2026-05-06 (Sprint Marts Overhaul), expanded 2026-05-09 (analytics-driven sprint).
**Hexagonal scope**: Application (mart loader + builder + sql_macros) + Infrastructure (Celery tasks + adapter integration)
**Sister specs**: [015-catalog-resources](../015-catalog-resources/spec.md), [016-serving-port](../016-serving-port/spec.md), [017-raw-layer](../017-raw-layer/spec.md), [022-mart-quality](../022-mart-quality/spec.md) (nightly audit + `serving_blocked`). [018-contracts-staging](../018-contracts-staging/spec.md) is **deprecated**.

**Recent capability updates (2026-05-09, analytics-driven sprint)**:
- **Macros now accept `require_all_columns=True` kwarg**: filters out tables that don't have ALL `expected_columns` before the cap check. Resolves the case where a broad pattern (e.g. `presupuesto_de_la_administracion_pu*`) matches hundreds of tables but only a small subset has the canonical FULL shape. Validation at parse time: `require_all_columns=True` requires `expected_columns` to be non-empty.
- **`SELECT DISTINCT` (or `ROW_NUMBER OVER PARTITION BY <business_key> WHERE rn=1`) is recommended for catalog/dimension marts**: shape clusters where each raw snapshot redundantly carries the same lookup table (e.g. presupuesto dimension marts) blow up with cartesian fanout if the mart simply concatenates with `UNION ALL`. The `ROW_NUMBER` form is preferable when a `ultima_actualizacion_fecha`-style column lets the mart pick the freshest snapshot per business key.
- **Mart selection driven by analytics**: a sprint of 11 new marts on staging was bootstrapped from `successful_queries` aggregates over 47 days × 38 unique users — query volume + raw availability picked the next batch. Operational changelog (per-mart rows + dates) lives in `MEMORY.md`, not here.

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
- **FR-010**: Empty-resolution macros (zero matching live rows) MUST emit an empty subquery valid in any SQL position. With `expected_columns` this is `(SELECT NULL::text AS "c1", … WHERE FALSE)` (`_typed_empty_select`), so the consuming mart still references a known schema and builds with 0 rows; without it, `(SELECT NULL AS dummy WHERE FALSE)`. *Corrected 2026-08-19 — the FR still described the pre-`expected_columns` behaviour that this spec's own 2026-05-06 changelog superseded.*
- **FR-011**: Every mart discovery path MUST filter `COALESCE(last_row_count, 0) > 0 AND NOT COALESCE(serving_blocked, FALSE)` — the first hides empty marts, the second hides marts deliberately withheld (migration 0054). The block MUST also be enforced at execution time, since NL2SQL can name a blocked mart directly. See [022-mart-quality](../022-mart-quality/spec.md) FR-015/FR-016. *Corrected 2026-08-19 — the FR named only the first half of the filter.*

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
- **DEBT-019-006 (CLOSED 2026-05-09)**: los 7 marts que referenciaban `public.cache_*` legacy ahora usan `live_table('<portal>::<source_id>')` macro y son schema-agnostic. Estado:
  - ✅ `legisladores_argentina` v0.3.0: diputados via `live_tables_by_table_pattern('diputados__bloques*')` + senado via `live_table('senado::listado_senadores')`. 584 rows.
  - ✅ `decretos_presidenciales` → `live_table('senado::decretos_presidenciales')`. 19,892 rows.
  - ✅ `inflacion_argentina` → 3 macros `live_table('indec::ipc_*')`. 83,836 rows.
  - ✅ `geografia_administrativa` → 4 macros `live_table('georef::*')`. 6,672 rows.
  - ✅ `compras_publicas_bac` → `live_table('bac::awards')`. 500,000 rows.
  - ✅ `autoridades_pen` → `live_table('mapa_estado::autoridades_pen')`. 6,138 rows.
  - ✅ `indicadores_provinciales` → 3 macros `live_table('datos_gob_ar::actividad_econ_mica_empleo_y_pob_*')`. 72 rows.
  - **Storage convention** (post-2026-05-09): ALL cached tables — whether produced by the standard collector path or by a vía-B connector — live in the `raw` schema. Vía-B connectors call `to_sql(..., schema='raw')` and `register_via_b_table(schema_name='raw')`. The DB role's `search_path = raw, public, "$user"` so any unqualified SQL (e.g. `FROM cache_x` in NL2SQL prompts) resolves automatically. Marts reference tables via the `live_table('<portal>::<sid>')` macro that expands to `raw."<table>"` at build time. **A fresh database is born in this shape** — there is no "legacy migration" step on a clean deploy.
  - **One-shot historical migration** (only relevant to a DB that accumulated `cache_*` legacy in public, e.g. a long-running staging): mass `ALTER TABLE … SET SCHEMA raw` + UPDATE of `raw_table_versions.schema_name` and `catalog_resources.materialized_table_name` + role `search_path` change + mass rebuild of marts. Captured in `MEMORY.md` / operational changelog for reproducibility. NOT permanent code; going-forward writes already target `raw`.
- **DEBT-019-007 (Open, 2026-05-08)**: INDEC unpivot fix cubre time-pivoted layout (~14 tablas IPC/PIB/IPI), pero quedan ~9 tablas INDEC con otros bugs de parser:
  - DuplicateColumnError en `cache_indec_balance_pagos_fob_cif`, `cache_indec_turismo_*`, `cache_indec_pobreza_*`: cuadros con headers idénticos (3 cols todas se llaman "Total mensual" o "Período"). Necesita dedup post-parse antes del `to_sql`.
  - Headers degradados a `col_1`, `col_2`, `col_3` ... en ~6 tablas: la fila de headers reales no se detecta. Necesita mejorar `_detect_data_header_row` para cuadros INDEC con cell-merging extremo.

- **DEBT-019-008 (CLOSED 2026-05-16, `delitos_caba` v0.1.1+)**: el cluster `caba::delitos*` (20 versiones, 2.78M filas tras merge) almacena `latitud`/`longitud` en **3 encodings incompatibles**: decimal limpio (`-34.530394`), entero sin punto (`-34526608`) y puntos-de-miles argentinos (`-34.629.861`, `-3.461.534`). Un `::numeric` crudo abortaba el build entero (commit `ab858c1`). El mart YAML ahora normaliza con un `CASE` que stripea no-dígitos y reinserta el punto tras el dígito 2 (toda coord argentina tiene parte entera de 2 dígitos), convergiendo los 3 encodings al mismo valor. 97.5% de coords recuperadas dentro del bounding box CABA; los 708 outliers restantes son data quality upstream de versiones source mis-georreferenciadas. `cantidad` con guard `digits-only` por el mismo motivo. El normalizador es portable a cualquier mart con coords argentinas.

- **DEBT-019-009 (CLOSED 2026-05-19, `delitos_caba` v0.1.2)**: la columna `franja` del mart era texto `"0"..."23"` y NL2SQL no podía traducir "horario nocturno" / "madrugada" / "tarde" a un rango. El YAML ahora documenta el formato + los 4 rangos canónicos (madrugada 0-5, mañana 6-11, tarde 12-19, nocturno 20-23 OR 0-5) y la receta de filtro con `CAST AS NUMERIC` + regex guard `^[0-9]+$`. Acompañado por un ejemplo en `nl2sql.txt` ("robos en horario nocturno → COUNT GROUP BY barrio"). Round v4.2 F3 ahora hit el mart y devuelve "Palermo 4.454, Balvanera 3.368, Flores 2.398" en lugar de 0 filas + deflección. Patrón aplicable a cualquier mart con columnas enum-como-texto-numérico.

- **DEBT-019-010 (CLOSED 2026-05-16, `delitos_argentina_snic` v0.1.1)**: la descripción original del mart decía *"equivalente a nivel provincial vía agregación"* — verdadero para `cantidad_*` pero falso para `tasa_*` (las tasas por 100K hab son no-aditivas; sumar la tasa de cada departamento de una provincia da basura inflada). El YAML ahora distingue explícitamente las dos clases de columnas y desactiva la agregación equivocada — el LLM en v4.2 cambió `SUM(tasa_hechos)` por `SUM(cantidad_hechos)` correctamente. Acompañado por la regla `non-additive metrics` en `nl2sql.txt` y el flag `nonadditive_warning` que el analyst recibe (ver spec `001-query-pipeline`).

---

## Unificar fuentes cuyas columnas cambian de nombre entre años

Escrito el 2026-08-24 al construir `pauta_oficial`, porque el patrón se va a
repetir: es la forma normal de las series estadísticas argentinas, no una
anomalía de esa fuente.

Las 179 tablas de pauta oficial nombran los mismos tres campos de quince
maneras: `MEDIO` / `medio` / `Medio`; `IMPORTE` / `Importe` / `importe` /
`importe_pesos_ars` / `MONTO` / `Monto total`; `FECHA` / `fecha` / `Fecha` /
`FECHA_PUBLICACION` / `FECHA-PUBLICACION`. No es que las fuentes sean
descuidadas: cada año lo publica un equipo distinto.

**La forma que funciona** es declarar *todas* las variantes en
`expected_columns` de un solo macro y unirlas con `COALESCE`. El macro emite
`NULL::text` para la columna que una tabla no tiene, así que la unión mantiene
su forma y `COALESCE` se queda con la que exista:

```sql
COALESCE("MEDIO", "medio", "Medio")::text AS medio
```

Una rama de UNION por variante también funciona y envejece peor: cada año nuevo
agrega una rama, y la n-ésima se olvida.

**Lo que esta forma no resuelve**: una fuente con otra *estructura*, no otro
nombre. Ciudad de Mendoza publica su pauta como tabla cruzada por mes —las
columnas son `ENERO / DIARIOS DIGITALES`, `FEBRERO / …`— y eso necesita un
unpivot. Quedó fuera del mart y dicho en su descripción: declarar una fuente que
aporta cero filas es reclamar una cobertura que no existe.

**Y el importe casi nunca es un número.** El formato argentino `1.234.567,89`
usa el punto como separador de miles, así que un `::numeric` crudo falla sobre
buena parte de las filas. El `CASE` que discrimina los tres encodings ya se
escribió dos veces —`delitos_caba` y ahora éste—; la tercera vez conviene que
sea una macro.
