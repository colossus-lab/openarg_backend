# Spec 016 — Serving Port

**Type**: Forward-engineered
**Status**: Implemented 2026-05-04 (`IServingPort` + `LegacyServingAdapter` + `ServingResolver` + DI provider live; pipeline calls integrated for sandbox planner hints + NL2SQL semantics + layer telemetry)
**Last updated**: 2026-05-05 (post Sprint 0.5–0.7: staging deprecation propagated; `_PREFIX_FREE_SCHEMAS` for sandbox; `mart.foo` qualified-name heuristic)
**Hexagonal scope**: Domain (port + DTOs) + Infrastructure (adapter + DI)
**Sister specs**: [015-catalog-resources](../015-catalog-resources/spec.md), [017-raw-layer](../017-raw-layer/spec.md), [019-marts](../019-marts/spec.md)

---

## 1. Context & Purpose

Historically, the LangGraph pipeline read directly from `cache_*` tables and `table_catalog`. Any change in the underlying schema layout broke the pipeline. The medallion migration (raw → mart) requires the pipeline to see a stable surface while the physical layer evolves.

The Serving Port is that stable surface. Four async methods cover everything the pipeline needs: discover resources, fetch a schema, run a read-only query, fetch full catalog metadata. The adapter behind the port chooses which physical layer to read from — `cache_*` legacy, `raw.*`, or `mart.*` — based on what `catalog_resources.materialized_table_name` and `mart_definitions` resolve to.

> **2026-05-04 architectural change**: the staging layer was dropped in migration 0042 (see [018-contracts-staging](../018-contracts-staging/spec.md)). The medallion now goes raw → mart directly with no intermediate validation table. References to `staging.*` parsing below are kept only as historical notes — the adapter no longer attempts to resolve staging-qualified names because the schema does not exist.

## 2. Interface

`app.domain.ports.serving.serving_port.IServingPort`:

```python
class IServingPort(ABC):
    async def discover(query_text, *, limit=10, portal=None, domain=None) -> list[Resource]
    async def get_schema(resource_id) -> Schema
    async def query(resource_id, sql, *, max_rows=1000, timeout_seconds=30) -> Rows
    async def explain(resource_id) -> CatalogEntry
```

DTOs at `app.domain.entities.serving`:

| DTO | Purpose |
|---|---|
| `Resource` | A discoverable consultable resource (resource_id, title, domain, portal, layer). |
| `Schema` | Column list + types + optional `semantics` map (from `COMMENT ON COLUMN`). |
| `Rows` | Read-only query result with `truncated` flag. |
| `CatalogEntry` | Resource + Schema + sample queries + tags + parser_version + last_refreshed_at. |
| `ServingLayer` | Enum: `RAW`, `STAGING` (deprecated, retained only for backward DTO compat), `MART`, `CONNECTOR_LIVE_API`, `CACHE_LEGACY`. Surfaced in DTOs so callers can measure mart-coverage. |

Errors: `ResourceNotFoundError`, `WriteAttemptedError`, `QueryTimeoutError`.

## 3. Adapter (`LegacyServingAdapter`)

Current implementation. Lives at `app.infrastructure.adapters.serving.legacy_serving_adapter`. Reads:
- `discover()` — lexical ILIKE against `catalog_resources` + (when `OPENARG_DISCOVER_MARTS=1`, default ON) `mart_definitions`. Marts come first.
- `get_schema()` — when `resource_id` starts with `mart::`, resolves via `mart_definitions` and pulls `pg_description` for `semantics`. Otherwise resolves via `catalog_resources.materialized_table_name`, parsing the qualified shape (`raw."<bare>"`, plain `<bare>`) into `(schema, table)`. The `staging.<bare>` case is no longer reachable post-mig 0042 but the parser still recognises it for forward compat with any tooling that emits the qualified name.
- `query()` — read-only enforcement via regex; `SET LOCAL statement_timeout`; bounded result fetch.
- `explain()` — `get_schema` + the `catalog_resources` row.

> **Known gap (Sprint 0.5)**: `query()` still returns `layer=ServingLayer.CACHE_LEGACY` regardless of which schema the SQL touches, and `explain()` for `mart::*` resources fails because it always tries to read metadata from `catalog_resources` which marts are not registered in. Both are zombie code paths today (no consumer reads `Rows.layer` or invokes `explain()`), so the bugs are dormant — see DEBT-016-003.

`_layer_for_schema(schema_name)` maps Postgres schemas to `ServingLayer` so every DTO carries the layer it came from — useful for metrics, debugging, and the planner that prefers marts.

## 4. ServingResolver (per-request facade)

`app.application.pipeline.connectors.serving_resolver.ServingResolver` wraps `IServingPort` for use inside LangGraph nodes:

- Per-request schema cache (avoids repeating `information_schema` queries when the same resource is touched multiple times).
- `discover_for_planner(query_text)` returns `(resources, layer_counts)` so the planner / metrics can report mart-coverage.
- `OPENARG_PIPELINE_USE_SERVING_PORT` flag (default ON) — when 0, every method raises `RuntimeError` so call sites use their legacy fallback. Rollback escape hatch.

## 5. Functional Requirements

- **FR-001**: `IServingPort` MUST live in `domain/ports/`; the adapter MUST live in `infrastructure/adapters/`. The pipeline MUST NOT import the adapter directly.
- **FR-002**: DTOs (`Resource`, `Schema`, `Rows`, `CatalogEntry`, `ServingLayer`) MUST be backward-compatible across phases. Adding a field is allowed; changing or removing an existing field is not.
- **FR-003**: `query()` MUST reject any SQL that contains a write keyword (INSERT/UPDATE/DELETE/DDL) before reaching the database, raising `WriteAttemptedError`.
- **FR-004**: `query()` MUST enforce a per-call timeout via `SET LOCAL statement_timeout`. Default 30s.
- **FR-005**: `discover()` MUST surface `mart.*` resources before `cache_*` legacy when both match, controlled by `OPENARG_DISCOVER_MARTS` (default ON).
- **FR-006**: `Schema.layer` MUST faithfully report which physical schema backed the response. The pipeline uses this for metrics and prompt-construction decisions.
- **FR-007**: `LegacyServingAdapter` MUST gracefully tolerate `mart_definitions` not existing yet (migration 0041 not applied) by falling through to catalog_resources only.
- **FR-008**: `ServingResolver.get_schema()` MUST cache results within a single request. Cache invalidation on demand is NOT supported (the request lifetime is the boundary).

## 6. Pipeline integration points

| Node | Integration |
|---|---|
| `discover_catalog_hints_for_planner` | `_serving_port_planner_hints()` prepends mart/staging hits to the legacy `table_catalog` block. The planner LLM sees marts first. |
| `execute_sandbox_step` | `_mart_semantics_block()` appends `COMMENT ON COLUMN` semantics for mart tables to the NL2SQL context. |
| `execute_sandbox_step` | `detect_serving_layer_in_sql()` annotates each `DataResult.metadata['served_from_layer']` and emits a Prometheus counter. Read-only telemetry. |

## 7. Out of Scope

- Vector-search-based discovery (`mart_definitions.embedding` provisioned by mig 0041 — **populated 2026-05-05 in Sprint 0.2**, see DEBT-016-001 update; full vector-discovery wiring still pending).
- A full refactor of NL2SQL / executor to call `IServingPort.query()` directly (the executor still hits `ISQLSandbox`).
- Deprecation of `table_catalog` (lives behind `DEBT-015-003` cleanup; the canonical store is `catalog_resources`).

## 8. Tech Debt

- **DEBT-016-001**: `LegacyServingAdapter.discover()` and `_get_mart_schema()` use lexical ILIKE. Vector search should be wired once `catalog_resources.embedding` and `mart_definitions.embedding` reach >50% coverage in production. **Update 2026-05-05**: `mart_definitions.embedding` is now 6/6 populated (Sprint 0.2 backfill); `catalog_resources.embedding` ≈7%. Vector discovery for marts is the next natural step.
- **DEBT-016-002**: `query()` enforces read-only via regex. A more principled implementation would use a parsing-grade SQL validator (sqlglot is already a dep). Today's regex is good enough for the cases the pipeline produces, but a future user with raw SQL access should not rely on it.
- **DEBT-016-003** (Sprint 0.5/0.6 audit): three dormant bugs in `LegacyServingAdapter`:
  1. `query()` always reports `Rows.layer = CACHE_LEGACY` regardless of which schema was hit. Today there is no consumer of `Rows.layer` outside the serving module, so the bug has zero runtime impact, but any future metric or routing decision based on `layer` will be misled.
  2. `explain(mart::<id>)` resolves the schema correctly via `_get_mart_schema()` then unconditionally queries `catalog_resources` for the metadata row, which marts do not have, so it raises `ResourceNotFoundError`. Same dormancy: nobody invokes `explain()` in runtime today.
  3. `nl2sql.execute_sql_node()` calls `sandbox.execute_readonly()` directly instead of `IServingPort.query()`, so the layer telemetry / read-routing baked into the port never fires. Equivalent runtime behaviour today; the divergence becomes a real bug the moment someone adds layer-based logic in the executor.

  All three should be tackled as a single cleanup pass before adding any consumer that depends on the port's enriched output.

- **DEBT-016-004** (Sprint 0.5): `_mart_semantic_block` heuristic now accepts three planner-emitted forms — bare `mart_id`, `mart::<id>`, and qualified `mart.<view>`. The earlier `if "." not in tn` shortcut silently skipped the qualified form. Mentioned here so the next refactor of the planner emission contract is aware that all three shapes must keep resolving.
