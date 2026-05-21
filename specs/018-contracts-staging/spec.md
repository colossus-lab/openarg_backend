# Spec 018 — Contracts + Staging Layer (medallion L2) — DEPRECATED

> **DEPRECATED 2026-05-04**. Migration **0042** dropped the `staging` schema
> and `staging_contract_state` table. The contracts module
> (`app.application.contracts`), the `openarg.promote_to_staging` task and
> the `pandera`-based validator have all been removed.
>
> **Why**: contracts as a *gatekeeper* for staging caused ~95% of resources
> to never reach the layer (no domain-specific contract matched). The new
> design (B-rebuild) puts marts directly on top of `raw.*` via SQL macros
> that resolve at build time. Domain semantics live in mart definitions,
> not in a separate contract layer.
>
> **Replaced by**: [019-marts/spec.md](../019-marts/spec.md) — the post-rebuild
> medallion (raw → mart, no staging).
>
> The original spec content is preserved below for historical reference.

---

# Spec 018 — Contracts + Staging Layer (medallion L2)

**Type**: Forward-engineered
**Status**: REMOVED 2026-05-04 (mig 0042 dropped staging schema + state table; module + task deleted from codebase).
**Hexagonal scope**: Application (contracts + auto-typer + validator) + Infrastructure (Celery task + state registry)

---

## 1. Context & Purpose

Raw tables hold "what came from the portal" — TEXT-everywhere, no PK semantically meaningful, no schema guarantees. The pipeline cannot answer queries reliably against TEXT columns that should be dates or numbers, and there is no way to detect when an upstream feed changes shape.

The staging layer fixes both:
- **Auto-typing**: per-column heuristic infers `date / int / float / bool` and casts; columns that don't pass an 80% parse threshold stay TEXT.
- **Contracts**: per-taxonomy YAML declares required columns, expected types, and row-level checks. A dataset whose contract fails does NOT contaminate staging — it stays in raw with a `staging_contract_state.status='fail'` audit row.

## 2. Module layout

`app.application.contracts`:
- `contract.py` — `Contract` dataclass + YAML loader + `find_matching_contract`.
- `auto_typer.py` — `auto_type_dataframe(df, threshold=0.8) -> (typed_df, inferred_types)`.
- `validator.py` — `validate_dataframe(contract, df, inferred_types) -> ContractValidationResult` using `pandera`.

Public surface re-exported via `from app.application.contracts import …`.

## 3. Contract YAML shape

Per `config/contracts/<taxonomy>.yaml`:

```yaml
id: presupuesto
version: 1.0.0
description: |
  Datasets de ejecución y crédito presupuestario nacional / provincial.
matches:
  portals: [presupuesto_abierto, datos_gob_ar]
  title_patterns: ["*presupuesto*", "*ejecuci*n*"]
columns:
  - {name: anio, type: int, required: false}
  - {name: jurisdiccion, type: text, required: false}
  - {name: monto, type: float, required: false}
checks:
  - {rule: "row_count > 50", severity: fail, description: "..."}
```

`type` ∈ `{text, int, float, date, timestamp, bool}`. `severity` ∈ `{warn, fail}`.

Resources match contracts by portal, optional domain, and title fnmatch patterns. First-match-wins (filename order = precedence).

## 4. `staging` schema + `staging_contract_state`

Mig 0040: `CREATE SCHEMA IF NOT EXISTS staging` + `staging_contract_state` registry.

| Column | Type | Notes |
|---|---|---|
| `resource_identity` | TEXT | `{portal}::{source_id}`. PK part 1. |
| `contract_id` | TEXT | PK part 2. |
| `contract_version` | TEXT | PK part 3. Allows historical audit when contracts are bumped. |
| `status` | TEXT | `pass | warn | fail | skipped`. CHECK-constrained. |
| `staging_schema`, `staging_table_name` | TEXT | Where the promoted table lives (NULL when status != promoted). |
| `findings_json` | JSONB | List of `{column, severity, code, detail}`. |
| `row_count` | BIGINT | Rows in the typed DataFrame (post auto-typing, pre-promote). |
| `last_validated_at` | TIMESTAMPTZ | Default `NOW()`. |

## 5. Promote pipeline (`openarg.promote_to_staging`)

For a given `resource_identity`:

1. Load `(portal, title, domain)` from `catalog_resources`.
2. Load contracts from `config/contracts/`. Find first matcher hit; if none → record `status='skipped' / no_contract_matched`, exit.
3. Read latest live `raw_table_versions` row + `pd.read_sql` capped to `OPENARG_STAGING_MAX_ROWS` (default 500K).
4. Drop lineage columns (`_ingest_row_id`, `_ingested_at`, `_source_url`, etc.) so the contract author describes business columns only.
5. `auto_type_dataframe()` → typed DF + inference map.
6. `validate_dataframe()` → `ContractValidationResult(status, findings, row_count)`.
7. If `status == 'pass'` (or `'warn'` and `OPENARG_STAGING_PROMOTE_ON_WARN=1`):
   - DROP + CREATE `staging.<contract_id>__<resource_slug>`.
   - `df.to_sql(if_exists='replace')`.
   - UPDATE `catalog_resources.materialized_table_name = staging."<name>"`.
8. UPSERT `staging_contract_state(resource_identity, contract_id, contract_version, status, ...)`.

The task is idempotent on the PK triplet: re-running with unchanged inputs UPDATEs the audit row instead of duplicating.

Auto-trigger: `_apply_cached_outcome` enqueues `promote_to_staging.apply_async([resource_identity])` after a successful raw materialization, gated by `OPENARG_AUTO_PROMOTE_TO_STAGING`. The promote is skipped when raw registration failed.

## 6. Functional Requirements

- **FR-001**: Contracts MUST live as YAML in `config/contracts/`. Code MUST NOT hardcode contract definitions.
- **FR-002**: Auto-typing MUST treat `_*` lineage columns as untouchable (skipped from inference and from cast).
- **FR-003**: A dataset with `status='fail'` MUST NOT have its `staging.*` table updated. Raw stays as the materialized layer.
- **FR-004**: `staging_contract_state` MUST be UPSERTed regardless of promote outcome — operators need to see `'fail'` audit rows.
- **FR-005**: The promote task MUST be idempotent on `(resource_identity, contract_id, contract_version)` — re-runs replace prior findings.
- **FR-006**: When a raw materialization succeeds AND `OPENARG_AUTO_PROMOTE_TO_STAGING=1`, the auto-promote MUST fire as a separate Celery task (not inline) so the collector path stays decoupled from contract latency.
- **FR-007**: Auto-promote MUST be skipped when `_register_raw_version` failed in the parent transaction. The staging task would race a missing raw version.
- **FR-008**: pandera schemas MUST coerce types compatibly (e.g. accept `int` for a column declared `float`) so the auto-typer's narrower-type-when-possible behaviour does not produce spurious schema mismatches.

## 7. Success Criteria

- **SC-001**: After `promote_to_staging` runs on a raw table that matches a contract with all `pass` checks, the staging table exists with typed columns and `catalog_resources.materialized_table_name = staging."..."`.
- **SC-002**: Re-running the same task is a no-op for the audit row's PK and produces an identical staging table.
- **SC-003**: A contract bump (v1.0.0 → v1.1.0) creates a new audit row without disturbing the prior version's row.

## 8. Out of Scope

- Per-portal contract authoring at scale (today three skeleton contracts ship: presupuesto, series_economicas, staff).
- Custom check expressions beyond `row_count > N` and pandas `df.eval()` (which uses the python engine — a fast-path engine could be added later).

## 9. Tech Debt

- **DEBT-018-001**: `pandera` schema coerces declared types from the auto-typer's narrower types. If a column was declared `text` in the contract but the auto-typer inferred `int`, pandera coerces back to text. Document this round-trip.
- **DEBT-018-002**: `_evaluate_row_checks` calls `df.eval()` with `engine='python'`. The numexpr engine would be faster but does not support all expressions. Switch when the row-check vocabulary stabilises.
