# 021 — Parser Hardening & Schema Recovery

**Status**: In progress (started 2026-05-08)
**Depends on**: 002-connectors, 006b-ingestion, 010-collector-tasks, 015-catalog-resources

## 1. Problem

Multiple bug classes in the existing parser path leave a meaningful fraction of
ingested tables structurally broken. Quantified on staging on 2026-05-08:

| Bug | Tables affected | Symptom |
|---|---|---|
| `col_N` placeholder columns (header detection failed) | **506** (1,468 cols) | Tables landed but cols are anonymous → NL2SQL can't reason about them |
| Minimal tables (1–3 cols, parse fail) | **1,776** | Header probably in row 2–5, parser skipped real header |
| Title-as-column ("Cuadro 3.1 Pop. ...") | **74** (1,022 cols) | Title row promoted to header by mistake |
| URL-as-column (PDF artifacts) | **158** (363 cols) | PDFs scraped with pandas-default fallback, not a real PDF parser |
| `Páginas:_2` style placeholders | 5 (18 cols) | Same root cause as above |
| Time-pivoted columns (months/years as col names) | **286** | Wide-format that should be melted to long |
| Mega-wide tables (≥200 cols) | 81 | Subset of time-pivot or pivot-crudo |
| `error_category='unknown'` | **22,517 / 26,704 (84%)** | Classifier exists but doesn't recognize most patterns |

These propagate downstream: marts can't be built over them, NL2SQL routes to
them but produces nonsense answers, and re-ranker decisions get noisy.

## 2. Goal

Ship a per-bug-type fix that comes in **two halves** and applies them gradually:

1. **Parser change** — collector path produces correct tables for new ingestions going forward.
2. **In-place repair** — reusable function that fixes already-landed tables via DDL/DML, applied in small batches without re-downloading from source.

Re-ingest from upstream is reserved for cases where in-place is structurally
impossible (e.g. `DuplicateColumnError` aborted table creation → no table to
repair). User preference (memory: `feedback_inplace_repair_vs_reingest.md`):
in-place + gradual over re-ingest masivo.

## 3. Non-goals

- Auto-typing TEXT → numeric/date (FIX-021). Reasoning: see `feedback_no_autotyping_priority.md` (TBD) — most queries don't fail because of typing, NL2SQL casts on-the-fly, and aggregation layer is the mart (which already declares types).
- New connectors / data coverage expansion (DDJJ scope, down portals).
- Schema/table catalog improvements beyond what these fixes touch.

## 4. Approach

Eight phases. Each phase ships parser change + repair function + tests.
Each phase is an independent PR to `staging`, with checkpoint before merge.

```
0: Baseline telemetry            (committed: scripts/parse_quality_diag.sql + this spec)
1: Promote INDEC funcs to shared   ◄─ unblocks 2, 5
2: Header detection robust         ◄─ depends on 1
3: PDF parser proper               (independent)
4: Schema-drift in marts           ◄─ depends on 1
5: Time-pivot generalized          ◄─ depends on 1
6: error_category fix              (independent)
7: Repair-in-place orchestrator    ◄─ depends on 1, 2, 3, 5 (replaces "re-ingest masivo")
8: Validation gate + new marts shipped
```

### Phase 0 — Baseline telemetry

- `scripts/parse_quality_diag.sql` committed.
- This spec committed.
- Capture initial values (snapshot 2026-05-08, see §5).

### Phase 1 — Promote INDEC functions to shared

`indec_tasks.py` already contains the right primitives (`_promote_buried_headers`,
`_dedupe_column_names`, `_truncate_utf8_bytes`, `_is_time_col`,
`_unpivot_if_time_pivoted`) but they're not reusable. Move them to
`src/app/application/pipeline/parsers/`:

```
parsers/
├── column_normalization.py    # truncate_utf8_bytes, dedupe, placeholder/url/title detectors
├── header_recovery.py         # detect_data_header_row, promote_buried_headers
├── time_pivot.py              # is_time_column, unpivot_if_time_pivoted
└── hierarchical_headers.py    # (already exists)
```

Tests for each module under `tests/unit/parsers/` covering real-world cases
that exist on staging today.

`indec_tasks.py` refactored to import from `parsers/` (no behavior change).

**No wiring to `collector_tasks.py` yet** — that's Phase 2.

**In-place repair**: not applicable in Phase 1 (no behavior change).

### Phase 2 — Header detection robust

Two new behaviors in `header_recovery.py`:

- `recover_buried_headers(df)` — when >40% of cols are `col_N`, search rows 1–8 for the most plausible header (high unique-text ratio, low NaN, low numeric).
- `forward_fill_merged_headers(df_raw, header_rows)` — generalize the existing `_forward_fill` to multi-row + multi-column merged Excel cells.

Wire into `_read_excel_frame` (collector_tasks.py:2912) and `_read_csv_preview` (collector_tasks.py:2137) — both already do a header retry, this extends that retry.

**In-place repair**: `repair_col_n_headers(table_name)`:
- If a table has cols like `col_1, col_2, ...`, query first 5 data rows.
- If row 0 looks like a header (text values, no nulls, unique), `ALTER TABLE RENAME COLUMN col_N TO <real_name>` and `DELETE FROM <table> WHERE ctid = <first_row_ctid>`.
- Idempotent. Logs every rename to a new table `parse_repair_audit` for rollback if needed.

Apply gradually: admin endpoint `POST /api/v1/admin/parse-repair` with `{phase: 'col_n', limit: 20, dry_run: bool}`.

### Phase 3 — PDF parser proper

Add `pdfplumber` as dep (pure Python, no Java). Create `parsers/pdf.py`:

- `extract_tables_from_pdf(path) -> list[pd.DataFrame]`
- `merge_consecutive_tables(tables)` for multi-page tables with same schema

Wire into collector when `format == 'pdf'` (currently routed to a generic fallback that produces the URL-as-column garbage).

**In-place repair**: not feasible (the data lost during PDF → garbage parse can't be reconstructed without re-parsing). Solution: re-parse from S3 cache if the original PDF is still there, or mark `policy_non_tabular` if not. Tracked as opt-in re-parse, not bulk re-ingest.

### Phase 4 — Schema-drift in marts

Macro `live_tables_by_table_pattern` accepts `optional_columns` — when a source
table doesn't have an optional col, the SELECT emits `NULL::type AS <name>`
for that source. YAML schema gains `optional_columns:` field.

Re-build affected marts (`mart.demografia_caba` v2, etc.).

**In-place repair**: drop + rebuild materialized views (already idempotent).

### Phase 5 — Time-pivot generalized

Wire `unpivot_if_time_pivoted` into `_read_excel_frame`/`_read_csv_preview`.
Tunable threshold per portal (CABA / censo can go below 0.50).

**In-place repair**: `repair_time_pivoted(table_name)`:
- Detect: ≥40% cols match time pattern.
- Build long format via SQL: `CREATE TABLE <table>_long AS SELECT id_vars, key AS periodo, value AS valor FROM <table>, LATERAL (VALUES (col_1, val_1), (col_2, val_2), ...) v(key, value)`.
- Swap names atomically: `_pivoted` ← `<table>`, `<table>` ← `_long`.
- Update `cached_datasets.table_name` if needed; emit catalog re-enrich event.

### Phase 6 — error_category fix

Sample 500 unknowns, expand `_classify_collector_error` patterns,
re-categorize legacy via idempotent script.

**In-place repair**: just an UPDATE pass on `cached_datasets`.

### Phase 7 — Repair orchestrator

Single Celery task `parse_repair_orchestrator(phase, batch_size=20)` that
walks `parse_quality_snapshot`, picks N candidates, runs the appropriate
repair function, records result. Beat schedule: hourly during rollout, then
disabled.

**Replaces** the originally-planned "re-ingest masivo" — gradual + observable.

### Phase 8 — Validation + new marts

Re-run `parse_quality_diag.sql`, compare with §5 baseline. If targets met, ship marts that were blocked:
- `mart.afiliados_obras_sociales` (post Phase 3 PDF)
- `mart.violencia_genero_caba` (post Phase 5 time-pivot)
- `mart.indec_turismo`, `mart.indec_pobreza`, `mart.indec_balance_pagos` (post Phase 2)
- `mart.demografia_caba` v2 (post Phase 4)

## 5. Acceptance criteria

| Metric | Baseline (2026-05-08) | Post-rollout (2026-05-09) | Target (Phase 8) | Status |
|---|---|---|---|---|
| Tables with `col_N` cols | 506 | 296 | < 50 | partial — long tail needs Unnamed: pattern + multi-row recovery |
| Tables with 1–3 cols (parse fail) | 1,776 | not re-measured | < 500 | follow-up |
| Tables with title-as-col (`Cuadro X.Y`) | 74 (1,022 cols) | 69 (281 cols) | < 10 | **cols −72 %** ✅ |
| Tables with url-as-col | 158 | 132 → 107 post col_n applies | < 20 | partial; many are data not parser garbage |
| Tables time-pivoted in raw | 286 | only INDEC/PDF unpivoted; rest deferred | < 50 | Phase 5 still pending in collector path |
| `error_category='unknown'` (with text) | 22,517 / 26,704 (84 %) | 22,194 success-path NULL-msg + 0 with text | < 5,000 | **100 % of text-bearing rows classified** ✅ |
| Marts shipped | 13 | 17 | 17+ | ✅ |

## 6. Execution log (2026-05-08 / 2026-05-09)

The plan was rolled out across two consecutive days against staging.
All operations are recorded in `parse_repair_audit` for reversibility.

**Phase 1 — refactor (commit 0dfe12e)**
  - 3 new modules in `parsers/`: `column_normalization.py`,
    `header_recovery.py`, `time_pivot.py` (~700 LOC).
  - `indec_tasks.py` reduced 784 → 510 LOC (uses shared primitives).
  - 48 unit tests, all green. No behavior change.

**Phase 2 — wire collector + repair (commit 0f9ce7f, half)**
  - `_post_parse_normalize` wired into `_read_excel_frame` and
    `_read_csv_preview`. Idempotent.
  - Repair module: `propose_col_n_rename` (pure) +
    `repair_col_n_table` (DDL).
  - Migration 0048: `parse_repair_audit` table.
  - Admin router `/api/v1/admin/parse-repair/{run,candidates,audit}`.
  - **56 col_n applies** on staging (semantic recovery: `col_2..col_5` →
    `Total Provincia_2010..2014` etc.).

**Phase 2.b — heuristic improvements (same commit)**
  - Year-only header rows are skipped by `find_data_start_row` so a
    row of repeated `2003, 2003, ...` doesn't trip as data.
  - Argentine number format `2.408.854` recognised as numeric.
  - Valid original col names forward-fill horizontally so a parent
    label like `Total Provincia` propagates through `col_2, col_3,
    col_4` children of merged Excel cells.
  - `_*` metadata cols preserved as-is.

**Phase 2.c — trailing garbage trim (same commit)**
  - `repair_trailing_garbage_cols` drops cols whose name is garbage
    AND whose contents are >99 % empty over a 5k-row sample.
  - **99 trailing_garbage applies** on staging, ~3,400 columns dropped.

**Phase 3 — PDF parser (same commit)**
  - `pdfplumber>=0.11.0` added. New `parsers/pdf.py`.
  - `%PDF-` magic-byte detection + `.pdf` URL ext → `fmt == "pdf"`
    branch in `collector_tasks.py`. Branch runs:
    `parse_pdf_file → unpivot_if_time_pivoted → _post_parse_normalize`.
  - Verified end-to-end: tucuman PDF "Precio Vagón Ingenio Azucar"
    went from `permanently_failed:parse_format` → 84 long-format rows
    `(Mes, periodo, valor)` ready to query.

**Phase 6 — error_category extension (same commit)**
  - Migration 0049 extends the CHECK constraint with three new
    buckets — `header_degraded`, `orchestration_rerouted`,
    `truncation_sampled`.
  - Classifier extended to recognise SSL failures, JSON/XML parse
    errors, pandas internals (`low_memory`, `truth value of array`,
    `unmatched`, etc.) and `NumericValueOutOfRange`.
  - UPDATE pass re-categorised legacy unknowns. After: every row with
    a non-null `error_message` is correctly classified.

**Cleanup operations (out of scope of original plan but executed in
same window)**
  - `cleanup_superseded`: 1,899 physical tables matching
    `raw_table_versions.superseded_at IS NOT NULL` AND no `cached_datasets.status='ready'` reference were dropped. 0 errors.
  - `cleanup_orphan`: 785 raw-form tables (`<connector>__<dataset>__hash__vN`)
    with no rtv entry AND no active CD reference were dropped. 0 errors.
  - Total physical tables: 27,776 → 25,210 (−2,566 / −9 %).

## 7. Tech debt opened

- **DEBT-021-001 — Phase 5 generalised**: `unpivot_if_time_pivoted` is
  wired only in INDEC tasks and the PDF branch. Generalising to
  `_read_excel_frame` + `_read_csv_preview` is risky (changes shape) but
  desirable. ~286 staging tables still time-pivoted in raw.
- **DEBT-021-002 — Long tail not recoverable by heuristic**: ~76 % of
  candidate tables ship with `no_improvement` from
  `propose_col_n_rename`. Their structures are genuinely heterogeneous
  (data transposed, multi-level NaN-hierarchical, sparse text). Either
  per-portal custom rules or LLM-assisted recovery is the next angle.
- **DEBT-021-003 — Legacy `cache_*` migration**: 5,611 `cache_*` tables
  are still in `public` and not registered in `raw_table_versions`.
  Many are in active use (`cached_datasets.status='ready'`). Migrating
  them to `raw.*` would unify the storage model and simplify routing,
  but the connectors that own them must move first.
- **DEBT-021-004 — Unnamed cols metric grew during re-collect** (CLOSED 2026-05-09, commit 59dfd8b):
  root cause was string-form NaN sentinels (`"None"`, `"nan"`, `"s/d"`, ...)
  emitted AS-IS by pandas when reading CSVs that use them as missing-value
  markers. The sentinel cells are non-NaN in pandas terms, so
  `df.dropna(axis=1, how='all')` left those cols intact, and the same
  blind spot existed in `repair_trailing_garbage_cols`'s populated-ratio
  SQL. Fixed by `_normalize_string_nan` (replace sentinels with real NaN
  before dropna) and an extended sentinel list in the repair function's
  CASE WHEN. Verified end-to-end on the egregious case
  `innovacion_sector_manufacturero` (1,400 → 9 cols). Global
  `Unnamed:` cols 20,296 → 4,935 (−76 %).

## 8. References

- Plan triggered by user feedback (2026-05-08): "los marts que no pudiste crear muchos fue porque la data estaba desordenada".
- Baseline measurement: `scripts/parse_quality_diag.sql` run against staging 2026-05-08.
- Devil's advocate against auto-typing as alternative: documented above (§3 non-goals).
- Audit trail: `SELECT phase, COUNT(*) FROM parse_repair_audit WHERE dry_run=false AND ok=true GROUP BY 1` (post-rollout: 56 col_n + 99 trailing_garbage + 1,899 cleanup_superseded + 785 cleanup_orphan = 2,839 ops).
