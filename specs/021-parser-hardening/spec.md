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

| Metric | Baseline (2026-05-08) | Target (post Phase 8) |
|---|---|---|
| Tables with `col_N` cols | 506 | < 50 |
| Tables with 1–3 cols (parse fail) | 1,776 | < 500 |
| Tables with title-as-col | 74 | < 10 |
| Tables with url-as-col | 158 | < 20 |
| Tables time-pivoted in raw | 286 | < 50 |
| `error_category='unknown'` | 22,517 | < 5,000 |
| Marts shipped | 13 | 17+ |

## 6. Tech debt opened

To be filled per phase.

## 7. References

- Plan triggered by user feedback (2026-05-08): "los marts que no pudiste crear muchos fue porque la data estaba desordenada".
- Baseline measurement: `scripts/parse_quality_diag.sql` run against staging 2026-05-08.
- Devil's advocate against auto-typing as alternative: documented above (§3 non-goals).
