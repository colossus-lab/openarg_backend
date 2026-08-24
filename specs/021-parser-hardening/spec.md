# 021 — Parser Hardening & Schema Recovery

**Status**: In progress (started 2026-05-08, expanded 2026-05-09 with title_as_columns repair)
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

**Phase 2.d — title-as-columns repair (2026-05-09)**
  - New repair function `repair_title_as_columns_table` +
    `propose_title_as_columns_rename` (pure) in
    `src/app/application/repair/parse_repair.py`.
  - Detects a distinct fingerprint from `col_N`: when ≥30 cols share a
    common prefix ≥20 chars (dedup of a merged-cell title row), row 0
    is mostly NULL (separator), row 1 has ≥5 alpha-bearing cells (real
    headers). Heuristic captures the case where pandas read the merged
    Excel title row as the header.
  - `propose_col_n_rename` rejects this fingerprint with
    `garbage_ratio_below_threshold` (the cols carry text, not
    `col_N` placeholders), so it needed a dedicated detector.
  - Helpers reused across both repairs: `_normalize_header_to_identifier`
    (snake_case + accent strip + leading-digit `col_` prefix),
    `_dedupe_identifiers` (`_2, _3, ...` suffixes), `_common_prefix`
    (longest common prefix trimmed of digits/whitespace).
  - Audited under `phase='title_as_columns'`, same `parse_repair_audit`
    table.
  - **13 PAMI tables repaired** (`pami__compras_y_contrataciones_de_nivel_central_2*`):
    cols `LISTADO DE LLAMADOS DE LICITACIONES PUBLICAS - AÑO XXXX / Gc_2..._31`
    → `n_l_p / expediente / objeto / destino / ...`. Unblocked
    `mart.pami_compras_publicas` (409 rows shipped 2026-05-09).
  - 6 unit tests in `tests/unit/test_parse_repair.py`: pami pattern,
    too_few_cols, no_common_prefix, row0_not_separator,
    row1_not_header_like, dedupe collision.

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

- **DEBT-021-001 — Phase 5 generalised** (CLOSED 2026-05-09, commit 6ff1b4a):
  `unpivot_if_time_pivoted` wired into `_post_parse_normalize` after the
  standard recover/dedup pass. Threshold gate (≥50 % time cols + ≥1 id
  col + ≥5 total cols) keeps small/non-pivoted tables untouched.
  `OPENARG_GENERIC_UNPIVOT` env flag (default ON) gives ops a kill
  switch without redeploy. Verified active on worker-collector via
  smoke test.
- **DEBT-021-002 — Long tail not recoverable by `col_N` heuristic** (PARTIAL 2026-05-09): ~76 % of
  candidate tables ship with `no_improvement` from
  `propose_col_n_rename`. Their structures are genuinely heterogeneous
  (data transposed, multi-level NaN-hierarchical, sparse text). One
  sub-pattern was extracted in Phase 2.d (title-as-columns: long
  shared prefix + NULL separator + buried real header) and now has its
  own detector + repair. Remaining patterns identified for future
  sprints: (a) transposed tables (rows-as-cols), (b) hierarchical NaN
  parents (Excel grouped headers with merged cells across rows AND
  cols), (c) sparse text without a header row at all (PDFs scraped to
  random cell positions). Either per-portal custom rules or
  LLM-assisted recovery is the next angle for those.
- **DEBT-021-003 — Legacy `cache_*` storage convention** (CLOSED 2026-05-09):
  Current policy: ALL cached tables (data persisted by either the standard collector path or any vía-B connector) live in the `raw` schema. The 11 vía-B connector tasks (`bcra`, `bac`, `cordoba_leg`, `dkan`, `georef`, `gobernadores`, `indec`, `mapa_estado`, `presupuesto`, `senado`, `series_tiempo`) write with `to_sql(..., schema='raw')` + `register_via_b_table(schema_name='raw')`. The Postgres role's `search_path = raw, public, "$user"` so unqualified SQL (e.g. examples in `nl2sql.txt`) resolves to the right schema automatically. Marts reference tables via the `live_table('<portal>::<sid>')` macro that expands to `raw."<table>"` at build time. **A fresh database is born in this shape** — there is no "legacy migration" step on a clean deploy.
  - 2 connectors intentionally NOT migrated: `staff_tasks.py` and `senado_staff_tasks.py` keep `schema_name="public"` for `senado_staff`, `staff_changes`, `staff_snapshots`. Those are operational metadata tables (not cached datasets) and legitimately live in public.
  - One-shot procedure for an existing DB that accumulated cache_* legacy in public (only applies to a DB that evolved incrementally): mass `ALTER TABLE … SET SCHEMA raw` + UPDATE `raw_table_versions.schema_name` + UPDATE `catalog_resources.materialized_table_name` to qualify with `raw."<table>"` + `ALTER ROLE … SET search_path` + mass rebuild of marts. Documented in `MEMORY.md` for operational reproducibility. NOT permanent code.
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
- Audit trail: `SELECT phase, COUNT(*) FROM parse_repair_audit GROUP BY 1` (snapshot 2026-05-09):
  - `cleanup_superseded`: 1,899
  - `col_n`: 1,647 (after subsequent batches)
  - `trailing_garbage`: 1,501 (after subsequent batches)
  - `cleanup_orphan`: 785
  - `title_as_columns`: 26 (13 dry_run + 13 apply, all PAMI)
  - **Total**: 5,858 audit ops.

---

## Round 2 — 2026-08-23

### R2.1 Repair and status never met

`parse_repair.py` has held fixes for these shapes since May and applied them
thousands of times — **always because a person went to an admin route and asked.**
A table could be fixed and its resource would stay `error` forever, because
nothing looked again. 546 sat like that, every one with its table intact on disk.

`rescue_rejected_resources` closes the loop: repair with the existing tiers, ask
whether the names are clean, and only then promote.

**Promotion is gated on the answer, not the attempt.** A repair that ran and
changed nothing must leave the resource rejected; flipping it to `ready` for
having tried would serve `col_3` to someone asking about poverty.

### R2.2 The gate let through what it was built to stop

`is_garbage_column` knows `col_3`, `Unnamed:` and URLs. It does not know the
shape most rejected tables actually have: a title row read as a header, so every
column is the same sentence with `_2`, `_3` glued on.

The first run promoted a table whose columns were `['Conformación Cartográfica
de Localidades Censales 2008 por De', '… por _2', …]`. **A gate that passes what
it was built to stop is worse than no gate, because it launders the result.**

Detection is by **longest common prefix**, not identical stems: Postgres
truncates identifiers at 63 bytes, so the first column often ends mid-word while
its siblings end in `_N`, and the stems never match.

### R2.3 A sibling repair, and the `.0` that hid a family of tables

`propose_title_as_columns_rename` expects a separator row: title as header, row 0
blank, real header in row 1. The shape found on 2026-08-23 has **no separator** —
the real header is row 0 itself. 116 **servable** tables carry it, holding
291,436 rows, and they are not obscure: `acceso_de_mujeres_a_la_salud`,
`casos_penales_contravencionales_violencia`, `educacion_sexual_integral`.

**Three columns is enough.** The May heuristic requires thirty on the reasoning
that this happens in wide tables. These are eight wide and just as unusable; the
width was never what caused the defect.

**And a gap in shared code**: pandas reads a year column as float, so a header
arrives as `2018.0` and `int()` refuses it. `['DEFUNCIONES MATERNAS', '2017',
'2018.0']` was not recognised while the same row without decimals was. A whole
family of statistical tables turned on that `.0`.

Years are named `anio_2018`, not `col_2018_0` — two spellings of the same idea,
neither saying what the column holds.

For narrow tables the shared detector needs three numeric cells and
`['Departamento','2020','2021']` has two. Rather than loosen a threshold that
changes the parser for everyone, the weaker evidence is judged locally and is
**stricter** to compensate: *every* numeric cell must be a year.

### R2.4 Retrying what failed because of us

`error_category` already separates our failures from the source's and nothing
read it that way. 1,031 resources sat at `orchestration_recovery_loop` since
2026-05-06.

**The first run contradicted its own premise**: 2 succeeded, 147 failed again,
134 at `placeholder_headers`. The files download; the parser cannot name their
columns. `orchestration_recovery_loop` recorded *how* they died, not *why*.

The sweep still earns its place, for a different reason than it was built for:
it is self-limiting — a retry rewrites `error_category` to the real reason, and
the real reasons are not retryable — and it converts an unusable label into a
usable one. **A population nobody can characterise is a population nobody fixes.**

### R2.5 Acceptance criteria for this round

Spec 021 states its requirements as measured criteria rather than FRs, so these
follow that form. Baseline measured 2026-08-23 before any of round 2 ran.

| Metric | Baseline (2026-08-23) | Target | Status |
|---|---|---|---|
| Rejected resources whose table is intact and unrepaired | 546 | < 50 | sweep scheduled 06:50 |
| Servable tables with a smeared title | 116 (291,436 rows) | 0 | sweep scheduled 06:30 |
| Resources stuck at `orchestration_recovery_loop` | 1,031 | < 100 | 653 eligible; self-limiting |
| A promoted resource with unusable column names | — | 0 always | gate + test |

Three rules this round adds, stated as invariants because they are what the
sweeps must never violate:

- A repaired table's resource is promoted **only** when its column names pass
  the cleanliness gate — never on the strength of a repair having run.
- The cleanliness gate rejects a smeared title by longest common prefix,
  independent of column count.
- A header row promoted to column names is deleted in the same transaction as
  the rename; a rename that landed without the delete would leave `anio_2018`
  holding the literal string `2018`.
- Only `error_category` values naming our own orchestration may be retried. The
  source's refusals and our own correct policy decisions must not.
