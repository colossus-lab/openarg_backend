# Plan 025 — Self-Repair

**Spec**: [./spec.md](./spec.md)
**Status**: Draft — awaiting approval
**Sequencing rule**: each phase is blocked by the one before it for a stated
reason. The reasons are not process; skipping any of them produces a system that
repairs the wrong thing.

---

## Starting state (verified 2026-08-21, not assumed)

Everything below was checked against the running systems, because the first
draft of this plan was written on inference and six of its claims were wrong.

**Staging** — alembic 0057; running CI images with **no hot-patches** (confirmed
by reading the files inside the containers); 27,578 snapshots over 27,487 tables;
203 pairs classified (91 same-table, 112 version); 5 actionable, all of them ours;
0 exonerated by any gate.

**Production** — alembic 0057; baseline complete (23,609 captured, 0 skipped);
running images built locally at `d8fd152`, so it **lacks `663003e`**: version
pairing, the superseded-inclusive baseline, and the G0/G2 context. No operational
effect there today — production holds 0 version pairs, because
`retain_raw_versions` removed 19,906 superseded tables in May — but the code
diverges and that should not be left standing.

**`main`** is still at `cb19f5e` (2026-08-01). Production runs `:latest` tags
applied locally, so a routine `docker compose pull` would revert them.

**What is already true and needs no work**

- The evidence store runs. `retry_s3_uploads` resumed the day
  `raw.cached_datasets` was restored and is uploading raw files.
- `uq_raw_table_versions_table_name` is UNIQUE on `(schema_name, table_name)`, so
  the report's LEFT JOIN onto the registry cannot multiply pairs. Checked because
  it was a real risk in code shipped the same day, not because it was obvious.
- `profile_similarity` returns `0.0` with no sampled values, so the verifier
  cannot be fed a manufactured score.

**What was fixed while auditing this plan**

- The report counted `legacy:unknown` as provenance, so coverage read 26,435 when
  the figure G1 could use was **0**. Both numbers are now reported.
- `024` DEBT-024-001 and DEBT-024-005, and `023` DEBT-023-005, described a world
  that no longer holds. Corrected in place.

---

## Phase 1 — Attribution

*Blocks everything. Without it the system cannot tell our regressions from the
portals' changes, and would "repair" our own improvements.*

### 1.1 A derived parser fingerprint

**New** `src/app/application/catalog/parser_fingerprint.py`

```
parser_fingerprint() -> str        # e.g. "p:9f3c1a7e"
normalization_fingerprint() -> str
```

Computed from the source of the modules that decide shape —
`app/application/pipeline/parsers/*` plus the parse entry points in
`collector_tasks` — hashed and truncated. Computed once per process and cached.

Chosen over a configured constant for a reason now confirmed rather than
suspected: `_DEFAULT_PARSER_VERSION` (`collector_tasks.py:268`) reads
`OPENARG_PARSER_VERSION`, and staging has it **set to the literal string
`2026-05-04`**. The mechanism is not broken — it has been faithfully stamping
what it was given for 21,989 rows. A value that only changes when a person edits
an environment file cannot distinguish a parser change from no change, which is
the one thing G1 needs.

*Risk*: the fingerprint changes on a comment edit, producing a spurious G1
exoneration. Mitigation: hash the parsed AST with docstrings stripped, not raw
bytes. Test: editing a comment must not change the fingerprint; changing a
literal must.

### 1.2 Migration 0058 — `raw_table_versions.normalization_version`

The column does not exist there (verified 2026-08-21); `raw_schema_snapshots` has
it since 0057. Nullable text, no backfill — historical rows are genuinely unknown
and inventing a value would defeat FR-005.

**Only worth shipping together with 1.1.** Nothing in the codebase computes a
general normalization version today — `censo2022_ingest.py` hardcodes `"1"` and
is the only writer. Adding the column first would create a second field that
means nothing, which is the exact defect this phase exists to remove.

### 1.3 Stamp on every write path

Verified: sites at `collector_tasks.py:5012` and `:6910` pass
`_DEFAULT_PARSER_VERSION`; sites at `:4602` and `:4835` forward a parameter
declared `parser_version: str | None = None` that callers do not supply. The
database corroborates the split exactly — 21,989 rows with the env value, 6,089
NULL. `_db.register_via_b_table` accepts no provenance argument at all.

All of them must record the fingerprints.

### 1.4 Read provenance from the registry

`schema_snapshot._PROVENANCE_SQL` reads `catalog_resources`, which holds the
resource's *current* value — identical on both sides of a historical pair, which
is why G1 is blind. Read `raw_table_versions` (per-version) first, fall back to
the catalog.

### 1.5 `Verdict.UNATTRIBUTABLE`

New verdict in `app/application/drift/classifier.py`, returned when provenance is
absent on either side. Distinct from `UNEXPLAINED`, counted separately by
`summarize`, and excluded from the actionable pile — a change we cannot attribute
is not a change we should act on.

**Done when**: SC-001 and SC-002. A pair captured across a fingerprint change is
exonerated by G1; today's five findings re-report as `UNATTRIBUTABLE`.

---

## Phase 2 — Evidence: fix what exists, do not build a second one

*Blocks the code lane only. The first draft of this plan proposed a new table and
a new capture path. That was wrong: the archival exists, is scheduled, and had
been failing since 2026-08-03 on the same missing `raw.cached_datasets` as
everything else. Restoring that table unblocked it, and uploads began succeeding
the same day.*

### 2.1 Confirm the archival keeps running

`s3_tasks.retry_s3_uploads` is on beat, `S3_BUCKET=openarg-datasets-staging` is
configured, `_upload_to_s3` stores the raw file. First run after the restore: 17
uploads and climbing, with 404s where the source URL has already died. No new
migration, no new table, no new capture path.

Work: watch the rate, measure the 404 share, and confirm it converges rather than
stalling on the same failures.

### 2.2 The real gap — bytes that are not the bytes

`upload_dataset_to_s3` re-downloads from `download_url` **at archival time**, so
it stores whatever the URL serves now, not what produced the stored table. For a
drift study that is precisely the assumption that cannot be made.

Two options, and the choice needs a measurement first:

- **Capture at ingest** — keep the bytes already in hand during the download the
  collector is doing anyway, instead of fetching them a second time later.
  Correct, and costs a second write on the ingest path.
- **Keep re-download, record the difference** — cheaper, but every sample needs a
  flag saying it may not match, and the corpus has to honour it.

Either way, FR-007b: store the content hash and compare against
`raw_table_versions.source_file_hash`, which already exists. A replay must be
able to refuse bytes that are not the ones under study.

### 2.3 Corpus builder

`app/application/repair/corpus.py::build_regression_corpus()` — joins
`parse_repair_audit` to the archived objects, yielding
`(sample, expected_columns)`.

Size, measured rather than assumed: **543** usable pairs (`operation='apply'`,
`ok`, not `dry_run`), all with `old_columns <> new_columns`. Not 8,287 — the rest
are `skip` rows where no repair was made. 543 is enough to validate a parser
change against a class, and not enough to train anything.

**Done when**: SC-006 — one historical repair replays end to end, against bytes
whose hash matches the version that produced it.

---

## Phase 3 — The router

*This is the piece that makes the hybrid real rather than rhetorical.*

### 3.1 Change signature

`app/application/repair/signature.py::signature_for(verdict) -> str`

Normalises a diff into a fingerprint that is independent of specific column
names, so two resources broken the same way collide. First cut, to be revised
against real data (DEBT-025-002):

- change class (`rename` / `reshape` / `additive` / …)
- shape predicates: *added names contain a long common prefix*, *removed names
  look like `col_N`*, *added names contain whitespace and punctuation typical of
  a title row*, *added names are the collector's own `_source_*` metadata*
- counts bucketed, never exact

The four PAMI findings must collide; the energy `reshape` must not join them.

### 3.2 Recurrence, measured before enforced

`report_signature_recurrence` — occurrences per signature over a window, by
distinct resource. **Reports only.** FR-011: the threshold comes out of this
distribution, not out of this document.

### 3.3 The routing decision

`app/application/repair/router.py::route(verdict, recurrence) -> Lane`

- `EXONERATED` / `UNATTRIBUTABLE` → no lane (FR-016)
- recurrence below threshold → `DATA`
- at or above → `CODE`, **and still `DATA` for this table** (FR-012): escalation
  is about the parser, the table is broken either way

**Done when**: SC-005 — a recurring signature is reported as a code-lane
candidate with its count, and nothing is written.

---

## Phase 4 — The data lane

### 4.0 A revert, before anything applies automatically

**New** `app/application/repair/revert.py::revert_repair(run_id | audit_id)`

Verified 2026-08-21: **no revert exists**. Nothing matching `revert`, `rollback`
or `undo` appears in `app/application/repair/` or the admin router. The audit
table records `old_columns`, so reversal is possible in principle — but that
makes reversibility a property of the *data*, not of the system, and "we could
reconstruct it by hand" is not a rollback.

Every argument in this spec for letting the data lane act rather than only
propose rests on its failure being cheap and reversible. Until this exists, that
argument is not true, and no automatic repair may ship.

Scope: read an audit row, rename the columns back, refuse if the current columns
no longer match `new_columns` (someone else changed the table since), and record
the reversal as its own audit row.

### 4.1 The verifier, second

`app/application/repair/verify.py::verify_repair()`

Open a transaction, apply, profile the result with `collect_snapshot`, compare
column-wise against the previous version's stored profile using the existing
`profile_similarity`, roll back unless it improves. Nothing in this phase may be
built before this exists — it is the only thing standing between self-repair and
self-harm.

*Known limitation* (DEBT-025-003): similarity is measured against a previous
version that may itself be wrong. In the PAMI cases v1 was right and v2 wrong,
and the profile alone does not say which. Until direction is solved, the verifier
should require the repaired profile to be closer to the *older* version — the
`title_as_columns` failure always moves away from clean names, so "closer to
older" is the right direction for this class. Stated as a limit, not a solution.

### 4.2 Deterministic tier

`app/application/repair/lane_data.py` maps a signature to the repair that already
exists — `repair_title_as_columns_table`, `repair_col_n_table`,
`repair_trailing_garbage_cols`. They have applied 543 times between them
(trailing_garbage 403, col_n 108, title_as_columns 32) and skipped far more
often, and they are reached today only through an admin HTTP route — the missing
wire this whole spec is about. Every call is gated by 4.1 and audited.

### 4.3 LLM tier

Only when no known signature matches. `repair_with_llm_assist` already exists,
already takes an LLM port, already defaults to `dry_run=True`, and is already
audited under `phase='llm_assisted'`. Wiring is: pass the Bedrock adapter, keep
dry-run until 4.1 has been exercised, gate identically.

*Open* (CL-025-003): whether to enable this before the deterministic hit rate is
known. If most findings match a known class, every LLM call is cost and risk
spent on a solved case.

**Done when**: SC-003 and SC-004 — the PAMI findings are repaired with no LLM
call, and a deliberately wrong repair is rejected.

---

## Phase 5 — The code lane (proposes, never writes)

`report_code_lane_candidates` emits, per signature above threshold: the
occurrence count and affected resources, the regression corpus slice, and
optionally an LLM-proposed parser diff **validated by replaying that corpus**.

A person reads it and writes the change. FR-019 — the lane never exceeds
`propose`. A parser change is the one action here whose blast radius is every
table and whose failure is silent, and this project has no test that would catch
a wrong one. The corpus this lane produces is how that stops being true.

---

## Phase 6 — Autonomy, per class, by measurement

Levels: `report` → `propose` → `apply_known` → `apply_broad`. Stored per
signature class, defaulting to `report`. Raised only with a recorded measurement
behind it, exactly as shadow mode is being held today on a measured 0/5.

---

## Execution order

| Phase | Blocked by | Why the block is real |
|---|---|---|
| 1 Attribution | — | Without it, repairs target our own improvements |
| 2 Evidence | — (parallel) | Archival exists and now runs; the gap is that its bytes may not be the parsed bytes |
| 3 Router | 1 | Routing an unattributable finding routes noise |
| 4.0 Revert | — | Every argument for letting the lane act assumes a rollback that does not exist |
| 4 Data lane | 1, 3, 4.0 | Repairing before routing repairs the wrong artefact; applying before a revert exists is unbounded |
| 5 Code lane | 2, 3, 4 | The corpus is the oracle; it does not exist yet |
| 6 Autonomy | 4 | A level needs a measurement, and 4 produces the first |

**Proposed first cut**: Phase 1 complete, plus 4.0 (the revert), plus watching
2.1. Phase 1 is the blocker for correctness; 4.0 is the blocker for safety and is
small; 2.1 needs observation rather than code now that the archival is running
again.

Deliberately *not* in the first cut: the signature design (Phase 3), because it
is the one piece with no verified basis yet and guessing it early is how the
router ends up routing by whatever the first ten examples happened to look like.

---

## What this plan does not do

- It does not push to the repository, at any phase.
- It does not repair anything the cascade exonerated, or could not attribute.
- It does not apply an LLM proposal that has not passed the same verifier as a
  deterministic one.
- It does not raise an autonomy level on confidence. Only on a number.

---

## Ready to execute

In order, in one pass:

1. **Phase 1.1** — `parser_fingerprint.py`, AST-based so a comment edit does not
   move it.
2. **Phase 1.3** — all four write paths record it, including
   `_db.register_via_b_table`, which accepts no provenance argument today.
3. **Phase 1.2** — migration 0058 adds `normalization_version`, shipped together
   with 1.1 so the column has a producer from its first day.
4. **Phase 1.4** — snapshots read provenance from `raw_table_versions` first.
5. **Phase 1.5** — `Verdict.UNATTRIBUTABLE`, excluded from actionable.
6. **Phase 4.0** — `revert_repair`, which must exist before anything applies
   automatically.
7. Re-run the report on staging and confirm SC-002: the five findings of
   2026-08-21 re-report as `UNATTRIBUTABLE` rather than `UNEXPLAINED`.
8. Deploy staging, then bring production to the same commit — it is a version
   behind and there is no reason to leave it there.

Not in this pass, and deliberately: the change signature (Phase 3). It is the
one component with no verified basis, and designing it before real signatures are
observed is how a router ends up encoding whatever the first ten examples looked
like.

