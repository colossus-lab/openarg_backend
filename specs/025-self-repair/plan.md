# Plan 025 — Self-Repair

**Spec**: [./spec.md](./spec.md)
**Status**: Draft — awaiting approval
**Sequencing rule**: each phase is blocked by the one before it for a stated
reason. The reasons are not process; skipping any of them produces a system that
repairs the wrong thing.

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

Chosen over a hand-bumped constant because `_DEFAULT_PARSER_VERSION`
(`collector_tasks.py:268`) is an env var defaulting to `"phase4"` that has never
been bumped: it cannot distinguish a parser change from no change, which is the
one thing G1 needs.

*Risk*: the fingerprint changes on a comment edit, producing a spurious G1
exoneration. Mitigation: hash the parsed AST with docstrings stripped, not raw
bytes. Test: editing a comment must not change the fingerprint; changing a
literal must.

### 1.2 Migration 0058 — `raw_table_versions.normalization_version`

The column does not exist (verified 2026-08-21); `raw_schema_snapshots` has it
since 0057. Nullable text, no backfill — historical rows are genuinely unknown
and inventing a value would defeat FR-005.

### 1.3 Stamp on every write path

Four call sites insert into `raw_table_versions`
(`collector_tasks.py:4756`, `:4883`, `_db.py:register_via_b_table`, plus the
`ops_fixes` re-registration). Two currently forward a `parser_version` argument
that arrives empty. All must pass the fingerprints.

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

## Phase 2 — Evidence retention

*Blocks the code lane, and only the code lane. Started early because it cannot be
backfilled: 0 of 26,780 cached datasets retain their bytes today, and every day
without it is a permanent hole in the corpus.*

### 2.1 Migration 0059 — `raw.parse_evidence`

`(id, resource_identity, version, source_url, content_hash, byte_prefix bytea,
prefix_bytes int, truncated bool, media_type, captured_at)`.

Separate table rather than a column on `parse_repair_audit`, because evidence is
captured at ingest and repairs happen later and repeatedly — the cardinalities do
not match.

### 2.2 Capture on ingest

In the collector's download path, retain the first N bytes (start: 64 KB,
`OPENARG_EVIDENCE_PREFIX_BYTES`). Best-effort with the same contract as the
snapshot hook: a failure here must never fail an ingest.

*Open* (CL-025-002): a ZIP's useful part is not its prefix, and a wide workbook
may need more. Start with the prefix, measure how often replay fails for lack of
bytes, then decide.

### 2.3 Corpus builder

`app/application/repair/corpus.py::build_regression_corpus()` — joins
`parse_repair_audit` (8,287 real repairs, each with `old_columns → new_columns`)
to `parse_evidence`, yielding `(sample, expected_columns)`.

**Done when**: SC-006 — one historical repair replays end to end.

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

### 4.1 The verifier, first

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
`repair_trailing_garbage_cols`. These have applied ~8,287 times and are reached
today only through an admin HTTP route, which is the missing wire this whole
spec is about. Every call is gated by 4.1 and audited.

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
| 2 Evidence | — (parallel) | Cannot be backfilled; every day is a permanent hole |
| 3 Router | 1 | Routing an unattributable finding routes noise |
| 4 Data lane | 1, 3 | Repairing before routing repairs the wrong artefact |
| 5 Code lane | 2, 3, 4 | The corpus is the oracle; it does not exist yet |
| 6 Autonomy | 4 | A level needs a measurement, and 4 produces the first |

**Proposed first cut**: Phase 1 complete, plus 2.1 and 2.2 so the corpus starts
filling while the rest is built.

---

## What this plan does not do

- It does not push to the repository, at any phase.
- It does not repair anything the cascade exonerated, or could not attribute.
- It does not apply an LLM proposal that has not passed the same verifier as a
  deterministic one.
- It does not raise an autonomy level on confidence. Only on a number.
