# Spec 023 — Pre-drop Schema Snapshots

**Type**: Forward-engineered
**Status**: Implemented 2026-08-21 (migration 0056)
**Last synced with code**: 2026-08-21
**Hexagonal scope**: Application (capture + diff) + Infrastructure (hook on the drop path)
**Related plan**: [./plan.md](./plan.md)
**Sister specs**: [017-raw-layer](../017-raw-layer/spec.md) (what gets dropped),
[024-drift-classification](../024-drift-classification/spec.md) (what reads this evidence),
[013-ingestion-validation](../013-ingestion-validation/spec.md) (where findings will land),
[021-parser-hardening](../021-parser-hardening/spec.md) (`parse_repair`, the same
record-before-you-mutate pattern)

---

## 1. Context & Purpose

**The system destroys the evidence of a format change as part of handling the
format change.**

When a re-ingest arrives with columns that do not match the existing table,
`pandas.to_sql` fails and `_to_sql_safe` responds by dropping the table and
recreating it. That has happened **19,293 times** in production. Three cleanup
tasks drop tables for their own reasons, bringing the audited total to 58,155.

In every one of those cases the previous shape ceased to exist at the moment of
the drop. Measured on production on 2026-08-21: of **644 consecutive `(v1, v2)`
version pairs** in `raw_table_versions`, **642 have no physical `v1` left** to
compare against. The system therefore cannot answer the most basic question
about its own inputs — *did this resource change shape, and how* — for any
resource, ever.

This module records what a table looked like immediately before it is dropped.
It does not repair anything, does not block anything and does not decide
anything. It exists so that the question becomes answerable.

**Why this is the precondition for everything else.** Any adaptive behaviour —
classifying a change, proposing a fix, verifying that a repair improved things —
needs a previous state to compare against. Today there is none. Every level of
autonomy above "log a warning" is blocked on this one table existing.

---

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Snapshot** | What one table looked like at one moment: its columns, their types and order, and (when available) a statistical profile of their values. |
| **Shape** | The set of column names, order-independent, excluding collector-added metadata. Two tables with the same shape have the same `schema_hash`. |
| **Value profile** | Per column: null fraction, distinct estimate, most common values, histogram sample. Read from `pg_stats`, never computed by scanning. |
| **Drift** | A difference between two snapshots of the same table or resource. |
| **Rename candidate** | A column that disappeared and one that appeared whose value profiles are close enough that they are probably the same column under a new name. |
| **Audited drop** | A `DROP TABLE` that goes through `_record_cache_drop`. Since this spec, every audited drop is also a snapshotted drop. |

---

## 3. User Stories

- **US-001 (P1)**: As an operator, I want to know what a table looked like before
  it was replaced, **so that** I can tell whether the upstream format changed or
  the parser did.
- **US-002 (P1)**: As the system, I need a previous shape to compare against,
  **so that** any future classification of change is grounded in evidence
  instead of assumption.
- **US-003 (P1)**: As an operator, I want to know **how often** formats actually
  change and in what way, **so that** the decision to build (or not build) an
  adaptive repair layer rests on a measured rate rather than on an intuition.
- **US-004 (P2)**: As the system, I want to recognise a renamed column from its
  values alone, **so that** a rename is not misread as one column lost plus one
  gained.
- **US-005 (P2)**: As an operator, I want the whole feature to be switchable off
  with an environment variable, **so that** anything unexpected on the drop path
  can be removed in a restart rather than a deploy.

---

## 4. Design decisions worth stating

### 4.1 The profile comes from `pg_stats`, not from the table

PostgreSQL's autovacuum already computes null fractions, distinct estimates,
most-common values and histogram bounds during `ANALYZE`. Reading them is an
**index scan on the catalog**; it never touches the data.

This matters more than usual here. The code runs on the path of a table that is
being dropped — frequently *because something already failed* — and adding a
table scan to that path would trade a bookkeeping improvement for an ingestion
slowdown. When a table was never analysed, the shape is recorded anyway and
`stats_available` is set to false: the columns and types are the part that
answers "did the format change", and those always come from the catalog.

### 4.2 One hook, not five

All four audited drop reasons funnel through `_record_cache_drop`. Hooking the
snapshot there covers `schema_mismatch_recreate`, `retain_raw_versions`,
`raw_orphan_cleanup` and `empty_raw_bloat` at once — and any drop path added
later gets it without anyone remembering to wire it.

### 4.3 No foreign key to `raw_table_versions`

Three of the dropping paths `DELETE` the registry row in the same transaction as
the drop. A reference would either block the drop or cascade the snapshot away
with it. `resource_identity` is stored as plain text so the record outlives
everything it describes.

### 4.4 The value profile is what makes renames detectable

A portal that renames `provincia` to `jurisdiccion` while keeping the values
produces, by name alone, one column lost and one gained. By profile it is
obviously the same column. This is the mechanism that twenty-three years of
*wrapper maintenance* literature identifies as the invariant that survives a
format change: **the shape of the values, not the shape of the container**.
Detecting it requires no model — only that the profile was stored.

---

## 5. Functional Requirements

- **FR-001**: Every audited drop MUST attempt a snapshot **before** the
  `DROP TABLE` executes. A snapshot taken afterwards is worthless.
- **FR-002**: The snapshot MUST record column names, PostgreSQL types and
  ordinal positions. These come from the catalog and are always available for an
  existing table.
- **FR-003**: The snapshot MUST record the per-column value profile when
  PostgreSQL has statistics for the table, and MUST set `stats_available=false`
  when it does not — so a consumer can distinguish "no statistics" from "every
  column is null".
- **FR-004**: The snapshot MUST NOT scan the table it describes.
- **FR-005**: `schema_hash` MUST be order-independent and MUST exclude
  collector-added metadata columns (`_source_dataset_id`, `_ingested_at`,
  `_source_url`), so the same upstream shape hashes identically regardless of
  ingest path.
- **FR-006**: `schema_hash` MUST share its construction with
  `collector_tasks._schema_suffix`, so a snapshot can be matched against the
  `_s<hash>` suffix the collector puts on schema-variant tables.
- **FR-007**: A failure anywhere in the capture path MUST NOT prevent the drop.
  This is guarded twice — inside the helper and at the call site — because the
  helper's own guard cannot cover a failure in its import.
- **FR-008**: A table that no longer exists MUST return `None` rather than
  raising. Every caller sits in front of `DROP TABLE IF EXISTS`, so losing the
  race to another worker is expected.
- **FR-009**: The value profile MUST be bounded: at most `MAX_PROFILE_VALUES`
  (20) entries per column, each truncated to `MAX_VALUE_CHARS` (120), and skipped
  entirely above `MAX_PROFILED_COLUMNS` (300) columns.
- **FR-010**: `row_count_estimate` MUST normalise `pg_class.reltuples = -1`
  (never analysed) to NULL, never to zero.
- **FR-011**: The feature MUST be switchable via `OPENARG_SCHEMA_SNAPSHOTS`
  (default on). Rollback is an env var and a worker restart.
- **FR-012**: `cleanup_invariants` MUST route its empty-orphan drop through
  `_record_cache_drop`. It was the only `DROP TABLE` in the codebase that did
  not, which made `cache_drop_audit` silently incomplete.
- **FR-013**: `diff_snapshots` MUST be pure — no database access — so it can run
  over stored rows long after both tables are gone.
- **FR-014**: `profile_similarity` MUST return `0.0` when neither column has
  sampled values. No evidence is not the same as no similarity, and returning a
  high score there would manufacture a rename for every unanalysed table.

---

## 6. Success Criteria

- **SC-001**: After one full cleanup cycle, `raw.raw_schema_snapshots` contains
  one row per audited drop.
- **SC-002**: Given two snapshots of a resource whose column set changed,
  `diff_snapshots` reports the added and removed columns and flags
  `schema_changed`.
- **SC-003**: Given a rename that preserves values, `diff_snapshots` reports it
  under `renamed_candidates` and not as an unrelated add plus remove.
- **SC-004**: A snapshot failure — pool exhaustion, a missing table, a raising
  hook — leaves the drop unaffected.
- **SC-005**: Capturing a snapshot issues no query against the table's data,
  observable as no sequential scan in `pg_stat_statements` attributable to the
  capture.
- **SC-006**: Storage stays under ~4 KB per snapshot, so the whole corpus of
  27,061 tables costs order-of-100 MB rather than the terabytes that retaining
  the tables themselves would.

---

## 7. Assumptions & Out of Scope

**Assumptions**

- Autovacuum analyses most tables. Where it has not, the shape is still
  captured and the profile is empty — degraded, not broken.
- `_record_cache_drop` remains the single funnel for audited drops. A drop added
  outside it would be invisible to this feature, exactly as it is invisible to
  `cache_drop_audit` today.

**Out of scope — deliberately**

- **Classifying the change.** This spec stores evidence. Deciding that a diff is
  "a rename" or "a breaking change" belongs to a consumer that does not exist yet.
- **Repairing anything.** No behaviour changes as a result of a snapshot.
- **Alerting.** The consumer added in DEBT-023-001 reports; it does not
  alert. Turning a summary into a notification is a calibration decision that
  needs the measured false-positive rate first.
- **Preventing the drop.** Whether `schema_mismatch_recreate` should version the
  table instead of overwriting it is a real question and a different change; it
  is entangled with the `OPENARG_USE_RAW_LAYER` cutover and is not decided here.

---

## 8. Open Questions

- **[NEEDS CLARIFICATION CL-023-001]** — The `0.6` similarity threshold for a
  rename candidate has no empirical basis yet; it was chosen so that identical
  value sets clear it comfortably. It should be recalibrated once real diffs
  accumulate.
- **[NEEDS CLARIFICATION CL-023-002]** — Snapshots are never pruned. At the
  observed drop rate the growth is negligible, but there is no retention policy
  and the irony of that would be worth avoiding.

---

## 9. Tech Debt Discovered

- **[DEBT-023-001] — RESOLVED 2026-08-21.** The consumer exists:
  `openarg.report_schema_drift` (weekly, Mondays 06:15 ART) pairs consecutive
  snapshots per table, runs each pair through
  [024-drift-classification](../024-drift-classification/spec.md) and logs the
  rate and classes of change broken down per exoneration gate. It runs in
  **shadow** — no notification, no findings row, no behaviour change — because
  the false-positive rate of the cascade is still unmeasured and two of its
  gates abstain on every call. See DEBT-023-005 for what shadow mode leaves open.

- **[DEBT-023-005] — The report is only as good as the drop rate.** A table needs
  two audited drops before anything is comparable. Measured on staging on
  2026-08-21: zero drops in the preceding seven days, because
  `raw.cached_datasets` has not existed since the 2026-08-03 `cleanup_raw_orphans`
  incident and nothing has been collected since. The snapshot corpus there will
  stay empty until collection resumes — which is a staging availability problem,
  not a defect in this module, but it does mean staging cannot be the environment
  that calibrates the thresholds.

- **[DEBT-023-002] — The snapshot commits independently of the drop.** The
  cleanup tasks wrap their loop in an outer transaction while
  `_record_cache_drop` opens its own. If the outer transaction rolls back, the
  snapshot survives for a table that was never dropped. That is the safer of the
  two failure modes — a spurious snapshot is inert, a missing one is
  unrecoverable — but it means a consumer cannot assume that every snapshot
  corresponds to a table that ceased to exist.

- **[DEBT-023-003] — Duplicated hash construction.**
  `schema_snapshot.schema_hash_for` reimplements
  `collector_tasks._schema_suffix` rather than importing it, to avoid a
  dependency from the application layer onto a Celery module. A test pins them
  together, but a change to one still requires remembering the other.

- **[DEBT-023-004] — Manual drops stay invisible.** AWS RDS does not expose
  SUPERUSER, so the `pg_event_trigger` planned in migration 0036 was replaced by
  application-side auditing in 0038. Anything dropped from `psql` bypasses both
  the audit and the snapshot. Inherited, not introduced.
