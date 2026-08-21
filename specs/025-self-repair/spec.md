# Spec 025 — Self-Repair: routing a failure to the artefact that is actually wrong

**Type**: Forward-engineered
**Status**: Draft — not implemented
**Hexagonal scope**: Application (routing, verification, reinduction) +
Infrastructure (evidence retention, scheduled lanes, LLM adapter)
**Related plan**: [./plan.md](./plan.md)
**Sister specs**: [023-schema-snapshots](../023-schema-snapshots/spec.md) (the
evidence), [024-drift-classification](../024-drift-classification/spec.md) (the
verdict), [021-parser-hardening](../021-parser-hardening/spec.md) (the repairs
that already exist), [013-ingestion-validation](../013-ingestion-validation/spec.md)

---

## 1. Context & Purpose

Specs 023 and 024 answer *did the shape change, and can we explain it away*.
They stop there on purpose. This spec is what happens next: turning a verdict
into a fix, without the fix being worse than the fault.

**The first measurement rules out the obvious design.** On 2026-08-21 the
classifier produced five actionable findings on staging. Read by hand, all five
were our own parser — four of them the same bug class, `title_as_columns`, for
which `parse_repair.py` has had a working repair since May. A system that had
been wired to "detect → ask an LLM → apply" would have spent its first five
actions rewriting tables that a deterministic function already knew how to fix,
in reaction to a change that was ours.

So the question this spec answers is not *how do we repair* but **what is
actually wrong, and therefore what should be repaired**.

### 1.1 The routing principle

Two repair traditions exist and they operate on different artefacts.

| | **Wrapper reinduction** | **Automated Program Repair** |
|---|---|---|
| Repairs | the mapping for one resource | the source code |
| Oracle | the previous version's value profile | a test suite |
| Blast radius of a bad fix | one table, reversible via `parse_repair_audit` | every table on the next ingest, silent |
| Lineage | Lerman / Minton / Knoblock, 20+ years on extractors breaking when sources change | Agentless and the APR literature |

Neither is "better". The choice is determined by **which artefact is wrong**, and
the observable that decides it is **recurrence**:

- A shape change seen for **one** resource is that resource's problem. Encoding
  one portal's quirk into a generic parser makes the code worse to fix one table.
- The **same** change signature seen across many resources is not a coincidence —
  it is a defect in the code, and repairing each table individually treats the
  symptom forever. Today's four PAMI findings are exactly this case, and
  `parse_repair_audit` shows the class has already been repaired 2,026 times.

### 1.2 The asymmetry that makes the hybrid safe

The two lanes are not peers, and the spec does not treat them as such.

**The data lane repairs. The code lane only ever proposes.** APR's failure mode
is unbounded and silent: a parser change applies to all 27,000 tables on the
next ingest and this project has no test that would catch a wrong one. The data
lane's failure mode is one table and is reversible. Blast radius decides who is
allowed to act.

**And the data lane manufactures the code lane's oracle.** `parse_repair_audit`
holds 10,973 rows, 8,287 of them real parse repairs, each carrying
`old_columns → new_columns`: a labelled example of what the parser produced and
what it should have produced. That is precisely the oracle APR needs and does
not otherwise have here. Reinduction running continuously is what makes code
repair verifiable later.

**One thing must start now or it is lost.** Replaying a candidate parser needs
the bytes it would parse. Measured 2026-08-21: **0 of 26,780** `cached_datasets`
rows carry an `s3_key`, and only 78 % of `raw_table_versions` rows still have a
`source_url`. The corpus can only ever cover repairs made *after* evidence
retention exists.

---

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Lane** | Which repair tradition a finding is routed to: `data` (reinduction) or `code` (APR). |
| **Change signature** | A normalised fingerprint of a diff — what kind of change it is, independent of the specific column names. Two resources that broke the same way share a signature. |
| **Recurrence** | How many distinct resources exhibit one signature in a window. The observable that routes a finding. |
| **Reinduction** | Re-deriving the correct mapping for one table from the evidence, and rewriting that table. Never touches code. |
| **Evidence sample** | A bounded prefix of the source bytes, retained so a candidate parser can be replayed against a real input. |
| **Regression corpus** | The set of `(evidence sample, expected columns)` pairs derived from `parse_repair_audit`. The oracle for the code lane. |
| **Verification** | Checking a proposed repair against evidence the proposer did not see, before it is allowed to apply. |
| **Autonomy level** | How far a lane may act without a human: report / propose / apply-known / apply-broad. |

---

## 3. User Stories

- **US-001 (P1)**: As the system, I must know whether a shape change was caused
  by *our* parser or by the portal, **so that** I never "repair" one of our own
  improvements.
- **US-002 (P1)**: As the system, I must route a finding to the artefact that is
  wrong, **so that** a one-off file problem does not become a change to a parser
  that 27,000 tables depend on.
- **US-003 (P1)**: As an operator, I need every automatic repair to be reversible
  and audited, **so that** a wrong repair costs a rollback rather than a dataset.
- **US-004 (P1)**: As the system, I must verify a proposed repair against evidence
  the proposer did not see, **so that** a plausible-looking fix cannot silently
  corrupt data.
- **US-005 (P2)**: As an operator, I want to be told when a repair class recurs
  often enough to be a code defect, **so that** I can fix the parser once instead
  of watching the same repair fire forever.
- **US-006 (P2)**: As an operator, I want autonomy to increase only where the
  measured precision justifies it, **so that** trust is earned per class rather
  than granted globally.

---

## 4. Design decisions worth stating

### 4.1 Attribution comes before everything, and it does not exist yet

Measured on staging: `raw_table_versions.parser_version` holds the literal
string `2026-05-04` for 21,989 rows — the date of the May backfill, not a parser
version — and NULL for 6,089. `catalog_resources` holds the placeholder
`legacy:unknown`. Of 699 consecutive version pairs, **zero** have two non-null,
differing provenance values.

G1 therefore cannot exonerate anything historical, and it fired zero times
against five findings that were all ours. Worse, `_DEFAULT_PARSER_VERSION` is an
environment variable defaulting to `"phase4"` that nobody bumps, so it would not
change even when parser behaviour does.

The fix is a **derived** fingerprint — computed from the parser sources
themselves, so it changes when behaviour changes and cannot drift from reality
through neglect.

### 4.2 A pair we cannot attribute is not an unexplained change

Reporting today's five findings as `UNEXPLAINED` overstates what is known. The
cascade could not evaluate G1 at all; the honest verdict is that the change is
**unattributable**, which is a different claim and leads to a different action
(record provenance, re-measure) than "the portal changed" (adapt).

### 4.3 The verifier does not trust the proposer

Every repair, deterministic or LLM-proposed, must pass the same gate: apply it
in a transaction, profile the result, compare against the previous version's
profile, and roll back unless similarity improves. `profile_similarity` already
exists and already returns `0.0` when there is no evidence — so a table with no
statistics cannot be repaired on a manufactured score.

This is the defence against patch overfitting, and it is the reason the LLM tier
is safe to enable at all.

### 4.4 The code lane never opens a pull request by itself

It emits a regression corpus, an aggregate ("this signature, N occurrences,
across M resources"), and — optionally — a proposed diff that has been validated
by replaying the corpus. A person reads that and writes the change. This is not
timidity: a parser change is the one action in this system whose blast radius is
every table and whose failure is silent.

---

## 5. Functional Requirements

### Attribution

- **FR-001**: `parser_version` MUST be derived from the parser sources, not
  declared in configuration, so it changes exactly when parse behaviour changes.
- **FR-002**: Every write path into `raw_table_versions` MUST record the
  fingerprint. Four call sites exist today and two pass a value that arrives
  empty.
- **FR-003**: `raw_table_versions` MUST carry `normalization_version` alongside
  `parser_version`. The column does not exist today; the snapshot table has it.
- **FR-004**: A snapshot MUST read provenance from `raw_table_versions` — which
  is per-version — before falling back to `catalog_resources`, which holds only
  the resource's current value and is therefore identical on both sides of a
  historical pair.
- **FR-005**: The classifier MUST distinguish `UNATTRIBUTABLE` (provenance
  missing on either side, G1 could not run) from `UNEXPLAINED` (G1 ran and did
  not exonerate).

### Evidence

- **FR-006**: When a resource is ingested, a bounded prefix of the source bytes
  MUST be retained, with its content hash, so a candidate parser can be replayed
  later against a real input.
- **FR-007**: Evidence retention MUST be bounded per sample and in total, and
  MUST degrade to "no sample" rather than failing the ingest.
- **FR-008**: The regression corpus MUST be derivable from `parse_repair_audit`
  joined to retained evidence, as `(sample, expected_columns)` pairs.

### Routing

- **FR-009**: A verdict MUST be reduced to a **change signature** that is
  independent of specific column names, so that two resources broken the same
  way collide.
- **FR-010**: Routing MUST be by measured recurrence of the signature over a
  window, not by a hand-assigned category.
- **FR-011**: The recurrence threshold MUST be reported before it is enforced.
  Its value is a measurement, not a guess.
- **FR-012**: A finding routed to the code lane MUST still have its own table
  repaired by the data lane. Escalation is about the parser; the broken table is
  broken either way.

### Repair

- **FR-013**: The data lane MUST attempt known deterministic repairs first,
  matched by signature, and MUST reach the LLM tier only when no known class
  matches.
- **FR-014**: Every repair MUST be audited in `parse_repair_audit` with the
  before and after columns, and MUST be reversible from that record.
- **FR-015**: No repair may apply unless it passes verification against the
  previous version's value profile.
- **FR-016**: A repair MUST NOT run against a resource whose change was
  exonerated, or whose verdict is `UNATTRIBUTABLE`.

### Autonomy

- **FR-017**: Each lane MUST operate at an explicit autonomy level, defaulting to
  the lowest.
- **FR-018**: A level MUST only be raised for a signature class whose measured
  precision supports it, and the measurement MUST be recorded.
- **FR-019**: The code lane MUST NOT exceed `propose`. It never writes to the
  repository.

---

## 6. Success Criteria

- **SC-001**: After attribution ships, a pair captured across a parser change is
  exonerated by G1 rather than reported as actionable.
- **SC-002**: Re-running the report over the five findings of 2026-08-21
  classifies them as `UNATTRIBUTABLE`, not `UNEXPLAINED`.
- **SC-003**: The four PAMI `title_as_columns` findings are matched to the
  existing repair by signature, with no LLM call.
- **SC-004**: A deliberately wrong repair is rejected by the verifier and leaves
  the table untouched.
- **SC-005**: A signature seen across many resources is reported as a code-lane
  candidate with its occurrence count, and no code is written automatically.
- **SC-006**: The regression corpus can replay at least one historical repair:
  given a retained sample, the current parser reproduces the recorded wrong
  output, and the repaired columns match `new_columns`.

---

## 7. Assumptions & Out of Scope

**Assumptions**

- `parse_repair_audit` remains reversible and append-only.
- The existing repairs in `parse_repair.py` are correct. They have applied ~8,287
  times; this spec wires them, it does not re-derive them.

**Out of scope**

- **Writing to the repository.** See FR-019.
- **Repairing marts.** A mart is derived; fixing its inputs is this spec's job,
  fixing its SQL is [019-marts](../019-marts/spec.md).
- **Re-ingesting from source as a repair strategy.** In-place repair is the
  project's established preference and re-ingest cannot recover a shape whose
  upstream file has already changed.
- **Backfilling provenance.** It was never recorded; it cannot be recovered.
  Historical pairs stay `UNATTRIBUTABLE` and that is the honest outcome.

---

## 8. Open Questions

- **[NEEDS CLARIFICATION CL-025-001]** — The recurrence threshold separating the
  lanes is unknown. FR-011 requires reporting the distribution first; the value
  should come from that, not from this document.
- **[NEEDS CLARIFICATION CL-025-002]** — How much of each source to retain.
  Enough to replay a header decision is likely tens of kilobytes, but a wide CSV
  or a multi-sheet workbook may need more, and the useful prefix of a ZIP is not
  a prefix at all.
- **[NEEDS CLARIFICATION CL-025-003]** — Whether the LLM tier should run at all
  before the deterministic lane has been measured. There is a case for leaving it
  off until the known-class hit rate is known, since every LLM call is a cost and
  a risk taken on a case the system may already handle.

---

## 9. Tech Debt Anticipated

- **[DEBT-025-001] — The corpus starts empty.** Evidence retention cannot be
  backfilled, so the code lane is inert until enough repairs accumulate with
  samples attached. This is a real wait, and it should be stated rather than
  discovered.
- **[DEBT-025-002] — Signature design is the whole ballgame and is unproven.**
  Too specific and nothing ever recurs, so everything looks like a one-off; too
  loose and unrelated failures collide and a parser gets changed for the wrong
  reason. It will need revision once real signatures are observed.
- **[DEBT-025-003] — Verification compares against a profile that may itself be
  wrong.** If the previous version was already mis-parsed, similarity to it is
  the wrong target. The four PAMI cases are exactly this: v1 was right and v2
  wrong, but nothing in the profile says which is which. Direction matters and
  the verifier does not yet know it.
