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
  symptom forever. Today's four PAMI findings are exactly this case: the
  `title_as_columns` class has been *attempted* 2,026 times and applied 32.

### 1.2 The asymmetry that makes the hybrid safe

The two lanes are not peers, and the spec does not treat them as such.

**The data lane repairs. The code lane only ever proposes.** APR's failure mode
is unbounded and silent: a parser change applies to all 27,000 tables on the
next ingest and this project has no test that would catch a wrong one. The data
lane's failure mode is one table and is reversible. Blast radius decides who is
allowed to act.

**And the data lane manufactures the code lane's oracle.** `parse_repair_audit`
rows carry `old_columns → new_columns`: a labelled example of what the parser
produced and what it should have produced. That is precisely the oracle APR
needs and does not otherwise have here.

The corpus is far smaller than the row count suggests, and the number matters
because it decides whether the code lane is viable at all. Of 10,973 rows
(staging, 2026-08-21):

| `operation` | `ok` | `dry_run` | rows | usable as an example? |
|---|---|---|---|---|
| `apply` | yes | no | **543** | **yes** |
| `dry_run` | yes | yes | 213 | proposal only, never applied |
| `skip` | no | no | 5,960 | no repair was made |
| `skip` | no | yes | 1,573 | no repair was made |
| cleanup phases | — | — | 2,684 | not parse repairs |

**543 usable pairs**, all of which do have `old_columns <> new_columns`. Not
8,287. The `skip` rows are not failures — the repair function looked and decided
nothing was needed — but they are not examples either.

Reinduction running continuously is what grows this corpus, and 543 is the
honest starting point.

**Evidence retention already exists, and was broken for the same reason
everything else was.** Replaying a candidate parser needs the bytes it would
parse. `s3_tasks._upload_to_s3` archives the raw file, `S3_BUCKET` is configured,
and `retry_s3_uploads` is on the beat schedule — but it had been failing on the
missing `raw.cached_datasets` since 2026-08-03, which is why 0 of 26,780 rows
carried an `s3_key`. Restoring that table unblocked it; uploads began succeeding
the same day.

This spec therefore does **not** build an archival subsystem. It has one, and the
real gap is narrower and worth stating precisely: `upload_dataset_to_s3`
re-downloads from `download_url` at archival time, so it captures *today's* bytes
rather than the bytes that produced the stored table. Observed on the first run:
some URLs already 404. For repairs made close to ingest the approximation holds;
for anything older it does not, and no amount of retry recovers it.

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

The stamping mechanism is not missing. It works, and it faithfully records a
value that carries no information.

`OPENARG_PARSER_VERSION` is set to the literal string `2026-05-04` in the staging
environment. Two of the four insert sites pass `_DEFAULT_PARSER_VERSION`, which
reads it; the other two forward a function parameter that defaults to `None` and
that callers do not supply. The database agrees exactly: 21,989 rows carry
`2026-05-04`, 6,089 carry NULL. `catalog_resources` holds `legacy:unknown` for
26,436 rows and NULL for 6,128 — never a real value. Of 699 consecutive version
pairs, **zero** have two non-null, differing provenance values.

So G1 fired zero times against five findings that were all ours, for two
separable reasons: a constant that never changes, and two write paths that record
nothing.

The fix is a **derived** fingerprint — computed from the parser sources
themselves, so it changes when behaviour changes and cannot be left stale by
whoever last edited an environment file. Plus making all four write paths record
it.

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
  `parser_version` — but only once something computes one. The column does not
  exist there today, and nothing in the codebase produces a general value:
  `censo2022_ingest.py` hardcodes `"1"` and that is the only writer. Adding the
  column before the fingerprint of §4.1 supplies a value would create a second
  field that means nothing, which is the defect this spec is trying to remove.
- **FR-004**: A snapshot MUST read provenance from `raw_table_versions` — which
  is per-version — before falling back to `catalog_resources`, which holds only
  the resource's current value and is therefore identical on both sides of a
  historical pair.
- **FR-005**: The classifier MUST distinguish `UNATTRIBUTABLE` (provenance
  missing on either side, G1 could not run) from `UNEXPLAINED` (G1 ran and did
  not exonerate).

### Evidence

- **FR-006**: The existing S3 archival MUST be the evidence store. It already
  uploads the raw file and is already scheduled; this spec adds no parallel
  mechanism.
- **FR-007**: Archival MUST move from re-download to capture-at-ingest, or the
  corpus MUST record which of the two produced each sample. Re-downloading from
  `download_url` at archival time captures whatever the URL serves *now* — which
  for a drift study is the one thing that cannot be assumed constant. Observed on
  the first successful run: some URLs already return 404.
- **FR-007b**: A sample MUST carry the content hash of the bytes that produced
  the stored table, so a replay can refuse to run against bytes that are not the
  ones being studied. `raw_table_versions.source_file_hash` already exists for
  this comparison.
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
- **FR-014**: A working revert MUST exist **before** any repair is applied
  without a human. `parse_repair_audit` records `old_columns`, so a revert is
  possible in principle, but no function implements one today — verified
  2026-08-21, nothing matching `revert`/`rollback`/`undo` exists in
  `app/application/repair/` or the admin router. Reversibility is currently a
  property of the data, not of the system, and automatic repair must not ship
  before it is a property of the system.
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

- `parse_repair_audit` remains append-only. (Reversibility is *not* assumed — see
  FR-014; it does not exist and must be built.)
- The existing repairs in `parse_repair.py` are correct where they applied. They
  have applied 543 times and skipped far more often; this spec wires them, it
  does not re-derive them. Their skip rate is itself unmeasured and may mean the
  heuristics are conservative, or that they are being pointed at tables they were
  never meant to fix.

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

---

## 10. Verification log

Every factual claim above was checked against the running system on 2026-08-21
(staging unless noted). This section exists because the first draft of this spec
contained six claims that turned out to be wrong, and each one would have
produced a phase of work aimed at the wrong thing.

### Verified true

| Claim | How |
|---|---|
| `catalog_resources` cannot distinguish versions | 32,564 rows = 32,564 resources = 32,564 tables; no version column |
| `catalog_resources.parser_version` is never a real value | `legacy:unknown` ×26,436, NULL ×6,128 |
| Zero version pairs carry differing non-null provenance | 699 pairs, 0 with both sides non-null and different |
| `raw_table_versions` lacks `normalization_version` | column list |
| All 543 applied repairs have `old_columns <> new_columns` | per-phase count |
| `profile_similarity` returns `0.0` without sampled values | executed |
| The repairs are reachable only from an admin HTTP route | no other caller in `src/` |
| `repair_with_llm_assist` takes an LLM port, defaults `dry_run=True`, audits as `llm_assisted` | read |
| Raw bytes are not kept on local disk | downloads go to `TemporaryDirectory` |
| Alembic head is 0057, so the next migration is 0058 | prod and staging |

### Found false, and corrected

| First draft said | Actually |
|---|---|
| 8,287 usable repair examples | **543**. The rest are `skip` (5,960 + 1,573), dry-runs (213) and cleanups |
| `title_as_columns` repaired 2,026 times | attempted 2,026, applied **32** |
| `parser_version` holds a backfill date | `OPENARG_PARSER_VERSION=2026-05-04` is **set in the environment**; the mechanism works and records exactly what it was given |
| No raw-byte retention exists | `s3_tasks._upload_to_s3` archives the raw file, `S3_BUCKET` is configured, `retry_s3_uploads` is on beat. It was failing on the missing `raw.cached_datasets`; restored the same day and now succeeding |
| `parse_repair_audit` is reversible | no revert exists anywhere. See FR-014 |
| `normalization_version` should be added for symmetry | nothing produces one; `censo2022_ingest.py` hardcodes `"1"` |

### Still assumed — flagged, not resolved

- That a change signature can be designed to make the four PAMI findings collide
  without also colliding unrelated failures. Untested. DEBT-025-002.
- That similarity to the previous version is the right verification target. In
  the PAMI cases the *older* version was the correct one, so the direction is
  not obvious. DEBT-025-003.
- That the 543 applied repairs are correct. They were applied by heuristics and
  never audited against ground truth; using them as an oracle inherits whatever
  they got wrong.
- That the archived bytes correspond to the parsed table. They do not, wherever
  archival ran later than ingest — which today is all of them. FR-007.

