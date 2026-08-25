# Spec 026 — Dataset refresh: reading a source more than once

**Type**: Forward-engineered
**Status**: Draft
**Hexagonal scope**: Application (freshness policy) + Infrastructure (eligibility, dispatch)
**Related plan**: [./plan.md](./plan.md)
**Sister specs**: [025-self-repair](../025-self-repair/spec.md) (blocked on this),
[023-schema-snapshots](../023-schema-snapshots/spec.md),
[013-ingestion-validation](../013-ingestion-validation/spec.md)

---

## 1. Context & Purpose

**A dataset is collected once and never again.**

`bulk_collect_all` selects on `d.is_cached = false AND NOT EXISTS (a
cached_datasets row with status ready / permanently_failed / downloading)`. Once
a row reaches `ready` it can never satisfy that predicate again. Nothing else
recycles it: `_recycle_stuck_downloads` only touches rows stuck in `downloading`,
and it *promotes* them to `ready`. Re-scraping a catalogue updates metadata and
does not re-dispatch collection. There is no TTL, no max-age, no refresh task.

Measured on staging, 2026-08-21:

| | |
|---|---|
| `ready` rows not updated in over 3 months | 24,097 (90 %) |
| Updated in the last week | 114 |
| Resources with exactly one version in the registry | 26,651 (97 %) |

So the platform answers today's questions with data most of which was fetched in
May, and 97 % of its resources have been read from source exactly once.

### 1.1 Why this spec exists inside the drift work

[025-self-repair](../025-self-repair/spec.md) presupposes re-ingestion. Its whole
apparatus — snapshots, the exoneration cascade, the router, the repair lanes —
answers the question *what changed between two readings of the same source*.

**There is no second reading.** The drift subsystem can be perfect and observe
nothing, because nothing looks at the source again. Every measurement in 025
points here in the end: zero attributable pairs, because a fingerprint reaches a
snapshot only through a new registration, and new registrations do not happen for
a resource already on disk.

This is not a dependency discovered by planning. It was found by asking why the
router had nothing to route.

### 1.2 It is a product problem first

The drift argument is the smaller one. A platform whose purpose is answering
questions about Argentine public data, serving numbers fetched three months ago
without saying so, is wrong in a way users cannot see. Freshness is the feature;
drift detection is a consequence of having it.

---

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Refresh** | Collecting a resource that already has data, to replace it. Distinct from a first collection and from retrying a failure. |
| **Freshness policy** | How often a given resource should be re-read. Not one number: a daily exchange rate and a decennial census are both correct at very different ages. |
| **Staleness** | How long since a resource was last successfully read from source, against its policy. |
| **Refresh budget** | The cap on refreshes per cycle. The constraint that keeps this from being an outage. |

---

## 3. User Stories

- **US-001 (P1)**: As a user, I want an answer drawn from data that is current for
  its subject, **so that** a question about this month is not answered with May.
- **US-002 (P1)**: As an operator, I want a bounded number of refreshes per cycle,
  **so that** re-reading the catalogue never repeats the load that restarted the
  RDS instance in May.
- **US-003 (P1)**: As the drift subsystem, I need resources to be read more than
  once, **so that** there is a second observation to compare against a first.
- **US-004 (P2)**: As an operator, I want a resource's refresh cadence to reflect
  what it is, **so that** a decennial census is not re-fetched daily and an
  exchange rate is not left for a quarter.
- **US-005 (P2)**: As a user, I want to be told how old the data behind an answer
  is, **so that** I can judge it. Today nothing surfaces the age at all.

---

## 4. Design decisions worth stating

### 4.1 Age is the wrong signal, and the right one was already in the table

The obvious design is a time-to-live. Measuring the catalogue ruled it out.

`datasets.last_updated_at` — the modification date the portal itself declares —
is populated for **32,565 of 32,566** rows and nothing was reading it. It says
this catalogue is mostly static:

| Last declared modification | Datasets |
|---|---|
| under a week | 493 |
| under a month | 1,889 |
| 1–3 months | 2,420 |
| 3–12 months | 2,866 |
| **over a year** | **24,897** (76 %) |

Per-portal medians run from 89 days (`neuquen_legislatura`) to **3,021**
(`cordoba_estadistica` — eight years), with `datos_gob_ar` at 1,075 and `magyp`
at 2,552. No single TTL survives that spread: one short enough to keep `energia`
current re-downloads Córdoba's static series hundreds of times for nothing.

So the primary signal is the portal saying it changed —
`last_updated_at > cd.updated_at` — which is exact rather than guessed, free
(the scraper already fetches it daily), and names a finite queue:

| | |
|---|---|
| Ready resources the portal declares changed | **3,431** |
| Ready resources that have not moved since we read them | 25,580 |

Age survives only as a **backstop**, because portals lie about this field: some
never update it, some touch it without changing the file. Ninety days is long
enough to cost little and short enough that a silent change is not invisible
forever. It is the one number here that is chosen rather than measured, and
deliberately the one that matters least.

Verified against production: `energia` alone yields 585 eligible resources, 251
of them portal-declared — a first portal small enough to watch for a week and
real enough to learn from.

### 4.2 A refresh must not be able to lose the current data

A refresh replaces something that works. That is the difference from a first
collection, where failure costs nothing that existed before, and it is where the
risk lives: a source that now 404s, or serves an error page as a CSV, must not
result in the previous version being gone.

The raw layer already versions, so the safe shape is available: write the new
version, and only then supersede the old one. What must not happen is a
drop-then-fetch.

### 4.3 The budget is a first-class input, not a safety valve

`bulk_collect_all` already carries scars from this: a mart-rebuild backpressure
check added after 152 concurrent collects plus a 52M-row matview restarted the
database. Refresh multiplies the steady-state load rather than adding a one-off,
so the cap belongs in the design rather than bolted on.

### 4.4 Freshness is worth surfacing even before it is fixed

Users cannot currently tell that an answer rests on May's data. That is true
today and will stay true for every resource still awaiting its first refresh, so
exposing the age is worth doing independently of the refresh cadence — and it is
the honest thing to do while a backlog of 24,097 stale resources drains.

---

## 5. Functional Requirements

- **FR-001**: A resource whose data is older than its policy MUST become eligible
  for collection again. Reaching `ready` MUST stop being terminal.
- **FR-002**: Refresh eligibility MUST be separate from first-collection
  eligibility, so that a backlog of one cannot starve the other.
- **FR-003**: The number of refreshes dispatched per cycle MUST be bounded, and
  the bound MUST be configurable without a deploy.
- **FR-004**: A refresh MUST NOT drop the existing table before the replacement
  is in place. A failed refresh MUST leave the previous version serving.
- **FR-005**: A failed refresh MUST NOT mark the resource as failed. The resource
  has data; the *refresh* failed, and those are different states with different
  consequences.
- **FR-006**: Eligibility MUST be driven by the portal's declared modification
  date, with age as a backstop and not as the mechanism. Participation MUST be
  expressible per portal, with a per-resource exemption for a closed series
  inside a live catalogue.
- **FR-006b**: The two grounds MUST be counted apart. "The portal says it
  changed" is evidence; "we have not looked in ninety days" is a precaution, and
  a run that is mostly the second means the metadata is not carrying its weight.
- **FR-007**: A refresh MUST record provenance exactly as a first collection
  does, so the pair it creates is attributable
  ([025](../025-self-repair/spec.md) FR-001).
- **FR-008**: The age of the data behind an answer MUST be exposed, so a consumer
  can judge it. This is independent of FR-001 and worth shipping first.
- **FR-009**: Refresh MUST respect the existing backpressure: no dispatch while a
  mart matview is being built or refreshed.

---

## 6. Success Criteria

- **SC-001**: A resource past its policy age is collected again, and both
  versions exist in `raw_table_versions`.
- **SC-002**: After a refresh, the pair of snapshots for that resource is
  **attributable** — both sides carry a real fingerprint — which is what
  [025](../025-self-repair/spec.md) has been waiting for.
- **SC-003**: A refresh whose source 404s leaves the previous version serving and
  the resource not marked failed.
- **SC-004**: Dispatch stays within the configured budget under a backlog of
  24,000 stale resources.
- **SC-005**: The share of `ready` rows older than three months falls, and the
  fall is visible in a metric rather than inferred.

---

## 7. Assumptions & Out of Scope

**Assumptions**

- The raw layer's versioning works well enough to hold two versions of a
  resource during a refresh. It does — 742 resources already have two.

**Out of scope**

- **Deciding the cadences themselves.** The mechanism takes a policy; which
  number belongs to which portal is a judgement about the data, and belongs with
  whoever knows the sources.
- **Conditional requests** (`If-Modified-Since` / `ETag`). A large saving and a
  separate piece of work; portals vary in whether they honour them, and the
  answer needs measurement.
- **Change detection at the source.** Knowing a file changed without downloading
  it would make most of this cheap. It is also a different problem.

---

## 8. Open Questions

- **[RESOLVED CL-026-001]** — There is no default cadence, because there should
  not be one. The portal's own `last_updated_at` answers the question exactly,
  for 99.997 % of rows, and it was already being collected. The only chosen
  number left is the 90-day backstop, and it applies to the case where the
  metadata is wrong rather than to the normal path.
- **[NEEDS CLARIFICATION CL-026-002]** — Whether to drain the 24,097-resource
  backlog on a schedule, or leave it to the ordinary cadence and accept that the
  oldest data takes longest to come back. The first is faster and is exactly the
  load pattern that caused the May incident.
- **[NEEDS CLARIFICATION CL-026-003]** — Whether a refresh that finds identical
  bytes should count as a read. It costs the download either way, and recording
  it is what makes "unchanged for six months" a statement rather than an absence.

---

## 7. Content-based change detection, built 2026-08-23

### 7.1 The correction this spec needed

This spec keys the refresh on the portal's `last_updated_at`. That is metadata,
not content, and the measurement is unambiguous: **68 re-collections produced
zero files that were actually different.** The portal moved a timestamp and we
re-read, re-parsed and re-embedded a file identical to the one we held.

The 2026 plan asked for detection by content and was right.

### 7.2 The column existed and nobody wrote it

`raw_table_versions.source_file_hash` has been there since migration 0039 and
every registration function threads it through as a parameter. **Nothing ever
computed it**: 0 of 31,266 live versions carried one.

This spec previously asserted the hash "is already stored". That was wrong, and
it was discovered by trying to use it.

### 7.3 Where the digest is taken, and why there

Where the file exists on disk and before anything can replace it — a hash taken
later describes a different file. Streamed in 1 MB chunks: these run to hundreds
of megabytes and a digest needing the file in memory trades one problem for a
worse one.

### 7.4 The skip, and the trap inside it

Downloading is unavoidable — the digest is of those bytes — but the parse, the
write and the embeddings are not, and that is where the cost is.

**An unchanged file is not a reason to keep a broken table.** The skip applies
only when the table the last parse produced still exists *and still holds rows*.
Otherwise a resource whose parse failed, or whose table a sweep dropped, would be
skipped forever on the grounds that its source never moved — which is how a gap
becomes permanent.

Uncertainty resolves toward re-collecting. Re-parsing an unchanged file costs one
collection; skipping a changed one costs serving stale data while believing it
fresh, and that asymmetry decides which way the function fails.

`updated_at` still moves on a skip: it records when we last *checked*, and
without it the refresh would re-select the same resource on every pass forever.

### Functional requirements added by this section

Continuing the numbering of section 4.

- **FR-010** — Every collection MUST record the SHA-256 of the bytes it
  downloaded, streamed rather than buffered.
- **FR-011** — A hashing failure MUST NOT fail the collection.
- **FR-012** — A collection MUST skip parse, write and embedding when the
  digest matches the live version AND that version's table exists with rows.
- **FR-013** — A skip MUST still advance `cached_datasets.updated_at`.

---

## 8. Tramo 3 closed out, 2026-08-24 — with two items that were never open

The 2026 plan lists six things under CKAN 2.11 ingestion. Measuring each before
building found that two of them describe a problem this system does not have,
and a third names the wrong formats.

| Item | Verdict |
|---|---|
| Reconciliation by `original_identifier` | Built. See spec 027 §3.4. |
| Content-based change detection | Built. See §7 above. |
| Canary per portal | Built. Found three broken portals on its first run. |
| `columns` via `datastore_search` | **Mostly the wrong route.** 29,001 of 32,086 empty columns were already in `cached_datasets.columns_json` — parsed by us, sitting in a different table from the one that uses them. Probing ten of the remainder against CKAN returned fields for **one** and 404 for eight: they are not in the DataStore. The plan's own §2.5 says as much — "DataStore con columnas tipadas en 199 recursos", about 3 % of the catalogue. |
| Harvest sources nuevas | **Already covered, by construction.** datos.gob.ar publishes 45 organizations; we hold datasets from **58** distinct organizations of that portal, INDEC and IGN among them. Scraping the aggregated catalogue includes whatever the harvest feeds into it. Nothing to add. |
| PDF / DTA / SAV | **None of the three exist in the catalogue** — zero rows each. The format the plan omits is the one that matters: **geojson, 4,135 resources, third-largest after csv and zip**, and it already collects at 98 %. |

### Where the format work actually is

Not in new format support. Measured by absolute losses:

```
csv    1,905 failures    (83.4 % success)
zip      689             (90.4 %)
xlsx     364             (85.7 %)
xls      118             (92.6 %)
```

Those are parser failures on formats already supported, which belongs to spec
021, not here. A new decoder would add nothing; `csv` alone loses more resources
than DTA and SAV would ever have contributed.

### The plan's warning, checked

§2.5 says: *"migración upstream incompleta (presupuesto, producción, defensa sin
gemelo nuevo → **no borrar era vieja sin gemelo**)"*.

Verified after the duplicate cleanup ran: **17,113 single-identity resources are
intact and servable**, and a group needs two members before any of it becomes a
candidate, so a resource without a twin was never eligible. The 461 identity
groups with no servable member hold 1,014 rows, all `error` or
`permanently_failed`, and **none** in the state the cleanup produces. Those
groups failed to collect; they were not emptied.
