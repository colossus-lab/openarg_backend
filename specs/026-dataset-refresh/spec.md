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

### 4.1 Age is not one number

A single global TTL is the obvious design and it is wrong in both directions at
once: too short and the platform re-downloads a census every week for nothing;
too long and BCRA indicators go stale in a system built to report them.

The cadence should derive from what the resource is. The first cut can be crude —
a per-portal default with per-resource override — because crude and stated beats
uniform and wrong. What matters is that the mechanism takes a policy rather than
a constant.

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
- **FR-006**: Refresh cadence MUST be expressible per portal with a per-resource
  override, and MUST have a documented default.
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

- **[NEEDS CLARIFICATION CL-026-001]** — The default cadence. Nothing in the
  system implies one, and picking it here would be inventing an answer that
  belongs to whoever knows what these datasets are.
- **[NEEDS CLARIFICATION CL-026-002]** — Whether to drain the 24,097-resource
  backlog on a schedule, or leave it to the ordinary cadence and accept that the
  oldest data takes longest to come back. The first is faster and is exactly the
  load pattern that caused the May incident.
- **[NEEDS CLARIFICATION CL-026-003]** — Whether a refresh that finds identical
  bytes should count as a read. It costs the download either way, and recording
  it is what makes "unchanged for six months" a statement rather than an absence.
