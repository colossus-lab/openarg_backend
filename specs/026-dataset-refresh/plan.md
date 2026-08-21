# Plan 026 — Dataset refresh

**Spec**: [./spec.md](./spec.md)
**Status**: Draft

The order below puts the honest, cheap thing first and the load-bearing,
dangerous thing behind a measurement. That is deliberate: the risky part of this
work is not deciding to refresh, it is refreshing 24,097 resources without
repeating the incident that restarted the database in May.

---

## Phase A — Say how old the data is

*Independent of everything else, worth shipping alone, and true today whether or
not refresh ever ships.*

Users cannot currently tell that an answer rests on May's data. Every resource
awaiting its first refresh will keep resting on it for a while, so the age is
worth exposing before the cadence is fixed rather than after.

- Surface the collected-at age of the tables behind an answer.
- A metric for the staleness distribution, so SC-005 can be observed rather than
  inferred.

Nothing here changes collection behaviour, so it carries none of the risk of the
phases that follow.

---

## Phase B — A freshness policy that is data, not a constant

`app/application/collection/freshness.py`

```
refresh_age_for(portal: str, resource_identity: str) -> timedelta | None
```

Per-portal default with a per-resource override, read from config rather than
compiled in. `None` means never refresh — the right answer for a resource that
genuinely does not change.

**The default is not decided here** (CL-026-001). A single global TTL is wrong in
both directions at once: too short and a census is re-downloaded weekly for
nothing, too long and BCRA indicators go stale in a platform built to report
them. The mechanism takes a policy; the numbers belong with whoever knows the
sources, and inventing them in a plan file would be the kind of confident guess
this work has spent the day removing.

Ship with everything `None` — refresh disabled — so Phase C can be built and
tested before any resource is actually re-read.

---

## Phase C — Eligibility, separated

`bulk_collect_all` selects on `is_cached = false AND NOT EXISTS (a ready row)`.
Refresh needs a second predicate — *has data, and it is older than its policy* —
and it must be **a separate query and a separate budget** (FR-002).

If refresh shared the first-collection path, one backlog would starve the other:
24,097 stale resources would crowd out every genuinely new dataset, or the
reverse, and which one won would depend on ordering nobody chose.

Also FR-009: reuse the existing `_mart_rebuild_in_progress` backpressure. It was
added after 152 concurrent collects and a 52M-row matview restarted RDS, and
refresh multiplies steady-state load rather than adding a one-off.

---

## Phase D — Refresh that cannot lose the current data

The difference from a first collection is that a refresh replaces something that
works. A source that now 404s, or serves an error page with a `.csv` extension,
must not end with the previous version gone.

- Write the new version, supersede the old one only after it lands (FR-004).
  Never drop-then-fetch.
- A failed refresh leaves the resource `ready` on its previous version and does
  **not** mark it failed (FR-005) — the resource has data, the *refresh* failed,
  and conflating them would eventually mark half the catalogue as broken.
- Record provenance exactly as a first collection does (FR-007), which is what
  makes the resulting pair attributable and closes the loop with
  [025](../025-self-repair/spec.md).

---

## Phase E — Turn it on, narrowly

One portal, a small budget, and a week of watching. Then widen.

The backlog question (CL-026-002) is deliberately left until there is evidence
from a narrow run: draining 24,097 resources on a schedule is faster and is
precisely the load pattern that caused the May incident.

---

## What this unblocks

[025-self-repair](../025-self-repair/spec.md) in its entirety. Its measurements
all end here: zero attributable pairs, because a fingerprint reaches a snapshot
only through a new registration and new registrations do not happen for a
resource already on disk. The first refresh of a resource that has been
snapshotted produces the first pair the drift subsystem can actually reason
about.

**Sequencing note.** 025's Phase 3 (the router) stays blocked until this runs,
and that is now a known wait rather than an open question. Building the signature
before then would mean designing it against imagined examples — which is the
failure this project has spent the day correcting in three other places.
