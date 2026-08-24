# Spec 027 — Catalogue integrity: making the registry's answers true

**Status**: implemented 2026-08-23 · **Owner**: backend
**Supersedes nothing. Extends**: 015-catalog-resources, 017-raw-layer

---

## 1. Context & Purpose

`live_table()`, the Serving Port and every mart ask the registry where a table
lives. **Nothing asks the table.** When the two disagree the registry wins and
the data loses: the macro resolves to a schema the table is not in, and the mart
fails on a column that exists.

That is not hypothetical. Three marts were down in production for exactly this
shape, and the cause took weeks to find because *a registry that lies does not
contradict itself* — every query it answers is internally consistent.

This spec covers the mechanisms that keep the registry's answers true, and the
mechanisms that decide what a "resource" even is when a portal renames it.

### 1.1 The three ways the registry was wrong

Measured in production on 2026-08-23, before any of this ran:

| Shape | Count | What it broke |
|---|---|---|
| Row says `raw`, table is in `public` | 82 | `live_table()` finds nothing; the mart fails |
| Row says live, table exists nowhere | 111 | Same, silently |
| Table is served, no row at all | 4,019 | Coverage gate stuck at 86.4 % |

None of the three produced an error anywhere. The registry was internally
consistent in all three states.

---

## 2. Ubiquitous Language

- **Phantom row** — a live registry row naming a table that exists in no schema.
- **Misplaced table** — a table present in a schema the registry does not name.
- **Canonical identity** — the earliest `source_id` seen for a `(url, title)`,
  recorded in `datasets.original_identifier`.
- **Re-identification** — a portal issuing a new id for a resource we already
  hold. CKAN 2.11 did this to datos.gob.ar around 2026-07-29.
- **Federation duplicate** — the same file catalogued by both the publishing
  ministry and the national aggregator. **46 % of duplicates are this**, not
  re-identification.

---

## 3. Design decisions worth stating

### 3.1 Move the table, never edit the row to match

A misplaced table is moved to the schema its live row already names. The
reconciliation never edits `schema_name` to match a table it found.

Editing the row would also produce a consistent state, and it is the wrong one:
the direction of travel is `raw`, so a row edited to say `public` would have to
be edited back. Moving the table makes the registry true without this component
deciding where anything ought to live.

### 3.2 A phantom is retired, never deleted

The row is the only surviving evidence that the table existed — its provenance,
its row count, its source URL. The drift work is built on exactly that kind of
evidence. `superseded_at` is set; `DELETE` is not used.

### 3.3 Backfilled rows carry `legacy:unknown` provenance

The 4,019 unregistered tables are registered in the schema they are **actually
in** (`public`), not the schema new tables go to. Writing `raw` because that is
where the future lives is precisely the defect §3.1 exists to repair.

Their `parser_version` is `legacy:unknown`, which `is_real_provenance` rejects.
We do not know which parser read them, and a fingerprint we did not measure
would feed the drift cascade false evidence — a worse outcome than the gap.

### 3.4 Identity: same URL **and** same title, or nothing

Two real resources can be published from one endpoint that takes parameters.
Requiring the title to agree is what separates a rename from a coincidence. In
production 63 URL groups carry different titles and are genuinely distinct.

The anchor is the **earliest** row, not the newest: the point is a name that
predates the renaming, so a row arriving under a fresh CKAN id is recognised as
something already held.

### 3.5 Reconciliation records; it does not merge

Populating `original_identifier` decides nothing. Which of the 7,201 apparently
redundant tables to remove is a separate decision with an owner, and the right
shape for reconciliation is to make that decision *answerable*.

### 3.6 What "redundant" safely means is smaller than it looks

Of the 7,201 rows that looked redundant, three filters removed a quarter:

- **791 groups hold different row counts** — the file changed between
  collections. Not copies.
- **620 tables are named in a mart's SQL** — dropping them breaks serving.
- **7,207 are live in the registry** — dropping the table without retiring the
  row manufactures the phantoms of §1.1.

5,522 survive all three. A destructive job that is measured before it runs
almost always shrinks.

### 3.7 A cleanup that leaves its work queued is a treadmill

The dispatcher selects on `datasets.is_cached = false`. The duplicates' rows
were `false`, so the first version of the cleanup dropped 5,344 tables and left
every one of them queued to be collected again.

`is_cached = true` is written in the same transaction as the drop. It is honest:
the resource *is* cached, under the canonical twin, and the candidate query has
already proved that twin exists and holds rows.

---

## 4. Functional Requirements

- **FR-027-001** — A live registry row naming a table in another schema MUST
  result in the table being moved to the named schema, never the row edited.
- **FR-027-002** — A live row naming a table that exists nowhere MUST be
  retired with `superseded_at`, never deleted.
- **FR-027-003** — A served table with no registry row MUST be registered with
  the schema it is physically in and `parser_version = 'legacy:unknown'`.
- **FR-027-004** — Registration MUST refuse tables with zero rows: a registry
  row pointing at an empty table is worse than a gap, because `live_table()`
  would resolve to it.
- **FR-027-005** — An identity group MUST require both URL and title to match.
- **FR-027-006** — A resource whose identity is already registered MUST be
  reported and skipped, never merged by guess.
- **FR-027-007** — Duplicate removal MUST exclude groups with differing row
  counts, tables named in any mart SQL, and any group whose survivor cannot be
  proved to exist with rows.
- **FR-027-008** — Dropping a duplicate MUST retire its registry row and mark
  `datasets.is_cached = true` in the same transaction as the `DROP`.
- **FR-027-009** — Every sweep here MUST refuse to run when the registry is
  absent or holds fewer than `_REGISTRY_MIN_ROWS` rows.
- **FR-027-010** — Users, conversations, checkpoints and API keys MUST appear in
  an explicit never-touch list, and a test MUST assert it.

## 5. Success Criteria

- **SC-027-001** — Registry coverage above the 90 % gate. *Achieved: 99.8 %.*
- **SC-027-002** — Zero misplaced tables and zero phantom rows. *Achieved.*
- **SC-027-003** — No mart regressed. *Achieved: 69 healthy before and after.*
- **SC-027-004** — Users and conversations unchanged. *Achieved: 2,995 / 4,059
  before and after.*

## 6. Out of scope

- Deciding the fate of the 791 differing-count groups. They need case-by-case
  review, not a sweep.
- The eight tables duplicated between `public` and `raw`, including the three
  LangGraph checkpoint tables. Choosing a schema there strands either 493 or 774
  conversations and is a decision about user data.
