# Spec 022 — Mart Quality Audit

**Type**: Reverse-engineered (from the July 2026 sprint, PRs #25/#33/#34/#35/#36/#41)
**Status**: Draft — written 2026-08-19, after the fact. The code shipped 2026-07-26 → 2026-08-01 without a spec.
**Last synced with code**: 2026-08-19
**Hexagonal scope**: Application (checks + auditor) + Infrastructure (Celery task + migrations)
**Related plan**: [./plan.md](./plan.md)
**Sister specs**: [019-marts](../019-marts/spec.md) (what is being audited),
[013-ingestion-validation](../013-ingestion-validation/spec.md) (the findings lifecycle this reuses),
[016-serving-port](../016-serving-port/spec.md) (who consumes the block)

---

## 1. Context & Purpose

A mart can be **broken in ways that every stored signal reports as healthy**.
`last_refresh_status = 'built'` and `last_row_count = 4,962` say nothing about
whether those rows are the right rows, whether an amount column is text, or
whether the mart was assembled from 6 % of the sources its description claims
to cover.

Three such defects were found by hand during July 2026, each by a person
reading an answer and noticing the number was wrong:

| Mart | Defect | How it looked from the catalog |
|---|---|---|
| `presupuesto_nacional_ejecutado` | Built from **32 of 560** candidate source tables; TEXT amount columns with mixed decimal formats; a corrupt `'2.022'` year | `built`, non-zero rows |
| `presupuesto_consolidado` | A `WHERE credito_devengado <= credito_vigente` guard dropped **3,219 of 7,635 source rows (42,2 %)**, taking 55,1 % of the executed budget with it | `built`, non-zero rows |
| `mediaciones_prejudiciales` | `refresh_failed` left `last_row_count = 0` over a view that still held **52,086,049 rows** → silently removed from serving | `refresh_failed`, zero rows |

This module turns those three hand-found defects into a **nightly sweep**, so
the fourth one is caught by a machine instead of by a user reading a wrong
answer. It also adds the switch the platform lacked: a way to say *"this mart
has rows, but they are wrong — do not serve it"* without deleting it.

**The module reports; it does not remediate.** Every finding names the fix and a
human applies it. See §6 for why.

---

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Check** | One quality question asked of one mart. Returns zero or more findings. |
| **Finding** | One concrete problem, with a severity, a message in Spanish for the operator, and a `remediation` describing the fix. Reuses the ingestion `Finding` entity. |
| **Audit context** | Everything known about a mart at audit time, gathered once and shared by all checks: its catalog registration, its materialized columns and approximate row count, the source tables it was built from, and its 30-day traffic. |
| **Candidate tables** | How many source tables a `live_tables_by_*` macro considered *before* its column filter. |
| **Kept tables** | How many survived that filter. `kept / candidate` is the mart's **source coverage**. |
| **Hidden mart** | A mart whose stored `last_row_count` is 0 while the materialized view holds rows — removed from discovery by bookkeeping, not by emptiness. |
| **Serving block** | A deliberate, reasoned withdrawal of a mart from serving, declared in the mart YAML. Distinct from a hidden mart, which is accidental. |
| **Finding key** | What distinguishes two findings from the same check on the same mart. Part of the idempotency tuple. |

---

## 3. User Stories

- **US-001 (P1)**: As an operator, I want a nightly report of every mart that
  carries a known defect, **so that** I stop discovering them by reading wrong
  answers in the chat.
- **US-002 (P1)**: As an operator, I want to withdraw a mart from serving
  without deleting it, and to record why, **so that** a mart known to be wrong
  stops reaching users while it is being fixed.
- **US-003 (P1)**: As a user of the chat, I want the system to refuse to answer
  from a mart that has been declared unfit, **so that** I am never given a
  confident number that the platform itself does not stand behind.
- **US-004 (P2)**: As an operator, I want findings that stop being reported to
  close automatically, **so that** the report reflects the present and does not
  become a list nobody reads.
- **US-005 (P2)**: As an operator, I want the report ordered by real traffic,
  **so that** a broken mart with 67 hits is not buried under one with none.
- **US-006 (P3)**: As a maintainer, I want to try a threshold change without
  rewriting the findings history, **so that** tuning a check is cheap.

---

## 4. The check suite (4 checks)

Ordered by how far upstream the damage starts. Reachability first: a mart
nobody can query makes the quality of its columns a secondary question.

### 4.1 `mart_hidden_despite_rows` — CRITICAL

Two signals about the mart's own bookkeeping:

- **CRITICAL** when `last_row_count = 0` but the view holds rows. The build
  failed and left the counter at zero over data that is still there; discovery
  filters the mart out and nothing reports it.
- **WARN** when `last_refresh_status ∈ {build_failed, refresh_failed}`,
  regardless of counts. Whatever is being served comes from an earlier build
  and its freshness is unknown.

Not applicable to a mart with `serving_blocked = true`: a mart withheld on
purpose is not hidden, and its reason is already recorded.

### 4.2 `mart_source_coverage` — WARN, CRITICAL below 25 %

Reports when a mart was built from a small share of its candidate sources.
Thresholds: **< 60 % → WARN**, **< 25 % → CRITICAL**, chosen so the measured
`presupuesto_nacional_ejecutado` case (6,4 %) lands as critical while an
ordinary fact-vs-dimension filter does not.

A low ratio is not a bug by itself — `require_all_columns=True` is *supposed*
to filter. What makes it reportable is that **nobody chose it and nobody can
see it**. Only applicable when the macro recorded a coverage marker and
considered more than one table.

### 4.3 `mart_amount_filter_before_aggregation` — CRITICAL

Detects a comparison between two amount columns inside the `WHERE` of an
aggregating query. That filter runs **per source row, before the `GROUP BY`**,
so it discards real execution to prevent a condition that usually does not
occur at the grain the mart serves.

Two signals, neither conclusive alone: the SQL shape (catchable without
touching data) and the source-rows-in vs mart-rows-out delta, which is reported
as **context, never as evidence** — a `GROUP BY` collapses rows by design.

Remediation is always the same: move the invariant to `HAVING` over the
aggregates.

### 4.4 `mart_amount_column_is_text` — CRITICAL for amounts, WARN for years

Flags columns whose name denotes a quantity (`credito`, `monto`, `importe`,
`total`, `cantidad`, `presupuesto`, `gasto`, …) or a year (`anio`, `ejercicio`,
`year`) that reached serving as a non-numeric Postgres type.

`_build_union` casts every column to `::text` so heterogeneous source tables
can be stacked; the mart's outer `SELECT` is supposed to cast back. When it
does not, an amount column holds `'1.234,56'` and `'1234.56'` in the same
column and every aggregate over it is either an error or a silent undercount.

Columns whose names look numeric but are identifiers (`_id`, `codigo`, `cuit`,
`dni`, `expediente`) are excluded: casting them would be wrong and their
leading zeros are meaningful.

---

## 5. Functional Requirements

**The sweep**

- **FR-001**: The audit MUST run over **every registered mart**, including
  those with no findings, so that "clean" is an observed state rather than an
  absence of data.
- **FR-002**: The audit MUST read only catalogs — `mart_definitions`,
  `pg_class`/`pg_attribute`, `raw_table_versions`, `query_analytics`. It MUST
  NOT query mart contents. A sweep over 71 marts has to stay cheap enough to
  run nightly.
- **FR-003**: A check that raises MUST NOT stop the other checks, nor the audit
  of the remaining marts. The failure is logged with the check name and the
  mart id.
- **FR-004**: Results MUST be ordered by 30-day traffic descending, so the
  report opens with what users actually hit.
- **FR-005**: The absence of `query_analytics` MUST NOT stop the sweep.
  Traffic is used for prioritising, not for deciding.
- **FR-006**: Row counts read from `pg_class.reltuples` are **estimates by
  design**. `-1` (never analysed) MUST be normalised to *unknown*, never to
  zero — that is exactly the distinction `mart_hidden_despite_rows` depends on.

**Findings lifecycle**

- **FR-007**: Findings MUST be persisted to `ingestion_findings` with
  `mode = 'mart_audit'` and `resource_id = 'mart::<mart_id>'`, reusing the
  existing idempotent upsert rather than standing up a parallel table.
- **FR-008**: A check that can emit more than one finding per mart MUST set a
  distinct `finding_key`. The persistence tuple is
  `(resource, detector, version, mode, input_hash)`; two findings sharing it
  overwrite each other silently.
- **FR-009**: When two findings still collide on the same key, the task MUST
  log an error naming the check, and persist only the first — rather than let
  the summary report a count the table does not hold.
- **FR-010**: Findings a mart no longer reports MUST be resolved, **including
  the partial case**: a mart with three bad columns, one of which gets fixed,
  MUST drop to two open findings.
- **FR-011**: Resolution MUST be scoped to `mode = 'mart_audit'` so it can
  never close an ingestion finding that happens to share a resource id.
- **FR-012**: The task MUST support a `persist=False` mode that runs the checks
  and returns the summary without touching the findings table.

**Serving block**

- **FR-013**: `mart_definitions.serving_blocked` MUST be driven from the mart
  YAML (`serving.blocked` / `serving.blocked_reason`), never written by the
  auditor. A DB-only flag is erased by the next `build_mart`, producing a block
  that silently disappears.
- **FR-014**: `serving.blocked: true` without a `blocked_reason` MUST be
  rejected at YAML load. A mart withheld without a recorded reason is
  indistinguishable from one withheld by accident.
- **FR-015**: Every mart **discovery** path MUST filter
  `NOT COALESCE(serving_blocked, FALSE)`.
- **FR-016**: The block MUST also be enforced **at execution**, not only at
  discovery: NL2SQL can name a blocked mart directly, the relation still exists
  in `mart.*`, and the prefix-free allowlist would wave it through.
- **FR-017**: The default MUST be `FALSE`, leaving every existing mart exactly
  as it was.

---

## 6. Success Criteria

- **SC-001**: The three defects that motivated the module
  (`presupuesto_nacional_ejecutado`, `presupuesto_consolidado`,
  `mediaciones_prejudiciales`) are each reported by at least one check on a
  sweep of the state in which they were found.
- **SC-002**: A full sweep over 71 marts completes inside the task's 600 s soft
  limit using catalog reads only.
- **SC-003**: Two consecutive sweeps with no change produce **zero new rows**
  in `ingestion_findings` — the second updates the first.
- **SC-004**: Fixing one of three findings on a mart leaves exactly two open.
- **SC-005**: A query naming a `serving_blocked` mart is refused with the
  recorded reason, whether the mart arrived via discovery or was named directly
  by NL2SQL.
- **SC-006**: A critical finding emits a `WARNING`-level log line, on purpose:
  it means something is being served that the platform cannot stand behind.

---

## 7. Assumptions & Out of Scope

**Assumptions**

- `mart_definitions.sql_definition` holds the **macro-resolved** SQL, so
  `raw."tabla__hash__vN"` references are literal text. The auditor parses them
  rather than re-resolving macros.
- The `macro_coverage: kept N of M` marker is present in the resolved SQL of
  marts built after PR #33. A mart built before it **cannot** be assessed for
  coverage, and the check stays silent rather than inventing a denominator.
  Rebuilding the mart populates it.

**Out of scope**

- **Automatic remediation.** The auditor never edits a mart, never rebuilds
  one, and never writes to `mart_definitions`. See FR-013 for why a
  self-applying block would be worse than none.
- **Content-level checks** (value distributions, nulls, referential integrity).
  Everything here is answerable from metadata; distribution checks need a
  different cost model. See [DEBT-022-002].
- **Alerting.** Findings land in a table. Who reads them, and when, is not
  solved here — and that is the module's biggest weakness. See [DEBT-022-001].

---

## 8. Open Questions

- **[NEEDS CLARIFICATION CL-022-001]** — The coverage thresholds (25 % / 60 %)
  were calibrated against a single measured case. No second data point exists
  yet to confirm that an ordinary dimension filter stays under the WARN line.
- **[NEEDS CLARIFICATION CL-022-002]** — `mart_amount_column_is_text` matches
  column names by substring. `total` matches `total_general` but also
  `subtotal_id`-style names that the `_ID_HINTS` list may not cover. The false
  positive rate has not been measured over the 71 marts.

---

## 9. Tech Debt Discovered

- **[DEBT-022-001] — No consumer.** Findings are written to a table that
  nothing reads. `audit_marts` has run nightly at 03:45 since 2026-07-27 and
  the four marts in `build_failed` in production on 2026-08-19 were discovered
  by a manual audit, not by the sweep it runs every night. **This is the single
  change that would make the module worth its existence**, and it is precisely
  what §5.5 of the 2026 plan calls out: *a metric without a consumer is
  decoration*.

- **[DEBT-022-002] — The 0-row silent failure is only half covered.**
  `mart_hidden_despite_rows` catches `stored = 0 ∧ actual > 0`. It does **not**
  catch `stored = 0 ∧ actual = 0` with `last_refresh_status = 'built'` — a mart
  that built successfully and legitimately produced nothing, which is how
  `presupuesto_consolidado` and `pobreza_indec_aglomerados` sit in production
  today. Those disappear from discovery with no finding at all. A `built` mart
  resolving to zero rows is almost always a macro that matched nothing, and
  deserves its own check.

- **[DEBT-022-003] — The serving block is not enforced in the Data API.**
  FR-015 holds in eight of ten discovery paths. The two exceptions are
  `data_router.py:241` (table listing) and `data_router.py:407` (semantic mart
  search) — the service-to-service API that TerritorIA consumes. A blocked mart
  is hidden from the chat and still served there. Latent today only because
  `DATA_SERVICE_TOKEN` is unset in production, which makes both endpoints
  return 503; configuring it — the exact step needed for TerritorIA to consume
  OpenArg — opens the gap.

- **[DEBT-022-004] — Traffic is read from one of two `query_analytics`.**
  Production holds two tables of that name, in `public` (373 rows) and `raw`
  (791 rows), and which one is read depends on the connection's `search_path`.
  The `hits_30d` that orders the report is therefore computed over part of the
  traffic. Tracked at the platform level, not here.

- **[DEBT-022-005] — Checks are instantiated in a flat list**
  (`build_default_mart_checks()`), matching the ingestion detectors' shape.
  There is no way to enable, disable or re-threshold a single check without
  editing code. Acceptable at four checks; revisit at ten.
