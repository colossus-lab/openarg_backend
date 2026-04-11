# Spec: Connector Staff (HCDN + Senado Legislative Personnel)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Application + Infrastructure
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Query connector over **Argentine legislative staff**: employees of the Chamber of Diputados (HCDN) and the Senado. Supports searches by legislator (how many advisors a given diputado has), change detection (employee hires/exits), aggregate statistics, and free-text search. Data is snapshotted weekly via a scrape of the CKAN datastore.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Legajo** | Unique employee identifier in HCDN. |
| **Escalafon** | Employee category/hierarchy. |
| **Area de desempeno** | Organizational unit of the employee (e.g. "Despacho del Diputado X"). |
| **Convenio** | Type of contract (permanent, temporary, etc.). |
| **Alta / Baja** | Hire / exit of an employee detected between snapshots. |
| **Asesor** | Employee attached to a legislator's office. |

## 3. User Stories

### US-001 (P1) — Count a legislator's advisors
**As** a user, **I want** to ask "how many advisors does X have?" and obtain the count with suggestions if the name does not match.

### US-002 (P1) — List a legislator's advisors
**As** a researcher, **I want** the detail of a diputado's advisors.

### US-003 (P2) — Recent changes
**As** an analyst, **I want** to see hires and exits detected in the latest snapshots.

### US-004 (P2) — Free-text search
**As** a user, **I want** to search by employee last name or first name.

### US-005 (P3) — Aggregate statistics
**As** an operator, **I want** to know the total employees, distinct areas, and escalafones.

## 4. Functional Requirements

- **FR-001**: MUST expose port `IStaffConnector` with 5 methods: `get_by_legislator`, `count_by_legislator`, `get_changes`, `search`, `stats`.
- **FR-002**: MUST implement smart name matching in cascade: full name → words ≥3 chars sorted by descending length → patterns until a match is found.
- **FR-003**: MUST query `senado_staff` first (by senator_name) and fall back to `staff_snapshots` (HCDN, by `area_desempeno ILIKE`) if there is no match.
- **FR-004**: MUST suggest `areas_similares` when there is no match to improve UX.
- **FR-005**: MUST escape LIKE special characters (`\`, `%`, `_`) to prevent SQL injection.
- **FR-006**: MUST diff consecutive snapshots to detect hires/exits and persist them in `staff_changes`.
- **FR-007**: MUST snapshot the full HCDN catalog weekly.
- **FR-008**: MUST paginate CKAN datastore downloads with 5000-record pages.

## 5. Success Criteria

- **SC-001**: Smart matching resolves ≥95% of queries with known legislator names.
- **SC-002**: Weekly snapshot completes in **<10 minutes**.
- **SC-003**: Hire/exit detection is 100% accurate (no false positives/negatives) when HCDN keeps legajos stable.
- **SC-004**: Zero SQL injections via `_escape_like()`.

## 6. Assumptions & Out of Scope

### Assumptions
- HCDN publishes the full roster in the CKAN datastore with a stable resource ID (`6e49506e-6757-44cd-94e9-0e75f3bd8c38`).
- Senadores have scrapeable profiles on the Senado website with a stable structure.
- Legajos are unique enough to diff.

### Out of scope
- Executive Branch personnel (belongs in `002n-mapa-estado/`).
- Judicial personnel.
- Salaries (the dataset does not expose them).
- Intraday changes (only changes between weekly snapshots are detected).

## 7. Open Questions

- **[NEEDS CLARIFICATION CL-001]** — What happens if HCDN changes the datastore resource_id? The scraper would break silently.
- **[RESOLVED CL-002]** — **Scraping works and is fresh** (verified in prod DB 2026-04-10). `senado_staff` has **908 records** across **72 distinct senadores** (24 provincias × 3 = 72 ✓ complete set). `MAX(created_at) = 2026-04-06 04:33 UTC` — matches the `snapshot_senado_staff` schedule (Monday 01:30 ART, Apr 6 was a Monday). **No visible failures** in the recent scraper runs.
- **[RESOLVED CL-003]** — **BUG DISCOVERED**: the diff in `staff_tasks.py:136-158` **only detects hires** (new `legajo` not present in previous snapshot) and **exits** (`legajo` present before and absent now). **Field changes on existing employees (e.g. `area_desempeno` X → Y) are NOT detected anywhere**. If an advisor is transferred between diputados' offices, it is invisible to the system. The `staff_changes` table only records hires/exits, never updates. See [`FIX_BACKLOG.md#fix-008`](../../FIX_BACKLOG.md).
- **[NEEDS CLARIFICATION CL-004]** — `areas_similares` is only offered when there is no match. Should it always be offered as a suggestion?

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Smart pattern cascade is inefficient** — multiple sequential SQL roundtrips instead of a single JOIN with OR.
- **[DEBT-002]** — **Snapshot-based model is rigid** — intermediate changes are not captured, diff depends on execution order.
- **[DEBT-003]** — **Senado scraping via HTML parsing** (`senado_staff_tasks.py`) — breaks if HTML structure changes, high maintenance burden.
- **[DEBT-004]** — **No full-text index (GIN) on `staff_snapshots`** — ILIKE full table scan in free-text search queries.
- **[DEBT-005]** — **`_escape_like()` is basic** — does not handle edge cases (empty patterns, complex escape sequences).
- **[DEBT-006]** — **`areas_similares` only on miss** — inconsistent UX.
- **[DEBT-007]** — **Hardcoded datastore resource ID** (`6e49506e-...`). No discovery mechanism.

---

**End of spec.md**
