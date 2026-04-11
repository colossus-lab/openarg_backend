# Spec: Connector CKAN Search (Federated Multi-Portal)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-11 (datastore pagination fix)
**Hexagonal scope**: Domain + Application + Infrastructure
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Federated search connector over **30+ national, provincial and municipal CKAN portals** of Argentina (nación, diputados, justicia, energía, transporte, salud, cultura, producción, MAGyP, ARSAT, ACuMAR, interior, PAMI, desarrollo social, turismo, SSN, CABA, legislatura CABA, PBA, Córdoba, Mendoza, Entre Ríos, Neuquén, Tucumán, Misiones, Chaco, Ciudad Mendoza, Corrientes). It implements a cascading retrieval strategy: **SQL-like datastore → CSV download → metadata only**.

It is the "thickest" connector in the system (321 lines in the adapter) and the one that handles the greatest variety of formats and upstream failures.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **CKAN portal** | Independent instance of CKAN published by an Argentine agency. |
| **Datastore** | CKAN feature that allows SQL-like queries over tabular resources. |
| **Resource** | Downloadable file associated with a dataset (CSV, JSON, XLS). |
| **package_search** | Standard CKAN endpoint for full-text search over datasets. |
| **Dead portal** | Portal that does not respond or that migrated to another system. OpenArg has a list of 10+. |

## 3. User Stories

### US-001 (P1) — Multi-portal free-text search
**As** a user, **I want** to search for "primary education datasets" and get results from all relevant portals.

### US-002 (P1) — Direct datastore query
**As** the system, **I want** to execute a SQL-like query on a specific resource when we know its `resourceId`.

### US-003 (P2) — Fallback if the datastore is not available
**As** a user, **I want** the system to download the CSV directly when the datastore fails, and parse the first 500 records.

### US-004 (P2) — Use cached tables first
**As** the pipeline, **I want** to search in the local `cache_*` tables before making HTTP calls, to reduce latency and upstream load.

### US-005 (P3) — Scheduled catalog scraping
**As** the system, **I need** to scrape the catalogs of the active portals daily on a staggered schedule (03:00-05:50).

## 4. Functional Requirements

- **FR-001**: MUST expose an `ICKANSearchConnector` port with `search_datasets(query, portal_id, rows)` and `query_datastore(portal_id, resource_id, q, limit)`.
- **FR-002**: MUST execute a parallel search on all active portals with `asyncio.gather`.
- **FR-003**: MUST apply cascading strategy: datastore → CSV download → metadata-only.
- **FR-004**: MUST truncate CSVs to **a maximum of 2MB and 500 rows** (hard limit).
- **FR-005**: MUST auto-detect delimiter (`,` vs `;`) and types (int/float/string).
- **FR-006**: MUST sanitize the query by removing stopwords before searching.
- **FR-007**: MUST prioritize local tables in `cache_*` over HTTP calls.
- **FR-008**: MUST isolate failures per portal: a downed portal does not invalidate results from others.
- **FR-009**: MUST maintain a documented list of dead portals with the reason for the death.
- **FR-010**: MUST scrape catalogs on staggered schedules so as not to hit portals simultaneously.
- **FR-011**: **CSV truncation transparency** — when a CSV download is truncated (either because the byte stream exceeded `MAX_CSV_BYTES = 2 MiB` or because the row count exceeded `MAX_CSV_ROWS = 500`), the connector MUST populate the returned `DataResult.metadata` with:
  - `truncated: True` — boolean flag (FR-011a)
  - `truncation_reason: "max_bytes_exceeded" | "max_rows_exceeded"` — which of the two limits fired first (FR-011b)
  - `truncation_limit: <int>` — the limit value that was hit, for self-describing metadata (FR-011c)
  This exists so the downstream analyst prompt and the frontend can surface the fact that the user is seeing a partial view of the dataset, instead of assuming the returned rows are the complete dataset. The limits themselves are not changing in this FR — only the visibility of the fact that they fired.
- **FR-012**: **Datastore pagination transparency** — when a `datastore_search` call returns only the first page of a larger dataset (upstream `total > limit`), the connector MUST populate `DataResult.metadata` with the same three keys as the CSV truncation path, but with a datastore-specific reason code:
  - `truncated: True` (FR-012a)
  - `truncation_reason: "datastore_pagination_limit"` (FR-012b)
  - `truncation_limit: <limit that was requested, e.g. 50>` (FR-012c)
  - `total_available: <int>` — the real `total` reported by the CKAN response, so the analyst and UI can compare "fetched 50 of 5000" (FR-012d)
  This closes the sibling silent data loss identified in CL-003: before FR-012, the datastore path returned the first 50 rows and reported `metadata.total_records = 50`, making the partial view look complete. The connector still only fetches one page — raising the page size or iterating would change upstream load characteristics and is deliberately out of scope for this FR. We only change visibility, same philosophy as FR-011. If a future iteration needs complete data, a separate FR can introduce a paging loop with a configurable row budget.

## 5. Success Criteria

- **SC-001**: Federated search completes in **<10 seconds (p95)** for 20+ live portals.
- **SC-002**: ≥80% of queries are resolved with **local cache** before making HTTP calls.
- **SC-003**: **Zero cascade failures** when 1+ portal is down.
- **SC-004**: Dead portals are updated manually at least once a month.

## 6. Assumptions & Out of Scope

### Assumptions
- The CKAN portals implement the standard `/api/3/action/package_search` API.
- The dead portals list is reviewed periodically.
- Users tolerate queries of up to 10s for complex multi-portal searches.

### Out of scope
- Ingestion of downloaded datasets (delegated to the collector).
- Parsing of exotic formats (Excel, JSON-LD, XML).
- Full-text search over the content of the CSVs (only over metadata).
- Non-CKAN portals (DKAN lives in another connector, see `002k-dkan-portals`).

## 7. Open Questions

- **[RESOLVED CL-001]** — Last substantial update of `dead_portals`: **2026-03-08** (commit `4eff38c1` — "feat: streaming collector, new portals, chunked CSV loading"). Commit `d81657bd` from 2026-03-20 was only a formatting pass. **No audit in >1 month** (today 2026-04-10). **Recommendation**: manual quarterly audit or an automated task that periodically pings the dead portals.
- **[RESOLVED CL-002]** — **Hardcoded, not configurable**. Module-level constants in `ckan_search_adapter.py:18-19`: `MAX_CSV_BYTES = 2 * 1024 * 1024` and `MAX_CSV_ROWS = 500`. No per-portal override, no env var. Changing them requires a code edit.
- **[RESOLVED CL-003]** — ~~**No pagination silent data loss**~~ **FIXED 2026-04-11**: `_fetch_datastore()` still makes a single `limit=50` call (upstream load considerations unchanged), but it now reads the CKAN response's `total` field and, when `total > limit`, populates `metadata.truncated`, `metadata.truncation_reason = "datastore_pagination_limit"`, `metadata.truncation_limit`, and `metadata.total_available` — see FR-012a/b/c/d above. The data loss is no longer silent; the downstream analyst and the frontend can see "50 of 5000 fetched" and surface it to the user. A full paging loop (fetching all rows) remains out of scope.
- **[RESOLVED CL-004]** — Simple heuristic: `delimiter = ";" if header.count(";") > header.count(",") else ","` (`ckan_search_adapter.py:298`). **Edge case NOT handled**: if the CSV uses `;` as delimiter but has commas inside quoted fields (e.g., `"name, address";age`), the heuristic counts the quoted commas as separators and incorrectly picks `,`. Downstream `csv.DictReader` respects quoting, but the prior detection already failed. **Debt**: naive detection, does not parse quoting before counting.

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Hardcoded list of 30+ portals** in `ckan_search_adapter.py:21-229`. Adding a portal requires a code change + deploy. No dynamic registration in the DB.
- **[DEBT-002]** — **Hardcoded dead portals list** in `ckan_search_adapter.py:233-279`. No automated health checks, no metrics.
- **[DEBT-003]** — ~~**Silent truncation of CSVs >2MB or >500 rows**~~ **FIXED 2026-04-11**: the connector now populates `metadata.truncated`, `metadata.truncation_reason` (`max_bytes_exceeded` / `max_rows_exceeded`), and `metadata.truncation_limit` whenever either CSV limit fires (FR-011a/b/c). Downstream consumers — the analyst prompt, the chart builder, the frontend sources panel — now have a self-describing signal that the dataset view is partial. The limits themselves (`MAX_CSV_BYTES = 2 MiB`, `MAX_CSV_ROWS = 500`) are unchanged; only the visibility of them firing is.
- **[DEBT-004]** — **Heuristic delimiter detection** — does not handle cases where both delimiters appear in the data (commas inside quoted strings with `;` as main delimiter).
- **[DEBT-005]** — **Silent exception swallowing per portal** in `ckan_search_adapter.py:423`. Upstream errors are logged as warnings but do not emit metrics — invisible in observability.
- **[DEBT-006]** — **No retry decorator explicitly applied** to the adapter.
- **[DEBT-007]** — **Cached-table search depends on the sandbox service** — if the sandbox is not available, there is no fallback to direct HTTP; it breaks.
- **[DEBT-008]** — **Hardcoded stopwords list** (`ckan.py:23-75`). Not multilingual, not tunable by context.

---

**End of spec.md**
