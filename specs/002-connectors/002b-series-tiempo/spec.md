# Spec: Connector Series de Tiempo (datos.gob.ar)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-11
**Hexagonal scope**: Domain + Application + Infrastructure
**Extends**: [`../spec.md`](../spec.md) (generic connector pattern)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The Series de Tiempo connector allows querying Argentina's macroeconomic indicators (inflation, unemployment, exchange rate, GDP, exports, wages, baskets, etc.) from the official **datos.gob.ar/series** API. It is the main provider of temporal statistical data in the system: when a user asks "how did inflation evolve in 2025?", this connector is the one that responds.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Time series** | Sequence of observations indexed by date (monthly, quarterly, annual). |
| **Series ID** | Unique identifier of a series in the datos.gob.ar catalog (e.g., `145.3_INGNACUAL_DICI_M_38`). |
| **Collapse** | Temporal aggregation (month → quarter → year) with a function (avg, sum, last). |
| **Representation** | Transformation (value, percent_change, change). |
| **Curated catalog** | 15 pre-validated series with keywords for quick lookup (inflacion, desempleo, emae, etc.). |

## 3. User Stories

### US-001 (P1) — Point query for an indicator
**As** a user, **I want** to ask "what was inflation in March?" and get the official CPI value. Trigger: planner emits `PlanStep(action="query_series", params={query: "inflacion"})`.

### US-002 (P1) — Historical series for an indicator
**As** an analyst, **I want** to request "emae for the last year", and get all the monthly values for the period.

### US-003 (P2) — Regional unemployment
**As** a user, **I want** to ask "unemployment in the NEA", and the system **detects** that I need the regional series (not the national one) and loads them. Automation via regex on the query (see [DEBT-002]).

### US-004 (P2) — Daily snapshot of key indicators
**As** the system, **I need** to pre-load 12 key series (inflacion_ipc, tipo_cambio, emae, desempleo, gasto_publico, reservas, base_monetaria, salarios, canasta_basica, exportaciones, importaciones, actividad_industrial) daily into PostgreSQL, for fast querying via the sandbox.

## 4. Functional Requirements

- **FR-001**: MUST expose a `ISeriesTiempoConnector` port with methods `search(query, limit)` and `fetch(series_ids, start_date, end_date, collapse, representation, limit)`.
- **FR-002**: MUST implement a curated catalog of ≥15 series with keywords for fast matching without HTTP.
- **FR-003**: MUST auto-complete missing parameters with catalog defaults when a series is identified by keyword.
- **FR-004**: MUST detect regional queries and upgrade national series to their regional equivalents (especially 7-region unemployment).
- **FR-005**: MUST apply `percent_change` conversion by multiplying by 100 when percent representation is requested.
- **FR-006**: MUST set `endDate` to today when the user does not specify a range.
- **FR-007**: MUST daily-snapshot 12 key series persisted in PG (tables `cache_series_*`).
- **FR-008**: MUST return `DataResult` with records containing `fecha` + human-readable fields per series.
- **FR-009**: When a catalog-matched `fetch()` returns an empty response AND the request carried an explicit `start_date`/`end_date`, the pipeline-level `execute_series_step` MUST retry the fetch once **without** the date range. Rationale: the user often asks for a specific month (e.g., *"IPC de febrero 2026"*) before INDEC publishes it, so the narrow-range call returns `data: []` and the adapter gives up. Retrying without the range returns the latest available data, which the analyst can then frame as *"los datos más recientes son de enero 2026: X%"*. Zero retry is attempted when the fetch itself (HTTP/connectivity) fails — retry only triggers on an empty successful response. (FIX-013, 2026-04-11.)

## 5. Success Criteria

- **SC-001**: Keyword matching resolves ≥90% of common queries without a call to the `/search` endpoint.
- **SC-002**: Query of an individual series responds in **<2 seconds (p95)**.
- **SC-003**: Daily snapshot completes the 12 series in **<3 minutes**.
- **SC-004**: Regional unemployment upgrade is triggered on 100% of queries with geographic keywords (provincia, region, gba, noa, nea, cuyo, pampeana, patagonia).

## 6. Assumptions & Out of Scope

### Assumptions
- The datos.gob.ar/series API maintains its current contract and operational stability.
- The 15 series IDs in the curated catalog remain valid in the upstream API.
- Users accept latency of ≤24h (daily snapshot) for analytical queries.

### Out of scope
- Series forecasting.
- Correlation between heterogeneous series.
- Series with daily frequency (the API is mostly limited to monthly/quarterly/annual).
- Advanced statistical transformations (differences, logarithms, decomposition).

## 7. Open Questions

- **[NEEDS CLARIFICATION CL-001]** — Is the curated catalog of 15 series sufficient? Is there an expansion or automation plan?
- **[RESOLVED CL-002]** — **Last substantial update: 2026-03-23** (commit `ddd5692` — "Pipeline quality: hardened E2E validations, fix typo handling, IPC nacional, desempleo regional..."). In that commit at least the inflation ID (`148.3_INIVELNAL_DICI_M_26`) and the regional unemployment logic were updated. Original catalog commit: 2026-02-28 (`225052f2`). Keywords reformat commit: 2026-03-20 (`d81657b`, CI autoformatting only). **Since 2026-03-23: 18 days without substantive changes** (today 2026-04-10). The 15 series IDs **have not been massively validated against upstream since then** — they could have obsolete IDs undetected.
- **[NEEDS CLARIFICATION CL-003]** — The regional-upgrade is hardcoded for unemployment. Should it be extended to other indicators (regional GDP, regional CPI)?
- **[NEEDS CLARIFICATION CL-004]** — What happens if the user asks for a series that does exist in datos.gob.ar but is not in the curated catalog? Today it falls back to dynamic `/search` — does it work well?
- **[RESOLVED CL-005]** — **It IS marked now.** `series_tiempo_adapter.py:314-319,338-339` sets `metadata["unit"] = "percent"` and `metadata["value_scale"] = "percentage_points"` whenever `representation == "percent_change"`, exactly so the analyst prompt, charts and UI know not to display "15.2%" as "1520%". Superseded by fixed `DEBT-005`. (resolved 2026-04-11 via code inspection)

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Hardcoded curated catalog** in `series_tiempo_adapter.py:20-189`. No dynamic registration, no automatic update mechanism. Impact: the catalog can become obsolete without warning.
- **[DEBT-002]** — **Fragile regex-based regional upgrade** in `pipeline/connectors/series.py:79-86`. Detection via substring matching on the query; does not use intent classification. Impact: false positives/negatives on ambiguous queries.
- **[DEBT-003]** — **Naive accent-stripping keyword matching** (`series_tiempo_adapter.py:196-200`). Does not handle synonyms or typos (e.g., "desempleyo" does not match).
- **[DEBT-004]** — **No cache for search results** — each query to the `/search` endpoint makes an HTTP call even if the response is identical to a previous one.
- **[DEBT-005]** — ~~**`percent_change` representation is silently multiplied by 100**~~ **FIXED 2026-04-10**: `series_tiempo_adapter.py` now sets `DataResult.metadata["unit"] = "percent"` and `metadata["value_scale"] = "percentage_points"` whenever `representation == "percent_change"`, making the unit explicit for the analyst, chart builder and UI. Also records `metadata["representation"]` for non-default modes.
- **[DEBT-006]** — Pipeline step depends on the concrete adapter (recurring pattern — see `../spec.md[DEBT-002]`).
- **[DEBT-007]** — Snapshot task bypasses DI, uses `asyncio.run()` (recurring pattern — see `../spec.md[DEBT-003,DEBT-007]`).

---

**End of spec.md**
