# Spec: Connector Georef (INDEC Geographic Normalization)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Application + Infrastructure
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Connector to the **INDEC Georef API** (`apis.datos.gob.ar/georef/api`) that normalizes Argentine geographic names (provincias, departamentos, municipios, localidades). It is the source of truth for resolving "what is the user referring to with X?" when mentioning a place. Supports hierarchical resolution with priority `provincia > departamento > municipio > localidad`.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Georef** | INDEC geocoding/normalization API. |
| **Provincia** | 23 provincias + CABA (24 total). |
| **Departamento** | Territorial subdivision within a provincia. |
| **Municipio** | Local administrative unit. |
| **Localidad** | Specific populated center. |

## 3. User Stories

### US-001 (P1) — Normalize an ambiguous name
**As** the system, **I want** to resolve "Córdoba" to "Provincia de Córdoba" (because it is more specific than the city) to feed downstream queries.

### US-002 (P2) — List provincias
**As** a developer, **I want** to obtain the full list of Argentine provincias.

### US-003 (P2) — Get departamentos of a provincia
**As** an analyst, **I want** all departamentos of Buenos Aires.

## 4. Functional Requirements

- **FR-001**: MUST expose port `IGeorefConnector` with methods for the 4 entities + `normalize_location(query)`.
- **FR-002**: MUST run the 4 queries in parallel with `asyncio.gather` when normalizing.
- **FR-003**: MUST apply priority provincia > departamento > municipio > localidad when resolving.
- **FR-004**: MUST use `@with_retry(max_retries=2, base_delay=1.0, service_name="georef")`.
- **FR-005**: MUST maintain a persistent `httpx.AsyncClient` (not reopened per request).
- **FR-006**: MUST return a `DataResult` with `portal_url` pointing to the Georef endpoint with the query.

## 5. Success Criteria

- **SC-001**: Normalization responds in **<2 seconds (p95)**.
- **SC-002**: 100% uptime when the upstream API is online (retry covers transients).
- **SC-003**: Correct resolution of ambiguous names in ≥90% of cases (metric not implemented).

## 6. Assumptions & Out of Scope

### Assumptions
- The Georef API follows its current, free contract.
- The provincia > departamento priority is reasonable for most queries.

### Out of scope
- Reverse geocoding (lat/lon → name).
- Distance or route calculation.
- Associated census data.
- Full shapefiles / GeoJSON.

## 7. Open Questions

- **[NEEDS CLARIFICATION CL-001]** — The hardcoded provincia > departamento > municipio > localidad priority can yield incorrect answers on queries like "La Plata" (which is a city, not a provincia). Should it be based on match quality?
- **[RESOLVED CL-002]** — **Partially insufficient**. Verified in prod DB (2026-04-10): provincias=24 ✓ (Argentina has exactly 24). But **departamentos=529**, **municipios=2082**, **localidades=4037** — the `max=100` limit is **insufficient for list-all operations**. It is still OK for **name searches** (`normalize_location("La Plata")` returns few matches, <100). It is NOT suitable for queries like "list all departamentos of X provincia" — it silently truncates. Acceptable if the only use case is name resolution.
- **[RESOLVED CL-003]** — The task is named `ingest_georef` (not `snapshot_georef`) and fetches **all 4 entities in full**: provincias, departamentos, municipios, localidades. Each in a separate table: `cache_georef_provincias`, `cache_georef_departamentos`, `cache_georef_municipios`, `cache_georef_localidades`. Plus registration in `datasets` + `cached_datasets`. See `georef_tasks.py:26-31` (ENDPOINTS dict) and `:113-115` (table naming).

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Hardcoded base URL** in `georef_adapter.py:19`. No configuration for alternative endpoints.
- **[DEBT-002]** — **Retry applied only to `_get_entities_internal`**, while the wrapping layer `_get_entities` also catches and re-raises — double handling.
- **[DEBT-003]** — **Hardcoded max query params** (provincias=24, others=100). No support for dynamic limits.
- **[DEBT-004]** — **Priority in `normalize_location` is arbitrary** (provincia > departamento). No ranking by match quality.
- **[DEBT-005]** — **`asyncio.gather(return_exceptions=True)` hides individual failures** — no per-failure logging.

---

**End of spec.md**
