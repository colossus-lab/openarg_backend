# Spec: Connector ArgentinaDatos

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Application + Infrastructure
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Connector to the public **argentinadatos.com** API for real-time financial data: dollar quotes (oficial, blue, bolsa, contado con liqui, cripto, mayorista, solidario, tarjeta), country risk (EMBI+) and monthly inflation. It is a complement to BCRA (which only has the official dollar) and provides the parallel market versions + macro indicators that BCRA does not expose directly.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Casa** | Exchange rate type (oficial, blue, bolsa, ccl, cripto, mayorista, solidario, tarjeta). |
| **Riesgo país** | Argentina EMBI+ index (JP Morgan), spread over US treasury bonds. |
| **Ultimo** | Flag indicating whether only the last value or the full series is wanted. |

## 3. User Stories

### US-001 (P1) — Blue dollar quote
**As** a user, **I want** to ask "how much is the blue?" and get the current value.

### US-002 (P1) — Current country risk
**As** an analyst, **I want** to ask about the country risk today.

### US-003 (P2) — Historical inflation series
**As** a user, **I want** to see the monthly evolution of inflation.

### US-004 (P2) — Compare casa vs official
**As** a user, **I want** to see the gap between blue and official (via query combination).

## 4. Functional Requirements

- **FR-001**: MUST expose an `IArgentinaDatosConnector` port with `fetch_dolar(casa)`, `fetch_riesgo_pais(ultimo)`, `fetch_inflacion()`.
- **FR-002**: MUST validate `casa` against a hardcoded allowlist.
- **FR-003**: MUST handle responses as a single dict or as a list (the upstream endpoint returns different formats).
- **FR-004**: MUST normalize records with fields (`fecha`, `casa`, `compra`, `venta`) for dollars; (`fecha`, `riesgo_pais`) for risk; (`fecha`, `inflacion`) for inflation.
- **FR-005**: MUST raise `ConnectorError(CN_ARGENTINA_DATOS_UNAVAILABLE)` on failures.

## 5. Success Criteria

- **SC-001**: Response in **<2 seconds (p95)** for individual queries.
- **SC-002**: Zero calls with invalid `casa` — 100% validation prior to HTTP.
- **SC-003**: Connector correctly handles both upstream response formats (dict vs list).

## 6. Assumptions & Out of Scope

### Assumptions
- The argentinadatos.com API is stable and free.
- The 8 casas in the allowlist are sufficient for common cases.

### Out of scope
- Intraday data (daily close only).
- Cross quotes (EUR/ARS, BRL/ARS).
- Full country risk history (latest values only).

## 7. Open Questions

- **[RESOLVED CL-001]** — **There is no scheduled snapshot**. Argentina Datos is 100% on-demand via the query pipeline. Confirmed: grep of `argentina_datos` in `infrastructure/celery/` returns zero tasks. The adapter is only invoked from `execute_argentina_datos_step`.
- **[PARTIAL CL-002]** — **Hardcoded** truncation in `argentina_datos_adapter.py:54` (`recent = data[-60:]`) with no explanatory comment. Same value in `fetch_riesgo_pais()` line 104. It is not a constant, not configurable. **Why 60**: not documented — original author's decision with no written rationale. We leave it as debt already captured in `[DEBT-002]`.
- **[RESOLVED CL-003]** — **No retry applied at any layer**. `argentina_datos_adapter.py` does not have `@with_retry` on its methods, and there is no wrapping layer. Inconsistency vs other connectors (series_tiempo, georef do have retry). Capture already existing in `[DEBT-005]`.

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Hardcoded allowlist** in `argentina_datos_adapter.py:18-29` (frozenset). Adding a new casa requires a code change + deploy.
- **[DEBT-002]** — **Data truncated to the last 60 records** with no pagination or full history. Arbitrary.
- **[DEBT-003]** — **No validation schema** on upstream responses — raw dicts with no type hints.
- **[DEBT-004]** — **No cache** between requests; redundant for low-frequency data (inflation is monthly).
- **[DEBT-005]** — **No `@with_retry`** visible at the adapter level (inconsistent pattern vs other connectors).
- **[DEBT-006]** — **No scheduled snapshot** visible for this connector — data is always live, increases latency.

---

**End of spec.md**
