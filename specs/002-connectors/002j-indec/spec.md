# Spec: Ingest INDEC

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Infrastructure (task only)
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Monthly ETL that downloads high-value datasets from **INDEC** (Instituto Nacional de Estadísticas y Censos) directly from fixed FTP/HTTP URLs, without going through CKAN. Covers IPC, EMAE, PIB, foreign trade, employment, and other official indicators. The datasets are cached in PG for querying through the sandbox.

## 2. User Stories

- **US-001 (P1)**: As an analyst, I want to query the historical IPC series directly from local tables.
- **US-002 (P2)**: As the system, I need to refresh the INDEC datasets monthly (after publications typically between days 10-15).

## 3. Functional Requirements

- **FR-001**: MUST download CSV/XLS from fixed, hardcoded INDEC URLs.
- **FR-002**: MUST parse with pandas and persist in `cache_indec_*` tables.
- **FR-003**: MUST register each dataset in `datasets` + `cached_datasets`.
- **FR-004**: MUST trigger embedding indexing for the new ones.

## 4. Success Criteria

- **SC-001**: Full monthly ingestion in **<10 minutes**.
- **SC-002**: Data available within ≤24h post-schedule.

## 5. Open Questions

- **[RESOLVED CL-001]** — **17 hardcoded INDEC datasets** in `indec_tasks.py:26-137` (list `INDEC_DATASETS`). Categories: **Prices** (IPC), **Economic Activity** (EMAE, PIB, IPI, ISAC), **Foreign Trade**, **Supermarkets**, **Wages** (CVS, Index, Variation), **Basic Basket**, **Poverty**, **Labor Market** (EPH), **Income**, **Inbound/Outbound Tourism**, **Balance of Payments**. URLs point to `https://www.indec.gob.ar/ftp/cuadros/...`.
- **[NEEDS CLARIFICATION CL-002]** — What happens if INDEC changes the structure of a CSV? Is there breaking-change detection?
- **[NEEDS CLARIFICATION CL-003]** — Does the schedule (day 15) match INDEC's publication calendar?

## 6. Tech Debt Discovered

- **[DEBT-001]** — **Fixed URLs hardcoded** in the task code.
- **[DEBT-002]** — **No domain port or adapter** (task-only pattern).
- **[DEBT-003]** — **No detection of breaking upstream schema changes**.
- **[DEBT-004]** — **No specific metrics**.

---

**End of spec.md**
