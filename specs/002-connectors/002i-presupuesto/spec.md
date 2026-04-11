# Spec: Ingest Presupuesto Abierto

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-11
**Hexagonal scope**: Infrastructure (task only)
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Monthly ETL that downloads data from **Presupuesto Abierto Nacional** (`presupuestoabierto.gob.ar`) plus dimensional tables from DGSIAF. Produces ~33 tables with ~640K rows (crédito, recurso, PEF, transversal) covering the years 2020-2026. It is the system's primary source of budget data and feeds public-spending queries via the SQL sandbox.

## 2. User Stories

- **US-001 (P1)**: As an analyst, I want to query executed spending by ministry in 2025 via the SQL sandbox against cached tables.
- **US-002 (P1)**: As the system, I need to refresh the budget tables monthly.
- **US-003 (P2)**: As an admin, I want to trigger ingestion on-demand from the admin panel.

## 3. Functional Requirements

- **FR-001**: MUST authenticate using `PRESUPUESTO_API_TOKEN` (env var).
- **FR-002**: MUST download crédito (executed), recurso (revenue), PEF (Presupuesto Ejecutado Físico — physical execution forecasts), and cross-cutting dimensions (classifiers).
- **FR-003**: MUST persist each dataset in `cache_presupuesto_*` tables.
- **FR-004**: MUST register each table in `datasets` + `cached_datasets`.
- **FR-005**: MUST trigger embedding indexing for the new datasets.

## 4. Success Criteria

- **SC-001**: Full monthly ingestion in **<20 minutes**.
- **SC-002**: ~33 tables with ~640K total rows.
- **SC-003**: Data available in the sandbox ≤24h after the schedule.

## 5. Open Questions

- **[RESOLVED CL-001]** — `PRESUPUESTO_API_TOKEN`: **does not expire and is not rotated** in the current flow. Stable token issued by the Presupuesto Abierto portal. Accepted debt. If the token is invalidated upstream, it is replaced manually via env var + restart.
- **[RESOLVED CL-002]** — **Separately, but on the same day.** Two distinct Celery tasks scheduled separately in `src/app/infrastructure/celery/app.py:229-238`: `openarg.ingest_presupuesto` (transactional credito/recurso/PEF/transversal) runs `crontab(day_of_month=5, hour=0, minute=0)`, and `openarg.ingest_presupuesto_dimensiones` (clasificadores presupuestarios from DGSIAF) runs 30 minutes later `crontab(day_of_month=5, hour=0, minute=30)`. Both target the `ingest` queue but are independent tasks with independent retry semantics — a failure in one does not block the other. (resolved 2026-04-11 via code inspection)
- **[NEEDS CLARIFICATION CL-003]** — Does the official endpoint impose rate limits? Not visible in the code.

## 6. Tech Debt Discovered

- **[DEBT-001]** — **No domain port** — it is a standalone task and does not follow the generic connector pattern (same as BCRA/DDJJ).
- **[DEBT-002]** — **Duplication of the `_register_dataset()` pattern** — same code as BCRA, with no shared helper.
- **[DEBT-003]** — **Token hardcoded as env var** — rotation requires a restart.
- **[DEBT-004]** — **No specific metrics** for budget ingestion.

---

**End of spec.md**
