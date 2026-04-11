# Spec: Ingest Gobernadores (Wikidata)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-11
**Hexagonal scope**: Infrastructure (task only)
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

ETL that queries **Wikidata SPARQL** to obtain the current list of governors of the 23 provinces plus the head of government of CABA. It is the only connector in the system that uses Wikidata as a source. Results are cached in PG so the pipeline can answer questions like "who is the governor of Mendoza?".

## 2. User Stories

- **US-001 (P1)**: As a user, I want to ask about the current governors of Argentina.
- **US-002 (P2)**: As the system, I need periodic refresh of the table.

## 3. Functional Requirements

- **FR-001**: MUST run a SPARQL query against Wikidata.
- **FR-002**: MUST persist the result in the `cache_gobernadores` table.
- **FR-003**: MUST register it in `datasets` + `cached_datasets`.

## 4. Open Questions

- **[RESOLVED CL-001]** — Task registered as `scrape_gobernadores` (not `snapshot_gobernadores`). Schedule: **`crontab(day_of_month=1, hour=2, minute=15)`** — first day of the month at 02:15 ART. Queue: `scraper`. See `celery/app.py:297-300`.
- **[NEEDS CLARIFICATION CL-002]** — Wikidata may contain incorrect or stale information. Is there any validation?
- **[RESOLVED CL-003]** — **No.** Glob for files matching `intendente*`, `alcalde*`, `mayor*` under `src/app/infrastructure/adapters/connectors` and `src/app/infrastructure/celery/tasks` returns zero files. Only `gobernadores_tasks.py` exists — mayors/intendentes are not ingested anywhere in the codebase. (resolved 2026-04-11 via code inspection)

## 5. Tech Debt Discovered

- **[DEBT-001]** — **SPARQL query hardcoded** in the task.
- **[DEBT-002]** — **Wikidata as the sole source** — there is no cross-validation with official sources.
- **[DEBT-003]** — **No domain port**.

---

**End of spec.md**
