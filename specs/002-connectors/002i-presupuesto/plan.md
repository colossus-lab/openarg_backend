# Plan: Ingest Presupuesto Abierto (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Infrastructure Task | `ingest_presupuesto`, `ingest_presupuesto_dimensiones` | `celery/tasks/presupuesto_tasks.py` |

There is no domain port or adapter (task-based pattern).

## 2. External Dependencies

| Attribute | Value |
|---|---|
| **Base URL** | `https://www.presupuestoabierto.gob.ar` |
| **Auth** | Token via env var `PRESUPUESTO_API_TOKEN` |
| **Data** | Crédito + Recurso + PEF + Transversal (classifiers) |

## 3. Persistence

Creates ~33 `cache_presupuesto_<slug>` tables with a dynamic schema (pandas `df.to_sql(..., if_exists="replace")`). Registers each one in `datasets` + `cached_datasets`.

## 4. Scheduled Work

| Task | Schedule | Queue |
|---|---|---|
| `ingest_presupuesto` | Day 5 of the month 00:30 ART | `ingest` |
| `ingest_presupuesto_dimensiones` | Day 5 of the month 00:30 ART | `ingest` |

## 5. Source Files

- `infrastructure/celery/tasks/presupuesto_tasks.py`
- Beat entry: `celery/app.py:232-236`

## 6. Deviations from Constitution

- Task-based pattern without a port (like BCRA, DDJJ).
- Raw SQL to `datasets`/`cached_datasets`.
- `df.to_sql` without an Alembic migration.

---

**End of plan.md**
