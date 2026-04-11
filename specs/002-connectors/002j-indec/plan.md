# Plan: Ingest INDEC (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Infrastructure Task | `ingest_indec` | `celery/tasks/indec_tasks.py` |

## 2. External Dependencies

- Fixed INDEC URLs (CSV/XLS) — hardcoded in the code.
- No auth.
- No explicit retry at the task level.

## 3. Persistence

`cache_indec_*` tables with a dynamic schema via pandas. Registered in `datasets`/`cached_datasets`.

## 4. Scheduled Work

| Task | Schedule | Queue |
|---|---|---|
| `ingest_indec` | Day 15 of the month 01:00 ART | `ingest` |

Beat entry: `celery/app.py:247-251`.

## 5. Source Files

- `infrastructure/celery/tasks/indec_tasks.py`

## 6. Deviations from Constitution

- Task-based without a port.
- URLs hardcoded in code.

---

**End of plan.md**
