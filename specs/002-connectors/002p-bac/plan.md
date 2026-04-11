# Plan: BAC Ingest (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Infrastructure Task | `ingest_bac` | `celery/tasks/bac_tasks.py` |

## 2. Scheduled Work

| Task | Schedule | Queue |
|---|---|---|
| `ingest-bac` | Sunday 01:00 ART | `ingest` |

Beat entry: `celery/app.py:242-246`.

## 3. Source Files

- `infrastructure/celery/tasks/bac_tasks.py`

---

**End of plan.md**
