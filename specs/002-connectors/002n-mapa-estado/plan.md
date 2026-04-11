# Plan: Mapa del Estado (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Infrastructure Task | `ingest_mapa_estado` | `celery/tasks/mapa_estado_tasks.py` |

## 2. External Dependencies

- Public CSV from mapadelestado.jefatura.gob.ar

## 3. Persistence

`cache_mapa_estado` with a dynamic pandas-derived schema.

## 4. Source Files

- `infrastructure/celery/tasks/mapa_estado_tasks.py`

---

**End of plan.md**
