# Plan: Gobernadores (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Infrastructure Task | `snapshot_gobernadores` | `celery/tasks/gobernadores_tasks.py` |

## 2. External Dependencies

- Wikidata SPARQL endpoint
- Query hardcoded for the 23 provinces + CABA

## 3. Persistence

`cache_gobernadores` (dynamic schema via pandas).

## 4. Source Files

- `infrastructure/celery/tasks/gobernadores_tasks.py`

---

**End of plan.md**
