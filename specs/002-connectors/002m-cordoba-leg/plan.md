# Plan: Córdoba Legislatura (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Infrastructure Task | crawler | `celery/tasks/cordoba_leg_tasks.py` |

## 2. Behavior

HTML crawler over WordPress pages → extract download links → download files → register in `datasets`/`cached_datasets`.

## 3. Source Files

- `infrastructure/celery/tasks/cordoba_leg_tasks.py`

---

**End of plan.md**
