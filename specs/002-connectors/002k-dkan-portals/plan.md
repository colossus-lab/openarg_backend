# Plan: DKAN Portals (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Infrastructure Task | `scrape_dkan_rosario`, `scrape_dkan_jujuy` | `celery/tasks/dkan_tasks.py` |

## 2. Scheduled Work

| Task | Schedule | Queue |
|---|---|---|
| `scrape-dkan-rosario` | Saturday 00:30 ART | `scraper` |
| `scrape-dkan-jujuy` | Saturday 01:00 ART | `scraper` |

Beat entries: `celery/app.py:252-260`.

## 3. Source Files

- `infrastructure/celery/tasks/dkan_tasks.py`

## 4. Deviations from Constitution

- Task-based without a domain port.
- Two portals hardcoded instead of a dynamic registry.

---

**End of plan.md**
