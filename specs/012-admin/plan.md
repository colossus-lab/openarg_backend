# Plan: Admin Tasks (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Presentation | `tasks_router.py` | `presentation/http/controllers/admin/tasks_router.py` |
| Infrastructure | `orchestrator_tasks.py` | `infrastructure/celery/tasks/orchestrator_tasks.py` |

## 2. Task Registry (hardcoded)

```python
TASK_REGISTRY: dict[str, dict] = {
    "scrape_catalog": {
        "celery_name": "openarg.scrape_catalog",
        "description": "Scrape CKAN portal catalog",
        "params": ["portal"],
        "queue": "scraper",
    },
    "collect_dataset": {
        "celery_name": "openarg.collect_data",
        "description": "Download and cache dataset",
        "params": ["dataset_id"],
        "queue": "collector",
    },
    "index_dataset_embedding": {
        "celery_name": "openarg.index_dataset",
        "description": "Generate embeddings",
        "params": ["dataset_id"],
        "queue": "embedding",
    },
    "bulk_collect_all": {
        "celery_name": "openarg.bulk_collect_all",
        "description": "Cache all datasets for portal",
        "params": ["portal"],
        "queue": "collector",
    },
    "snapshot_staff": {
        "celery_name": "openarg.snapshot_staff",
        "description": "HCDN staff snapshot",
        "params": [],
        "queue": "scraper",
    },
    "snapshot_bcra": {
        "celery_name": "openarg.snapshot_bcra",
        "description": "BCRA cotizaciones snapshot",
        "params": [],
        "queue": "ingest",
    },
    "score_portal_health": {
        "celery_name": "openarg.score_portal_health",
        "description": "Portal health scoring",
        "params": ["portal"],
        "queue": "transparency",
    },
    # ... more tasks
}
```

## 3. Endpoints

| Method | Path | Auth | Behavior |
|---|---|---|---|
| POST | `/api/v1/admin/tasks/{task_name}` | X-Admin-Key | Dispatch Celery task with params |
| GET | `/api/v1/admin/tasks/{task_id}` | X-Admin-Key | Get AsyncResult status |
| GET | `/api/v1/admin/tasks` | X-Admin-Key | List registry entries |

## 4. Auth

```python
def verify_admin_key(x_admin_key: str = Header(...)) -> str:
    expected = os.getenv("ADMIN_API_KEY") or os.getenv("BACKEND_API_KEY")
    if not expected:
        raise HTTPException(503, "Admin API not configured")
    if not secrets.compare_digest(x_admin_key, expected):
        raise HTTPException(401)
    return x_admin_key
```

## 5. Source Files

- `presentation/http/controllers/admin/tasks_router.py`
- `infrastructure/celery/tasks/orchestrator_tasks.py`
- `infrastructure/celery/app.py` — celery_app reference

## 6. Deviations from Constitution

- **Principle XII (Security)**: no rate limiting, no audit log, no RBAC. Acceptable for alpha, open debt.

---

**End of plan.md**
