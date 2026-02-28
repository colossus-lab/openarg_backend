# Create new Celery worker: $ARGUMENTS

Create a new worker with its task and dedicated queue. Follow these steps:

## 1. Create the tasks file

Create `src/app/infrastructure/celery/tasks/{name}_tasks.py`:

```python
from src.app.infrastructure.celery.app import celery_app

@celery_app.task(
    name="openarg.{name}_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def {name}_task(self, param1: str, param2: str | None = None):
    """Description of what the worker does."""
    try:
        # 1. Setup (resolve dependencies, create session)
        # 2. Execute logic
        # 3. Return result or dispatch next task
        pass
    except Exception as exc:
        self.retry(exc=exc)
```

Reference: `src/app/infrastructure/celery/tasks/scraper_tasks.py` (scrape + dispatch), `src/app/infrastructure/celery/tasks/analyst_tasks.py` (multi-step pipeline).

## 2. Configure queue routing

Edit `src/app/infrastructure/celery/app.py`, add to the `task_routes` dict:

```python
task_routes = {
    ...
    "openarg.{name}_task": {"queue": "{name}"},
}
```

## 3. Add Make command

Edit the `Makefile`, add:

```makefile
workers.{name}:
	celery -A src.app.infrastructure.celery.app worker -Q {name} -c {concurrency} --loglevel=info -n {name}@%h
```

Reference: look for `workers.scraper`, `workers.collector` in the Makefile.

## 4. Add Docker service

Edit `docker-compose.yaml`, add:

```yaml
  worker-{name}:
    build:
      context: .
      dockerfile: docker/worker.Dockerfile
    command: celery -A src.app.infrastructure.celery.app worker -Q {name} -c {concurrency} --loglevel=info
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
      - CELERY_RESULT_BACKEND=redis://redis:6379/1
      - DATABASE_URL=postgresql+psycopg://openarg:openarg@postgres:5432/openarg
    depends_on:
      - redis
      - postgres
```

Reference: look for `worker-scraper`, `worker-analyst` in `docker-compose.yaml`.

## 5. Dispatch the task

From an endpoint or from another worker:
```python
from src.app.infrastructure.celery.tasks.{name}_tasks import {name}_task

{name}_task.delay(param1, param2)
# or with explicit routing:
{name}_task.apply_async(args=[param1], queue="{name}")
```

## Checklist
- [ ] Task created in `celery/tasks/{name}_tasks.py`
- [ ] Task registered with `name="openarg.{name}_task"`
- [ ] Queue routing configured in `celery/app.py`
- [ ] `make workers.{name}` command added
- [ ] Docker service added in `docker-compose.yaml`
- [ ] Retry policy configured (max_retries, default_retry_delay)
- [ ] Tested: task executes from endpoint or dispatch
