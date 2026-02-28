from __future__ import annotations

import os

from celery import Celery
from celery.schedules import crontab


def create_celery() -> Celery:
    broker = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    backend = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

    app = Celery(
        "openarg",
        broker=broker,
        backend=backend,
        include=[
            "app.infrastructure.celery.tasks.scraper_tasks",
            "app.infrastructure.celery.tasks.embedding_tasks",
            "app.infrastructure.celery.tasks.collector_tasks",
            "app.infrastructure.celery.tasks.analyst_tasks",
        ],
    )

    app.conf.task_routes = {
        "openarg.scrape_catalog": {"queue": "scraper"},
        "openarg.index_dataset": {"queue": "embedding"},
        "openarg.index_sesiones": {"queue": "embedding"},
        "openarg.collect_data": {"queue": "collector"},
        "openarg.analyze_query": {"queue": "analyst"},
    }

    app.conf.task_default_queue = "default"
    app.conf.worker_prefetch_multiplier = 4
    app.conf.worker_max_tasks_per_child = 500
    app.conf.task_serializer = "json"
    app.conf.result_serializer = "json"
    app.conf.accept_content = ["json"]
    app.conf.timezone = "America/Argentina/Buenos_Aires"

    # Celery Beat — periodic catalog scraping (daily at 03:00 ART)
    app.conf.beat_schedule = {
        "scrape-datos-gob-ar": {
            "task": "openarg.scrape_catalog",
            "schedule": crontab(hour=3, minute=0),
            "args": ["datos_gob_ar"],
        },
        "scrape-caba": {
            "task": "openarg.scrape_catalog",
            "schedule": crontab(hour=3, minute=30),
            "args": ["caba"],
        },
    }

    return app


celery_app = create_celery()


@celery_app.on_after_finalize.connect
def _initial_scrape(sender, **kwargs):
    """Dispatch initial scrape on first startup."""
    from app.infrastructure.celery.tasks.scraper_tasks import scrape_catalog
    scrape_catalog.delay("datos_gob_ar")
    scrape_catalog.delay("caba")

    # Index congressional session chunks in pgvector (idempotent — skips if already indexed)
    from app.infrastructure.celery.tasks.embedding_tasks import index_sesiones_chunks
    index_sesiones_chunks.delay()
