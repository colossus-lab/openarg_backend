from __future__ import annotations

import os

from celery import Celery


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

    return app


celery_app = create_celery()
