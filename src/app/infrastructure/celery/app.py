from __future__ import annotations

import logging
import os

from celery import Celery
from celery.schedules import crontab

logger = logging.getLogger(__name__)

ALL_PORTALS = [
    "datos_gob_ar",
    "caba",
    "diputados",
    "justicia",
    "buenos_aires_prov",
    "cordoba_prov",
    "santa_fe",
    "mendoza",
    "entre_rios",
    "neuquen_legislatura",
]


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
            "app.infrastructure.celery.tasks.transparency_tasks",
            "app.infrastructure.celery.tasks.s3_tasks",
        ],
    )

    app.conf.task_routes = {
        "openarg.scrape_catalog": {"queue": "scraper"},
        "openarg.index_dataset": {"queue": "embedding"},
        "openarg.index_sesiones": {"queue": "embedding"},
        "openarg.collect_data": {"queue": "collector"},
        "openarg.bulk_collect_all": {"queue": "collector"},
        "openarg.analyze_query": {"queue": "analyst"},
        "openarg.score_portal_health": {"queue": "scraper"},
        "openarg.detect_ddjj_anomalies": {"queue": "scraper"},
        "openarg.analyze_session_topics": {"queue": "scraper"},
        "openarg.retry_s3_uploads": {"queue": "s3"},
        "openarg.upload_to_s3": {"queue": "s3"},
    }

    app.conf.task_default_queue = "default"
    app.conf.worker_prefetch_multiplier = 4
    app.conf.worker_max_tasks_per_child = 500
    app.conf.task_serializer = "json"
    app.conf.result_serializer = "json"
    app.conf.accept_content = ["json"]
    app.conf.timezone = "America/Argentina/Buenos_Aires"

    # Celery Beat — periodic catalog scraping (daily, staggered every 15 min)
    _beat_schedule = {
        # hour, minute for each portal
        "datos_gob_ar": (3, 0),
        "caba": (3, 15),
        "diputados": (3, 30),
        "justicia": (3, 45),
        "buenos_aires_prov": (4, 0),
        "cordoba_prov": (4, 15),
        "santa_fe": (4, 30),
        "mendoza": (4, 45),
        "entre_rios": (5, 0),
        "neuquen_legislatura": (5, 15),
    }
    app.conf.beat_schedule = {
        f"scrape-{portal.replace('_', '-')}": {
            "task": "openarg.scrape_catalog",
            "schedule": crontab(hour=hour, minute=minute),
            "args": [portal],
        }
        for portal, (hour, minute) in _beat_schedule.items()
    }

    # Bulk collect — download all uncached datasets after scraping (05:30 ART)
    app.conf.beat_schedule["bulk-collect-datasets"] = {
        "task": "openarg.bulk_collect_all",
        "schedule": crontab(hour=5, minute=30),
    }

    # Transparency analysis — runs after scraping completes (~06:00 ART)
    app.conf.beat_schedule.update({
        "transparency-health-scoring": {
            "task": "openarg.score_portal_health",
            "schedule": crontab(hour=6, minute=0),
        },
        "transparency-ddjj-anomalies": {
            "task": "openarg.detect_ddjj_anomalies",
            "schedule": crontab(hour=6, minute=15),
        },
        "transparency-session-topics": {
            "task": "openarg.analyze_session_topics",
            "schedule": crontab(hour=6, minute=30),
        },
        "retry-s3-uploads": {
            "task": "openarg.retry_s3_uploads",
            "schedule": crontab(hour=6, minute=45),
        },
    })

    return app


celery_app = create_celery()


@celery_app.on_after_finalize.connect
def _initial_scrape(sender, **kwargs):
    """Dispatch scrape only for portals with no datasets in the DB."""
    from sqlalchemy import text

    from app.infrastructure.celery.tasks.scraper_tasks import (
        _get_sync_engine,
        scrape_catalog,
    )

    try:
        engine = _get_sync_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT portal, COUNT(*) FROM datasets GROUP BY portal")
            ).fetchall()
        populated = {row[0] for row in rows if row[1] > 0}
    except Exception:
        logger.warning("Could not check dataset counts — scraping all portals")
        populated = set()

    empty_portals = [p for p in ALL_PORTALS if p not in populated]

    if empty_portals:
        logger.info("Initial scrape for empty portals: %s", empty_portals)
        for portal in empty_portals:
            scrape_catalog.delay(portal)
    else:
        logger.info("All portals already populated — skipping initial scrape")

    if populated:
        skipped = [p for p in ALL_PORTALS if p in populated]
        logger.info("Skipping portals with existing data: %s", skipped)

    # Index congressional session chunks in pgvector (idempotent — skips if already indexed)
    from app.infrastructure.celery.tasks.embedding_tasks import index_sesiones_chunks
    index_sesiones_chunks.delay()
