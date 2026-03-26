from __future__ import annotations

import logging
import os

from celery import Celery
from celery.schedules import crontab

from app.setup.logging_config import setup_logging, setup_sentry

# Initialise structured logging and Sentry for Celery workers
setup_logging(log_level=os.getenv("LOG_LEVEL", "INFO"))
setup_sentry()

logger = logging.getLogger(__name__)

ALL_PORTALS = [
    # Nacionales
    "datos_gob_ar",
    "diputados",
    "justicia",
    "energia",
    "produccion",
    "magyp",
    "salud",
    "transporte",
    "acumar",
    "mininterior",
    "cultura",
    "pami",
    "arsat",
    "desarrollo_social",
    "turismo",
    "ssn",
    # CABA
    "caba",
    "legislatura_caba",
    # Provincias
    "buenos_aires_prov",
    "cordoba_prov",
    "cordoba_estadistica",
    "mendoza",
    "entre_rios",
    "neuquen_legislatura",
    "tucuman",
    "chaco",
    "misiones",
    # Municipios
    "ciudad_mendoza",
    "corrientes",
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
            "app.infrastructure.celery.tasks.staff_tasks",
            "app.infrastructure.celery.tasks.presupuesto_tasks",
            "app.infrastructure.celery.tasks.bcra_tasks",
            "app.infrastructure.celery.tasks.bac_tasks",
            "app.infrastructure.celery.tasks.indec_tasks",
            "app.infrastructure.celery.tasks.dkan_tasks",
            "app.infrastructure.celery.tasks.senado_tasks",
            "app.infrastructure.celery.tasks.cordoba_leg_tasks",
            "app.infrastructure.celery.tasks.senado_staff_tasks",
            "app.infrastructure.celery.tasks.georef_tasks",
            "app.infrastructure.celery.tasks.series_tiempo_tasks",
            "app.infrastructure.celery.tasks.mapa_estado_tasks",
            "app.infrastructure.celery.tasks.gobernadores_tasks",
            "app.infrastructure.celery.tasks.orchestrator_tasks",
            "app.infrastructure.celery.tasks.reporting_tasks",
            "app.infrastructure.celery.tasks.catalog_enrichment_tasks",
        ],
    )

    app.conf.task_routes = {
        "openarg.scrape_all_portals": {"queue": "scraper"},
        "openarg.scrape_catalog": {"queue": "scraper"},
        "openarg.index_dataset": {"queue": "embedding"},
        "openarg.index_sesiones": {"queue": "embedding"},
        "openarg.collect_data": {"queue": "collector"},
        "openarg.bulk_collect_all": {"queue": "collector"},
        "openarg.analyze_query": {"queue": "analyst"},
        "openarg.score_portal_health": {"queue": "transparency"},
        "openarg.analyze_session_topics": {"queue": "transparency"},
        "openarg.retry_s3_uploads": {"queue": "s3"},
        "openarg.upload_to_s3": {"queue": "s3"},
        "openarg.recover_stuck_tasks": {"queue": "default"},
        "openarg.reset_failed_collectors": {"queue": "default"},
        "openarg.snapshot_staff": {"queue": "scraper"},
        "openarg.reindex_all_embeddings": {"queue": "embedding"},
        # New data source tasks (dedicated ingest queue)
        "openarg.ingest_presupuesto": {"queue": "ingest"},
        "openarg.ingest_presupuesto_dimensiones": {"queue": "ingest"},
        "openarg.snapshot_bcra": {"queue": "ingest"},
        "openarg.ingest_bac": {"queue": "ingest"},
        "openarg.ingest_indec": {"queue": "ingest"},
        "openarg.scrape_dkan_rosario": {"queue": "scraper"},
        "openarg.scrape_dkan_jujuy": {"queue": "scraper"},
        "openarg.scrape_senado": {"queue": "scraper"},
        "openarg.scrape_cordoba_legislatura": {"queue": "scraper"},
        "openarg.scrape_senado_staff": {"queue": "scraper"},
        "openarg.ingest_georef": {"queue": "ingest"},
        "openarg.ingest_series_tiempo": {"queue": "ingest"},
        "openarg.run_pipeline": {"queue": "scraper"},
        "openarg.scrape_mapa_estado": {"queue": "scraper"},
        "openarg.scrape_gobernadores": {"queue": "scraper"},
        "openarg.report_failed_tasks": {"queue": "collector"},
        "openarg.enrich_single_table": {"queue": "embedding"},
        "openarg.enrich_all_tables": {"queue": "embedding"},
    }

    app.conf.task_default_queue = "scraper"
    app.conf.worker_max_tasks_per_child = 500
    app.conf.task_serializer = "json"
    app.conf.result_serializer = "json"
    app.conf.accept_content = ["json"]
    app.conf.timezone = "America/Argentina/Buenos_Aires"

    # --- Resilience: ACK after completion, re-enqueue on worker lost ---
    app.conf.task_acks_late = True
    app.conf.task_reject_on_worker_lost = True
    app.conf.worker_prefetch_multiplier = 1  # Required for acks_late to be effective

    # --- Default time limits (overridden per-task via decorator) ---
    app.conf.task_soft_time_limit = 600  # 10 min soft (raises SoftTimeLimitExceeded)
    app.conf.task_time_limit = 720  # 12 min hard kill

    # --- Result backend: expire after 1 hour, compress payloads ---
    app.conf.result_expires = 3600
    app.conf.task_compression = "gzip"
    app.conf.result_compression = "gzip"

    # Celery Beat — periodic catalog scraping (daily, staggered every 10 min, no collisions)
    _beat_schedule = {
        # 03:00 – 03:50 (large national portals)
        "datos_gob_ar": (3, 0),
        "caba": (3, 10),
        "diputados": (3, 20),
        "justicia": (3, 30),
        "buenos_aires_prov": (3, 40),
        "cordoba_prov": (3, 50),
        # 04:00 – 04:50 (national sectoral + provinces)
        "energia": (4, 0),
        "produccion": (4, 5),
        "magyp": (4, 10),
        "salud": (4, 15),
        "transporte": (4, 20),
        "acumar": (4, 25),
        "mininterior": (4, 30),
        "cultura": (4, 35),
        "pami": (4, 40),
        "desarrollo_social": (4, 45),
        "mendoza": (4, 50),
        "entre_rios": (4, 55),
        # 05:00 – 05:35 (provinces + municipalities)
        "neuquen_legislatura": (5, 0),
        "tucuman": (5, 5),
        "chaco": (5, 10),
        "arsat": (5, 15),
        "misiones": (5, 20),
        "ciudad_mendoza": (5, 25),
        "corrientes": (5, 30),
        "turismo": (5, 35),
        "ssn": (5, 40),
        "legislatura_caba": (5, 45),
        "cordoba_estadistica": (5, 50),
    }
    app.conf.beat_schedule = {
        f"scrape-{portal.replace('_', '-')}": {
            "task": "openarg.scrape_catalog",
            "schedule": crontab(hour=hour, minute=minute),
            "args": [portal],
            "options": {"queue": "scraper"},
        }
        for portal, (hour, minute) in _beat_schedule.items()
    }

    # Bulk collect — download all uncached datasets after scraping (05:45 ART)
    app.conf.beat_schedule["bulk-collect-datasets"] = {
        "task": "openarg.bulk_collect_all",
        "schedule": crontab(hour=5, minute=45),
        "options": {"queue": "collector"},
    }

    # Transparency analysis — runs after scraping completes (~06:15 ART)
    app.conf.beat_schedule.update(
        {
            "transparency-health-scoring": {
                "task": "openarg.score_portal_health",
                "schedule": crontab(hour=6, minute=0),
                "options": {"queue": "transparency"},
            },
            "transparency-session-topics": {
                "task": "openarg.analyze_session_topics",
                "schedule": crontab(hour=6, minute=30),
                "options": {"queue": "transparency"},
            },
            "retry-s3-uploads": {
                "task": "openarg.retry_s3_uploads",
                "schedule": crontab(hour=6, minute=45),
                "options": {"queue": "s3"},
            },
            "recover-stuck-tasks": {
                "task": "openarg.recover_stuck_tasks",
                "schedule": crontab(minute="*/15"),
                "options": {"queue": "default"},
            },
            "snapshot-staff-weekly": {
                "task": "openarg.snapshot_staff",
                "schedule": crontab(hour=2, minute=30, day_of_week=1),  # Monday 2:30 AM ART
                "options": {"queue": "scraper"},
            },
            # --- New data sources ---
            "ingest-presupuesto": {
                "task": "openarg.ingest_presupuesto",
                "schedule": crontab(day_of_month=5, hour=0, minute=0),  # Monthly, day 5
                "options": {"queue": "ingest"},
            },
            "ingest-presupuesto-dimensiones": {
                "task": "openarg.ingest_presupuesto_dimensiones",
                "schedule": crontab(day_of_month=5, hour=0, minute=30),  # Monthly, day 5, 00:30
                "options": {"queue": "ingest"},
            },
            "snapshot-bcra": {
                "task": "openarg.snapshot_bcra",
                "schedule": crontab(hour=4, minute=0),  # Daily 4:00 AM ART
                "options": {"queue": "ingest"},
            },
            "ingest-bac": {
                "task": "openarg.ingest_bac",
                "schedule": crontab(day_of_week=0, hour=1, minute=0),  # Sunday 1:00 AM ART
                "options": {"queue": "ingest"},
            },
            "ingest-indec": {
                "task": "openarg.ingest_indec",
                "schedule": crontab(day_of_month=15, hour=1, minute=0),  # Monthly, day 15
                "options": {"queue": "ingest"},
            },
            "scrape-dkan-rosario": {
                "task": "openarg.scrape_dkan_rosario",
                "schedule": crontab(day_of_week=6, hour=0, minute=30),  # Saturday 0:30 AM ART
                "options": {"queue": "scraper"},
            },
            "scrape-dkan-jujuy": {
                "task": "openarg.scrape_dkan_jujuy",
                "schedule": crontab(day_of_week=6, hour=1, minute=0),  # Saturday 1:00 AM ART
                "options": {"queue": "scraper"},
            },
            "scrape-senado": {
                "task": "openarg.scrape_senado",
                "schedule": crontab(day_of_week=0, hour=2, minute=0),  # Sunday 2:00 AM ART
                "options": {"queue": "scraper"},
            },
            "scrape-cordoba-legislatura": {
                "task": "openarg.scrape_cordoba_legislatura",
                "schedule": crontab(day_of_month=1, hour=2, minute=30),  # Monthly, day 1
                "options": {"queue": "scraper"},
            },
            "scrape-senado-staff": {
                "task": "openarg.scrape_senado_staff",
                "schedule": crontab(day_of_week=1, hour=1, minute=30),  # Monday 1:30 AM ART
                "options": {"queue": "scraper"},
            },
            "reset-failed-collectors": {
                "task": "openarg.reset_failed_collectors",
                "schedule": crontab(day_of_week=0, hour=5, minute=0),  # Sunday 5:00 AM ART
                "options": {"queue": "collector"},
            },
            "ingest-georef": {
                "task": "openarg.ingest_georef",
                "schedule": crontab(day_of_month=1, hour=0, minute=30),  # Monthly, day 1
                "options": {"queue": "ingest"},
            },
            "ingest-series-tiempo": {
                "task": "openarg.ingest_series_tiempo",
                "schedule": crontab(day_of_month=1, hour=1, minute=30),  # Monthly, day 1
                "options": {"queue": "ingest"},
            },
            "scrape-mapa-estado": {
                "task": "openarg.scrape_mapa_estado",
                "schedule": crontab(day_of_week=1, hour=2, minute=0),  # Monday 2:00 AM ART
                "options": {"queue": "scraper"},
            },
            "scrape-gobernadores": {
                "task": "openarg.scrape_gobernadores",
                "schedule": crontab(day_of_month=1, hour=2, minute=15),  # Monthly, day 1
                "options": {"queue": "scraper"},
            },
            # --- Reporting / Dead Letter visibility ---
            "report-failed-tasks": {
                "task": "openarg.report_failed_tasks",
                "schedule": crontab(
                    hour=7, minute=0
                ),  # Daily 7:00 AM ART (after all scraping/collecting)
                "options": {"queue": "collector"},
            },
        }
    )

    return app


celery_app = create_celery()


@celery_app.on_after_finalize.connect
def _initial_scrape(sender, **kwargs):
    """Dispatch scrape only for portals with no datasets in the DB."""
    from sqlalchemy import text

    from app.infrastructure.celery.tasks._db import get_sync_engine
    from app.infrastructure.celery.tasks.scraper_tasks import scrape_catalog

    try:
        engine = get_sync_engine()
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

    # Transparency tasks — run on first startup if tables are empty
    _initial_transparency()

    # Bulk collect — download uncached datasets after scrapers finish
    _initial_bulk_collect()


def _initial_transparency(engine=None):
    """Dispatch transparency analysis tasks if their tables are empty."""
    from sqlalchemy import text

    from app.infrastructure.celery.tasks._db import get_sync_engine
    from app.infrastructure.celery.tasks.transparency_tasks import (
        analyze_session_topics,
        score_portal_health,
    )

    if engine is None:
        engine = get_sync_engine()

    try:
        with engine.connect() as conn:
            health_count = (
                conn.execute(text("SELECT COUNT(*) FROM dataset_health_scores")).scalar() or 0
            )
            topics_count = conn.execute(text("SELECT COUNT(*) FROM session_topics")).scalar() or 0
    except Exception:
        logger.warning("Could not check transparency tables — dispatching all")
        health_count = topics_count = 0

    # Portal health needs datasets — delay 120s to let scrapers finish
    if health_count == 0:
        logger.info("No health scores found — dispatching score_portal_health (120s delay)")
        score_portal_health.apply_async(countdown=120)
    else:
        logger.info("Health scores already exist (%d) — skipping", health_count)

    # Session topics — can run immediately (uses pre-loaded session chunks)
    if topics_count == 0:
        logger.info("No session topics found — dispatching analyze_session_topics")
        analyze_session_topics.delay()
    else:
        logger.info("Session topics already exist (%d) — skipping", topics_count)


def _initial_bulk_collect():
    """Dispatch bulk_collect_all if there are uncached datasets."""
    from sqlalchemy import text

    from app.infrastructure.celery.tasks._db import get_sync_engine
    from app.infrastructure.celery.tasks.collector_tasks import bulk_collect_all

    try:
        engine = get_sync_engine()
        with engine.connect() as conn:
            uncached = (
                conn.execute(text("SELECT COUNT(*) FROM datasets WHERE is_cached = false")).scalar()
                or 0
            )
        engine.dispose()
    except Exception:
        logger.warning("Could not check uncached datasets — dispatching bulk_collect_all")
        uncached = 1  # assume there's work to do

    if uncached > 0:
        # Delay 180s to let scrapers index datasets first
        logger.info(
            "Found %d uncached datasets — dispatching bulk_collect_all (180s delay)", uncached
        )
        bulk_collect_all.apply_async(countdown=180)
    else:
        logger.info("All datasets already cached — skipping bulk_collect_all")
