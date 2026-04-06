"""
Pipeline Orchestrator — single entry point to dispatch all tasks.

Usage:
    # All groups (full pipeline):
    run_pipeline.delay()

    # Specific groups:
    run_pipeline.delay(groups=["scrapers", "ingest"])

    # Skip countdown delays (everything fires immediately):
    run_pipeline.delay(skip_delays=True)

Groups & phases (executed in order):
    1. scrapers  — CKAN portals + DKAN + Senado + Córdoba Legislatura
    1. staff     — Staff snapshots (Diputados + Senado)
    1. autoridades — PEN authorities (Mapa del Estado) + Governors (Wikidata)
    2. ingest    — Presupuesto, BCRA, BAC, INDEC, Georef, Series de Tiempo
    3. collect   — bulk_collect_all (download & cache uncached datasets)
    4. embeddings— Session chunks + dataset embeddings
    5. transparency — Portal health scoring + session topic analysis
    6. maintenance  — Recover stuck tasks, retry S3, reset failed collectors
"""

from __future__ import annotations

import logging

from app.infrastructure.celery.app import celery_app

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
#  Task group definitions — single source of truth
# ──────────────────────────────────────────────────────────────

TASK_GROUPS: dict[str, dict] = {
    "scrapers": {
        "phase": 1,
        "delay_seconds": 0,
        "description": "Scrape all CKAN/DKAN portals + Senado + Córdoba Legislatura",
        "tasks": [
            {"name": "openarg.scrape_all_portals"},
            {"name": "openarg.scrape_dkan_rosario"},
            {"name": "openarg.scrape_dkan_jujuy"},
            {"name": "openarg.scrape_senado"},
            {"name": "openarg.scrape_cordoba_legislatura"},
        ],
    },
    "staff": {
        "phase": 1,
        "delay_seconds": 0,
        "description": "Staff snapshots (Diputados + Senado)",
        "tasks": [
            {"name": "openarg.snapshot_staff"},
            {"name": "openarg.scrape_senado_staff"},
        ],
    },
    "autoridades": {
        "phase": 1,
        "delay_seconds": 0,
        "description": "Scrape authorities (PEN from Mapa del Estado + Governors from Wikidata)",
        "tasks": [
            {"name": "openarg.scrape_mapa_estado"},
            {"name": "openarg.scrape_gobernadores"},
        ],
    },
    "ingest": {
        "phase": 2,
        "delay_seconds": 60,
        "description": "Ingest external APIs (Presupuesto, BCRA, BAC, INDEC, Georef, Series de Tiempo)",
        "tasks": [
            {"name": "openarg.ingest_presupuesto"},
            {"name": "openarg.ingest_presupuesto_dimensiones"},
            {"name": "openarg.snapshot_bcra"},
            {"name": "openarg.ingest_bac"},
            {"name": "openarg.ingest_indec"},
            {"name": "openarg.ingest_georef"},
            {"name": "openarg.ingest_series_tiempo"},
        ],
    },
    "collect": {
        "phase": 3,
        "delay_seconds": 1800,  # 30 min — wait for scrapers
        "description": "Download & cache all uncached datasets into PostgreSQL",
        "tasks": [
            {"name": "openarg.bulk_collect_all"},
        ],
    },
    "embeddings": {
        "phase": 4,
        "delay_seconds": 3600,  # 1 hour — wait for collectors
        "description": "Index session chunks (dataset reindex is manual/on-demand)",
        "tasks": [
            {"name": "openarg.index_sesiones"},
        ],
    },
    "transparency": {
        "phase": 5,
        "delay_seconds": 5400,  # 1.5 hours
        "description": "Portal health scoring + session topic analysis",
        "tasks": [
            {"name": "openarg.score_portal_health"},
            {"name": "openarg.analyze_session_topics"},
        ],
    },
    "maintenance": {
        "phase": 6,
        "delay_seconds": 0,
        "description": "Recover stuck tasks, retry S3 uploads, reset failed collectors",
        "tasks": [
            {"name": "openarg.recover_stuck_tasks"},
            {"name": "openarg.retry_s3_uploads"},
            {"name": "openarg.reset_failed_collectors"},
        ],
    },
}

ALL_GROUPS = sorted(TASK_GROUPS.keys(), key=lambda g: TASK_GROUPS[g]["phase"])


@celery_app.task(
    name="openarg.run_pipeline",
    soft_time_limit=60,
    time_limit=120,
)
def run_pipeline(
    groups: list[str] | None = None,
    skip_delays: bool = False,
):
    """Dispatch the full data pipeline (or selected groups) in phase order.

    Args:
        groups: List of group keys to run. None = all groups.
        skip_delays: If True, dispatch everything immediately (no countdown).

    Returns:
        Dict with dispatched task names per group.
    """
    target_groups = groups or ALL_GROUPS
    invalid = [g for g in target_groups if g not in TASK_GROUPS]
    if invalid:
        return {"error": f"Unknown groups: {invalid}", "valid_groups": ALL_GROUPS}

    # Sort by phase
    sorted_groups = sorted(target_groups, key=lambda g: TASK_GROUPS[g]["phase"])

    dispatched: dict[str, list[str]] = {}
    cumulative_delay = 0

    for group_key in sorted_groups:
        group = TASK_GROUPS[group_key]
        phase_delay = 0 if skip_delays else group["delay_seconds"]

        # Only accumulate delay when phase changes
        if dispatched:
            prev_phase = TASK_GROUPS[sorted_groups[sorted_groups.index(group_key) - 1]]["phase"]
            if group["phase"] > prev_phase:
                cumulative_delay += phase_delay

        group_tasks = []
        for task_def in group["tasks"]:
            task_name = task_def["name"]
            args = task_def.get("args", [])
            kwargs = task_def.get("kwargs", {})
            countdown = cumulative_delay if not skip_delays else 0

            celery_app.send_task(
                task_name,
                args=args,
                kwargs=kwargs,
                countdown=countdown,
            )
            group_tasks.append(task_name)
            logger.info(
                "Dispatched %s (group=%s, phase=%d, countdown=%ds)",
                task_name,
                group_key,
                group["phase"],
                countdown,
            )

        dispatched[group_key] = group_tasks

    total = sum(len(v) for v in dispatched.values())
    logger.info("Pipeline dispatched: %d tasks across %d groups", total, len(dispatched))

    return {
        "dispatched": dispatched,
        "total_tasks": total,
        "skip_delays": skip_delays,
        "groups_run": list(dispatched.keys()),
    }
