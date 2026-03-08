"""
Admin Tasks Router — Manual trigger and status for all background tasks.

Protected by ADMIN_API_KEY (env var). Falls back to BACKEND_API_KEY.
Header: X-Admin-Key
"""
from __future__ import annotations

import logging
import os
import secrets
from datetime import UTC, datetime

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/tasks", tags=["admin-tasks"])


def _get_admin_key() -> str:
    return os.getenv("ADMIN_API_KEY", os.getenv("BACKEND_API_KEY", ""))


def verify_admin_key(x_admin_key: str = Header(..., alias="X-Admin-Key")) -> str:
    """Validate admin API key from header."""
    expected = _get_admin_key()
    if not expected:
        raise HTTPException(status_code=503, detail="Admin key not configured")
    if not secrets.compare_digest(x_admin_key, expected):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    return x_admin_key


# ── Registry of all tasks ────────────────────────────────────────

TASK_REGISTRY: dict[str, dict] = {
    # --- CKAN scraping ---
    "scrape_catalog": {
        "celery_name": "openarg.scrape_catalog",
        "description": "Scrape CKAN catalog for a portal",
        "params": ["portal"],
        "queue": "scraper",
    },
    "scrape_all_portals": {
        "celery_name": "openarg.scrape_all_portals",
        "description": "Scrape all CKAN portals",
        "params": [],
        "queue": "scraper",
    },
    # --- Collection ---
    "bulk_collect_all": {
        "celery_name": "openarg.bulk_collect_all",
        "description": "Download and cache all uncached datasets",
        "params": ["portal"],
        "queue": "collector",
    },
    "collect_dataset": {
        "celery_name": "openarg.collect_data",
        "description": "Download and cache a single dataset by ID",
        "params": ["dataset_id"],
        "queue": "collector",
    },
    # --- Embeddings ---
    "index_dataset_embedding": {
        "celery_name": "openarg.index_dataset",
        "description": "Generate embeddings for a dataset",
        "params": ["dataset_id"],
        "queue": "embedding",
    },
    "index_sesiones_chunks": {
        "celery_name": "openarg.index_sesiones",
        "description": "Index congressional session chunks in pgvector",
        "params": [],
        "queue": "embedding",
    },
    # --- Transparency ---
    "score_portal_health": {
        "celery_name": "openarg.score_portal_health",
        "description": "Score dataset quality across portals",
        "params": ["portal"],
        "queue": "transparency",
    },
    "analyze_session_topics": {
        "celery_name": "openarg.analyze_session_topics",
        "description": "Analyze congressional session topics with LLM",
        "params": [],
        "queue": "transparency",
    },
    # --- Staff ---
    "snapshot_staff": {
        "celery_name": "openarg.snapshot_staff",
        "description": "Snapshot HCDN staff (nómina + diff)",
        "params": [],
        "queue": "scraper",
    },
    "scrape_senado_staff": {
        "celery_name": "openarg.scrape_senado_staff",
        "description": "Scrape Senado senator profiles for staff assignments (72 senators)",
        "params": [],
        "queue": "scraper",
    },
    # --- S3 ---
    "retry_s3_uploads": {
        "celery_name": "openarg.retry_s3_uploads",
        "description": "Retry failed S3 uploads",
        "params": [],
        "queue": "s3",
    },
    # --- Recovery ---
    "recover_stuck_tasks": {
        "celery_name": "openarg.recover_stuck_tasks",
        "description": "Recover tasks stuck in intermediate states",
        "params": [],
        "queue": "collector",
    },
    # --- New data sources ---
    "ingest_presupuesto": {
        "celery_name": "openarg.ingest_presupuesto",
        "description": "ETL Presupuesto Abierto (MECON ZIPs → PG)",
        "params": ["prefixes", "years"],
        "queue": "collector",
    },
    "snapshot_bcra": {
        "celery_name": "openarg.snapshot_bcra",
        "description": "Daily snapshot BCRA exchange rates + monetary variables",
        "params": [],
        "queue": "collector",
    },
    "ingest_bac": {
        "celery_name": "openarg.ingest_bac",
        "description": "ETL Buenos Aires Compras (OCDS CSVs → PG)",
        "params": [],
        "queue": "collector",
    },
    "ingest_indec": {
        "celery_name": "openarg.ingest_indec",
        "description": "ETL INDEC high-value datasets (XLS/CSV → PG)",
        "params": [],
        "queue": "collector",
    },
    "scrape_dkan_rosario": {
        "celery_name": "openarg.scrape_dkan_rosario",
        "description": "Scrape Rosario DKAN catalog → CSVs → PG",
        "params": [],
        "queue": "scraper",
    },
    "scrape_senado": {
        "celery_name": "openarg.scrape_senado",
        "description": "Scrape Senado datos abiertos → CSVs/XLS → PG",
        "params": [],
        "queue": "scraper",
    },
    "scrape_cordoba_legislatura": {
        "celery_name": "openarg.scrape_cordoba_legislatura",
        "description": "Scrape Córdoba Legislatura portal → CSVs/XLS → PG",
        "params": [],
        "queue": "scraper",
    },
    "scrape_dkan_jujuy": {
        "celery_name": "openarg.scrape_dkan_jujuy",
        "description": "Scrape Jujuy DKAN catalog → CSVs → PG",
        "params": [],
        "queue": "scraper",
    },
    "ingest_presupuesto_dimensiones": {
        "celery_name": "openarg.ingest_presupuesto_dimensiones",
        "description": "ETL Presupuesto dimension tables (clasificadores DGSIAF → PG)",
        "params": [],
        "queue": "ingest",
    },
    "ingest_georef": {
        "celery_name": "openarg.ingest_georef",
        "description": "Ingest GeoRef geographic reference data → PG",
        "params": [],
        "queue": "ingest",
    },
    "ingest_series_tiempo": {
        "celery_name": "openarg.ingest_series_tiempo",
        "description": "Ingest Series de Tiempo economic indicators → PG",
        "params": [],
        "queue": "ingest",
    },
    "reset_failed_collectors": {
        "celery_name": "openarg.reset_failed_collectors",
        "description": "Reset permanently failed collector tasks for retry",
        "params": [],
        "queue": "collector",
    },
    # --- Embeddings (full reindex) ---
    "reindex_all_embeddings": {
        "celery_name": "openarg.reindex_all_embeddings",
        "description": "Re-generate embeddings for all datasets",
        "params": [],
        "queue": "embedding",
    },
    # --- Pipeline orchestrator ---
    "run_pipeline": {
        "celery_name": "openarg.run_pipeline",
        "description": "Run full pipeline (all groups) or selected groups. Params: groups (list), skip_delays (bool)",
        "params": ["groups", "skip_delays"],
        "queue": "scraper",
    },
}


# ── List all tasks ───────────────────────────────────────────────

@router.get("/", dependencies=[Depends(verify_admin_key)])
async def list_tasks():
    """List all available background tasks with their descriptions."""
    return {
        task_id: {
            "description": info["description"],
            "params": info["params"],
            "queue": info["queue"],
        }
        for task_id, info in TASK_REGISTRY.items()
    }


# ── Trigger a task ──────────────────────────────────────────────

@router.post("/{task_id}/run", dependencies=[Depends(verify_admin_key)])
async def run_task(task_id: str, params: dict | None = None):
    """
    Trigger a background task manually.

    Body (optional JSON):
    - For scrape_catalog: {"portal": "datos_gob_ar"}
    - For collect_dataset / index_dataset_embedding: {"dataset_id": "uuid"}
    - For bulk_collect_all / score_portal_health: {"portal": "datos_gob_ar"}
    - For ingest_presupuesto: {"prefixes": [...], "years": [...]}
    """
    if task_id not in TASK_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown task: {task_id}")

    info = TASK_REGISTRY[task_id]
    celery_name = info["celery_name"]
    params = params or {}

    # Build args/kwargs based on task
    args: list = []
    kwargs: dict = {}

    for param_name in info["params"]:
        if param_name in params:
            val = params[param_name]
            if param_name in ("portal", "dataset_id"):
                args.append(val)
            else:
                kwargs[param_name] = val

    result = celery_app.send_task(celery_name, args=args, kwargs=kwargs)

    return {
        "status": "dispatched",
        "task_id": task_id,
        "celery_task_id": result.id,
        "celery_name": celery_name,
        "params": params,
        "dispatched_at": datetime.now(UTC).isoformat(),
    }


# ── Check task status ───────────────────────────────────────────

@router.get("/status/{celery_task_id}", dependencies=[Depends(verify_admin_key)])
async def get_task_status(celery_task_id: str):
    """
    Check the status of a dispatched task by its Celery task ID.

    Returns:
    - state: PENDING | STARTED | SUCCESS | FAILURE | RETRY | REVOKED
    - result: task return value (if SUCCESS)
    - error: error message (if FAILURE)
    - info: progress metadata (if available)
    """
    result = AsyncResult(celery_task_id, app=celery_app)

    response: dict = {
        "celery_task_id": celery_task_id,
        "state": result.state,
    }

    if result.state == "SUCCESS":
        response["result"] = result.result
        response["completed"] = True
    elif result.state == "FAILURE":
        response["error"] = str(result.result) if result.result else "Unknown error"
        response["completed"] = True
    elif result.state == "STARTED":
        response["completed"] = False
        response["info"] = result.info if result.info else None
    elif result.state == "RETRY":
        response["completed"] = False
        response["info"] = "Task is being retried"
    elif result.state == "PENDING":
        response["completed"] = False
        response["info"] = "Task is queued or unknown"
    elif result.state == "REVOKED":
        response["completed"] = True
        response["info"] = "Task was cancelled"
    else:
        response["completed"] = False

    return response


# ── Bulk status for all recent tasks ─────────────────────────────

@router.get("/active", dependencies=[Depends(verify_admin_key)])
async def get_active_tasks():
    """
    List all currently active, reserved, and scheduled tasks across workers.
    """
    inspect = celery_app.control.inspect(timeout=5.0)

    active = inspect.active() or {}
    reserved = inspect.reserved() or {}
    scheduled = inspect.scheduled() or {}

    def _summarize(tasks_by_worker: dict) -> list[dict]:
        items = []
        for worker, tasks in tasks_by_worker.items():
            for t in tasks:
                items.append({
                    "worker": worker,
                    "celery_task_id": t.get("id", ""),
                    "name": t.get("name", ""),
                    "args": t.get("args", ""),
                    "started": t.get("time_start"),
                })
        return items

    return {
        "active": _summarize(active),
        "reserved": _summarize(reserved),
        "scheduled_count": sum(len(v) for v in scheduled.values()),
    }


# ── Data source status (DB-level) ────────────────────────────────

@router.get("/sources/status", dependencies=[Depends(verify_admin_key)])
async def get_sources_status():
    """
    Overview of all data sources: how many datasets cached, last update, row counts.
    """
    from app.infrastructure.celery.tasks._db import get_sync_engine

    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            # Datasets per portal
            portal_stats = conn.execute(text("""
                SELECT portal,
                       COUNT(*) AS total_datasets,
                       SUM(CASE WHEN is_cached THEN 1 ELSE 0 END) AS cached,
                       MAX(last_updated_at) AS last_updated
                FROM datasets
                GROUP BY portal
                ORDER BY portal
            """)).fetchall()

            # Cached tables status
            cache_stats = conn.execute(text("""
                SELECT status, COUNT(*) AS count
                FROM cached_datasets
                GROUP BY status
            """)).fetchall()

            # Total rows in cache
            total_cached_rows = conn.execute(text(
                "SELECT COALESCE(SUM(row_count), 0) FROM cached_datasets WHERE status = 'ready'"
            )).scalar()

        return {
            "portals": [
                {
                    "portal": row.portal,
                    "total_datasets": row.total_datasets,
                    "cached": row.cached,
                    "last_updated": row.last_updated.isoformat() if row.last_updated else None,
                }
                for row in portal_stats
            ],
            "cache_status": {row.status: row.count for row in cache_stats},
            "total_cached_rows": total_cached_rows,
        }
    finally:
        engine.dispose()


# ── Cancel a task ────────────────────────────────────────────────

@router.post("/cancel/{celery_task_id}", dependencies=[Depends(verify_admin_key)])
async def cancel_task(celery_task_id: str):
    """Revoke/cancel a pending or running task."""
    celery_app.control.revoke(celery_task_id, terminate=True, signal="SIGTERM")
    return {
        "status": "revoked",
        "celery_task_id": celery_task_id,
    }
