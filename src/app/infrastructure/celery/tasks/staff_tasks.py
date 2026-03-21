"""Weekly HCDN staff snapshot: download payroll CSV from CKAN, diff, persist."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

import httpx
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# CKAN datastore_search endpoint for the HCDN staff resource
_CKAN_BASE = "https://datos.hcdn.gob.ar"
_RESOURCE_ID = "6e49506e-6757-44cd-94e9-0e75f3bd8c38"
_PAGE_SIZE = 5000
_MAX_PAGES = 20  # safety limit: 20 * 5000 = 100k records max


def _safe_str(value) -> str:
    """Convert a value to str, treating None as empty string."""
    if value is None:
        return ""
    return str(value).strip()


def _fetch_all_records() -> list[dict]:
    """Download the full HCDN staff list via CKAN datastore_search pagination."""
    records: list[dict] = []
    offset = 0
    with httpx.Client(timeout=60) as client:
        for _ in range(_MAX_PAGES):
            resp = client.get(
                f"{_CKAN_BASE}/api/3/action/datastore_search",
                params={"resource_id": _RESOURCE_ID, "limit": _PAGE_SIZE, "offset": offset},
            )
            resp.raise_for_status()
            data = resp.json()

            if not data.get("success", False):
                error_msg = data.get("error", {}).get("message", "unknown error")
                raise RuntimeError(f"CKAN API returned success=false: {error_msg}")

            batch = data.get("result", {}).get("records", [])
            if not batch:
                break
            records.extend(batch)
            offset += len(batch)
            if len(batch) < _PAGE_SIZE:
                break
    return records


def _normalize_record(raw: dict) -> dict:
    """Map CKAN field names to our schema."""
    return {
        "legajo": _safe_str(raw.get("Legajo") or raw.get("legajo")),
        "apellido": _safe_str(raw.get("Apellido") or raw.get("apellido")),
        "nombre": _safe_str(raw.get("Nombre") or raw.get("nombre")),
        "escalafon": _safe_str(
            raw.get("Escalafón") or raw.get("Escalafon") or raw.get("escalafon")
        ),
        "area_desempeno": _safe_str(
            raw.get("Área de Desempeño")
            or raw.get("Area de Desempeño")
            or raw.get("area_desempeno")
            or raw.get("estructura_desempeno")
        ),
        "convenio": _safe_str(raw.get("Convenio") or raw.get("convenio")),
    }


@celery_app.task(
    name="openarg.snapshot_staff",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    soft_time_limit=600,
    time_limit=720,
)
def snapshot_staff(self):
    """Download HCDN staff list, compute diff against previous snapshot, persist."""
    today = date.today()
    engine = get_sync_engine()

    # 1. Download current payroll from CKAN
    try:
        raw_records = _fetch_all_records()
    except Exception as exc:
        logger.error("Failed to download HCDN staff list: %s", exc)
        raise self.retry(exc=exc)

    if not raw_records:
        logger.warning("HCDN staff download returned 0 records — aborting")
        return {"status": "empty", "records": 0}

    current = [_normalize_record(r) for r in raw_records]
    current_legajos = {r["legajo"] for r in current if r["legajo"]}
    logger.info("Downloaded %d staff records from HCDN", len(current))

    # 2. Get legajos from previous snapshot
    try:
        with engine.connect() as conn:
            prev_date_row = conn.execute(
                text("SELECT MAX(snapshot_date) FROM staff_snapshots WHERE snapshot_date < :today"),
                {"today": today},
            ).scalar()

            prev_legajos: set[str] = set()
            prev_by_legajo: dict[str, dict] = {}
            if prev_date_row:
                rows = conn.execute(
                    text(
                        "SELECT legajo, apellido, nombre, area_desempeno "
                        "FROM staff_snapshots WHERE snapshot_date = :d"
                    ),
                    {"d": prev_date_row},
                ).fetchall()
                for r in rows:
                    prev_legajos.add(r.legajo)
                    prev_by_legajo[r.legajo] = {
                        "apellido": r.apellido,
                        "nombre": r.nombre,
                        "area_desempeno": r.area_desempeno,
                    }
    except SoftTimeLimitExceeded:
        raise
    except Exception as exc:
        logger.error("Failed to read previous snapshot: %s", exc)
        raise self.retry(exc=exc)

    # 3. Diff: altas (new) and bajas (gone)
    is_first_run = not prev_date_row
    now = datetime.now(UTC)
    altas = [r for r in current if r["legajo"] and r["legajo"] not in prev_legajos]
    bajas_legajos = prev_legajos - current_legajos

    # On first run, skip recording changes (all 3600+ would be "altas")
    changes: list[dict] = []
    if not is_first_run:
        for r in altas:
            changes.append({**r, "tipo": "alta", "detected_at": now})
        for leg in bajas_legajos:
            info = prev_by_legajo.get(leg, {})
            changes.append(
                {
                    "legajo": leg,
                    "apellido": info.get("apellido", ""),
                    "nombre": info.get("nombre", ""),
                    "area_desempeno": info.get("area_desempeno", ""),
                    "tipo": "baja",
                    "detected_at": now,
                }
            )
    else:
        logger.info("First staff snapshot — skipping change detection")

    # 4. Persist snapshot + changes in a single transaction
    try:
        with engine.begin() as conn:
            if current:
                conn.execute(
                    text(
                        "INSERT INTO staff_snapshots "
                        "(legajo, apellido, nombre, escalafon, area_desempeno, convenio, snapshot_date) "
                        "VALUES (:legajo, :apellido, :nombre, :escalafon, :area_desempeno, :convenio, :snap) "
                        "ON CONFLICT (legajo, snapshot_date) DO NOTHING"
                    ),
                    [{**r, "snap": today} for r in current],
                )

            if changes:
                conn.execute(
                    text(
                        "INSERT INTO staff_changes "
                        "(legajo, apellido, nombre, area_desempeno, tipo, detected_at) "
                        "VALUES (:legajo, :apellido, :nombre, :area_desempeno, :tipo, :detected_at)"
                    ),
                    [
                        {
                            "legajo": c["legajo"],
                            "apellido": c["apellido"],
                            "nombre": c["nombre"],
                            "area_desempeno": c["area_desempeno"],
                            "tipo": c["tipo"],
                            "detected_at": c["detected_at"],
                        }
                        for c in changes
                    ],
                )
    except SoftTimeLimitExceeded:
        raise
    except Exception as exc:
        logger.error("Failed to persist staff snapshot: %s", exc)
        raise self.retry(exc=exc)

    logger.info(
        "Staff snapshot persisted: %d employees, %d altas, %d bajas (first_run=%s)",
        len(current),
        len(altas),
        len(bajas_legajos),
        is_first_run,
    )
    return {
        "status": "ok",
        "snapshot_date": str(today),
        "total": len(current),
        "altas": len(altas),
        "bajas": len(bajas_legajos),
        "first_run": is_first_run,
    }
