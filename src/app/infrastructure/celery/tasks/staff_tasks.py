"""Weekly HCDN staff snapshot: download payroll CSV from CKAN, diff, persist."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime

import httpx
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.application.collection.field_mapping import (
    FieldSpec,
    Mapping,
    learned_aliases,
    propose_mapping_with_llm,
    remember_mapping,
    resolve_mapping,
    with_learned,
)
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.infrastructure.celery.tasks._llm_gate import model_if_it_answers

logger = logging.getLogger(__name__)


def _model_id(llm: object) -> str | None:
    """Which model produced a mapping, when the adapter says so."""
    for attr in ("model_id", "model", "model_name"):
        value = getattr(llm, attr, None)
        if isinstance(value, str) and value:
            return value
    return None


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


# What we need from the payroll, and the source spellings we already know.
# `legajo` is the identity: it is the upsert key, and unmapped it collapses a
# whole payroll into one row — which is exactly what happened on 2026-08-10.
_STAFF_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("legajo", identity=True),
    FieldSpec("apellido"),
    FieldSpec("nombre"),
    FieldSpec("escalafon", aliases=("escalafón",)),
    FieldSpec(
        "area_desempeno",
        # The 2026-08-10 rename kept alongside the old names rather than
        # replacing them: this field has changed twice and may change back.
        aliases=("Área de Desempeño", "Area de Desempeño", "estructura_desempeno", "estructura"),
    ),
    FieldSpec("convenio"),
)

# Read by the model tier when a name is new to us. Values, not names, are what
# it decides on — but knowing what we are looking for is what makes the values
# legible.
_STAFF_DESCRIPTIONS = {
    "legajo": "número de legajo del agente, entero",
    "apellido": "apellido del agente",
    "nombre": "nombre de pila del agente",
    "escalafon": "categoría escalafonaria, ej. A-3-T",
    "area_desempeno": "área, dirección o dependencia donde se desempeña",
    "convenio": "régimen laboral, ej. PLANTA PERMANENTE (LEY 24.600)",
}


_CONNECTOR = "staff_hcdn"


def _resolve_staff_mapping(records: list[dict], *, llm=None, engine=None) -> Mapping:
    """Work out which source key feeds each of our fields.

    Deterministic first — on the 2026-08-10 change that alone recovers five of
    the six fields, because five were only a case change. The model is asked
    only about what is left over, and only when one is available; see
    `field_mapping` for why the two tiers are not interchangeable.
    """
    if not records:
        return Mapping()
    specs = _STAFF_FIELDS
    if engine is not None:
        # Whatever the model worked out on a previous run resolves for free now.
        specs = with_learned(specs, learned_aliases(engine, _CONNECTOR))
    mapping = resolve_mapping(specs, list(records[0].keys()))
    if mapping.unmapped and llm is not None:
        import asyncio

        mapping = asyncio.run(
            propose_mapping_with_llm(
                specs,
                mapping,
                records,
                llm=llm,
                descriptions=_STAFF_DESCRIPTIONS,
            )
        )
    return mapping


def _normalize_record(raw: dict) -> dict:
    """Project one record, resolving its mapping deterministically.

    Kept for the single-record case. The batch path in `snapshot_staff` resolves
    once and can escalate to the model; this cannot, and says so.
    """
    return _resolve_staff_mapping([raw]).apply(raw)


# Half. Below that it is a bad batch; above it, the mapping stopped matching the
# source and writing the result would destroy the last good snapshot's meaning.
_DEGENERATE_SHARE = 0.5


def _degenerate_reason(records: list[dict]) -> str | None:
    """Why this batch must not be written, or `None` if it is fine.

    The last line of defence, after both mapping tiers. The failure it exists
    for is not a download that breaks — that one raises and retries and somebody
    sees it. It is a download that **succeeds and means nothing**.

    `legajo` is checked on its own because it is the identity: with it empty the
    upsert key collapses and a whole payroll becomes one row.
    """
    if not records:
        return None
    total = len(records)
    sin_legajo = sum(1 for r in records if not r.get("legajo"))
    if sin_legajo > total * _DEGENERATE_SHARE:
        return (
            f"{sin_legajo} de {total} registros vienen sin legajo — el upsert los "
            f"colapsaría en una fila. Campos recibidos: {sorted(records[0].keys())}"
        )
    vacios = sum(1 for r in records if not any(str(v).strip() for v in r.values()))
    if vacios > total * _DEGENERATE_SHARE:
        return f"{vacios} de {total} registros quedaron completamente vacíos al mapear"
    return None


# FIX-008 / FR-006 / FR-009: tracked fields for update detection. Only
# these three fields cause a "tipo='update'" event; other differences
# (apellido casing, nombre whitespace) are ignored so the changes feed
# stays signal-heavy.
_TRACKED_STAFF_FIELDS: tuple[str, ...] = ("area_desempeno", "escalafon", "convenio")


def _compute_staff_changes(
    prev_by_legajo: dict[str, dict],
    current: list[dict],
    now: datetime,
    *,
    is_first_run: bool,
) -> tuple[list[dict], int, int, int]:
    """Compute altas / bajas / updates between two consecutive staff snapshots.

    Pure function — no DB access, no clock, no Celery — so unit tests
    can exercise it directly with dict fixtures. The Celery task
    ``snapshot_staff`` is the only production caller and is responsible
    for fetching the two snapshots and persisting the returned events.

    Parameters:
        prev_by_legajo: map of ``legajo → {apellido, nombre,
            area_desempeno, escalafon, convenio}`` from the previous
            snapshot row.
        current: list of normalized records from the current snapshot
            (shape produced by ``_normalize_record``).
        now: UTC timestamp to stamp onto every event's ``detected_at``.
        is_first_run: if True, return an empty event list regardless
            (FR-013 — no events on a cold start so the first run does
            not emit 3600+ false altas).

    Returns:
        (changes, alta_count, baja_count, update_count) where
        ``changes`` is the full list of event dicts ready for the
        INSERT, and the three counts are the per-category totals for
        logging and metrics.
    """
    if is_first_run:
        return [], 0, 0, 0

    prev_legajos = set(prev_by_legajo.keys())
    current_legajos = {r["legajo"] for r in current if r["legajo"]}

    altas = [r for r in current if r["legajo"] and r["legajo"] not in prev_legajos]
    bajas_legajos = prev_legajos - current_legajos

    changes: list[dict] = []
    for r in altas:
        changes.append({**r, "tipo": "alta", "detected_at": now, "changes_json": None})

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
                "changes_json": None,
            }
        )

    # FR-009/FR-010: for each legajo present in both snapshots, compare
    # the tracked fields. Record one "update" event per legajo with a
    # changes_json naming only the fields that actually differed. An
    # empty diff never becomes an event.
    update_count = 0
    for curr_rec in current:
        legajo = curr_rec["legajo"]
        if not legajo or legajo not in prev_by_legajo:
            continue
        prev_info = prev_by_legajo[legajo]
        diff: dict[str, dict] = {}
        for field in _TRACKED_STAFF_FIELDS:
            old_val = prev_info.get(field)
            new_val = curr_rec.get(field)
            if old_val != new_val:
                diff[field] = {"from": old_val, "to": new_val}
        if not diff:
            continue
        changes.append(
            {
                "legajo": legajo,
                "apellido": curr_rec.get("apellido", ""),
                "nombre": curr_rec.get("nombre", ""),
                "area_desempeno": curr_rec.get("area_desempeno", ""),
                "tipo": "update",
                "detected_at": now,
                "changes_json": diff,
            }
        )
        update_count += 1

    return changes, len(altas), len(bajas_legajos), update_count


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

    # Resolve the mapping once for the batch, escalating to the model only for
    # names the strings could not place. A source that renamed a field is now a
    # thing this connector can absorb; a source that stopped sending one is not,
    # and the guard below is what stops us writing the difference.
    llm, canary_detail = model_if_it_answers()
    mapping = _resolve_staff_mapping(raw_records, llm=llm, engine=engine)
    logger.info("HCDN staff mapping (%s): %s", canary_detail, mapping.describe())
    current = [mapping.apply(r) for r in raw_records]
    logger.info("Downloaded %d staff records from HCDN", len(current))

    if not mapping.usable:
        # The identity field is gone. Not a partial success: writing this batch
        # collapses the payroll, so nothing is written and a person is told.
        degenerate: str | None = (
            f"no se pudo mapear {', '.join(mapping.unmapped_identity)}. "
            f"Campos recibidos: {sorted(raw_records[0].keys())}"
        )
    else:
        degenerate = _degenerate_reason(current)

    # Refuse to write a snapshot that parsed to nothing. Writing it is worse
    # than writing nothing: the mart reads the newest snapshot, so a degenerate
    # batch silently replaces a good payroll with a blank row.
    if degenerate:
        logger.error("HCDN staff snapshot looks degenerate, not writing — %s", degenerate)
        try:
            from app.application.quality.alerting import Alert, notify

            notify(
                engine,
                [
                    Alert(
                        kind="staff_source_changed",
                        # Keyed on the source, not the run: the portal stays
                        # changed until somebody fixes the mapping, and a weekly
                        # repeat of the same message trains people to ignore it.
                        key="staff_hcdn::snapshots",
                        title="El padrón de Diputados dejó de mapear",
                        detail=degenerate[:300],
                    )
                ],
                heading="OpenArg · la fuente cambió de forma",
            )
        except Exception:
            logger.warning("staff snapshot: alerting skipped", exc_info=True)
        return {"status": "degenerate", "records": len(current), "reason": degenerate}

    # The batch is good, so whatever the model contributed to reading it has now
    # earned its place. Recorded here and not at resolution time: a mapping that
    # produced nothing must not be remembered as one that worked.
    remember_mapping(engine, _CONNECTOR, mapping, model_id=_model_id(llm))

    # 2. Get legajos from previous snapshot
    try:
        with engine.connect() as conn:
            prev_date_row = conn.execute(
                text("SELECT MAX(snapshot_date) FROM staff_snapshots WHERE snapshot_date < :today"),
                {"today": today},
            ).scalar()

            prev_by_legajo: dict[str, dict] = {}
            if prev_date_row:
                # FIX-008: include escalafon and convenio in the SELECT
                # so _compute_staff_changes can detect update events on
                # the three tracked fields (FR-006, FR-009).
                rows = conn.execute(
                    text(
                        "SELECT legajo, apellido, nombre, area_desempeno, "
                        "escalafon, convenio "
                        "FROM staff_snapshots WHERE snapshot_date = :d"
                    ),
                    {"d": prev_date_row},
                ).fetchall()
                for r in rows:
                    prev_by_legajo[r.legajo] = {
                        "apellido": r.apellido,
                        "nombre": r.nombre,
                        "area_desempeno": r.area_desempeno,
                        "escalafon": r.escalafon,
                        "convenio": r.convenio,
                    }
    except SoftTimeLimitExceeded:
        raise
    except Exception as exc:
        logger.error("Failed to read previous snapshot: %s", exc)
        raise self.retry(exc=exc)

    # 3. Diff: altas (new), bajas (gone), and updates (FR-006/FR-009 — FIX-008).
    #    Pure function — no DB access, no clock — so unit tests can hit it
    #    directly with dict fixtures.
    is_first_run = not prev_date_row
    now = datetime.now(UTC)
    changes, alta_count, baja_count, update_count = _compute_staff_changes(
        prev_by_legajo, current, now, is_first_run=is_first_run
    )
    if is_first_run:
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
                # FR-010: serialize changes_json to a JSON string for
                # psycopg's JSONB binding. None values stay None so
                # altas/bajas leave the column NULL.
                conn.execute(
                    text(
                        "INSERT INTO staff_changes "
                        "(legajo, apellido, nombre, area_desempeno, tipo, "
                        "detected_at, changes_json) "
                        "VALUES (:legajo, :apellido, :nombre, :area_desempeno, "
                        ":tipo, :detected_at, CAST(:changes_json AS JSONB))"
                    ),
                    [
                        {
                            "legajo": c["legajo"],
                            "apellido": c["apellido"],
                            "nombre": c["nombre"],
                            "area_desempeno": c["area_desempeno"],
                            "tipo": c["tipo"],
                            "detected_at": c["detected_at"],
                            "changes_json": (
                                json.dumps(c["changes_json"], ensure_ascii=False)
                                if c.get("changes_json") is not None
                                else None
                            ),
                        }
                        for c in changes
                    ],
                )
    except SoftTimeLimitExceeded:
        raise
    except Exception as exc:
        logger.error("Failed to persist staff snapshot: %s", exc)
        raise self.retry(exc=exc)

    # Register the snapshot table in `raw_table_versions` so marts that
    # query `staff_hcdn::snapshots` (e.g. `staff_estado`) can resolve it
    # via `live_table()`. Idempotent — runs every snapshot to keep
    # `row_count` fresh.
    from app.infrastructure.celery.tasks._db import register_via_b_table

    register_via_b_table(
        engine,
        resource_identity="staff_hcdn::snapshots",
        table_name="staff_snapshots",
        schema_name="raw",
        row_count=len(current),
    )
    register_via_b_table(
        engine,
        resource_identity="staff_hcdn::changes",
        table_name="staff_changes",
        schema_name="raw",
    )

    logger.info(
        "Staff snapshot persisted: %d employees, %d altas, %d bajas, %d updates (first_run=%s)",
        len(current),
        alta_count,
        baja_count,
        update_count,
        is_first_run,
    )
    return {
        "status": "ok",
        "snapshot_date": str(today),
        "total": len(current),
        "altas": alta_count,
        "bajas": baja_count,
        "updates": update_count,
        "first_run": is_first_run,
    }
