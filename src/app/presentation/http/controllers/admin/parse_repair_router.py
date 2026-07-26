"""Admin Parse-Repair Router — gradual in-place fix for tables with
placeholder columns.

Protected by ADMIN_API_KEY (Header: `X-Admin-Key`). Same auth pattern as
the rest of `/admin/*`.

Use case: see `specs/021-parser-hardening` Phase 2. The collector path
already produces clean tables for new ingests; this router lets ops apply
the same logic to ~500 already-landed tables in small batches.

Recommended workflow:
  1. `POST /admin/parse-repair/run?phase=col_n&limit=10&dry_run=true`
     → see proposals without writes
  2. Inspect the response; if proposals look reasonable:
  3. `POST /admin/parse-repair/run?phase=col_n&limit=10&dry_run=false`
  4. `GET /admin/parse-repair/audit?run_id=<uuid>`
     → confirm what was applied
  5. Iterate with bigger batches once confident.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text

from app.application.repair import (
    list_col_n_candidates,
    list_trailing_garbage_candidates,
    repair_col_n_table,
    repair_trailing_garbage_cols,
)
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.presentation.http.controllers.admin.tasks_router import verify_admin_key

router = APIRouter(prefix="/admin/parse-repair", tags=["admin-parse-repair"])


@router.post("/run", dependencies=[Depends(verify_admin_key)])
def run_repair(
    phase: str = Query(default="col_n", description="Repair phase to run"),
    limit: int = Query(default=10, ge=1, le=100, description="Max tables per batch"),
    dry_run: bool = Query(default=True, description="If true, propose but don't apply"),
    min_garbage_ratio: float = Query(default=0.40, ge=0.0, le=1.0),
) -> dict[str, Any]:
    """Run one repair batch and return per-table outcomes.

    Each batch picks `limit` worst-offender tables ordered by garbage_ratio
    desc, runs the repair function (sync), and returns a structured
    response. Dry-run is the default for safety; flip explicitly to apply.
    """
    if phase not in ("col_n", "trailing_garbage"):
        return {
            "ok": False,
            "error": f"unsupported phase: {phase!r}; supported: col_n, trailing_garbage",
        }

    engine = get_sync_engine()
    if phase == "col_n":
        candidates = list_col_n_candidates(engine, limit=limit, min_garbage_ratio=min_garbage_ratio)
    else:
        candidates = list_trailing_garbage_candidates(engine, limit=limit, min_garbage_count=5)
    run_id = uuid.uuid4()
    results: list[dict[str, Any]] = []
    applied = 0
    skipped = 0
    failed = 0

    for schema, table_name, _cols in candidates:
        if phase == "col_n":
            outcome = repair_col_n_table(
                engine,
                table_schema=schema,
                table_name=table_name,
                run_id=run_id,
                dry_run=dry_run,
            )
        else:
            outcome = repair_trailing_garbage_cols(
                engine,
                table_schema=schema,
                table_name=table_name,
                run_id=run_id,
                dry_run=dry_run,
            )
        results.append(outcome.as_log_dict())
        if outcome.ok:
            applied += 1
        elif outcome.error_message:
            failed += 1
        else:
            skipped += 1

    return {
        "ok": True,
        "run_id": str(run_id),
        "phase": phase,
        "dry_run": dry_run,
        "candidates_considered": len(candidates),
        "applied": applied,
        "skipped": skipped,
        "failed": failed,
        "outcomes": results,
    }


@router.get("/candidates", dependencies=[Depends(verify_admin_key)])
def list_candidates(
    phase: str = Query(default="col_n"),
    limit: int = Query(default=50, ge=1, le=500),
    min_garbage_ratio: float = Query(default=0.40, ge=0.0, le=1.0),
) -> dict[str, Any]:
    """List tables eligible for repair without running anything.

    Useful to size the work before committing to a batch run.
    """
    if phase not in ("col_n", "trailing_garbage"):
        return {"ok": False, "error": f"unsupported phase: {phase!r}"}
    engine = get_sync_engine()
    if phase == "col_n":
        candidates = list_col_n_candidates(engine, limit=limit, min_garbage_ratio=min_garbage_ratio)
    else:
        candidates = list_trailing_garbage_candidates(engine, limit=limit, min_garbage_count=5)
    return {
        "ok": True,
        "phase": phase,
        "count": len(candidates),
        "tables": [
            {
                "schema": s,
                "name": t,
                "garbage_columns": sum(
                    1
                    for c in cols
                    if str(c).startswith("col_") or str(c).lower().startswith("unnamed:")
                ),
            }
            for s, t, cols in candidates
        ],
    }


@router.get("/audit", dependencies=[Depends(verify_admin_key)])
def get_audit(
    run_id: str | None = Query(default=None, description="Filter by run id"),
    table: str | None = Query(default=None, description="Filter by `schema.table`"),
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict[str, Any]:
    """Read recent rows from `parse_repair_audit`."""
    where = []
    params: dict[str, Any] = {"lim": limit}
    if run_id:
        where.append("run_id = CAST(:run_id AS uuid)")
        params["run_id"] = run_id
    if table and "." in table:
        sch, tname = table.split(".", 1)
        where.append("table_schema = :sch AND table_name = :tname")
        params["sch"] = sch
        params["tname"] = tname
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT id, run_id, phase, table_schema, table_name, operation,
               old_columns, new_columns, rows_deleted, ok, error_message,
               dry_run, applied_at
          FROM parse_repair_audit
          {where_clause}
         ORDER BY applied_at DESC
         LIMIT :lim
    """
    engine = get_sync_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().fetchall()
    return {"ok": True, "count": len(rows), "rows": [dict(r) for r in rows]}
