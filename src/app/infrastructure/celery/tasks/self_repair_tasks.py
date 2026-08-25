"""Fix what feeds a mart, then say what happened — the whole ladder, unattended.

Three things existed and never met. The repair sweeps knew how to fix a table
but not whether anything read it. The marts knew they were smaller than they
should be but not why. The alerting knew how to reach a phone but was only ever
handed things nobody had tried to fix.

This is the loop that closes them, in the order a person would do it:

    detectar → heurísticas → LLM → reconstruir los marts → avisar el resultado

The message is the difference. Before this, an alert said *"algo se rompió"* and
a person went to find out whether it mattered and whether it was fixable. Now it
says which rung fixed it, or that five heuristics and a model all declined —
which is the only version of the message that is worth interrupting somebody
for.

**Why it works on the mart-feeding tables specifically.** Measured on production
2026-08-24: 3,160 tables in `raw` carry a parse defect and **105 of them feed a
mart**. The other 3,055 are already the scheduled sweeps' territory and nobody
is served by them today. Ordering the work by whether a person is downstream is
the difference between a number too large to act on and a list.

**And it can refuse.** Renaming a column some mart names by hand fixes a table
and breaks a view. Every rung is guarded by `marts.consumers`, and a repair that
would cost more than it fixes is handed to a person with the marts listed rather
than applied because the sweep happened to be running.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# The same four symptoms `/admin/data-health` counts, plus the delimiter in a
# column name that marks an unsplit CSV. Read from `pg_catalog` rather than
# `information_schema`: over 27,000 tables the latter's per-row permission
# filtering turned this into a 90-second query.
_BROKEN_SQL = text(
    r"""
    WITH cols AS (
        SELECT c.oid AS tbl, n.nspname AS schema_name, c.relname AS table_name,
               a.attname AS column_name,
               count(*) OVER (PARTITION BY c.oid) AS n_cols
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname IN ('raw', 'public')
          AND c.relkind = 'r'
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND a.attname NOT LIKE '\_%'
    )
    SELECT schema_name, table_name, n_cols,
           bool_or(column_name ~ '^col_[0-9]+$')      AS col_n,
           bool_or(column_name ~ '^[Uu]nnamed')        AS unnamed,
           bool_or(length(column_name) > 60)           AS long_name,
           bool_or(column_name ~ '[,;|]'
                   OR position(chr(9) in column_name) > 0) AS delimiter_in_name
    FROM cols
    GROUP BY schema_name, table_name, n_cols
    HAVING bool_or(column_name ~ '^col_[0-9]+$')
        OR bool_or(column_name ~ '^[Uu]nnamed')
        OR bool_or(length(column_name) > 60)
        OR bool_or(column_name ~ '[,;|]' OR position(chr(9) in column_name) > 0)
        OR max(n_cols) <= 2
    """
)

_MAX_PER_RUN = int(os.getenv("OPENARG_MART_SOURCE_REPAIR_MAX_PER_RUN", "40"))


def _symptoms(row: Any) -> list[str]:
    named = (
        ("col_n", row.col_n),
        ("unnamed", row.unnamed),
        ("long_name", row.long_name),
        ("delimiter_in_name", row.delimiter_in_name),
    )
    found = [name for name, flag in named if flag]
    if row.n_cols <= 2:
        found.append("one_or_two_columns")
    return found


@celery_app.task(
    name="openarg.repair_mart_sources",
    bind=True,
    soft_time_limit=2400,
    time_limit=3000,
)
def repair_mart_sources(
    self,
    *,
    limit: int | None = None,
    dry_run: bool = True,
    use_llm: bool = True,
    rebuild: bool = True,
) -> dict[str, Any]:
    """Repair the broken tables that feed marts, then rebuild and report.

    Defaults to `dry_run=True`: run by hand it walks the whole ladder, asks the
    guard, and writes nothing — so the plan is readable before it is trusted.
    The scheduled entry passes `dry_run=False` explicitly, which keeps the
    decision to write visible in the schedule instead of buried in a default.

    `use_llm` is honoured only when the model answers the canary. A model that
    cannot name a column of CUITs does not get to rename anything, and the run
    continues on the heuristics alone rather than aborting — five deterministic
    rungs are still worth running.
    """
    from app.application.marts.consumers import build_consumer_index
    from app.application.repair.approval import target_set_is_plausible
    from app.application.repair.escalation import Escalation, escalate_table
    from app.application.repair.quarantine import (
        MAX_PER_RUN as QUARANTINE_MAX_PER_RUN,
    )
    from app.application.repair.quarantine import (
        is_quarantinable,
        quarantine,
        release,
    )

    engine = get_sync_engine()
    run_id = uuid.uuid4()
    cap = limit or _MAX_PER_RUN

    index = build_consumer_index(engine)
    with engine.connect() as conn:
        broken = conn.execute(_BROKEN_SQL).fetchall()
        conn.rollback()

    # The whole ordering decision, in one line: work on what somebody is served
    # by. Widest first inside that, because a table with more columns feeding a
    # mart is more of the mart.
    feeding = [r for r in broken if index.marts_for(r.schema_name, r.table_name)]

    # Antes de ejecutar nada: ¿es una cantidad de trabajo normal? Los dos
    # incidentes que dieron forma a esta comprobación —el job de remediación de
    # Azure, el Diskerase de Google— fueron acciones correctas contra un
    # conjunto objetivo equivocado.
    plausible, motivo = target_set_is_plausible(len(feeding), cap)
    if not plausible:
        logger.error("repair_mart_sources: conjunto objetivo implausible — %s", motivo)
        return {
            "aborted": "implausible_target_set",
            "reason": motivo,
            "feeding_marts": len(feeding),
            "attempted": 0,
        }

    feeding.sort(key=lambda r: (-int(r.n_cols or 0), r.table_name))
    selected = feeding[:cap]

    llm = None
    canary_detail = "no consultado"
    if use_llm:
        llm, canary_detail = _model_if_it_answers()

    results: list[Escalation] = []
    sintomas_por_tabla: dict[str, list[str]] = {}
    for row in selected:
        sintomas_por_tabla[row.table_name] = _symptoms(row)
        marts = index.marts_for(row.schema_name, row.table_name)

        def guard(columns, _s=row.schema_name, _t=row.table_name) -> list[str]:
            blocking: set[str] = set()
            for col in columns:
                blocking.update(index.marts_referencing_column(_s, _t, col))
            return sorted(blocking)

        result = escalate_table(
            engine,
            table_schema=row.schema_name,
            table_name=row.table_name,
            guard=guard,
            llm=llm,
            run_id=run_id,
            dry_run=dry_run,
        )
        logger.info("escalation: %s | marts=%s", result.as_log_dict(), list(marts))
        results.append(result)

    fixed = [r for r in results if r.fixed]
    blocked = [r for r in results if r.blocked_by_marts]
    # Esperar una firma no es fallar: la escalera hizo su trabajo y paró donde
    # debía. Contarlos como fallas los mandaría a cuarentena, que sería retirar
    # del servicio una tabla que tiene arreglo listo.
    awaiting = [r for r in results if r.proposal_id]
    unfixed = [r for r in results if not r.fixed and not r.blocked_by_marts and not r.proposal_id]

    # El tercer resultado. Una tabla que nadie pudo arreglar y cuyas columnas son
    # ilegibles no es sólo poco útil: es peligrosa, porque el modelo le va a
    # inferir un significado a `col_1` y va a contestar con fluidez y con cita
    # desde una columna que nadie leyó.
    aisladas: list[str] = []
    liberadas: list[str] = []
    if not dry_run:
        for r in unfixed:
            if len(aisladas) >= QUARANTINE_MAX_PER_RUN:
                # El límite es la lección de todos los incidentes revisados: la
                # acción estaba bien y el alcance estaba mal.
                logger.warning(
                    "quarantine: tope de %d por corrida alcanzado", QUARANTINE_MAX_PER_RUN
                )
                break
            if is_quarantinable(sintomas_por_tabla.get(r.table_name, [])) and quarantine(
                engine, r.table_name
            ):
                aisladas.append(r.table_name)
        # Una reparación exitosa devuelve la tabla al servicio: la cuarentena es
        # una pausa, no un veredicto.
        for r in fixed:
            if release(engine, r.table_name):
                liberadas.append(r.table_name)

    rebuilt = _rebuild_affected(index, fixed) if (rebuild and not dry_run) else []

    report: dict[str, Any] = {
        "run_id": str(run_id),
        "dry_run": dry_run,
        "canary": canary_detail,
        "broken_tables_total": len(broken),
        "feeding_marts": len(feeding),
        "attempted": len(selected),
        "fixed": len(fixed),
        "fixed_by_tier": _count(r.tier or "?" for r in fixed),
        # Refused on purpose, which is not the same as failed and must never be
        # counted as one. These are the ones where repairing costs more than the
        # defect, and they are the reason a person is still in this loop.
        "blocked_by_marts": len(blocked),
        "unfixed": len(unfixed),
        "unfixed_by_reason": _count(r.reason or "?" for r in unfixed),
        # Retiradas del servicio, que no es lo mismo que falladas: el chat deja
        # de ofrecerlas en vez de servir columnas que nadie puede interpretar.
        "awaiting_approval": [
            {"tabla": r.table, "id": r.proposal_id, "detalle": r.action_detail} for r in awaiting
        ],
        "quarantined": aisladas,
        "released": liberadas,
        "marts_rebuilt": rebuilt,
    }
    report["alerting"] = _tell_a_person(
        engine,
        index,
        fixed,
        blocked,
        unfixed,
        dry_run=dry_run,
        quarantined=set(aisladas),
        awaiting=awaiting,
    )
    logger.info("repair_mart_sources: %s", report)
    return report


def _count(values: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for v in values:
        out[v] = out.get(v, 0) + 1
    return out


def _model_if_it_answers() -> tuple[Any, str]:
    """The canary gate, shared with the connectors that also map with a model."""
    from app.infrastructure.celery.tasks._llm_gate import model_if_it_answers

    return model_if_it_answers()


def _rebuild_affected(index: Any, fixed: list[Any]) -> list[str]:
    """Rebuild every mart that reads a table we just changed.

    A repaired table does not reach a person through the matview until it is
    rebuilt, and leaving that to the nightly schedule is how a fix sits invisible
    for a day. Dispatched through `dispatch_build_mart`, which debounces per mart
    so a run touching ten tables of one mart enqueues one build.
    """
    from app.infrastructure.celery.tasks.mart_tasks import dispatch_build_mart

    targets: set[str] = set()
    for r in fixed:
        targets.update(index.marts_for(r.table_schema, r.table_name))

    dispatched: list[str] = []
    for mart_id in sorted(targets):
        try:
            dispatch_build_mart(mart_id)
            dispatched.append(mart_id)
        except Exception:
            # A failed dispatch is not a failed repair. The table is fixed and
            # the nightly build picks the mart up.
            logger.warning("repair_mart_sources: could not dispatch %s", mart_id, exc_info=True)
    return dispatched


def _tell_a_person(
    engine: Any,
    index: Any,
    fixed: list[Any],
    blocked: list[Any],
    unfixed: list[Any],
    *,
    dry_run: bool,
    quarantined: set[str] | None = None,
    awaiting: list[Any] | None = None,
) -> dict[str, Any]:
    """One message, three kinds of news, and silence when there is none.

    **A dry run tells nobody.** The first production dry run of this task sent
    five real messages and, worse, recorded all twenty-five findings in
    `alert_log` — so the real run that followed would have been deduplicated
    into silence about the very things it was built to report. A rehearsal that
    consumes the alert budget is not a rehearsal.

    Deduplicated by `alerting.notify` on the identity of the problem rather than
    of the sighting, so a table that stays broken is reported once and not every
    time the sweep runs.

    A repair is announced keyed on table **and rung**, so a table fixed by the
    heuristics today and by the model next month says so twice. That is
    deliberate: the second message means the defect came back in a different
    shape, which is exactly the thing worth knowing.
    """
    try:
        from app.application.quality.alerting import Alert, notify
    except Exception:  # pragma: no cover — import guard
        return {"considered": 0, "new": 0, "sent": 0}

    alerts: list[Alert] = []
    for r in unfixed:
        marts = index.marts_for(r.table_schema, r.table_name)
        alerts.append(
            Alert(
                kind="broken_unrepaired",
                key=r.table,
                title=f"No pude arreglar {r.table_name[:60]}",
                detail=(
                    f"probé {len(r.attempts)} vía(s), ninguna aplicó ({r.reason}). "
                    + (
                        "La retiré del servicio hasta que alguien la mire. "
                        if r.table_name in (quarantined or set())
                        else ""
                    )
                    + f"Alimenta: {', '.join(marts[:3]) or 'ningún mart'}"
                ),
            )
        )
    for r in awaiting or []:
        alerts.append(
            Alert(
                kind="repair_needs_approval",
                key=r.table,
                title=f"{r.table_name[:60]} tiene arreglo, falta una firma",
                detail=f"{r.action_detail} · id {r.proposal_id}",
            )
        )
    for r in blocked:
        alerts.append(
            Alert(
                kind="repair_would_break_mart",
                key=r.table,
                title=f"{r.table_name[:60]} está roto y no lo toco",
                detail=(
                    f"renombrar {', '.join(r.changed_columns[:3])} rompería "
                    f"{', '.join(r.blocked_by_marts[:3])}. Necesita una persona."
                ),
            )
        )
    for r in fixed:
        marts = index.marts_for(r.table_schema, r.table_name)
        alerts.append(
            Alert(
                kind="repaired",
                key=f"{r.table}::{r.tier}",
                title=f"Arreglado solo: {r.table_name[:60]}",
                detail=(
                    f"vía {r.tier} · {len(r.changed_columns)} columna(s) · "
                    f"reconstruyo {', '.join(marts[:3])}"
                ),
            )
        )

    if dry_run:
        # Composed so the rehearsal shows what it would say, and then not sent —
        # `notify` both delivers and claims the fingerprint, and claiming it here
        # would silence the real run.
        return {
            "considered": len(alerts),
            "new": 0,
            "sent": 0,
            "dry_run": True,
            "would_say": [f"[{a.kind}] {a.title}" for a in alerts[:5]],
        }

    try:
        return notify(engine, alerts, heading="OpenArg · reparación de fuentes de marts")
    except Exception:
        # Never let the notification cost the run that produced it.
        logger.warning("repair_mart_sources: alerting skipped", exc_info=True)
        return {"considered": len(alerts), "new": 0, "sent": 0}


@celery_app.task(
    name="openarg.apply_approved_repairs",
    bind=True,
    soft_time_limit=900,
    time_limit=1200,
)
def apply_approved_repairs(self, *, limit: int = 25) -> dict[str, Any]:
    """Apply the repairs a person signed off on.

    Deciding and executing are separate acts on purpose: a decision made in a
    hurry should not also be a write made in a hurry, and a proposal that was
    approved yesterday still runs against today's table.
    """
    import uuid as _uuid

    from app.application.repair.approval import approved
    from app.application.repair.escalation import heuristic_tiers

    engine = get_sync_engine()
    por_nombre = dict(heuristic_tiers())
    aplicadas: list[str] = []
    fallidas: list[str] = []

    for item in approved(engine, limit=limit):
        fn = por_nombre.get(item["tier"])
        if fn is None:
            fallidas.append(f"{item['table_name']}:tier_desconocido")
            continue
        try:
            outcome = fn(
                engine,
                table_schema=item["table_schema"],
                table_name=item["table_name"],
                run_id=_uuid.uuid4(),
                dry_run=False,
            )
        except Exception:
            logger.warning("apply_approved: %s falló", item["table_name"], exc_info=True)
            fallidas.append(f"{item['table_name']}:raised")
            continue
        (aplicadas if outcome.ok else fallidas).append(item["table_name"])
        _mark_applied(engine, item["id"], ok=outcome.ok)

    report = {"aplicadas": len(aplicadas), "fallidas": len(fallidas), "detalle": fallidas[:5]}
    logger.info("apply_approved_repairs: %s", report)
    return report


def _mark_applied(engine: Any, proposal_id: str, *, ok: bool) -> None:
    """Close the proposal so the next run does not try it again."""
    from sqlalchemy import text as _text

    try:
        with engine.begin() as conn:
            conn.execute(
                _text(
                    "UPDATE public.repair_proposals SET status = :s, decided_at = now() "
                    "WHERE id = CAST(:id AS uuid)"
                ),
                {"s": "applied" if ok else "failed", "id": proposal_id},
            )
    except Exception:
        logger.warning("apply_approved: no se pudo cerrar %s", proposal_id, exc_info=True)
