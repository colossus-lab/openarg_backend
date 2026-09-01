"""Nightly quality sweep over every built mart.

Runs after `refresh-via-b-marts-daily` (03:00) so it audits the state users
will actually be served, not the previous day's.

Findings land in `ingestion_findings` under `mode='mart_audit'`, keyed by
`resource_id = mart::<mart_id>`. The upsert there is idempotent per
(resource, detector, version, mode, input_hash), so a problem that persists
across nights updates one row instead of accumulating.

Closing them is this task's job, and it has to handle the partial case: a mart
with three badly typed columns, one of which gets fixed, must drop to two open
findings. Resolving only when a mart comes back completely clean leaves the
other two open forever, and a report that only grows is a report people stop
reading.

The sweep never writes to `mart_definitions`. Blocking a mart is a YAML edit
(see migration 0054): a DB-only `serving_blocked` is erased by the next
`build_mart`, so an auditor that set it would create a block that quietly
vanishes at the worst possible moment.
"""

from __future__ import annotations

import logging

from app.application.marts.quality import audit_all, finding_discriminator, summarize
from app.application.validation.detector import Mode, ResourceContext, Severity
from app.application.validation.findings_repository import persist_findings, resolve_missing
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


@celery_app.task(
    name="openarg.audit_marts",
    bind=True,
    soft_time_limit=600,
    time_limit=900,
)
def audit_marts(self, *, persist: bool = True) -> dict:
    """Audit every registered mart. Returns a summary; details go to findings.

    `persist=False` runs the checks and reports without touching the findings
    table — for trying a threshold change without rewriting history.
    """
    engine = get_sync_engine()
    results = audit_all(engine)

    from app.application.marts.quality.serving_gate import (
        MAX_PER_RUN as GATE_MAX,
    )
    from app.application.marts.quality.serving_gate import (
        block_empty,
        unblock_if_recovered,
    )

    persisted = 0
    resolved = 0
    # El detector `empty_despite_stored_count` viene avisando hace semanas que un
    # mart vacío "sigue siendo elegible para el routing y responde vacío". Acá
    # deja de ser un aviso: se corrige el conteo y se lo saca de circulación.
    # Bloqueo y desbloqueo, porque un mart que vuelve a tener filas tiene que
    # volver solo — si no, cada recuperación necesita a una persona.
    bloqueados: list[str] = []
    devueltos: list[str] = []
    for ctx, findings in results:
        resource_id = f"mart::{ctx.mart_id}"
        if not persist:
            continue

        # `finding_key` viaja en el payload (`check.py:53`), no como atributo:
        # leerlo mal daría siempre False y el bloqueo nunca correría.
        vacio = any(
            (f.payload or {}).get("finding_key") == "empty_despite_stored_count" for f in findings
        )
        if vacio and len(bloqueados) < GATE_MAX:
            if block_empty(engine, ctx.mart_id, stored=int(ctx.last_row_count or 0)):
                bloqueados.append(ctx.mart_id)
        elif vacio:
            # El tope: más marts vacíos que esto de golpe es un problema
            # sistémico, y sacar medio catálogo en silencio lo empeora.
            logger.warning(
                "serving gate: tope de %d por corrida, %s queda elegible", GATE_MAX, ctx.mart_id
            )
        elif not vacio and ctx.serving_blocked and unblock_if_recovered(engine, ctx.mart_id):
            devueltos.append(ctx.mart_id)
        if not findings:
            # Nothing wrong now: close whatever this sweep reported before.
            # Scoped to this mode so it cannot touch ingestion findings that
            # happen to share the resource id.
            resolved += resolve_missing(engine, resource_id, mode=Mode.MART_AUDIT, keep_hashes=[])
            continue
        audit_ctx = ResourceContext(
            resource_id=resource_id,
            table_name=ctx.qualified,
            metadata={
                "mart_id": ctx.mart_id,
                "hits_30d": ctx.hits_30d,
                "serving_blocked": ctx.serving_blocked,
            },
        )
        seen: set[str] = set()
        for finding in findings:
            input_hash = f"{finding.detector_name}:{finding_discriminator(finding, ctx.mart_id)}"
            if input_hash in seen:
                # Two findings racing for one row. Whichever lands second wins
                # and the other disappears, so say it rather than let the
                # summary report a number the table does not hold.
                logger.error(
                    "mart audit: %s emitted findings sharing key %r on %s — "
                    "one will overwrite the other; the check must set a distinct "
                    "`finding_key`",
                    finding.detector_name,
                    input_hash,
                    ctx.mart_id,
                )
                continue
            seen.add(input_hash)
            persisted += persist_findings(engine, audit_ctx, [finding], input_hash=input_hash)

        # Whatever this mart used to report and no longer does is fixed — or
        # was rekeyed by a check that changed shape. Either way it must stop
        # showing up, or the report only ever grows and starts being ignored.
        resolved += resolve_missing(engine, resource_id, mode=Mode.MART_AUDIT, keep_hashes=seen)

    summary = summarize(results)
    summary["persisted"] = persisted
    summary["resolved"] = resolved
    summary["retirados_del_servicio"] = bloqueados
    summary["devueltos_al_servicio"] = devueltos
    summary["persist"] = persist

    critical = summary.get("by_severity", {}).get(Severity.CRITICAL.value, 0)
    if critical:
        # Loud on purpose: a critical mart finding means something is being
        # served that we cannot stand behind.
        logger.warning("mart audit: %d critical finding(s) — %s", critical, summary)
    else:
        logger.info("mart audit done: %s", summary)
    return summary
