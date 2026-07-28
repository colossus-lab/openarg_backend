"""Nightly quality sweep over every built mart.

Runs after `refresh-via-b-marts-daily` (03:00) so it audits the state users
will actually be served, not the previous day's.

Findings land in `ingestion_findings` under `mode='mart_audit'`, keyed by
`resource_id = mart::<mart_id>`. The upsert there is idempotent per
(resource, detector, version, mode, input_hash), so a problem that persists
across nights updates one row instead of accumulating; one that goes away stops
being refreshed and can be resolved.

The sweep never writes to `mart_definitions`. Blocking a mart is a YAML edit
(see migration 0054): a DB-only `serving_blocked` is erased by the next
`build_mart`, so an auditor that set it would create a block that quietly
vanishes at the worst possible moment.
"""

from __future__ import annotations

import logging

from app.application.marts.quality import audit_all, finding_discriminator, summarize
from app.application.validation.detector import ResourceContext, Severity
from app.application.validation.findings_repository import mark_resolved, persist_findings
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

    persisted = 0
    for ctx, findings in results:
        resource_id = f"mart::{ctx.mart_id}"
        if not persist:
            continue
        if not findings:
            # Nothing wrong now: close whatever this sweep reported before.
            mark_resolved(engine, resource_id)
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

    summary = summarize(results)
    summary["persisted"] = persisted
    summary["persist"] = persist

    critical = summary.get("by_severity", {}).get(Severity.CRITICAL.value, 0)
    if critical:
        # Loud on purpose: a critical mart finding means something is being
        # served that we cannot stand behind.
        logger.warning("mart audit: %d critical finding(s) — %s", critical, summary)
    else:
        logger.info("mart audit done: %s", summary)
    return summary
