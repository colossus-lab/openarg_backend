"""WS0.5 — periodic state-machine invariants sweep.

Runs `StateMachineEnforcer.enforce()`. Per the WS0.5 risk mitigation, by
default it runs in `dry_run=True` (registers violations only). After 1 week
of soak, set `OPENARG_WS0_5_AUTO_ENFORCE=1` to enable auto-correction.

Violations are also persisted as findings under mode='state_invariant' so
they share the same audit trail as ingestion findings.
"""

from __future__ import annotations

import logging
import os

from app.application.state_machine import StateMachineEnforcer
from app.application.validation.detector import (
    Finding,
    Mode,
    ResourceContext,
    Severity,
)
from app.application.validation.findings_repository import persist_findings
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


def _auto_enforce_enabled() -> bool:
    return os.getenv("OPENARG_WS0_5_AUTO_ENFORCE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


@celery_app.task(
    name="openarg.ws0_5_state_invariants_sweep",
    bind=True,
    soft_time_limit=300,
    time_limit=420,
)
def state_invariants_sweep(self) -> dict:
    engine = get_sync_engine()
    enforcer = StateMachineEnforcer()
    violations = enforcer.scan(engine)

    # Persist each violation as a finding (mode=state_invariant) so the
    # dashboards line up with WS0 ingestion findings.
    detector_version = "1"
    for v in violations:
        ctx = ResourceContext(
            resource_id=v.dataset_id,
            dataset_id=v.dataset_id,
            table_name=v.table_name,
            metadata={"violation_kind": v.kind, **v.detail},
        )
        finding = Finding(
            detector_name=v.kind,
            detector_version=detector_version,
            severity=Severity.WARN if not v.auto_fixable else Severity.CRITICAL,
            mode=Mode.STATE_INVARIANT,
            payload={"detail": v.detail, "auto_fixable": v.auto_fixable},
            should_redownload=False,
            message=v.kind,
        )
        persist_findings(
            engine,
            ctx,
            [finding],
            input_hash=f"{v.kind}:{v.dataset_id}",
        )

    dry_run = not _auto_enforce_enabled()
    summary = enforcer.enforce(engine, dry_run=dry_run)
    summary["mode"] = "dry_run" if dry_run else "enforce"
    logger.info("ws0.5 sweep done: %s", summary)
    return summary
