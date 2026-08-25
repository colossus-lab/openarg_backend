"""Schedule the registry reconciliation.

Both acts default to `dry_run=True`, so running either by hand reports rather
than acts. The scheduled entries pass `dry_run=False` explicitly: the decision
to write belongs in the schedule where it can be read, not in a default where it
cannot.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


@celery_app.task(
    name="openarg.reconcile_registry_locations",
    bind=True,
    soft_time_limit=900,
    time_limit=1200,
)
def reconcile_registry_locations(self, *, limit: int = 500, dry_run: bool = True) -> dict[str, Any]:
    """Move tables to the schema their live registry row already names."""
    from app.application.catalog.registry_reconcile import (
        RegistryUnavailable,
        reconcile_locations,
    )

    engine = get_sync_engine()
    try:
        outcome = reconcile_locations(engine, run_id=uuid.uuid4(), dry_run=dry_run, limit=limit)
    except RegistryUnavailable as exc:
        # Refusing is the result, not an error to retry into.
        logger.error("reconcile_registry_locations refused: %s", exc)
        return {"refused": str(exc), "moved": 0}
    result = outcome.as_dict()
    logger.info("registry locations: %s", result)
    return result


@celery_app.task(
    name="openarg.retire_phantom_registry_rows",
    bind=True,
    soft_time_limit=900,
    time_limit=1200,
)
def retire_phantom_registry_rows(self, *, limit: int = 500, dry_run: bool = True) -> dict[str, Any]:
    """Mark live rows superseded when their table no longer exists anywhere."""
    from app.application.catalog.registry_reconcile import (
        RegistryUnavailable,
        retire_phantom_rows,
    )

    engine = get_sync_engine()
    try:
        outcome = retire_phantom_rows(engine, run_id=uuid.uuid4(), dry_run=dry_run, limit=limit)
    except RegistryUnavailable as exc:
        logger.error("retire_phantom_registry_rows refused: %s", exc)
        return {"refused": str(exc), "retired": 0}
    result = outcome.as_dict()
    logger.info("registry phantoms: %s", result)
    return result


@celery_app.task(
    name="openarg.backfill_legacy_registry",
    bind=True,
    soft_time_limit=1800,
    time_limit=2400,
)
def backfill_legacy_registry_task(
    self, *, limit: int = 5000, dry_run: bool = True
) -> dict[str, Any]:
    """Register served tables the registry never learned about."""
    from app.application.catalog.registry_reconcile import (
        RegistryUnavailable,
        backfill_legacy_registry,
    )

    engine = get_sync_engine()
    try:
        outcome = backfill_legacy_registry(
            engine, run_id=uuid.uuid4(), dry_run=dry_run, limit=limit
        )
    except RegistryUnavailable as exc:
        logger.error("backfill_legacy_registry refused: %s", exc)
        return {"refused": str(exc), "registered": 0}
    result = outcome.as_dict()
    logger.info("registry backfill: %s", result)
    return result
