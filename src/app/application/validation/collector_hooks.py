"""Convenience hooks to wire `IngestionValidator` into the collector worker
without rewriting `collector_tasks.py`.

Two entry points:
  - `validate_pre_parse` — call after download, before pandas. Returns the
    first critical finding (or None) and persists ALL findings.
  - `validate_post_parse` — call after materialization, before status='ready'.

Both are best-effort: if the validator itself fails, they log and return
None so the collector can continue (failure-open).
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from typing import Any

from sqlalchemy.engine import Engine

from app.application.validation.detector import (
    Finding,
    Mode,
    ResourceContext,
    Severity,
)
from app.application.validation.findings_repository import persist_findings
from app.application.validation.ingestion_validator import (
    IngestionValidator,
    default_validator,
)

logger = logging.getLogger(__name__)

_validator_singleton: IngestionValidator | None = None


def get_validator() -> IngestionValidator:
    """Process-wide validator singleton (Celery worker pre-fork friendly).

    The detector list is immutable per worker; building it once avoids the
    slight import overhead per task.
    """
    global _validator_singleton  # noqa: PLW0603
    if _validator_singleton is None:
        _validator_singleton = default_validator()
    return _validator_singleton


def _disabled() -> bool:
    """Feature flag — set OPENARG_DISABLE_INGESTION_VALIDATOR=1 to skip."""
    return os.getenv("OPENARG_DISABLE_INGESTION_VALIDATOR", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _build_ctx(
    dataset_id: str,
    *,
    portal: str | None = None,
    source_id: str | None = None,
    download_url: str | None = None,
    declared_format: str | None = None,
    raw_byte_sample: bytes | None = None,
    declared_size_bytes: int | None = None,
    table_name: str | None = None,
    materialized_columns: list[str] | None = None,
    materialized_row_count: int | None = None,
    declared_row_count: int | None = None,
    columns_json: str | None = None,
    zip_member_names: list[str] | None = None,
    zip_member_sizes: dict[str, int] | None = None,
    http_status: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> ResourceContext:
    return ResourceContext(
        resource_id=dataset_id,
        dataset_id=dataset_id,
        portal=portal,
        source_id=source_id,
        download_url=download_url,
        declared_format=declared_format,
        raw_byte_sample=raw_byte_sample,
        declared_size_bytes=declared_size_bytes,
        table_name=table_name,
        materialized_columns=materialized_columns,
        materialized_row_count=materialized_row_count,
        declared_row_count=declared_row_count,
        columns_json=columns_json,
        zip_member_names=zip_member_names,
        zip_member_sizes=zip_member_sizes,
        http_status=http_status,
        metadata=metadata or {},
    )


def _persist(engine: Engine, ctx: ResourceContext, findings: Iterable[Finding]) -> None:
    findings_list = list(findings)
    if not findings_list:
        return
    input_hash = IngestionValidator.input_hash(ctx)
    persist_findings(engine, ctx, findings_list, input_hash=input_hash)


def validate_pre_parse(engine: Engine, **kwargs: Any) -> Finding | None:
    """Run pre-parse detectors. Returns the first critical Finding (or None)."""
    if _disabled():
        return None
    try:
        ctx = _build_ctx(**kwargs)
        validator = get_validator()
        findings = validator.run(ctx, Mode.PRE_PARSE)
        _persist(engine, ctx, findings)
        return validator.first_critical(findings)
    except Exception:
        logger.exception("pre_parse validator hook failed")
        return None


def validate_post_parse(engine: Engine, **kwargs: Any) -> Finding | None:
    """Run post-parse detectors. Returns the first critical Finding (or None)."""
    if _disabled():
        return None
    try:
        ctx = _build_ctx(**kwargs)
        validator = get_validator()
        findings = validator.run(ctx, Mode.POST_PARSE)
        _persist(engine, ctx, findings)
        return validator.first_critical(findings)
    except Exception:
        logger.exception("post_parse validator hook failed")
        return None


def validate_retrospective(engine: Engine, **kwargs: Any) -> list[Finding]:
    """Run all detectors in retrospective mode. Returns ALL findings.

    Used by the Celery beat sweep — caller decides what to do with them.
    """
    try:
        ctx = _build_ctx(**kwargs)
        validator = get_validator()
        findings = validator.run(ctx, Mode.RETROSPECTIVE)
        _persist(engine, ctx, findings)
        return findings
    except Exception:
        logger.exception("retrospective validator hook failed")
        return []


def soft_flip_enabled() -> bool:
    """Whether retrospective sweep should auto-flip materialization_status to
    `materialization_corrupted` for critical findings.

    Default false — per WS0 risk mitigation, run as `severity=warn` for 1 week
    before enabling auto-flip in production.
    """
    return os.getenv("OPENARG_SWEEP_AUTOFLIP", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def is_critical(finding: Finding | None) -> bool:
    return finding is not None and finding.severity == Severity.CRITICAL
