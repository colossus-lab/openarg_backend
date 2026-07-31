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
import re
from collections.abc import Iterable
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.application.validation.detector import (
    Finding,
    Mode,
    ResourceContext,
    Severity,
)
from app.application.validation.findings_repository import persist_findings, resolve_missing
from app.application.validation.ingestion_validator import (
    IngestionValidator,
    default_validator,
)

logger = logging.getLogger(__name__)

_validator_singleton: IngestionValidator | None = None


def _placeholder_header_finding(ctx: ResourceContext, *, mode: Mode) -> Finding | None:
    columns = [str(col or "").strip() for col in (ctx.materialized_columns or [])]
    if len(columns) < 3:
        return None

    placeholder_count = 0
    numeric_count = 0
    for col in columns:
        lowered = col.lower()
        if lowered.startswith("unnamed:") or re.fullmatch(r"col_[0-9]+", lowered):
            placeholder_count += 1
        if re.fullmatch(r"[0-9]+", lowered):
            numeric_count += 1

    threshold = max(3, int(len(columns) * 0.35))
    if placeholder_count < threshold and numeric_count < threshold:
        return None

    return Finding(
        detector_name="placeholder_headers",
        detector_version="1",
        severity=Severity.CRITICAL,
        mode=mode,
        payload={
            "column_count": len(columns),
            "placeholder_count": placeholder_count,
            "numeric_count": numeric_count,
            "sample": columns[:12],
        },
        message=(
            "materialized columns look like placeholder headers "
            f"(placeholders={placeholder_count}, numeric={numeric_count}, total={len(columns)})"
        ),
    )


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
        findings = list(validator.run(ctx, Mode.POST_PARSE))
        extra = _placeholder_header_finding(ctx, mode=Mode.POST_PARSE)
        if extra is not None:
            findings.append(extra)
        _persist(engine, ctx, findings)
        return validator.first_critical(findings)
    except Exception:
        logger.exception("post_parse validator hook failed")
        return None


def _resolve_placeholder_headers(engine: Engine, resource_id: str) -> int:
    """Close `placeholder_headers` findings across modes once the headers are fine.

    `resolve_missing` is deliberately mode-scoped so an audit cannot resolve
    findings it knows nothing about, and that is the right default. This is the
    one detector where the scoping is wrong rather than cautious: the finding is
    stored under POST_PARSE but derives from `materialized_columns` alone, which
    the retrospective sweep reads straight from `information_schema`. It has
    exactly the evidence the parse path had — no bytes, no parser required.

    Without this the finding is immortal. Nothing else re-evaluates it, and
    `_close_resolved_findings_query` needs a re-collection that never comes for
    a table whose headers were repaired in place. On staging 2026-07-31 that was
    117 tables held out of serving with headers reading `Apellido, Nombre,
    Cargo` — withheld for a defect fixed months earlier.
    """
    try:
        with engine.begin() as conn:
            res = conn.execute(
                text(
                    "UPDATE ingestion_findings SET resolved_at = NOW() "
                    "WHERE resource_id = :rid AND resolved_at IS NULL "
                    "  AND detector_name = 'placeholder_headers'"
                ),
                {"rid": resource_id},
            )
            return int(res.rowcount or 0)
    except Exception:
        logger.exception("Failed to resolve placeholder_headers for %s", resource_id)
        return 0


def validate_retrospective(
    engine: Engine, *, resolve_stale: bool = False, **kwargs: Any
) -> list[Finding]:
    """Run all detectors in retrospective mode. Returns ALL findings.

    Used by the Celery beat sweep — caller decides what to do with them.

    With `resolve_stale=True` the run also *closes* findings this resource no
    longer produces, which is what makes the sweep a synchronisation rather
    than an append-only log. Without it the upsert only ever re-opens
    (`persist_findings` resets `resolved_at` on conflict) and nothing else
    closes a retrospective finding: `_close_resolved_findings_query` requires
    the dataset to have been re-processed *after* the finding, so a table that
    got fixed and was never re-collected keeps its finding open forever.
    Measured 2026-07-31 on prod: 1459 open retrospective findings, the oldest
    dating to 2026-05-06.

    Same semantics the mart auditor already uses via `resolve_missing`: keep
    what this run reported, close the rest, so a partially-fixed resource ends
    up with fewer open findings instead of the same ones forever.
    """
    try:
        ctx = _build_ctx(**kwargs)
        validator = get_validator()
        findings = list(validator.run(ctx, Mode.RETROSPECTIVE))
        # `placeholder_headers` reads nothing but `materialized_columns`, which
        # this hook observes directly, so there was never a reason for it to be
        # exclusive to the parse path — it just grew there. Left that way it is
        # written once and re-checked never: measured on staging 2026-07-31,
        # 117 of the 121 tables carrying an open one had clean columns, some
        # since May.
        placeholder = _placeholder_header_finding(ctx, mode=Mode.RETROSPECTIVE)
        if placeholder is not None:
            findings.append(placeholder)
        _persist(engine, ctx, findings)
        if resolve_stale and ctx.resource_id:
            # The hash covers this run's inputs, so anything stored under a
            # different one describes a state the resource has left behind.
            keep = [IngestionValidator.input_hash(ctx)] if findings else []
            resolve_missing(engine, ctx.resource_id, mode=Mode.RETROSPECTIVE, keep_hashes=keep)
            if placeholder is None:
                _resolve_placeholder_headers(engine, ctx.resource_id)
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
