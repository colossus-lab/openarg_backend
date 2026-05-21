"""IngestionValidator: orchestrates a list of pluggable detectors."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable

from app.application.validation.detector import (
    Detector,
    Finding,
    Mode,
    ResourceContext,
    Severity,
)

logger = logging.getLogger(__name__)


class IngestionValidator:
    """Strategy-pattern coordinator that runs a list of `Detector`s against a
    resource and returns findings.

    Same instance can be reused across the three modes (pre-parse,
    post-parse, retrospective) — each detector decides whether it applies.

    Findings are returned in deterministic order (detectors registered) so
    callers can rely on the first critical finding to abort early.
    """

    def __init__(self, detectors: Iterable[Detector] | None = None) -> None:
        self._detectors: list[Detector] = list(detectors or [])

    def register(self, detector: Detector) -> None:
        self._detectors.append(detector)

    @property
    def detectors(self) -> tuple[Detector, ...]:
        return tuple(self._detectors)

    def run(self, ctx: ResourceContext, mode: Mode) -> list[Finding]:
        findings: list[Finding] = []
        for detector in self._detectors:
            try:
                if not detector.applicable_to(ctx):
                    continue
                finding = detector.run(ctx, mode)
            except Exception:
                logger.exception(
                    "Detector %s@%s crashed for resource %s in mode %s",
                    detector.name,
                    detector.version,
                    ctx.resource_id,
                    mode,
                )
                continue
            if finding is not None:
                findings.append(finding)
        return findings

    def has_critical(self, findings: Iterable[Finding]) -> bool:
        return any(f.severity == Severity.CRITICAL for f in findings)

    def first_critical(self, findings: Iterable[Finding]) -> Finding | None:
        for f in findings:
            if f.severity == Severity.CRITICAL:
                return f
        return None

    @staticmethod
    def input_hash(ctx: ResourceContext) -> str:
        """Deterministic hash of detector inputs for idempotency keys."""
        h = hashlib.sha256()
        if ctx.raw_byte_sample is not None:
            h.update(ctx.raw_byte_sample)
        elif ctx.raw_bytes is not None:
            h.update(ctx.raw_bytes[:4096])
        h.update(b"|")
        h.update((ctx.declared_format or "").encode("utf-8"))
        h.update(b"|")
        h.update(json.dumps(ctx.materialized_columns or [], sort_keys=True).encode("utf-8"))
        h.update(b"|")
        h.update(str(ctx.materialized_row_count or 0).encode("utf-8"))
        return h.hexdigest()


def default_validator() -> IngestionValidator:
    """Build a validator with the full standard detector suite registered.

    Imports are local to avoid circulars when individual detectors are loaded
    in tests.
    """
    from app.application.validation.detectors import build_default_detectors

    return IngestionValidator(build_default_detectors())
