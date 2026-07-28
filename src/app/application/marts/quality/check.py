"""Base class for mart quality checks.

Mirrors `validation.Detector` in spirit and reuses its `Finding`, but returns a
*list*: one mart legitimately yields several problems at once (three columns
typed TEXT that hold numbers is three findings, not one), and the ingestion ABC
caps a detector at one.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from app.application.marts.quality.context import MartAuditContext
from app.application.validation.detector import Finding, Mode, Severity


class MartCheck(ABC):
    """One quality question asked of one mart."""

    name: str = "mart_check"
    version: str = "1"
    severity: Severity = Severity.WARN

    def applicable_to(self, ctx: MartAuditContext) -> bool:
        """Skip marts this check cannot say anything about."""
        return True

    @abstractmethod
    def run(self, ctx: MartAuditContext) -> list[Finding]:
        """Return every problem found. An empty list means the mart is clean."""

    def _finding(
        self,
        *,
        message: str,
        payload: dict[str, Any] | None = None,
        severity: Severity | None = None,
        key: str = "",
    ) -> Finding:
        """Build a finding.

        `key` distinguishes findings a check emits about the same mart. It
        ends up in the persistence layer's idempotency tuple, so omitting it
        when a check can emit more than one finding per mart makes the later
        one overwrite the earlier: observed on the first run of
        `mart_hidden_despite_rows`, where the WARN about a failed refresh
        silently replaced the CRITICAL about 52 million unreachable rows.
        Checks that emit at most one finding per mart can leave it empty.
        """
        body = dict(payload or {})
        if key:
            body.setdefault("finding_key", key)
        return Finding(
            detector_name=self.name,
            detector_version=self.version,
            severity=severity or self.severity,
            mode=Mode.MART_AUDIT,
            payload=body,
            should_redownload=False,
            message=message,
        )
