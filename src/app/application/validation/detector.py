"""Detector interface and shared dataclasses for ingestion validation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class Severity(StrEnum):
    INFO = "info"
    WARN = "warn"
    CRITICAL = "critical"


class Mode(StrEnum):
    PRE_PARSE = "pre_parse"
    POST_PARSE = "post_parse"
    RETROSPECTIVE = "retrospective"
    STATE_INVARIANT = "state_invariant"


@dataclass
class ResourceContext:
    """Inputs available to a detector at runtime.

    Not all fields are populated in all modes — pre-parse has raw_bytes but no
    materialized state, post-parse has both, retrospective may have neither
    (only metadata).
    """

    resource_id: str
    dataset_id: str | None = None
    portal: str | None = None
    source_id: str | None = None
    download_url: str | None = None
    declared_format: str | None = None
    declared_content_type: str | None = None
    raw_bytes: bytes | None = None
    raw_byte_sample: bytes | None = None
    table_name: str | None = None
    materialized_columns: list[str] | None = None
    materialized_row_count: int | None = None
    declared_row_count: int | None = None
    declared_size_bytes: int | None = None
    columns_json: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None
    zip_member_names: list[str] | None = None
    zip_member_sizes: dict[str, int] | None = None


@dataclass
class Finding:
    """A single detector outcome for a single resource."""

    detector_name: str
    detector_version: str
    severity: Severity
    mode: Mode
    payload: dict[str, Any] = field(default_factory=dict)
    should_redownload: bool = False
    message: str = ""


class Detector(ABC):
    """Pluggable detector. Implementations declare a name + version and run
    against a `ResourceContext`. Returning `None` means "no issue found"."""

    name: str
    version: str
    severity: Severity = Severity.WARN

    def applicable_to(self, ctx: ResourceContext) -> bool:
        """Return True when this detector should run against this resource.

        Defaults to True. Override for detectors that only apply to certain
        formats, portals or modes (e.g. `MissingKeyColumnDetector` only for
        certain known datasets)."""
        return True

    @abstractmethod
    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        """Execute the check. Should be deterministic and idempotent."""
        raise NotImplementedError

    def _finding(
        self,
        mode: Mode,
        payload: dict[str, Any] | None = None,
        message: str = "",
        severity: Severity | None = None,
        should_redownload: bool = False,
    ) -> Finding:
        return Finding(
            detector_name=self.name,
            detector_version=self.version,
            severity=severity or self.severity,
            mode=mode,
            payload=payload or {},
            message=message,
            should_redownload=should_redownload,
        )
