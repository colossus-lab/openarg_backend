"""Mart quality checks, in the order their findings should be read.

Flat list, no registry — same shape as
`validation/detectors/__init__.py::build_default_detectors()`. A check is added
by instantiating it here.
"""

from __future__ import annotations

from app.application.marts.quality.check import MartCheck
from app.application.marts.quality.checks.numeric_typing import NumericTypingCheck
from app.application.marts.quality.checks.row_filter import RowFilterCheck
from app.application.marts.quality.checks.source_coverage import SourceCoverageCheck

__all__ = [
    "NumericTypingCheck",
    "RowFilterCheck",
    "SourceCoverageCheck",
    "build_default_mart_checks",
]


def build_default_mart_checks() -> list[MartCheck]:
    return [
        # Ordered by how far upstream the damage starts: what the mart was
        # built from, then how it filtered, then how it typed the result.
        SourceCoverageCheck(),
        RowFilterCheck(),
        NumericTypingCheck(),
    ]
