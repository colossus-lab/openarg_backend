"""Deciding what a change between two schema snapshots actually was.

Built as a cascade of gates that can only *exonerate* — prove a diff is not
upstream drift — never accuse. See `classifier` for why.
"""

from app.application.drift.classifier import (
    ChangeClass,
    DriftContext,
    DriftVerdict,
    Verdict,
    classify_change,
    summarize,
)

__all__ = [
    "ChangeClass",
    "DriftContext",
    "DriftVerdict",
    "Verdict",
    "classify_change",
    "summarize",
]
