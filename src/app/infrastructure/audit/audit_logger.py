"""Structured audit logger — emits JSON audit events for security and analytics."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

_audit = logging.getLogger("audit")


def _log_event(event_type: str, **kwargs: object) -> None:
    _audit.info(
        json.dumps(
            {"event": event_type, "ts": datetime.now(UTC).isoformat(), **kwargs},
            ensure_ascii=False,
            default=str,
        )
    )


def audit_query(user: str, question: str, intent: str, duration_ms: int) -> None:
    _log_event("query", user=user, question=question[:200], intent=intent, duration_ms=duration_ms)


def audit_injection_blocked(user: str, question: str, score: float) -> None:
    _log_event("injection_blocked", user=user, question=question[:200], score=round(score, 3))


def audit_rate_limited(user: str, endpoint: str) -> None:
    _log_event("rate_limited", user=user, endpoint=endpoint)


def audit_cache_hit(user: str, question: str) -> None:
    _log_event("cache_hit", user=user, question=question[:200])
