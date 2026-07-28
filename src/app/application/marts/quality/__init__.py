"""Quality auditing for built marts.

Answers "which of the 71 marts have the defects we already found by hand", so
the next one is caught by a sweep instead of by someone reading an answer and
noticing the number is wrong.

Reports; does not remediate. `serving_blocked` is driven from the mart YAML
(migration 0054) precisely because a DB-only flag is erased by the next
`build_mart` — so an auditor that flipped it in the database would produce a
block that silently disappears. Findings name the fix; a human applies it.
"""

from app.application.marts.quality.auditor import (
    audit_all,
    collect_contexts,
    run_checks,
    summarize,
)
from app.application.marts.quality.check import MartCheck
from app.application.marts.quality.checks import build_default_mart_checks
from app.application.marts.quality.context import MartAuditContext, MartColumn, SourceTable

__all__ = [
    "MartAuditContext",
    "MartCheck",
    "MartColumn",
    "SourceTable",
    "audit_all",
    "build_default_mart_checks",
    "collect_contexts",
    "run_checks",
    "summarize",
]
