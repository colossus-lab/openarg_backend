"""Explicit state machine for `cached_datasets` + invariant enforcer.

Targets the 305 orchestration bugs (30% of all failures) confirmed in prod
April 2026:
  - 79 schema_mismatch loops with retry_count=4
  - 134 'Exhausted retries while stuck in downloading'
  - 144 'Table missing: marked for re-download'
  - 43 stuck in 'pending' for 1-3 weeks

Provides:
  - `Status` enum + permitted `Transition` set
  - `StateMachineEnforcer.scan(engine)` — pure invariant scan, returns
    violations as `(dataset_id, violation_kind, suggested_fix)`
  - `StateMachineEnforcer.enforce(engine, *, dry_run)` — applies the
    deterministic fixes (status flips for retry>=MAX, pending timeout)
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Mirror of `collector_tasks.MAX_TOTAL_ATTEMPTS`. If you bump that, bump this.
MAX_TOTAL_ATTEMPTS = 5
PENDING_TIMEOUT_DAYS = 7


class Status(StrEnum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    PARSING = "parsing"
    MATERIALIZED = "materialized"
    READY = "ready"
    ERROR = "error"
    PERMANENTLY_FAILED = "permanently_failed"
    SCHEMA_MISMATCH = "schema_mismatch"


@dataclass(frozen=True)
class Transition:
    src: Status
    dst: Status

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"{self.src.value}→{self.dst.value}"


# Authoritative transition table.
ALLOWED_TRANSITIONS: frozenset[Transition] = frozenset(
    {
        Transition(Status.PENDING, Status.DOWNLOADING),
        Transition(Status.PENDING, Status.PERMANENTLY_FAILED),
        Transition(Status.DOWNLOADING, Status.PARSING),
        Transition(Status.DOWNLOADING, Status.ERROR),
        Transition(Status.DOWNLOADING, Status.READY),  # legacy fast-path
        Transition(Status.DOWNLOADING, Status.PERMANENTLY_FAILED),
        Transition(Status.DOWNLOADING, Status.SCHEMA_MISMATCH),
        Transition(Status.PARSING, Status.MATERIALIZED),
        Transition(Status.PARSING, Status.ERROR),
        Transition(Status.MATERIALIZED, Status.READY),
        Transition(Status.MATERIALIZED, Status.ERROR),
        Transition(Status.READY, Status.ERROR),  # corruption discovered
        Transition(Status.READY, Status.DOWNLOADING),  # re-materialize
        Transition(Status.ERROR, Status.PENDING),  # re-enqueue when retry<MAX
        Transition(Status.ERROR, Status.DOWNLOADING),
        Transition(Status.ERROR, Status.PERMANENTLY_FAILED),
        Transition(Status.SCHEMA_MISMATCH, Status.ERROR),
        Transition(Status.SCHEMA_MISMATCH, Status.PERMANENTLY_FAILED),
    }
)


def is_allowed(src: Status, dst: Status) -> bool:
    return Transition(src, dst) in ALLOWED_TRANSITIONS


@dataclass
class Violation:
    dataset_id: str
    table_name: str | None
    kind: str
    detail: dict
    auto_fixable: bool
    suggested_status: Status | None = None


class StateMachineEnforcer:
    """Pure scan + targeted auto-correction. Designed to run as a Celery beat
    sweep every ~30min.

    Invariants enforced:
      I1: retry_count >= MAX_TOTAL_ATTEMPTS  ⇒  status = 'permanently_failed'
      I2: status = 'ready'                   ⇒  table referenced exists
      I3: status = 'pending' for >7d         ⇒  permanently_failed by timeout
      I4: status = 'schema_mismatch' with retry_count >= MAX  ⇒  permanently_failed
    """

    def __init__(
        self,
        *,
        max_attempts: int = MAX_TOTAL_ATTEMPTS,
        pending_timeout_days: int = PENDING_TIMEOUT_DAYS,
    ) -> None:
        self.max_attempts = max_attempts
        self.pending_timeout_days = pending_timeout_days

    # ---------- scan ----------

    def scan(self, engine: Engine) -> list[Violation]:
        violations: list[Violation] = []
        violations.extend(self._scan_retry_invariant(engine))
        violations.extend(self._scan_pending_timeout(engine))
        violations.extend(self._scan_orphan_ready(engine))
        violations.extend(self._scan_schema_mismatch_loop(engine))
        return violations

    def _scan_retry_invariant(self, engine: Engine) -> Iterable[Violation]:
        sql = text(
            "SELECT dataset_id::text AS dataset_id, table_name, retry_count, status, error_message "
            "FROM cached_datasets "
            "WHERE retry_count >= :max AND status = 'error'"
        )
        with engine.connect() as conn:
            for row in conn.execute(sql, {"max": self.max_attempts}).fetchall():
                yield Violation(
                    dataset_id=row.dataset_id,
                    table_name=row.table_name,
                    kind="invariant_retry_max_status_error",
                    detail={
                        "retry_count": row.retry_count,
                        "error_message": (row.error_message or "")[:200],
                    },
                    auto_fixable=True,
                    suggested_status=Status.PERMANENTLY_FAILED,
                )

    def _scan_pending_timeout(self, engine: Engine) -> Iterable[Violation]:
        sql = text(
            "SELECT dataset_id::text AS dataset_id, table_name, retry_count, updated_at "
            "FROM cached_datasets "
            "WHERE status = 'pending' "
            "  AND updated_at < NOW() - (:days || ' days')::interval"
        )
        with engine.connect() as conn:
            for row in conn.execute(sql, {"days": self.pending_timeout_days}).fetchall():
                yield Violation(
                    dataset_id=row.dataset_id,
                    table_name=row.table_name,
                    kind="invariant_pending_timeout",
                    detail={
                        "stale_for_days": self.pending_timeout_days,
                        "retry_count": row.retry_count,
                    },
                    auto_fixable=True,
                    suggested_status=Status.PERMANENTLY_FAILED,
                )

    def _scan_orphan_ready(self, engine: Engine) -> Iterable[Violation]:
        """status='ready' but table doesn't exist (the 13 + 144 'Table missing')."""
        sql = text(
            "SELECT cd.dataset_id::text AS dataset_id, cd.table_name "
            "FROM cached_datasets cd "
            "LEFT JOIN information_schema.tables t "
            "  ON t.table_name = cd.table_name AND t.table_schema = 'public' "
            "WHERE cd.status = 'ready' AND cd.table_name IS NOT NULL AND t.table_name IS NULL"
        )
        with engine.connect() as conn:
            for row in conn.execute(sql).fetchall():
                # Ambiguous (root cause unknown) — register, don't auto-flip.
                yield Violation(
                    dataset_id=row.dataset_id,
                    table_name=row.table_name,
                    kind="invariant_ready_missing_table",
                    detail={"missing_table": row.table_name},
                    auto_fixable=False,
                )

    def _scan_schema_mismatch_loop(self, engine: Engine) -> Iterable[Violation]:
        """Bug 1: 79 schema_mismatch with retry_count>=MAX still in error/schema_mismatch."""
        sql = text(
            "SELECT dataset_id::text AS dataset_id, table_name, retry_count "
            "FROM cached_datasets "
            "WHERE retry_count >= :max "
            "  AND (status = 'schema_mismatch' OR error_message ILIKE '%schema_mismatch%')"
            "  AND status NOT IN ('permanently_failed','ready')"
        )
        with engine.connect() as conn:
            for row in conn.execute(sql, {"max": self.max_attempts}).fetchall():
                yield Violation(
                    dataset_id=row.dataset_id,
                    table_name=row.table_name,
                    kind="invariant_schema_mismatch_loop",
                    detail={"retry_count": row.retry_count},
                    auto_fixable=True,
                    suggested_status=Status.PERMANENTLY_FAILED,
                )

    # ---------- enforce ----------

    def enforce(self, engine: Engine, *, dry_run: bool = True) -> dict:
        """Apply auto-fixable corrections. `dry_run=True` only counts."""
        violations = self.scan(engine)
        by_kind: dict[str, int] = {}
        applied = 0
        for v in violations:
            by_kind[v.kind] = by_kind.get(v.kind, 0) + 1
            if not v.auto_fixable or v.suggested_status is None:
                continue
            if dry_run:
                continue
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            "UPDATE cached_datasets "
                            "SET status = :st, "
                            "    error_message = COALESCE(error_message, '') || "
                            "      CASE WHEN POSITION(:tag IN COALESCE(error_message,'')) > 0 "
                            "           THEN '' ELSE :tag_full END, "
                            "    error_category = 'orchestration_recovery_loop', "
                            "    updated_at = NOW() "
                            "WHERE dataset_id = CAST(:did AS uuid)"
                        ),
                        {
                            "st": v.suggested_status.value,
                            "did": v.dataset_id,
                            "tag": "[ws0_5_enforced]",
                            "tag_full": " | [ws0_5_enforced]",
                        },
                    )
                    applied += 1
            except Exception:
                logger.exception(
                    "WS0.5 enforce failed for dataset %s (kind=%s)", v.dataset_id, v.kind
                )
        return {
            "violations_total": len(violations),
            "violations_by_kind": by_kind,
            "applied": applied,
            "dry_run": dry_run,
        }
