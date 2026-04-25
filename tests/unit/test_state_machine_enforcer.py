"""Tests for the WS0.5 state-machine enforcer.

Pure-Python checks of the transition table + violation classification logic.
The DB-touching scan/enforce paths are exercised in integration suites.
"""

from __future__ import annotations

from app.application.state_machine import (
    MAX_TOTAL_ATTEMPTS,
    PENDING_TIMEOUT_DAYS,
    Status,
    StateMachineEnforcer,
)
from app.application.state_machine.cached_dataset_enforcer import (
    ALLOWED_TRANSITIONS,
    Transition,
    is_allowed,
)


def test_max_attempts_matches_collector_constant():
    # If you change MAX_TOTAL_ATTEMPTS in collector_tasks.py, change it here too.
    assert MAX_TOTAL_ATTEMPTS == 5


def test_pending_timeout_default_is_seven_days():
    assert PENDING_TIMEOUT_DAYS == 7


def test_happy_path_transitions_allowed():
    # pending → downloading → parsing → materialized → ready
    chain = [
        (Status.PENDING, Status.DOWNLOADING),
        (Status.DOWNLOADING, Status.PARSING),
        (Status.PARSING, Status.MATERIALIZED),
        (Status.MATERIALIZED, Status.READY),
    ]
    for src, dst in chain:
        assert is_allowed(src, dst), f"{src.value}→{dst.value} should be allowed"


def test_error_to_permanently_failed_allowed():
    assert is_allowed(Status.ERROR, Status.PERMANENTLY_FAILED)


def test_pending_to_permanently_failed_for_timeout_allowed():
    # Bug 4 fix — without this transition the 43 stuck-in-pending have no exit.
    assert is_allowed(Status.PENDING, Status.PERMANENTLY_FAILED)


def test_schema_mismatch_to_permanently_failed_allowed():
    # Bug 1 fix — the 79 schema_mismatch loops need this exit.
    assert is_allowed(Status.SCHEMA_MISMATCH, Status.PERMANENTLY_FAILED)


def test_permanently_failed_is_terminal():
    for dst in Status:
        assert Transition(Status.PERMANENTLY_FAILED, dst) not in ALLOWED_TRANSITIONS


def test_ready_to_pending_is_not_allowed():
    # Going back to pending bypasses retry tracking and would create the
    # silent ghost-loop pattern WS0.5 is trying to kill.
    assert not is_allowed(Status.READY, Status.PENDING)


def test_enforcer_reports_max_attempts_setting():
    e = StateMachineEnforcer(max_attempts=7)
    assert e.max_attempts == 7
