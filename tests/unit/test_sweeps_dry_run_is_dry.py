"""A dry run must not write. Checked by behaviour, not by reading the code.

Eight sweeps with the same shape were written in one day, each with its own
`dry_run` branch. Reading them proves nothing: the branch can be correct and a
helper called before it can still write. So each sweep is run against a
connection that raises on any statement that mutates, and the test is simply
that nothing raises.

The one deliberate exception is stated below rather than hidden: the
reconciliation sweeps write an audit row in dry-run mode, recording what they
*would* have done. That is a decision, and a decision worth being able to see.
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from unittest.mock import patch

import pytest

# Anchored on the statement's leading verb, and applied after comments are
# stripped. A first version matched the word anywhere and flagged a `SELECT`
# whose comment explained when a table would be *dropped* — the test finding
# itself rather than the code.
_MUTATING = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE)\b", re.IGNORECASE
)
_SQL_COMMENT = re.compile(r"--[^\n]*")


def _is_mutating(sql: str) -> bool:
    return bool(_MUTATING.search(_SQL_COMMENT.sub("", sql).lstrip()))


class _WriteRefused(AssertionError):
    """Raised when a dry run attempts to mutate."""


class _ReadOnlyConn:
    """Answers reads with nothing; refuses anything that would change state."""

    def __init__(self, allow_audit: bool = False):
        self.allow_audit = allow_audit
        self.writes: list[str] = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        if _is_mutating(sql):
            if self.allow_audit and (
                "parse_repair_audit" in sql or "cache_drop_audit" in sql
            ):
                self.writes.append(sql)
            else:
                raise _WriteRefused(sql[:120])
        return SimpleNamespace(
            fetchall=lambda: [], fetchone=lambda: None, scalar=lambda: 5000,
            rowcount=0, __iter__=lambda s: iter([]),
        )

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Engine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn

    def begin(self):
        return self._conn


@pytest.mark.parametrize(
    "module,func,kwargs",
    [
        ("app.application.catalog.registry_reconcile", "reconcile_locations", {}),
        ("app.application.catalog.registry_reconcile", "retire_phantom_rows", {}),
        ("app.application.catalog.registry_reconcile", "backfill_legacy_registry", {}),
        ("app.application.catalog.duplicate_cleanup", "cleanup_duplicate_tables", {}),
    ],
)
def test_a_dry_run_writes_nothing_but_its_own_audit(module, func, kwargs):
    import importlib

    mod = importlib.import_module(module)
    conn = _ReadOnlyConn(allow_audit=True)
    getattr(mod, func)(_Engine(conn), dry_run=True, **kwargs)


@pytest.mark.parametrize(
    "module,task,kwargs",
    [
        ("app.infrastructure.celery.tasks.columns_backfill",
         "backfill_dataset_columns", {}),
        ("app.infrastructure.celery.tasks.retry_our_failures",
         "retry_our_own_failures", {}),
        ("app.infrastructure.celery.tasks.identity_reconcile",
         "reconcile_dataset_identities", {}),
    ],
)
def test_a_dry_run_task_writes_nothing_at_all(module, task, kwargs):
    """These have no audit exception: a dry run is entirely read-only."""
    import importlib

    mod = importlib.import_module(module)
    conn = _ReadOnlyConn(allow_audit=False)
    with patch.object(mod, "get_sync_engine", return_value=_Engine(conn)):
        getattr(mod, task)(dry_run=True, **kwargs)
    assert conn.writes == []
