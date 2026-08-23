"""Columns we already have, moved to where they are used.

The plan reads the 98.5 % empty `datasets.columns` as a CKAN integration. The
measurement says 29,001 of those rows were parsed by us and their headers are
already stored — in a different table from the one that uses them. The embedding
builder skips the column chunk entirely when the list is empty, so 89 % of the
catalogue has never had one, and cannot be found by column name.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from app.infrastructure.celery.tasks.columns_backfill import (
    _CANDIDATES_SQL,
    backfill_dataset_columns,
)


class _Conn:
    def __init__(self, rows=None, remaining=0):
        self.rows = rows or []
        self.remaining = remaining
        self.updates: list[dict] = []

    def execute(self, stmt, params=None):
        s = str(stmt)
        if "UPDATE datasets" in s:
            self.updates.append(params or {})
            return SimpleNamespace()
        if "count(*)" in s:
            return SimpleNamespace(scalar=lambda: self.remaining)
        return SimpleNamespace(fetchall=lambda: self.rows)

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Eng:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn

    def begin(self):
        return self._conn


def _run(rows, **kw):
    conn = _Conn(rows)
    with patch(
        "app.infrastructure.celery.tasks.columns_backfill.get_sync_engine",
        return_value=_Eng(conn),
    ), patch(
        "app.infrastructure.celery.tasks.scraper_tasks.index_dataset_embedding"
    ) as idx:
        out = backfill_dataset_columns(dry_run=False, **kw)
    return out, conn, idx


def test_it_copies_the_parsed_header():
    rows = [SimpleNamespace(id="d1", columns_json='["provincia", "valor"]')]
    out, conn, _ = _run(rows)
    assert out["filled"] == 1
    assert conn.updates[0]["c"] == '["provincia", "valor"]'


def test_the_query_refuses_an_empty_header():
    """`columns_json` holding `[]` is the same absence written differently.

    Copying it would turn an empty column into an empty column while reporting
    progress, which is worse than leaving the gap: the row stops being a
    candidate and nothing ever revisits it.
    """
    sql = str(_CANDIDATES_SQL)
    assert "cd.columns_json::text NOT IN ('[]', 'null', '\"\"')" in sql
    assert "cd.status = 'ready'" in sql


def test_filling_a_row_re_embeds_it():
    """Filling the column changes what the chunk says, and a chunk nobody
    re-embeds is a change nobody can search."""
    rows = [SimpleNamespace(id="d1", columns_json='["a"]')]
    out, _, idx = _run(rows)
    assert out["reindex_dispatched"] == 1
    idx.delay.assert_called_once_with("d1")


def test_reindex_can_be_withheld():
    rows = [SimpleNamespace(id="d1", columns_json='["a"]')]
    out, _, idx = _run(rows, reindex=False)
    assert out["reindex_dispatched"] == 0
    assert not idx.delay.called


def test_dry_run_writes_nothing():
    conn = _Conn([SimpleNamespace(id="d1", columns_json='["a"]')])
    with patch(
        "app.infrastructure.celery.tasks.columns_backfill.get_sync_engine",
        return_value=_Eng(conn),
    ):
        out = backfill_dataset_columns(dry_run=True)
    assert out["candidates"] == 1
    assert out["filled"] == 0
    assert conn.updates == []


def test_one_bad_row_does_not_cost_the_batch():
    class _Flaky(_Conn):
        def execute(self, stmt, params=None):
            if "UPDATE datasets" in str(stmt) and (params or {}).get("i") == "malo":
                raise RuntimeError("nope")
            return super().execute(stmt, params)

    rows = [
        SimpleNamespace(id="malo", columns_json='["a"]'),
        SimpleNamespace(id="bueno", columns_json='["b"]'),
    ]
    conn = _Flaky(rows)
    with patch(
        "app.infrastructure.celery.tasks.columns_backfill.get_sync_engine",
        return_value=_Eng(conn),
    ), patch("app.infrastructure.celery.tasks.scraper_tasks.index_dataset_embedding"):
        out = backfill_dataset_columns(dry_run=False)
    assert out["filled"] == 1
