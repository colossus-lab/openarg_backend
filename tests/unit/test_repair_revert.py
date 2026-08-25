"""Tests for the repair revert.

The behaviour that matters most is refusal. A revert that renames back from a
state it does not recognise corrupts a table instead of restoring one, and the
whole case for letting the data lane act rests on this being safe.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from app.application.repair.revert import revert_repair


class _Audit:
    def __init__(self, **kw):
        d = {
            "id": 7,
            "run_id": "r",
            "phase": "title_as_columns",
            "table_schema": "raw",
            "table_name": "t",
            "operation": "apply",
            "old_columns": ["destino", "estado"],
            "new_columns": ["LISTADO / G_1", "LISTADO / G_2"],
            "rows_deleted": 0,
            "ok": True,
            "dry_run": False,
            "applied_at": None,
        }
        d.update(kw)
        for k, v in d.items():
            setattr(self, k, v)


def _engine(audit_row, current_columns):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    results = [MagicMock(fetchone=lambda: audit_row)]
    if audit_row is not None:
        results.append(
            MagicMock(fetchall=lambda: [MagicMock(column_name=c) for c in current_columns])
        )
    conn.execute.side_effect = results
    return engine


def _renames(engine):
    write = engine.begin.return_value.__enter__.return_value
    return [str(c.args[0]) for c in write.execute.call_args_list if "RENAME" in str(c.args[0])]


# ── se niega cuando corresponde ────────────────────────────────


def test_it_refuses_when_the_table_changed_since_the_repair():
    """Something else edited the table — another repair, a re-ingest, a hand-run
    migration. Renaming back from an unrecognised state corrupts rather than
    restores."""
    result = revert_repair(
        _engine(_Audit(), ["algo", "completamente", "distinto"]),
        audit_id=7,
        dry_run=False,
    )

    assert not result.ok
    assert result.reason == "table_changed_since_repair"


def test_it_refuses_when_nothing_was_ever_applied():
    """The corpus is mostly `skip` rows. Reading those as reverts that went
    wrong would misreport a sweep over the whole audit log."""
    for kw in ({"operation": "skip", "ok": False}, {"dry_run": True}, {"ok": False}):
        result = revert_repair(_engine(_Audit(**kw), ["a"]), audit_id=7, dry_run=False)
        assert not result.ok
        assert result.reason == "nothing_was_applied", kw


def test_it_refuses_when_the_table_is_gone():
    result = revert_repair(_engine(_Audit(), []), audit_id=7, dry_run=False)

    assert not result.ok
    assert result.reason == "table_no_longer_exists"


def test_it_refuses_on_a_truncated_audit_row():
    """Unequal lengths mean the record cannot describe a rename mapping."""
    result = revert_repair(
        _engine(_Audit(old_columns=["a"], new_columns=["x", "y"]), ["x", "y"]),
        audit_id=7,
        dry_run=False,
    )

    assert not result.ok
    assert result.reason == "audit_row_incomplete"


def test_a_missing_audit_row_is_reported_not_raised():
    result = revert_repair(_engine(None, []), audit_id=999, dry_run=False)

    assert not result.ok
    assert result.reason == "audit_row_not_found"


# ── revierte cuando corresponde ────────────────────────────────


def test_it_restores_the_original_names():
    engine = _engine(_Audit(), ["LISTADO / G_1", "LISTADO / G_2"])
    result = revert_repair(engine, audit_id=7, dry_run=False)

    assert result.ok
    assert result.restored_columns == ["destino", "estado"]
    renames = _renames(engine)
    assert len(renames) == 2
    assert 'RENAME COLUMN "LISTADO / G_1" TO "destino"' in renames[0]


def test_extra_columns_added_later_do_not_block_the_revert():
    """A re-ingest can append the collector's own metadata columns. Those are
    not part of what the repair renamed, so they must not look like tampering."""
    engine = _engine(_Audit(), ["LISTADO / G_1", "LISTADO / G_2", "_source_url", "_ingested_at"])
    result = revert_repair(engine, audit_id=7, dry_run=False)

    assert result.ok
    assert len(_renames(engine)) == 2


def test_dry_run_touches_nothing():
    """A revert is itself a mutation; the caller should see it before it runs."""
    engine = _engine(_Audit(), ["LISTADO / G_1", "LISTADO / G_2"])
    result = revert_repair(engine, audit_id=7, dry_run=True)

    assert result.ok
    assert result.reason == "dry_run"
    assert result.restored_columns == ["destino", "estado"]
    assert _renames(engine) == []


def test_deleted_rows_are_reported_as_unrecoverable():
    """Only the rename is reversible. A repair that removed a buried header row
    cannot give it back, and claiming otherwise would let a caller make a worse
    decision than knowing."""
    engine = _engine(_Audit(rows_deleted=3), ["LISTADO / G_1", "LISTADO / G_2"])
    result = revert_repair(engine, audit_id=7, dry_run=False)

    assert result.ok
    assert result.rows_not_recoverable == 3


def test_a_jsonb_column_returned_as_text_is_handled():
    """psycopg gives back a list; a driver or fixture can give back a string."""
    engine = _engine(
        _Audit(old_columns=json.dumps(["destino"]), new_columns=json.dumps(["G_1"])),
        ["G_1"],
    )
    result = revert_repair(engine, audit_id=7, dry_run=False)

    assert result.ok
    assert result.restored_columns == ["destino"]
